package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/cachestore"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// stubPhaseReader is a narrow, DB-free phase lookup for handler tests.
type stubPhaseReader struct {
	phases map[string]*model.ExperimentPhase
}

func (s stubPhaseReader) GetPhase(_ context.Context, id string) (*model.ExperimentPhase, error) {
	if p, ok := s.phases[id]; ok {
		return p, nil
	}
	return nil, store.ErrNotFound
}

func postIngest(t *testing.T, s *Server, env ingestEnvelope) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/cache/ingest", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.handleCacheIngest(w, req)
	return w
}

// postIngestBody posts a raw JSON ingest body. Used where a test must pin the
// wire names the SDK sends (and the absence of a field) instead of
// round-tripping through ingestEnvelope's own tags.
func postIngestBody(t *testing.T, s *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/cache/ingest", bytes.NewReader([]byte(body)))
	w := httptest.NewRecorder()
	s.handleCacheIngest(w, req)
	return w
}

// ingestOutcome decodes a 200 ingest response into (accepted, duplicate).
func ingestOutcome(t *testing.T, w *httptest.ResponseRecorder) (accepted int, duplicate bool) {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", w.Code, w.Body.String())
	}
	var res ingestResult
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return res.Accepted, res.Duplicate
}

// TestIngest_RejectsInactivePair pins MANT-3 ingest admission (INV-5): a push
// is accepted only when (experiment_id, phase_id) names an active recording
// pair. An experiment_id that does not match the phase, a non-running phase, a
// non-persist phase, and an unknown phase are all rejected — there is no
// inference fallback from phase_id alone.
func TestIngest_RejectsInactivePair(t *testing.T) {
	reader := stubPhaseReader{phases: map[string]*model.ExperimentPhase{
		"phase-rec":       {ID: "phase-rec", ExperimentID: "exp-1", Status: "running", PersistCache: true},
		"phase-done":      {ID: "phase-done", ExperimentID: "exp-1", Status: "completed", PersistCache: true},
		"phase-nopersist": {ID: "phase-nopersist", ExperimentID: "exp-1", Status: "running", PersistCache: false},
	}}

	newServer := func() *Server {
		return &Server{
			phaseReader: reader,
			cacheStore:  cachestore.New(t.TempDir()),
			logger:      discardLogger(),
		}
	}

	entries := []atroposdk.CacheBoxWireEntry{{Key: "k1"}}

	// Active pair, matching experiment_id → accepted.
	s := newServer()
	w := postIngest(t, s, ingestEnvelope{ExperimentID: "exp-1", PhaseID: "phase-rec", Service: "frontend", Instance: "i1", BatchSeq: 1, Entries: entries})
	if w.Code != http.StatusOK {
		t.Fatalf("active pair: status=%d body=%s, want 200", w.Code, w.Body.String())
	}
	var ok struct {
		Accepted  int  `json:"accepted"`
		Duplicate bool `json:"duplicate"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &ok); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if ok.Accepted != 1 || ok.Duplicate {
		t.Fatalf("active pair body = %+v, want {accepted:1 duplicate:false}", ok)
	}

	// Retried batch (same batch_seq) → duplicate, not re-appended.
	w = postIngest(t, s, ingestEnvelope{ExperimentID: "exp-1", PhaseID: "phase-rec", Service: "frontend", Instance: "i1", BatchSeq: 1, Entries: entries})
	if w.Code != http.StatusOK {
		t.Fatalf("retry: status=%d, want 200", w.Code)
	}
	_ = json.Unmarshal(w.Body.Bytes(), &ok)
	if !ok.Duplicate || ok.Accepted != 0 {
		t.Fatalf("retry body = %+v, want {accepted:0 duplicate:true}", ok)
	}
	if got := s.cacheStore.ReceivedCount("exp-1", "phase-rec", "frontend", "i1"); got != 1 {
		t.Fatalf("received after retry = %d, want 1", got)
	}

	// experiment_id that does not match the phase's experiment → 409, no record.
	s = newServer()
	w = postIngest(t, s, ingestEnvelope{ExperimentID: "exp-WRONG", PhaseID: "phase-rec", Service: "frontend", Instance: "i1", BatchSeq: 1, Entries: entries})
	if w.Code != http.StatusConflict {
		t.Fatalf("experiment mismatch: status=%d, want 409", w.Code)
	}
	assertErrorBody(t, w, "phase_not_recording")
	if got := s.cacheStore.ReceivedCount("exp-WRONG", "phase-rec", "frontend", "i1"); got != 0 {
		t.Fatalf("mismatch must not record, got count=%d", got)
	}

	// Completed phase → not an active recording pair → 409.
	w = postIngest(t, newServer(), ingestEnvelope{ExperimentID: "exp-1", PhaseID: "phase-done", Service: "frontend", Instance: "i1", BatchSeq: 1, Entries: entries})
	if w.Code != http.StatusConflict {
		t.Fatalf("completed phase: status=%d, want 409", w.Code)
	}

	// persist_cache=false → 409.
	w = postIngest(t, newServer(), ingestEnvelope{ExperimentID: "exp-1", PhaseID: "phase-nopersist", Service: "frontend", Instance: "i1", BatchSeq: 1, Entries: entries})
	if w.Code != http.StatusConflict {
		t.Fatalf("non-persist phase: status=%d, want 409", w.Code)
	}

	// Unknown phase → 404.
	w = postIngest(t, newServer(), ingestEnvelope{ExperimentID: "exp-1", PhaseID: "phase-missing", Service: "frontend", Instance: "i1", BatchSeq: 1, Entries: entries})
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown phase: status=%d, want 404", w.Code)
	}
}

// TestIngest_EpochInDedupeKey pins desired-state v1 ingest at the wire: the
// batch's epoch (the instance's start time, unix nanoseconds) is part of the
// dedupe key. A restarted instance sends batch_seq 1 again under a new epoch
// and is accepted, a retry under the same epoch is a duplicate, and a body
// without epoch (an old client) decodes as epoch 0 and keeps the old key.
func TestIngest_EpochInDedupeKey(t *testing.T) {
	reader := stubPhaseReader{phases: map[string]*model.ExperimentPhase{
		"phase-rec": {ID: "phase-rec", ExperimentID: "exp-1", Status: "running", PersistCache: true},
	}}
	s := &Server{phaseReader: reader, cacheStore: cachestore.New(t.TempDir()), logger: discardLogger()}
	entries := []atroposdk.CacheBoxWireEntry{{Key: "k1"}}
	batch := func(epoch int64) ingestEnvelope {
		return ingestEnvelope{ExperimentID: "exp-1", PhaseID: "phase-rec", Service: "frontend", Instance: "i1", BatchSeq: 1, Epoch: epoch, Entries: entries}
	}
	expect := func(step string, w *httptest.ResponseRecorder, wantAccepted int, wantDuplicate bool) {
		t.Helper()
		if acc, dup := ingestOutcome(t, w); acc != wantAccepted || dup != wantDuplicate {
			t.Fatalf("%s = {accepted:%d duplicate:%v}, want {accepted:%d duplicate:%v}", step, acc, dup, wantAccepted, wantDuplicate)
		}
	}

	// The same batch_seq under epoch 1 then epoch 2: the restarted instance's
	// first batch must not be swallowed as a retry of its previous process's.
	expect("epoch 1, seq 1", postIngest(t, s, batch(1)), 1, false)
	expect("epoch 2, seq 1", postIngest(t, s, batch(2)), 1, false)
	// A retry within the new epoch is still a duplicate.
	expect("epoch 2, seq 1 retried", postIngest(t, s, batch(2)), 0, true)
	if got := s.cacheStore.ReceivedCount("exp-1", "phase-rec", "frontend", "i1"); got != 2 {
		t.Fatalf("received = %d, want 2 (one per accepted epoch, none for the retry)", got)
	}

	// The wire name is "epoch", what the SDK sends beside batch_seq. A struct
	// round trip cannot catch a misspelt tag (both sides would agree), so these
	// bodies are literal JSON.
	const (
		epoch7  = `{"service":"frontend","instance":"i2","experiment_id":"exp-1","phase_id":"phase-rec","batch_seq":1,"epoch":7,"entries":[{"key":"k1"}]}`
		epoch8  = `{"service":"frontend","instance":"i2","experiment_id":"exp-1","phase_id":"phase-rec","batch_seq":1,"epoch":8,"entries":[{"key":"k1"}]}`
		noEpoch = `{"service":"frontend","instance":"i-old","experiment_id":"exp-1","phase_id":"phase-rec","batch_seq":1,"entries":[{"key":"k1"}]}`
	)
	expect("wire epoch 7", postIngestBody(t, s, epoch7), 1, false)
	expect("wire epoch 7 retried", postIngestBody(t, s, epoch7), 0, true)
	expect("wire epoch 8", postIngestBody(t, s, epoch8), 1, false)
	if got := s.cacheStore.ReceivedCount("exp-1", "phase-rec", "frontend", "i2"); got != 2 {
		t.Fatalf("i2 received = %d, want 2", got)
	}

	// A body without epoch (an old client) is epoch 0: the old key, so its
	// retry is a duplicate exactly as before.
	expect("no epoch, seq 1", postIngestBody(t, s, noEpoch), 1, false)
	expect("no epoch, seq 1 retried", postIngestBody(t, s, noEpoch), 0, true)
	if got := s.cacheStore.ReceivedCount("exp-1", "phase-rec", "frontend", "i-old"); got != 1 {
		t.Fatalf("i-old received = %d, want 1", got)
	}
}

// TestIngest_MalformedEpochIs400 pins that epoch gets no leniency the other
// fields lack: a value that is not a JSON integer is refused with 400 like a
// malformed batch_seq, and nothing is recorded. The failure this guards against
// is the silent kind: a decoder that took "7" (or dropped the field) would read
// the batch as epoch 0, the old client's key, and a restarted instance's batches
// would again be swallowed as retries of its previous process's. The bodies are
// literal JSON, for the same reason as in TestIngest_EpochInDedupeKey.
func TestIngest_MalformedEpochIs400(t *testing.T) {
	reader := stubPhaseReader{phases: map[string]*model.ExperimentPhase{
		"phase-rec": {ID: "phase-rec", ExperimentID: "exp-1", Status: "running", PersistCache: true},
	}}
	newServer := func() *Server {
		return &Server{phaseReader: reader, cacheStore: cachestore.New(t.TempDir()), logger: discardLogger()}
	}
	body := func(batchSeq, epoch string) string {
		return `{"service":"frontend","instance":"i1","experiment_id":"exp-1","phase_id":"phase-rec",` +
			`"batch_seq":` + batchSeq + `,"epoch":` + epoch + `,"entries":[{"key":"k1"}]}`
	}

	// The well-formed twin is accepted, so a 400 below is the one field's doing.
	if acc, dup := ingestOutcome(t, postIngestBody(t, newServer(), body("1", "7"))); acc != 1 || dup {
		t.Fatalf("well-formed body = {accepted:%d duplicate:%v}, want {accepted:1 duplicate:false}", acc, dup)
	}

	for _, tc := range []struct{ name, batchSeq, epoch string }{
		{"epoch as a string", "1", `"7"`},
		{"epoch with a fraction", "1", `7.5`},
		{"batch_seq as a string (the comparison)", `"1"`, `7`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newServer()
			w := postIngestBody(t, s, body(tc.batchSeq, tc.epoch))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s, want 400", w.Code, w.Body.String())
			}
			var e struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
				t.Fatalf("decode error body: %v", err)
			}
			if !strings.HasPrefix(e.Error, "invalid json") {
				t.Fatalf("error=%q, want the invalid-json refusal", e.Error)
			}
			if got := s.cacheStore.ReceivedCount("exp-1", "phase-rec", "frontend", "i1"); got != 0 {
				t.Fatalf("received = %d, want 0 (a refused body records nothing)", got)
			}
		})
	}
}

// TestPushEndpoint_AcceptedDuringDraining pins MANT-2 (W2): the push endpoint
// keeps accepting recorded batches while the phase is draining, so SDKs can
// flush their tail — this is what removes the mark-completed-then-409 loss window.
func TestPushEndpoint_AcceptedDuringDraining(t *testing.T) {
	reader := stubPhaseReader{phases: map[string]*model.ExperimentPhase{
		"phase-draining": {ID: "phase-draining", ExperimentID: "exp-1", Status: "draining", PersistCache: true},
	}}
	s := &Server{phaseReader: reader, cacheStore: cachestore.New(t.TempDir()), logger: discardLogger()}

	w := postIngest(t, s, ingestEnvelope{
		ExperimentID: "exp-1", PhaseID: "phase-draining", Service: "frontend", Instance: "i1", BatchSeq: 1,
		Entries: []atroposdk.CacheBoxWireEntry{{Key: "k1"}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("push during draining: status=%d body=%s, want 200", w.Code, w.Body.String())
	}
	if got := s.cacheStore.ReceivedCount("exp-1", "phase-draining", "frontend", "i1"); got != 1 {
		t.Fatalf("received = %d, want 1 (draining push must persist)", got)
	}
}

func assertErrorBody(t *testing.T, w *httptest.ResponseRecorder, want string) {
	t.Helper()
	var e struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if e.Error != want {
		t.Fatalf("error=%q, want %q", e.Error, want)
	}
}
