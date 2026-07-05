package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
