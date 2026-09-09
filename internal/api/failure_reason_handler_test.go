package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestFailureReason_ReadOuts pins where a failed row's reason surfaces —
// the experiment detail row and its nested phases[], the nested phase GET,
// the flat /phases/{id} detail, the /phases list item and the experiment
// list row — and that a healthy row omits the key entirely.
func TestFailureReason_ReadOuts(t *testing.T) {
	e := newEditTestEnv(t)
	exp := e.experiment("running")
	failed := e.phase(exp, "isolation-cartservice", 0)
	healthy := e.phase(exp, "baseline", 1)
	if err := e.exp.UpdatePhaseStatus(e.ctx, failed.ID, "running"); err != nil {
		t.Fatalf("run phase: %v", err)
	}
	const phaseReason = `zeus rejected run run-1 for workflow wf-checkout: status 422: {"error":"dataset schema mismatch"}`
	if ok, err := e.exp.FailPhase(e.ctx, failed.ID, phaseReason, "running"); err != nil || !ok {
		t.Fatalf("FailPhase: ok=%v err=%v", ok, err)
	}
	expReason := "phase isolation-cartservice failed: " + phaseReason
	if ok, err := e.exp.FailExperiment(e.ctx, exp.ID, expReason, "running"); err != nil || !ok {
		t.Fatalf("FailExperiment: ok=%v err=%v", ok, err)
	}

	decode := func(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
		t.Helper()
		if w.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		var m map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
			t.Fatalf("decode: %v (body=%s)", err, w.Body.String())
		}
		return m
	}
	reasonIs := func(t *testing.T, where string, m map[string]any, want string) {
		t.Helper()
		if got, _ := m["failure_reason"].(string); got != want {
			t.Errorf("%s: failure_reason=%q, want %q", where, got, want)
		}
	}
	absent := func(t *testing.T, where string, m map[string]any) {
		t.Helper()
		if v, ok := m["failure_reason"]; ok {
			t.Errorf("%s: healthy row carries failure_reason=%v, want the key omitted", where, v)
		}
	}
	// findInPages walks a paginated list handler until it finds id.
	findInPages := func(t *testing.T, h http.HandlerFunc, path, query, id string) map[string]any {
		t.Helper()
		for offset := 0; ; offset += 200 {
			w := e.do(h, http.MethodGet, fmt.Sprintf("%s?limit=200&offset=%d%s", path, offset, query), "", nil)
			if w.Code != http.StatusOK {
				t.Fatalf("%s: status=%d body=%s", path, w.Code, w.Body.String())
			}
			var env struct {
				Data []map[string]any `json:"data"`
				Page struct {
					Total int `json:"total"`
				} `json:"page"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
				t.Fatalf("%s: decode: %v", path, err)
			}
			for _, it := range env.Data {
				if it["id"] == id {
					return it
				}
			}
			if offset+200 >= env.Page.Total {
				t.Fatalf("%s: %s not found (total=%d)", path, id, env.Page.Total)
			}
		}
	}

	t.Run("experiment detail: row and nested phases[]", func(t *testing.T) {
		w := e.do(e.s.handleGetExperiment, http.MethodGet, "/api/v1/experiments/"+exp.ID, "", map[string]string{"id": exp.ID})
		m := decode(t, w)
		reasonIs(t, "experiment", m, expReason)
		phases, _ := m["phases"].([]any)
		seen := 0
		for _, raw := range phases {
			ph, _ := raw.(map[string]any)
			switch ph["id"] {
			case failed.ID:
				reasonIs(t, "phases[failed]", ph, phaseReason)
				seen++
			case healthy.ID:
				absent(t, "phases[healthy]", ph)
				seen++
			}
		}
		if seen != 2 {
			t.Errorf("saw %d of the 2 seeded phases in %v", seen, phases)
		}
	})

	t.Run("nested phase GET", func(t *testing.T) {
		w := e.do(e.s.handleGetPhase, http.MethodGet, "/api/v1/experiments/"+exp.ID+"/phases/"+failed.ID, "",
			map[string]string{"id": exp.ID, "phaseId": failed.ID})
		reasonIs(t, "phase", decode(t, w), phaseReason)
		w = e.do(e.s.handleGetPhase, http.MethodGet, "/api/v1/experiments/"+exp.ID+"/phases/"+healthy.ID, "",
			map[string]string{"id": exp.ID, "phaseId": healthy.ID})
		absent(t, "phase", decode(t, w))
	})

	t.Run("flat phase detail", func(t *testing.T) {
		w := e.do(e.s.handleGetPhaseDetail, http.MethodGet, "/api/v1/phases/"+failed.ID, "", map[string]string{"phaseId": failed.ID})
		reasonIs(t, "phase detail", decode(t, w), phaseReason)
		w = e.do(e.s.handleGetPhaseDetail, http.MethodGet, "/api/v1/phases/"+healthy.ID, "", map[string]string{"phaseId": healthy.ID})
		absent(t, "phase detail", decode(t, w))
	})

	t.Run("phase list item", func(t *testing.T) {
		reasonIs(t, "list item", findInPages(t, e.s.handleListPhases, "/api/v1/phases", "", failed.ID), phaseReason)
		absent(t, "list item", findInPages(t, e.s.handleListPhases, "/api/v1/phases", "", healthy.ID))
	})

	t.Run("experiment list row", func(t *testing.T) {
		reasonIs(t, "experiment row",
			findInPages(t, e.s.handleListExperiments, "/api/v1/experiments", "&status=failed", exp.ID), expReason)
	})
}
