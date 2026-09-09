package zeus

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Pins the run wire contract the poller's load-health warning depends on:
// zeus stamps reason="thresholds breached" on a COMPLETED run when k6 exits
// 99 (client-side gates crossed). Status stays completed — a breach is a
// health annotation, never a run failure.
func TestRunInfoDecodesBreachReason(t *testing.T) {
	var info RunInfo
	raw := `{"id":"run-1","status":"completed","reason":"thresholds breached","extra":"ignored"}`
	if err := json.Unmarshal([]byte(raw), &info); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if info.Status != "completed" || info.Reason != "thresholds breached" {
		t.Fatalf("got status=%q reason=%q", info.Status, info.Reason)
	}
}

// TestStartRun_ResponseError pins the typed refusal the orchestrator's
// failure reasons read: a non-2xx answer from zeus is a *ResponseError
// carrying the status and body, and its text is the historical message
// unchanged. StartAttack shares the shape.
func TestStartRun_ResponseError(t *testing.T) {
	const body = `{"error":"dataset schema mismatch"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	ctx := context.Background()

	_, err := c.StartRun(ctx, "wf-1", RunRequest{RunID: "run-1"})
	var re *ResponseError
	if !errors.As(err, &re) {
		t.Fatalf("StartRun err = %v (%T), want *ResponseError", err, err)
	}
	if re.Status != http.StatusUnprocessableEntity || re.Body != body {
		t.Errorf("ResponseError = %+v, want status 422 with the body", re)
	}
	if want := `zeus: start run for workflow "wf-1": status 422: ` + body; err.Error() != want {
		t.Errorf("err = %v, want %q", err, want)
	}

	_, err = c.StartAttack(ctx, AttackRequest{ID: "atk-1"})
	re = nil
	if !errors.As(err, &re) || re.Status != http.StatusUnprocessableEntity || re.Body != body {
		t.Fatalf("StartAttack err = %v, want *ResponseError 422 with the body", err)
	}
	if want := "zeus: start attack: status 422: " + body; err.Error() != want {
		t.Errorf("err = %v, want %q", err, want)
	}
}
