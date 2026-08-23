package zeus

import (
	"encoding/json"
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
