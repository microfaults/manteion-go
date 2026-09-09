package model

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestFailureReason_WireShape pins the read-out contract for failure_reason
// (migration 11): the key is present only on a row that failed, so a healthy
// phase / experiment / phase list item serializes without it.
func TestFailureReason_WireShape(t *testing.T) {
	cases := []struct {
		name    string
		healthy any
		failed  any
	}{
		{"phase",
			ExperimentPhase{ID: "p", Status: "completed"},
			ExperimentPhase{ID: "p", Status: "failed", FailureReason: "zeus rejected run run-1"}},
		{"experiment",
			Experiment{ID: "e", Status: "completed"},
			Experiment{ID: "e", Status: "failed", FailureReason: "phase iso failed: zeus rejected run run-1"}},
		{"phase list item",
			PhaseListItem{ID: "p", Status: "completed"},
			PhaseListItem{ID: "p", Status: "failed", FailureReason: "zeus rejected run run-1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, err := json.Marshal(tc.healthy)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(h), "failure_reason") {
				t.Errorf("healthy row serialized failure_reason: %s", h)
			}
			f, err := json.Marshal(tc.failed)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(f), `"failure_reason":"`) {
				t.Errorf("failed row lacks failure_reason: %s", f)
			}
		})
	}
}
