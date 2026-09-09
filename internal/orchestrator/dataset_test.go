package orchestrator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

// attachWorkflows creates one workflow row per config and attaches them all
// to the phase in a single AttachPhaseWorkflows call (the store replaces the
// phase's rows wholesale, so multi-row phases must attach at once). Returns
// the minted workflow ids in config order.
func attachWorkflows(t *testing.T, phaseID string, pws ...model.PhaseWorkflow) []string {
	t.Helper()
	ctx := context.Background()
	ids := make([]string, len(pws))
	for i := range pws {
		wf := &model.Workflow{
			ID:        id.New("wf"),
			Name:      "ds-test-" + id.New("n"),
			DSL:       json.RawMessage(`{"type":"request","method":"GET","url":"http://target:8080/"}`),
			CreatedAt: time.Now(),
		}
		if err := testWorkflowRepo.Create(ctx, wf); err != nil {
			t.Fatalf("create workflow: %v", err)
		}
		pws[i].PhaseID = phaseID
		pws[i].WorkflowID = wf.ID
		ids[i] = wf.ID
	}
	if err := testExpRepo.AttachPhaseWorkflows(ctx, phaseID, pws); err != nil {
		t.Fatalf("attach phase workflows: %v", err)
	}
	return ids
}

// TestRunDatasetID pins the per-row fallback rule: the row's dataset_id wins;
// an empty one falls back to the process-wide MANTEION_ZEUS_DATASET_ID.
func TestRunDatasetID(t *testing.T) {
	cases := []struct{ row, fallback, want string }{
		{"ds-row", "ds-env", "ds-row"},
		{"", "ds-env", "ds-env"},
		{"ds-row", "", "ds-row"},
		{"", "", ""},
	}
	for _, tc := range cases {
		if got := runDatasetID(tc.row, tc.fallback); got != tc.want {
			t.Errorf("runDatasetID(%q, %q) = %q, want %q", tc.row, tc.fallback, got, tc.want)
		}
	}
}

// TestStartPhaseRuns_DatasetPerWorkflow pins product decision 3 (dataset per
// workflow): the RunRequest zeus receives for a phase_workflows row carries
// THAT row's dataset_id, and a row without one falls back to the process-wide
// MANTEION_ZEUS_DATASET_ID so existing expctl plans keep working. The
// dataset_id must survive the store round trip (attach → list) to get here.
func TestStartPhaseRuns_DatasetPerWorkflow(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL)
	o.WithZeusDatasetID("ds-env")

	exp, phases := mkExperiment(t, 1)
	wfIDs := attachWorkflows(t, phases[0].ID,
		model.PhaseWorkflow{VUs: 1, DurationSec: 1, DatasetID: "ds-row"},
		model.PhaseWorkflow{VUs: 1, DurationSec: 1},
	)
	withRow, withoutRow := wfIDs[0], wfIDs[1]

	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}

	// Both rows get a run id stamped once their StartRun succeeded.
	runIDs := map[string]string{} // workflow id -> zeus run id
	waitFor(t, "both workflow runs started", 5*time.Second, func() bool {
		pws, err := testExpRepo.ListPhaseWorkflows(ctx, phases[0].ID)
		if err != nil || len(pws) != 2 {
			return false
		}
		for _, pw := range pws {
			if pw.ZeusRunID == "" {
				return false
			}
			runIDs[pw.WorkflowID] = pw.ZeusRunID
		}
		return true
	})

	if got, ok := fz.runDataset(runIDs[withRow]); !ok || got != "ds-row" {
		t.Errorf("run for the row WITH dataset_id carried dataset_id=%q (seen=%v), want ds-row", got, ok)
	}
	if got, ok := fz.runDataset(runIDs[withoutRow]); !ok || got != "ds-env" {
		t.Errorf("run for the row WITHOUT dataset_id carried dataset_id=%q (seen=%v), want the env fallback ds-env", got, ok)
	}

	// Let the phase finish so no poller outlives the test.
	fz.completeAllRuns()
	waitFor(t, "phase completes", 5*time.Second, func() bool {
		return getPhase(t, phases[0].ID).Status == "completed"
	})
}

// TestStartPhaseRuns_NoDatasetAnywhere pins the "no dataset" wire shape: with
// neither a row dataset nor an env fallback, the run request carries no
// dataset_id at all (zeus stages an empty pool map — unchanged behaviour).
func TestStartPhaseRuns_NoDatasetAnywhere(t *testing.T) {
	ctx := context.Background()
	fz := newFakeZeus(t)
	o := newOrch(t, fz.srv.URL) // zeusDatasetID left empty

	exp, phases := mkExperiment(t, 1)
	attachWorkflows(t, phases[0].ID, model.PhaseWorkflow{VUs: 1, DurationSec: 1})

	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start experiment: %v", err)
	}
	var runID string
	waitFor(t, "workflow run started", 5*time.Second, func() bool {
		pws, err := testExpRepo.ListPhaseWorkflows(ctx, phases[0].ID)
		if err != nil || len(pws) != 1 || pws[0].ZeusRunID == "" {
			return false
		}
		runID = pws[0].ZeusRunID
		return true
	})
	if got, ok := fz.runDataset(runID); !ok || got != "" {
		t.Errorf("run carried dataset_id=%q (seen=%v), want none", got, ok)
	}
	fz.completeAllRuns()
	waitFor(t, "phase completes", 5*time.Second, func() bool {
		return getPhase(t, phases[0].ID).Status == "completed"
	})
}
