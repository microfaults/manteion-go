package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

// TestExperimentRepo_ErrorClassification pins how the experiment repo tags
// its refusals: model validation and unknown foreign keys are ErrValidation
// (message unchanged), unique collisions are ErrConflict, and a phase under
// an unknown experiment is ErrNotFound. Handlers map the tags to the error
// envelope; nothing here reads a Postgres error code.
func TestExperimentRepo_ErrorClassification(t *testing.T) {
	ctx := context.Background()
	repo := NewExperimentRepo(testDB)

	// --- experiments ---
	err := repo.Create(ctx, &model.Experiment{ID: id.New("exp"), Status: "planned"})
	if !errors.Is(err, ErrValidation) || err.Error() != "experiment: name required" {
		t.Errorf("Create(invalid) = %v, want ErrValidation with the unchanged message", err)
	}

	exp := &model.Experiment{ID: id.New("exp"), Name: "errs-" + id.New("n"), Status: "planned", CreatedAt: time.Now()}
	if err := repo.Create(ctx, exp); err != nil {
		t.Fatalf("create experiment: %v", err)
	}
	t.Cleanup(func() { _, _ = testDB.Exec("DELETE FROM experiments WHERE id = $1", exp.ID) })
	dup := *exp
	if err := repo.Create(ctx, &dup); !errors.Is(err, ErrConflict) {
		t.Errorf("Create(duplicate id) = %v, want ErrConflict", err)
	}

	name := ""
	if err := repo.UpdateMetadata(ctx, exp.ID, &name, nil, nil); !errors.Is(err, ErrValidation) || err.Error() != "experiment: name required" {
		t.Errorf("UpdateMetadata(empty name) = %v, want ErrValidation with the unchanged message", err)
	}

	// --- phases ---
	err = repo.CreatePhase(ctx, &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: "exp-nope", Name: "p", Status: "pending"})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("CreatePhase(unknown experiment) = %v, want ErrNotFound", err)
	}
	err = repo.CreatePhase(ctx, &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: exp.ID, Name: "", Status: "pending"})
	if !errors.Is(err, ErrValidation) || err.Error() != "experiment phase: name required" {
		t.Errorf("CreatePhase(invalid) = %v, want ErrValidation with the unchanged message", err)
	}
	ph := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: exp.ID, Name: "taken", Position: 0, Status: "pending"}
	if err := repo.CreatePhase(ctx, ph); err != nil {
		t.Fatalf("create phase: %v", err)
	}
	err = repo.CreatePhase(ctx, &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: exp.ID, Name: "taken", Position: 1, Status: "pending"})
	if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "taken") {
		t.Errorf("CreatePhase(duplicate name) = %v, want ErrConflict naming the phase", err)
	}

	// --- attachments ---
	err = repo.AttachPhaseWorkflows(ctx, ph.ID, []model.PhaseWorkflow{{WorkflowID: "wf-nope", VUs: 1, DurationSec: 1}})
	if !errors.Is(err, ErrValidation) || !strings.Contains(err.Error(), "wf-nope") {
		t.Errorf("AttachPhaseWorkflows(unknown workflow) = %v, want ErrValidation naming wf-nope", err)
	}
	err = repo.AttachPhaseWorkflows(ctx, ph.ID, []model.PhaseWorkflow{{WorkflowID: "wf-nope", VUs: 0, DurationSec: 1}})
	if !errors.Is(err, ErrValidation) || !strings.Contains(err.Error(), "vus must be > 0") {
		t.Errorf("AttachPhaseWorkflows(vus 0) = %v, want ErrValidation with the model message", err)
	}
	err = repo.AttachPhaseRules(ctx, ph.ID, []string{"rule-nope"})
	if !errors.Is(err, ErrValidation) || !strings.Contains(err.Error(), "rule-nope") {
		t.Errorf("AttachPhaseRules(unknown rule) = %v, want ErrValidation naming rule-nope", err)
	}
	if err := repo.AttachPhaseRules(ctx, ph.ID, []string{""}); !errors.Is(err, ErrValidation) {
		t.Errorf("AttachPhaseRules(empty id) = %v, want ErrValidation", err)
	}

	// UpdatePhase composes the same attach bodies inside its transaction.
	edit := PhaseEdit{Workflows: []model.PhaseWorkflow{{WorkflowID: "wf-nope", VUs: 1, DurationSec: 1}}}
	if err := repo.UpdatePhase(ctx, ph, edit); !errors.Is(err, ErrValidation) {
		t.Errorf("UpdatePhase(unknown workflow) = %v, want ErrValidation", err)
	}
}
