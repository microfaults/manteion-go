package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
	"manteion-go/internal/zeus"
)

// =========================================================================
// Dataset preflight (product decision 3: dataset per workflow; a phase's
// datasets are the union over its workflows).
//
// A phase_workflows row may name the zeus dataset its k6 run reads
// (workflows[].dataset_id; empty = MANTEION_ZEUS_DATASET_ID fallback at run
// start). zeus holds datasets in memory with a TTL, so a plan referencing
// one can fail in two silent ways: the dataset is already gone (zeus
// restarted, TTL passed) or it gets reaped mid-run. Both create handlers
// (POST /experiments, POST /experiments/{id}/phases) and both start
// handlers (POST .../start, POST .../phases/{phaseId}/start) therefore check
// the plan against GET /datasets before any row is written or any phase is
// claimed, answering 422 with a machine code + the offending ids, or 502
// when zeus cannot be asked (fail closed). A plan with no dataset ids never
// touches zeus.
// =========================================================================

const (
	codeDatasetMissing  = "dataset_missing"
	codeDatasetExpiring = "dataset_expiring"
	codeZeusUnreachable = "zeus_unreachable"

	// datasetPreflightSlack pads the plan length: phase enter/teardown, poll
	// lag and the harvest all run after the last k6 iteration.
	datasetPreflightSlack = 5 * time.Minute
)

// datasetErrorResponse is the envelope of a failed preflight. It keeps the
// standard `error` string (the UI and expctl read it) and adds `code`
// (dataset_missing | dataset_expiring | zeus_unreachable) plus, on the 422s,
// the offending `dataset_ids`.
type datasetErrorResponse struct {
	Error      string   `json:"error"`
	Code       string   `json:"code"`
	DatasetIDs []string `json:"dataset_ids,omitempty"`
}

// datasetUnion is the derived read-only phase field: the set-union of the
// rows' non-empty dataset ids in first-seen order. Never nil, so it
// serializes as [] rather than null.
func datasetUnion(pws []model.PhaseWorkflow) []string {
	out := []string{}
	seen := make(map[string]bool, len(pws))
	for _, pw := range pws {
		if pw.DatasetID == "" || seen[pw.DatasetID] {
			continue
		}
		seen[pw.DatasetID] = true
		out = append(out, pw.DatasetID)
	}
	return out
}

// planDatasetIDs is datasetUnion over every phase of the plan: phase order,
// then row order.
func planDatasetIDs(plan [][]model.PhaseWorkflow) []string {
	var all []model.PhaseWorkflow
	for _, pws := range plan {
		all = append(all, pws...)
	}
	return datasetUnion(all)
}

// planLength is how long the plan needs its datasets for: phases run
// sequentially and a phase's workflows concurrently, so Σ over phases of
// max(duration_sec), plus datasetPreflightSlack.
func planLength(plan [][]model.PhaseWorkflow) time.Duration {
	var total time.Duration
	for _, pws := range plan {
		var longest time.Duration
		for _, pw := range pws {
			if d := time.Duration(pw.DurationSec) * time.Second; d > longest {
				longest = d
			}
		}
		total += longest
	}
	return total + datasetPreflightSlack
}

// checkDatasets partitions the plan's dataset ids into those zeus does not
// hold (missing) and those zeus would delete at or before now + planLength
// (expiring; a ttl_s of 0 never expires). Both lists keep first-seen order.
func checkDatasets(now time.Time, plan [][]model.PhaseWorkflow, known []zeus.Dataset) (missing, expiring []string) {
	byID := make(map[string]zeus.Dataset, len(known))
	for _, d := range known {
		byID[d.ID] = d
	}
	needUntil := now.Add(planLength(plan))
	for _, dsID := range planDatasetIDs(plan) {
		d, ok := byID[dsID]
		if !ok {
			missing = append(missing, dsID)
			continue
		}
		if exp := d.ExpiresAt(); !exp.IsZero() && !exp.After(needUntil) {
			expiring = append(expiring, dsID)
		}
	}
	return missing, expiring
}

// preflightDatasets validates the plan's datasets against zeus. It returns
// (0, nil) when the plan may proceed; otherwise the HTTP status and the
// envelope to write. Missing ids are reported ahead of expiring ones — a
// dataset that is gone is the more fundamental failure.
func (s *Server) preflightDatasets(ctx context.Context, plan [][]model.PhaseWorkflow) (int, *datasetErrorResponse) {
	if len(planDatasetIDs(plan)) == 0 {
		return 0, nil
	}
	if s.zeus == nil {
		return http.StatusBadGateway, &datasetErrorResponse{
			Error: "zeus unreachable: no zeus client configured", Code: codeZeusUnreachable}
	}
	known, err := s.zeus.ListDatasets(ctx)
	if err != nil {
		s.logger.Error("dataset preflight: zeus unreachable", "error", err)
		return http.StatusBadGateway, &datasetErrorResponse{
			Error: "zeus unreachable: " + err.Error(), Code: codeZeusUnreachable}
	}
	missing, expiring := checkDatasets(time.Now(), plan, known)
	if len(missing) > 0 {
		return http.StatusUnprocessableEntity, &datasetErrorResponse{
			Error:      "dataset(s) not found in zeus: " + strings.Join(missing, ", "),
			Code:       codeDatasetMissing,
			DatasetIDs: missing,
		}
	}
	if len(expiring) > 0 {
		return http.StatusUnprocessableEntity, &datasetErrorResponse{
			Error: fmt.Sprintf("dataset(s) expire before the plan would finish (plan length %s, incl. %s slack): %s",
				planLength(plan), datasetPreflightSlack, strings.Join(expiring, ", ")),
			Code:       codeDatasetExpiring,
			DatasetIDs: expiring,
		}
	}
	return 0, nil
}

// preflightRequestPlan runs the preflight over the phases a create request
// describes. Returns false once a refusal has been written.
func (s *Server) preflightRequestPlan(w http.ResponseWriter, r *http.Request, phases []createPhaseRequest) bool {
	plan := make([][]model.PhaseWorkflow, len(phases))
	for i, ph := range phases {
		plan[i] = ph.Workflows
	}
	return s.writePreflight(w, r, plan)
}

// preflightExperimentDatasets runs the preflight over the stored plan of an
// experiment about to start: every phase still to run (non-terminal), in
// position order. A missing experiment yields an empty plan and passes —
// the orchestrator reports it as today. Returns false once a response has
// been written.
func (s *Server) preflightExperimentDatasets(w http.ResponseWriter, r *http.Request, experimentID string) bool {
	ctx := r.Context()
	phases, err := s.experiments.ListPhasesForExperiment(ctx, experimentID)
	if err != nil {
		s.logger.Error("dataset preflight: list phases failed", "error", err, "experiment_id", experimentID)
		writeError(w, http.StatusInternalServerError, "failed to list phases")
		return false
	}
	plan, err := s.storedPlan(ctx, phases)
	if err != nil {
		s.logger.Error("dataset preflight: load phase workflows failed", "error", err, "experiment_id", experimentID)
		writeError(w, http.StatusInternalServerError, "failed to load phase workflows")
		return false
	}
	return s.writePreflight(w, r, plan)
}

// preflightPhaseDatasets runs the preflight over one phase about to start
// (fresh start or resume — a resume re-launches its k6 runs). An unknown or
// terminal phase yields an empty plan and passes so the orchestrator's own
// refusal is what the caller sees. Returns false once a response has been
// written.
func (s *Server) preflightPhaseDatasets(w http.ResponseWriter, r *http.Request, phaseID string) bool {
	ctx := r.Context()
	phase, err := s.experiments.GetPhase(ctx, phaseID)
	if errors.Is(err, store.ErrNotFound) {
		return true
	}
	if err != nil {
		s.logger.Error("dataset preflight: get phase failed", "error", err, "phase_id", phaseID)
		writeError(w, http.StatusInternalServerError, "failed to get phase")
		return false
	}
	plan, err := s.storedPlan(ctx, []*model.ExperimentPhase{phase})
	if err != nil {
		s.logger.Error("dataset preflight: load phase workflows failed", "error", err, "phase_id", phaseID)
		writeError(w, http.StatusInternalServerError, "failed to load phase workflows")
		return false
	}
	return s.writePreflight(w, r, plan)
}

// storedPlan loads the workflow rows of the phases that still have to run
// (non-terminal), in the order given.
func (s *Server) storedPlan(ctx context.Context, phases []*model.ExperimentPhase) ([][]model.PhaseWorkflow, error) {
	plan := make([][]model.PhaseWorkflow, 0, len(phases))
	for _, p := range phases {
		switch p.Status {
		case "completed", "failed", "skipped":
			continue
		}
		pws, err := s.experiments.ListPhaseWorkflows(ctx, p.ID)
		if err != nil {
			return nil, fmt.Errorf("phase %s: %w", p.ID, err)
		}
		plan = append(plan, pws)
	}
	return plan, nil
}

// writePreflight runs preflightDatasets and writes the refusal, if any.
// Returns false once a response has been written.
func (s *Server) writePreflight(w http.ResponseWriter, r *http.Request, plan [][]model.PhaseWorkflow) bool {
	status, resp := s.preflightDatasets(r.Context(), plan)
	if resp == nil {
		return true
	}
	writeJSON(w, status, resp)
	return false
}
