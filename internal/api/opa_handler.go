// OPA harness endpoints: CRUD over the Rego policies the harness evaluates,
// their decision log, and a one-shot evaluate that defaults to a dry run.
// The harness itself (internal/opaharness) runs only when the policy engine
// is on; without it the evaluate endpoint answers 503 and the rest still
// works.
package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/opaharness"
	"manteion-go/internal/store"
)

// opaPolicyStore is the OPA policy repository as the handlers use it
// (*store.OPAPolicyRepo).
type opaPolicyStore interface {
	Create(ctx context.Context, p *model.OPAPolicy) error
	Get(ctx context.Context, id string) (*model.OPAPolicy, error)
	Update(ctx context.Context, p *model.OPAPolicy) error
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]*model.OPAPolicy, error)
}

// policyDecisionLister pages a policy's decision log, newest first
// (*store.PolicyDecisionRepo).
type policyDecisionLister interface {
	List(ctx context.Context, policyID string, p store.Page) ([]*model.PolicyDecision, int, error)
}

// opaEvaluator evaluates one policy on demand (*opaharness.Harness).
type opaEvaluator interface {
	EvaluateOnce(ctx context.Context, p *model.OPAPolicy, apply bool) *opaharness.Evaluation
}

// errOPAHarnessDisabled is the 503 message of evaluate without a harness.
const errOPAHarnessDisabled = "opa harness disabled (MANTEION_POLICY_ENGINE)"

// checkOPAPolicy validates p (applying its defaults in place) and compiles
// its Rego. It writes the 422 and returns false when either fails.
func checkOPAPolicy(w http.ResponseWriter, p *model.OPAPolicy) bool {
	if err := p.Validate(); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return false
	}
	if _, err := opaharness.Compile(p.Rego); err != nil {
		writeError(w, http.StatusUnprocessableEntity, "rego: "+err.Error())
		return false
	}
	return true
}

// handleCreateOPAPolicy creates a Rego policy evaluated by the OPA harness.
//
// @Summary      Create OPA policy
// @Description  Persist a Rego (v1 syntax) policy. The module must declare
// @Description  `package atropos` and define `mutations` (a set of rule mutations);
// @Description  it is compiled at save time and rejected with 422 on parse, type or
// @Description  unsafe-builtin (http.send) errors. inputs[] declare the telemetry
// @Description  (prometheus | loki instant metric queries) exposed as input.inputs.<name>;
// @Description  scope.services bounds which services' rules the policy may mutate.
// @Description  Policies are created disabled unless "enabled": true is explicit.
// @Description  interval_s defaults to 30 and an input's kind to "scalar".
// @Tags         autorules
// @Accept       json
// @Produce      json
// @Param        policy  body      model.OPAPolicy  true  "Policy definition"
// @Success      201     {object}  model.OPAPolicy
// @Failure      400     {object}  api.ErrorResponse  "invalid JSON"
// @Failure      409     {object}  api.ErrorResponse  "policy name already exists"
// @Failure      422     {object}  api.ErrorResponse  "validation error, or Rego compile error (rego: ...)"
// @Failure      500     {object}  api.ErrorResponse
// @Router       /opa/policies [post]
func (s *Server) handleCreateOPAPolicy(w http.ResponseWriter, r *http.Request) {
	var p model.OPAPolicy
	if err := readJSON(r, &p); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if p.ID == "" {
		p.ID = generateID("opa")
	}
	now := time.Now()
	p.CreatedAt, p.UpdatedAt = now, now
	if !checkOPAPolicy(w, &p) {
		return
	}
	if err := s.opaPolicies.Create(r.Context(), &p); err != nil {
		s.writeOPAPolicyWriteError(w, err, p.ID, "create", "failed to create policy")
		return
	}
	s.logger.Info("opa policy created", "id", p.ID, "name", p.Name, "enabled", p.Enabled)
	writeJSON(w, http.StatusCreated, p)
}

// writeOPAPolicyWriteError maps a Create/Update failure: not found → 404,
// a name collision → 409, a repository validation refusal → 422, anything
// else → 500 with unexpected (logged at Error).
func (s *Server) writeOPAPolicyWriteError(w http.ResponseWriter, err error, id, op, unexpected string) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "policy not found")
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "policy name already exists")
	case errors.Is(err, store.ErrValidation):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	default:
		s.logger.Error(op+" opa policy failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, unexpected)
	}
}

// handleListOPAPolicies returns every OPA policy.
//
// @Summary      List OPA policies
// @Description  Every OPA harness policy, oldest created first; [] when there are none.
// @Tags         autorules
// @Produce      json
// @Success      200  {array}   model.OPAPolicy
// @Failure      500  {object}  api.ErrorResponse
// @Router       /opa/policies [get]
func (s *Server) handleListOPAPolicies(w http.ResponseWriter, r *http.Request) {
	ps, err := s.opaPolicies.List(r.Context())
	if err != nil {
		s.logger.Error("list opa policies failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list policies")
		return
	}
	if ps == nil {
		ps = []*model.OPAPolicy{}
	}
	writeJSON(w, http.StatusOK, ps)
}

// getOPAPolicy loads the policy named by the path's {id}, writing the 404 or
// 500 and returning nil when it cannot.
func (s *Server) getOPAPolicy(w http.ResponseWriter, r *http.Request) *model.OPAPolicy {
	id := r.PathValue("id")
	p, err := s.opaPolicies.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "policy not found")
		return nil
	}
	if err != nil {
		s.logger.Error("get opa policy failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get policy")
		return nil
	}
	return p
}

// handleGetOPAPolicy returns one OPA policy.
//
// @Summary      Get OPA policy
// @Description  One OPA harness policy by id.
// @Tags         autorules
// @Produce      json
// @Param        id   path      string  true  "Policy ID"
// @Success      200  {object}  model.OPAPolicy
// @Failure      404  {object}  api.ErrorResponse  "policy not found"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /opa/policies/{id} [get]
func (s *Server) handleGetOPAPolicy(w http.ResponseWriter, r *http.Request) {
	if p := s.getOPAPolicy(w, r); p != nil {
		writeJSON(w, http.StatusOK, p)
	}
}

// handleUpdateOPAPolicy replaces an OPA policy.
//
// @Summary      Update OPA policy
// @Description  Replace the policy with the path's id (a body id is ignored). The
// @Description  body is validated and its Rego compiled exactly as on create (422 on
// @Description  failure, the stored policy untouched). created_at is kept and
// @Description  updated_at set; the response is the stored policy.
// @Tags         autorules
// @Accept       json
// @Produce      json
// @Param        id      path      string           true  "Policy ID"
// @Param        policy  body      model.OPAPolicy  true  "Policy definition"
// @Success      200     {object}  model.OPAPolicy
// @Failure      400     {object}  api.ErrorResponse  "invalid JSON"
// @Failure      404     {object}  api.ErrorResponse  "policy not found"
// @Failure      409     {object}  api.ErrorResponse  "policy name already exists"
// @Failure      422     {object}  api.ErrorResponse  "validation error, or Rego compile error (rego: ...)"
// @Failure      500     {object}  api.ErrorResponse
// @Router       /opa/policies/{id} [put]
func (s *Server) handleUpdateOPAPolicy(w http.ResponseWriter, r *http.Request) {
	var p model.OPAPolicy
	if err := readJSON(r, &p); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	p.ID = r.PathValue("id")
	p.UpdatedAt = time.Now()
	if !checkOPAPolicy(w, &p) {
		return
	}
	if err := s.opaPolicies.Update(r.Context(), &p); err != nil {
		s.writeOPAPolicyWriteError(w, err, p.ID, "update", "failed to update policy")
		return
	}
	stored := s.getOPAPolicy(w, r)
	if stored == nil {
		return
	}
	s.logger.Info("opa policy updated", "id", stored.ID, "name", stored.Name, "enabled", stored.Enabled)
	writeJSON(w, http.StatusOK, stored)
}

// handleDeleteOPAPolicy deletes an OPA policy and its decision log.
//
// @Summary      Delete OPA policy
// @Description  Delete the policy; its decision log goes with it.
// @Tags         autorules
// @Param        id   path  string  true  "Policy ID"
// @Success      204  "policy deleted"
// @Failure      404  {object}  api.ErrorResponse  "policy not found"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /opa/policies/{id} [delete]
func (s *Server) handleDeleteOPAPolicy(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.opaPolicies.Delete(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "policy not found")
			return
		}
		s.logger.Error("delete opa policy failed", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete policy")
		return
	}
	s.logger.Info("opa policy deleted", "id", id)
	w.WriteHeader(http.StatusNoContent)
}

// handleListOPADecisions pages a policy's decision log, newest first.
//
// Each stored decision's mutations is a []opaharness.MutationResult: on an
// applying evaluation the scope refusals followed by the mutator's results;
// on a dry run (applied=false) the refusals followed by one outcome "dry_run"
// result per in-scope would-be mutation.
//
// @Summary      List OPA policy decisions
// @Description  The policy's decision log, newest first (evaluated_at, then id), in the
// @Description  standard {data, page} envelope. One decision per evaluation, dry runs
// @Description  included. Each decision's mutations is an array of opaharness.MutationResult
// @Description  ({op, service, name, outcome, error?, rule_id?}): for an applied evaluation
// @Description  the scope refusals (outcome "refused") followed by the mutator's results
// @Description  (created | updated | deleted | unchanged | failed); for a dry run
// @Description  (applied=false) the refusals followed by one outcome "dry_run" entry per
// @Description  in-scope would-be mutation. error is set when the evaluation failed.
// @Tags         autorules
// @Produce      json
// @Param        id      path      string  true   "Policy ID"
// @Param        limit   query     int     false  "Page size (default 20, max 200)"
// @Param        offset  query     int     false  "Page offset (default 0)"
// @Success      200     {object}  api.PageEnvelope[model.PolicyDecision]
// @Failure      404     {object}  api.ErrorResponse  "policy not found"
// @Failure      500     {object}  api.ErrorResponse
// @Router       /opa/policies/{id}/decisions [get]
func (s *Server) handleListOPADecisions(w http.ResponseWriter, r *http.Request) {
	p := s.getOPAPolicy(w, r)
	if p == nil {
		return
	}
	limit, offset := parsePagination(r)
	decs, total, err := s.policyDecisions.List(r.Context(), p.ID, store.Page{Limit: limit, Offset: offset})
	if err != nil {
		s.logger.Error("list opa decisions failed", "id", p.ID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list decisions")
		return
	}
	items := make([]model.PolicyDecision, 0, len(decs))
	for _, d := range decs {
		items = append(items, *d)
	}
	writePage(w, http.StatusOK, items, total, limit, offset)
}

// handleEvaluateOPAPolicy evaluates a policy once, by default as a dry run.
//
// The answer is the full Evaluation: the raw mutations the policy emitted and
// results — on a dry run only the scope refusals (nothing was applied), on
// ?apply=true the refusals plus the mutator's results. Either way the
// evaluation is recorded in the decision log (a dry run's decision lists its
// would-be mutations as "dry_run" results); a decision that could not be
// recorded is reported in decision_error with 200, since the results (an
// apply included) happened. Without a harness (policy engine off) it answers
// 503.
//
// @Summary      Evaluate OPA policy
// @Description  Evaluate the policy once against live telemetry, enabled or not.
// @Description  Defaults to a dry run: the mutations are computed and scope-checked but
// @Description  not applied, and results holds only the scope refusals. With
// @Description  apply=true the in-scope mutations are applied through the rule
// @Description  repository and results holds the refusals plus the mutator's results;
// @Description  an applying evaluation that changed something starts the policy's
// @Description  cooldown. Every evaluation is recorded in the decision log (see
// @Description  /opa/policies/{id}/decisions). Compile, input, eval and apply failures
// @Description  are reported in the Evaluation's error with 200; a decision that could
// @Description  not be recorded is reported in decision_error, also with 200 (the
// @Description  results, an apply included, happened). 503 when the harness is disabled
// @Description  (MANTEION_POLICY_ENGINE off).
// @Tags         autorules
// @Produce      json
// @Param        id     path      string  true   "Policy ID"
// @Param        apply  query     bool    false  "apply the mutations (default false: dry run)"
// @Success      200    {object}  opaharness.Evaluation
// @Failure      404    {object}  api.ErrorResponse  "policy not found"
// @Failure      503    {object}  api.ErrorResponse  "opa harness disabled (MANTEION_POLICY_ENGINE)"
// @Router       /opa/policies/{id}/evaluate [post]
func (s *Server) handleEvaluateOPAPolicy(w http.ResponseWriter, r *http.Request) {
	if s.opa == nil {
		writeError(w, http.StatusServiceUnavailable, errOPAHarnessDisabled)
		return
	}
	p := s.getOPAPolicy(w, r)
	if p == nil {
		return
	}
	apply := r.URL.Query().Get("apply") == "true"
	writeJSON(w, http.StatusOK, s.opa.EvaluateOnce(r.Context(), p, apply))
}
