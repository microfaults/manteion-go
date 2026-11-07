package opaharness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

// applyTimeout bounds an evaluation's apply and decision record together,
// which run detached from the caller's cancellation (see detached).
const applyTimeout = 10 * time.Second

// detached returns a context that carries ctx's values but not its
// cancellation, bounded by applyTimeout. Once an evaluation has decided what
// to apply, the apply and the decision record run under it, so a shutdown or
// a client disconnect cannot leave rules written with no decision row (the
// row the cooldown is restored from).
func detached(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), applyTimeout)
}

// PolicySource lists the policies the loop evaluates (*store.OPAPolicyRepo).
type PolicySource interface {
	ListEnabled(ctx context.Context) ([]*model.OPAPolicy, error)
}

// DecisionLog stores one decision per evaluation and answers the cooldown
// witness: the evaluated_at of the policy's newest applied decision that
// changed something (*store.PolicyDecisionRepo).
type DecisionLog interface {
	Record(ctx context.Context, d *model.PolicyDecision) error
	LastChangeAt(ctx context.Context, policyID string) (time.Time, bool, error)
}

// RuleLister lists the rules, in priority order, for input.rules
// (*store.RuleRepo). The harness never writes rules through it: writes go
// through the Mutator only.
type RuleLister interface {
	List(ctx context.Context) ([]*model.Rule, error)
}

// InstanceLister lists a service's SDK instances for input.instances
// (*store.SDKRepo). It is read-only: the harness never contacts, pushes to or
// otherwise acts on an instance; its mutations reach instances only through
// the desired-state path (rule generations the instances poll).
type InstanceLister interface {
	ForService(ctx context.Context, service string) ([]*model.SDKInstance, error)
}

// Deps are the Harness's collaborators. Logger, Now and NewID default to
// slog.Default(), time.Now and id.New when nil.
type Deps struct {
	Policies  PolicySource
	Decisions DecisionLog
	Rules     RuleLister
	Instances InstanceLister
	Providers Providers
	Mutator   Mutator
	Logger    *slog.Logger
	Now       func() time.Time
	NewID     func(prefix string) string
}

// Evaluation is the outcome of one policy evaluation. Inputs are the values
// bound to input.inputs, InputDigest their digest (see inputDigest),
// Mutations what the policy emitted, Results the scope refusals followed by
// the mutator's results (refusals only on a dry run). Applied is true when the
// mutator ran and applied something or finished; Changed when a result
// created, updated or deleted a rule. Error is the first compile, input,
// input-document, eval or apply failure; the stages after it are skipped.
// DecisionError is the decision-log write failure ("record decision: ..."):
// the evaluation, an apply included, happened, it was only not recorded.
type Evaluation struct {
	PolicyID    string           `json:"policy_id"`
	EvaluatedAt time.Time        `json:"evaluated_at"`
	Inputs      map[string]any   `json:"inputs"`
	InputDigest string           `json:"input_digest"`
	Mutations   []Mutation       `json:"mutations"`
	Results     []MutationResult `json:"results"`
	Applied     bool             `json:"applied"`
	Changed     bool             `json:"changed"`
	Error       string           `json:"error,omitempty"`
	// DecisionError is set when recording the decision failed.
	DecisionError string `json:"decision_error,omitempty"`
}

// Harness evaluates the enabled OPA policies on their intervals, applies
// their in-scope mutations through the Mutator, records every evaluation in
// the decision log and spaces a policy's applying evaluations by its cooldown
// after one that changed something.
type Harness struct {
	deps       Deps
	resolution time.Duration

	// mu serialises ticks.
	mu sync.Mutex

	// stateMu guards nextAt. EvaluateOnce may run outside a tick (the API)
	// and moves a policy's schedule when it applies a change, so it takes
	// stateMu, never mu; tick holds stateMu only around its nextAt reads and
	// writes, never across an evaluation. nextAt only moves forward
	// (setNextAt keeps the later time), so a tick finishing after a
	// concurrent API apply cannot shorten the cooldown that apply started; a
	// consequence is that a shortened interval_s or cooldown_s takes effect
	// once the wait already scheduled lapses.
	stateMu sync.Mutex
	nextAt  map[string]time.Time

	// prepMu guards prepared; EvaluateOnce may run outside a tick (the API).
	prepMu   sync.Mutex
	prepared map[string]*Prepared
}

// New returns a Harness over d, filling d's nil defaults.
func New(d Deps) *Harness {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.NewID == nil {
		d.NewID = id.New
	}
	return &Harness{
		deps:       d,
		resolution: time.Second,
		nextAt:     map[string]time.Time{},
		prepared:   map[string]*Prepared{},
	}
}

func (h *Harness) now() time.Time { return h.deps.Now() }

// Run ticks every resolution (1 s) until ctx is done. Ticks run on Run's
// goroutine, one at a time, so they never overlap. A tick in flight when ctx
// is cancelled finishes the evaluation it is in (its apply and record are
// detached, see EvaluateOnce) and stops before the next policy; Run returns
// only after that tick returned, so a caller waiting for Run to return knows
// no evaluation is still writing.
func (h *Harness) Run(ctx context.Context) {
	h.deps.Logger.Info("opa harness: starting", "resolution", h.resolution.String())
	t := time.NewTicker(h.resolution)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.tick(ctx, h.now())
		}
	}
}

// tick evaluates, with apply, every enabled policy that is due at now. A
// policy is due on its first sighting unless LastChangeAt + CooldownS is still
// ahead (the decision log is the cooldown's source of truth, so a restart
// keeps it); afterwards it is due IntervalS after its last evaluation, or
// CooldownS after it when that evaluation changed something and the cooldown
// is the longer. A policy's failure is recorded and logged and the tick moves
// on to the next one. Policies no longer listed drop their schedule and
// compiled query. A ctx cancelled mid-tick (shutdown) stops the tick before
// the next policy: the remaining ones are not evaluated against a cancelled
// context, and no state is pruned from the partial pass.
func (h *Harness) tick(ctx context.Context, now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	log := h.deps.Logger
	policies, err := h.deps.Policies.ListEnabled(ctx)
	if err != nil {
		log.ErrorContext(ctx, "opa harness: list policies", "error", err)
		return
	}
	seen := make(map[string]bool, len(policies))
	for _, p := range policies {
		if ctx.Err() != nil {
			return
		}
		seen[p.ID] = true
		interval := time.Duration(p.IntervalS) * time.Second
		cooldown := time.Duration(p.CooldownS) * time.Second
		h.stateMu.Lock()
		next, known := h.nextAt[p.ID]
		h.stateMu.Unlock()
		if known {
			if now.Before(next) {
				continue
			}
		} else {
			last, found, err := h.deps.Decisions.LastChangeAt(ctx, p.ID)
			if err != nil {
				log.ErrorContext(ctx, "opa harness: last change", "policy_id", p.ID, "error", err)
				found = false
			}
			if found && cooldown > 0 && now.Before(last.Add(cooldown)) {
				h.setNextAt(p.ID, last.Add(cooldown))
				continue
			}
		}
		ev := h.EvaluateOnce(ctx, p, true)
		wait := interval
		if ev.Changed && cooldown > wait {
			wait = cooldown
		}
		h.setNextAt(p.ID, now.Add(wait))
	}
	h.stateMu.Lock()
	for pid := range h.nextAt {
		if !seen[pid] {
			delete(h.nextAt, pid)
		}
	}
	h.stateMu.Unlock()
	h.prepMu.Lock()
	for pid := range h.prepared {
		if !seen[pid] {
			delete(h.prepared, pid)
		}
	}
	h.prepMu.Unlock()
}

// setNextAt records when policy id is next due, keeping the time already
// held when it is the later one.
func (h *Harness) setNextAt(id string, at time.Time) {
	h.stateMu.Lock()
	if held, ok := h.nextAt[id]; !ok || at.After(held) {
		h.nextAt[id] = at
	}
	h.stateMu.Unlock()
}

// prepare returns p's compiled query, compiling it when it is not cached or
// its source changed since.
func (h *Harness) prepare(p *model.OPAPolicy) (*Prepared, error) {
	h.prepMu.Lock()
	defer h.prepMu.Unlock()
	if pq, ok := h.prepared[p.ID]; ok && pq.Hash() == SourceHash(p.Rego) {
		return pq, nil
	}
	pq, err := Compile(p.Rego)
	if err != nil {
		delete(h.prepared, p.ID)
		return nil, err
	}
	h.prepared[p.ID] = pq
	return pq, nil
}

// EvaluateOnce evaluates p once: compile, fetch the inputs, build the input
// document, evaluate, refuse out-of-scope mutations and, when apply is true,
// hand the rest to the Mutator. With apply false (a dry run) the Mutator is
// never called. Every evaluation, dry runs included, is recorded as one
// decision whose mutations column is a []MutationResult: for a dry run
// (applied=false) the refusals plus one OutcomeDryRun result per in-scope
// would-be mutation, otherwise the refusals plus the mutator's results.
//
// EvaluateOnce never fails as a call: compile, input, input-document, eval
// and apply failures are reported in Evaluation.Error and recorded, and a
// decision that cannot be recorded is reported in Evaluation.DecisionError
// ("record decision: ...", logged at Error) with everything else kept, since
// an apply that already happened must still reach the caller.
//
// Compiling, fetching the inputs, building the input document and evaluating
// run under ctx; the apply and the decision record run under detached(ctx),
// so once the evaluation has decided, a cancelled ctx (shutdown, a client
// gone) no longer stops it from applying and recording what it decided.
//
// An applying evaluation that changed something moves p's schedule exactly as
// a tick-driven one does: p is next due max(IntervalS, CooldownS) after it,
// so an apply triggered through the API starts the cooldown the loop honours.
func (h *Harness) EvaluateOnce(ctx context.Context, p *model.OPAPolicy, apply bool) *Evaluation {
	start := time.Now()
	now := h.now()
	ev := &Evaluation{
		PolicyID: p.ID, EvaluatedAt: now,
		Inputs: map[string]any{}, Mutations: []Mutation{}, Results: []MutationResult{},
	}
	var refused int

	pq, err := h.prepare(p)
	if err != nil {
		ev.Error = "compile: " + err.Error()
	}
	if ev.Error == "" {
		for _, in := range p.Inputs {
			prov, ok := h.deps.Providers[in.Source]
			if !ok || prov == nil {
				ev.Error = fmt.Sprintf("input %s: no provider for source %q", in.Name, in.Source)
				break
			}
			v, err := prov.Fetch(ctx, in)
			if err != nil {
				ev.Error = fmt.Sprintf("input %s: %v", in.Name, err)
				break
			}
			ev.Inputs[in.Name] = v
		}
		ev.InputDigest = inputDigest(ev.Inputs)
	}
	var doc any
	if ev.Error == "" {
		if doc, err = h.buildInput(ctx, p, now, ev.Inputs); err != nil {
			ev.Error = "input document: " + err.Error()
		}
	}
	var allowed []Mutation
	if ev.Error == "" {
		muts, err := pq.Eval(ctx, doc)
		if err != nil {
			ev.Error = "eval: " + err.Error()
		} else {
			ev.Mutations = muts
			var refusals []MutationResult
			allowed, refusals = enforceScope(p.Scope, muts)
			refused = len(refusals)
			ev.Results = append(ev.Results, refusals...)
		}
	}
	// From here on the evaluation has decided: the apply and the record run
	// detached from ctx's cancellation, bounded by applyTimeout.
	dctx, cancel := detached(ctx)
	defer cancel()
	if ev.Error == "" && apply {
		// Apply may return results together with an error (it aborted after
		// applying some): keep them, they are what happened.
		res, err := h.deps.Mutator.Apply(dctx, p.ID, allowed)
		ev.Results = append(ev.Results, res...)
		for _, r := range res {
			if r.Changed() {
				ev.Changed = true
			}
		}
		ev.Applied = err == nil || len(res) > 0
		if err != nil {
			ev.Error = "apply: " + err.Error()
		}
		if ev.Changed {
			h.setNextAt(p.ID, now.Add(max(time.Duration(p.IntervalS), time.Duration(p.CooldownS))*time.Second))
		}
	}

	// The decision's mutations column is always []MutationResult. A dry run
	// records its refusals followed by one OutcomeDryRun result per in-scope
	// would-be mutation; ev.Results keeps only the refusals (nothing was
	// applied) and ev.Mutations the raw would-be mutations.
	recorded := ev.Results
	if !apply {
		recorded = append(append([]MutationResult{}, ev.Results...), dryRunResults(allowed)...)
	}
	raw, err := json.Marshal(recorded)
	if err != nil {
		// Mutations and results are plain data; this cannot happen in practice.
		raw = []byte("[]")
	}
	d := &model.PolicyDecision{
		ID: h.deps.NewID("pdec"), PolicyID: p.ID, EvaluatedAt: now, InputDigest: ev.InputDigest,
		Mutations: raw, Applied: ev.Applied, Error: ev.Error,
	}
	recErr := h.deps.Decisions.Record(dctx, d)

	attrs := []any{
		"policy_id", p.ID, "policy", p.Name, "input_digest", ev.InputDigest,
		"mutations", len(ev.Mutations), "refused", refused, "applied", ev.Applied,
		"changed", ev.Changed, "dry_run", !apply, "duration_ms", time.Since(start).Milliseconds(),
	}
	if ev.Error != "" {
		h.deps.Logger.WarnContext(ctx, "opa harness: policy evaluated", append(attrs, "error", ev.Error)...)
	} else {
		h.deps.Logger.InfoContext(ctx, "opa harness: policy evaluated", attrs...)
	}
	if recErr != nil {
		ev.DecisionError = "record decision: " + recErr.Error()
		h.deps.Logger.ErrorContext(ctx, "opa harness: record decision", "policy_id", p.ID, "error", recErr)
	}
	return ev
}

// dryRunResults reports each would-be mutation as OutcomeDryRun.
func dryRunResults(ms []Mutation) []MutationResult {
	out := make([]MutationResult, 0, len(ms))
	for _, m := range ms {
		svc, name := m.Key()
		out = append(out, MutationResult{Op: m.Op, Service: svc, Name: name, Outcome: OutcomeDryRun})
	}
	return out
}

// inputDocument is the Rego input document, the contract policy authors
// write against:
//
//   - now: the evaluation time, RFC 3339 with nanoseconds, UTC
//   - now_ns: the same instant in unix nanoseconds (an exact integer)
//   - inputs: {name: value} as the input providers returned them (a scalar is
//     a number or null, a vector [{"metric": {...}, "value": v}, ...]; a NaN
//     or infinite value is null, see VectorProvider)
//   - rules: every rule whose service is in the policy's scope, as model.Rule
//     JSON, in the priority order RuleLister.List returns
//   - services: the policy's scope services
//   - instances: the SDK instances of every scope service, as
//     model.SDKInstance JSON. These are every registered instance, stale and
//     dead ones included: filter on "status" ("alive") where it matters.
type inputDocument struct {
	Now       string               `json:"now"`
	NowNs     int64                `json:"now_ns"`
	Inputs    map[string]any       `json:"inputs"`
	Rules     []*model.Rule        `json:"rules"`
	Services  []string             `json:"services"`
	Instances []*model.SDKInstance `json:"instances"`
}

// buildInput assembles the input document for p at now and round-trips it
// through JSON (numbers decoded as json.Number, so now_ns keeps its
// precision), giving the evaluator the exact shape a policy author sees.
func (h *Harness) buildInput(ctx context.Context, p *model.OPAPolicy, now time.Time, inputs map[string]any) (any, error) {
	all, err := h.deps.Rules.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list rules: %w", err)
	}
	doc := inputDocument{
		Now: now.UTC().Format(time.RFC3339Nano), NowNs: now.UnixNano(), Inputs: inputs,
		Rules: []*model.Rule{}, Services: []string{}, Instances: []*model.SDKInstance{},
	}
	for _, r := range all {
		if p.Scope.Allows(r.Service) {
			doc.Rules = append(doc.Rules, r)
		}
	}
	for _, svc := range p.Scope.Services {
		doc.Services = append(doc.Services, svc)
		insts, err := h.deps.Instances.ForService(ctx, svc)
		if err != nil {
			return nil, fmt.Errorf("instances for %s: %w", svc, err)
		}
		doc.Instances = append(doc.Instances, insts...)
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return out, nil
}

// inputDigest is the lowercase hex sha256 of the JSON encoding of inputs.
// encoding/json sorts map keys at every level, so equal values give the same
// bytes, and the same digest, on every run.
func inputDigest(inputs map[string]any) string {
	raw, err := json.Marshal(inputs)
	if err != nil {
		raw = []byte(fmt.Sprintf("unencodable inputs: %v", err))
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
