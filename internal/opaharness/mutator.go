package opaharness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// RuleStore is the slice of the rule repository the mutator writes through.
// Create, Update and Delete bump the service's desired-state generation in
// their own transaction, so every change reaches instances on their next
// poll; the mutator never touches instances directly.
type RuleStore interface {
	List(ctx context.Context) ([]*model.Rule, error)
	Create(ctx context.Context, rule *model.Rule) error
	Update(ctx context.Context, rule *model.Rule) error
	Delete(ctx context.Context, id string) error
}

// SpecStore is the slice of the fault repository set_params writes through.
// UpdateSpec runs the fault catalog's strict params validation.
// ListCompositions and GetComposition let set_params find the compositions
// that use a spec: ListCompositions names them (without members, as the
// store answers it) and GetComposition loads each one's members.
type SpecStore interface {
	GetSpec(ctx context.Context, id string) (*model.FaultSpec, error)
	UpdateSpec(ctx context.Context, spec *model.FaultSpec) error
	ListCompositions(ctx context.Context) ([]*model.FaultComposition, error)
	GetComposition(ctx context.Context, id string) (*model.FaultComposition, error)
}

var (
	_ RuleStore = (*store.RuleRepo)(nil)
	_ SpecStore = (*store.FaultRepo)(nil)
)

// Mutator applies a policy's mutations to the rule store.
//
// Apply returns one MutationResult per mutation, in order. Its error is
// infrastructure only (listing the rules failed, which aborts the run and
// returns the results gathered so far); a mutation the store rejects is an
// OutcomeFailed result and the run continues with the next one.
type Mutator interface {
	Apply(ctx context.Context, policyID string, ms []Mutation) ([]MutationResult, error)
}

// RuleMutator is the Mutator over the rule and fault-spec repositories.
// Every op is idempotent: a mutation that would leave the store as it is
// reports OutcomeUnchanged and writes nothing, so a policy re-emitting the
// same mutations each evaluation causes no generation churn.
type RuleMutator struct {
	rules RuleStore
	specs SpecStore
	newID func(prefix string) string
	now   func() time.Time
}

var _ Mutator = (*RuleMutator)(nil)

// NewRuleMutator returns a RuleMutator writing through rules and specs.
func NewRuleMutator(rules RuleStore, specs SpecStore) *RuleMutator {
	return &RuleMutator{rules: rules, specs: specs, newID: id.New, now: time.Now}
}

// Apply applies ms in order. The rule list is reloaded before each mutation,
// so a mutation sees the effect of the ones before it. Rules are found by
// (service, name) from Mutation.Key. policyID names the policy the
// mutations come from; it is not written to the rules.
func (m *RuleMutator) Apply(ctx context.Context, policyID string, ms []Mutation) ([]MutationResult, error) {
	_ = policyID
	results := make([]MutationResult, 0, len(ms))
	for _, mut := range ms {
		all, err := m.rules.List(ctx)
		if err != nil {
			return results, fmt.Errorf("list rules: %w", err)
		}
		service, name := mut.Key()
		res := MutationResult{Op: mut.Op, Service: service, Name: name}
		existing := findRule(all, service, name)
		switch mut.Op {
		case OpUpsertRule:
			m.upsert(ctx, &res, mut, existing)
		case OpSetEnabled:
			m.setEnabled(ctx, &res, mut, existing)
		case OpDeleteRule:
			m.deleteRule(ctx, &res, existing)
		case OpSetParams:
			m.setParams(ctx, &res, mut, existing, all)
		default:
			fail(&res, fmt.Errorf("mutation: unknown op %q", mut.Op))
		}
		results = append(results, res)
	}
	return results, nil
}

// upsert creates the rule when absent, updates it in place when it differs
// (keeping the stored ID and CreatedAt; the policy's ID and timestamps are
// ignored as server-owned) and reports unchanged when it does not. A rule
// that fails Validate is OutcomeFailed with no write.
func (m *RuleMutator) upsert(ctx context.Context, res *MutationResult, mut Mutation, existing *model.Rule) {
	if mut.Rule == nil {
		fail(res, fmt.Errorf("mutation: upsert_rule: rule required"))
		return
	}
	want := *mut.Rule
	want.Service, want.Name = res.Service, res.Name
	if existing != nil {
		want.ID, want.CreatedAt = existing.ID, existing.CreatedAt
	} else {
		want.ID = m.newID("rule")
	}
	if err := want.Validate(); err != nil {
		fail(res, err)
		return
	}
	res.RuleID = want.ID
	if existing == nil {
		now := m.now()
		want.CreatedAt, want.UpdatedAt = now, now
		if err := m.rules.Create(ctx, &want); err != nil {
			fail(res, err)
			return
		}
		res.Outcome = OutcomeCreated
		return
	}
	if sameRule(existing, &want) {
		res.Outcome = OutcomeUnchanged
		return
	}
	want.UpdatedAt = m.now()
	if err := m.rules.Update(ctx, &want); err != nil {
		fail(res, err)
		return
	}
	res.Outcome = OutcomeUpdated
}

// setEnabled flips the rule's Enabled flag; an absent rule is OutcomeFailed,
// a rule already in the wanted state is OutcomeUnchanged.
func (m *RuleMutator) setEnabled(ctx context.Context, res *MutationResult, mut Mutation, existing *model.Rule) {
	if existing == nil {
		fail(res, fmt.Errorf("rule %s/%s not found", res.Service, res.Name))
		return
	}
	res.RuleID = existing.ID
	if mut.Enabled == nil {
		fail(res, fmt.Errorf("mutation: set_enabled: enabled required"))
		return
	}
	if existing.Enabled == *mut.Enabled {
		res.Outcome = OutcomeUnchanged
		return
	}
	updated := *existing
	updated.Enabled = *mut.Enabled
	updated.UpdatedAt = m.now()
	if err := m.rules.Update(ctx, &updated); err != nil {
		fail(res, err)
		return
	}
	res.Outcome = OutcomeUpdated
}

// deleteRule deletes the rule; an absent rule is OutcomeUnchanged (the
// desired end state already holds).
func (m *RuleMutator) deleteRule(ctx context.Context, res *MutationResult, existing *model.Rule) {
	if existing == nil {
		res.Outcome = OutcomeUnchanged
		return
	}
	res.RuleID = existing.ID
	if err := m.rules.Delete(ctx, existing.ID); err != nil {
		fail(res, err)
		return
	}
	res.Outcome = OutcomeDeleted
}

// setParams replaces the params of the rule's fault spec, then re-saves the
// rule so its service's generation bumps and pollers recompile the spec. It
// is a two-step write, not a transaction: UpdateSpec first, then the rule's
// Update for the generation bump. A failure between them (the rule's Update
// failing) reports OutcomeFailed with the spec already changed and no bump;
// instances pick the new params up at the service's next generation.
// Refusals and failures before the first write, none of which write:
//   - absent rule, or a rule whose action is not fault_spec: OutcomeFailed;
//   - a spec another rule also references: OutcomeRefused (mutating it would
//     change rules the policy did not name);
//   - a spec any composition uses, directly or through a child composition:
//     OutcomeRefused (a composition's rules, of any service, would change);
//   - listing or loading the compositions fails: OutcomeFailed;
//   - params the fault catalog rejects (UpdateSpec): OutcomeFailed.
//
// Params equal to the stored ones as JSON (whitespace ignored) are
// OutcomeUnchanged.
func (m *RuleMutator) setParams(ctx context.Context, res *MutationResult, mut Mutation, existing *model.Rule, all []*model.Rule) {
	if existing == nil {
		fail(res, fmt.Errorf("rule %s/%s not found", res.Service, res.Name))
		return
	}
	res.RuleID = existing.ID
	if existing.Action.Type != "fault_spec" {
		fail(res, fmt.Errorf("set_params requires a fault_spec rule"))
		return
	}
	specID := existing.Action.FaultSpecID
	spec, err := m.specs.GetSpec(ctx, specID)
	if err != nil {
		fail(res, err)
		return
	}
	users := 0
	for _, r := range all {
		if r.Action.Type == "fault_spec" && r.Action.FaultSpecID == specID {
			users++
		}
	}
	if users > 1 {
		res.Outcome = OutcomeRefused
		res.Error = fmt.Sprintf("fault spec %s is shared by %d rules; give the rule a dedicated spec", specID, users)
		return
	}
	comp, err := m.compositionUsing(ctx, specID)
	if err != nil {
		fail(res, err)
		return
	}
	if comp != "" {
		res.Outcome = OutcomeRefused
		res.Error = fmt.Sprintf("fault spec %s is used by composition %s; give the rule a dedicated spec", specID, comp)
		return
	}
	if jsonEqual(spec.Params, mut.Params) {
		res.Outcome = OutcomeUnchanged
		return
	}
	spec.Params = mut.Params
	if err := m.specs.UpdateSpec(ctx, spec); err != nil {
		fail(res, err)
		return
	}
	touch := *existing
	touch.UpdatedAt = m.now()
	if err := m.rules.Update(ctx, &touch); err != nil {
		fail(res, err)
		return
	}
	res.Outcome = OutcomeUpdated
}

// compositionUsing returns the id of the first composition, in id order,
// whose members reference specID directly or through a child composition
// (followed recursively; a cycle is not followed twice), or "" when none
// does.
func (m *RuleMutator) compositionUsing(ctx context.Context, specID string) (string, error) {
	listed, err := m.specs.ListCompositions(ctx)
	if err != nil {
		return "", fmt.Errorf("list compositions: %w", err)
	}
	ids := make([]string, 0, len(listed))
	for _, c := range listed {
		ids = append(ids, c.ID)
	}
	slices.Sort(ids)
	loaded := map[string]*model.FaultComposition{}
	load := func(id string) (*model.FaultComposition, error) {
		if c, ok := loaded[id]; ok {
			return c, nil
		}
		c, err := m.specs.GetComposition(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("get composition %s: %w", id, err)
		}
		loaded[id] = c
		return c, nil
	}
	var uses func(id string, visiting map[string]bool) (bool, error)
	uses = func(id string, visiting map[string]bool) (bool, error) {
		if visiting[id] {
			return false, nil
		}
		visiting[id] = true
		c, err := load(id)
		if err != nil {
			return false, err
		}
		for _, mem := range c.Members {
			if mem.FaultSpecID == specID {
				return true, nil
			}
			if mem.ChildCompositionID != "" {
				if ok, err := uses(mem.ChildCompositionID, visiting); ok || err != nil {
					return ok, err
				}
			}
		}
		return false, nil
	}
	for _, id := range ids {
		ok, err := uses(id, map[string]bool{})
		if err != nil {
			return "", err
		}
		if ok {
			return id, nil
		}
	}
	return "", nil
}

func fail(res *MutationResult, err error) {
	res.Outcome = OutcomeFailed
	res.Error = err.Error()
}

func findRule(rules []*model.Rule, service, name string) *model.Rule {
	for _, r := range rules {
		if r.Service == service && r.Name == name {
			return r
		}
	}
	return nil
}

// sameRule reports whether want would leave the stored rule as it is: the
// fields a policy controls, compared after Validate has defaulted both.
func sameRule(a, b *model.Rule) bool {
	if a.Enabled != b.Enabled || a.Priority != b.Priority || a.Mode != b.Mode || a.StartPolicy != b.StartPolicy ||
		a.Match.InjectionPoint != b.Match.InjectionPoint || !maps.Equal(a.Match.Labels, b.Match.Labels) ||
		a.Action.Type != b.Action.Type || a.Action.FaultSpecID != b.Action.FaultSpecID ||
		a.Action.FaultCompID != b.Action.FaultCompID {
		return false
	}
	ca, cb := a.Action.CacheBox, b.Action.CacheBox
	if ca == nil || cb == nil {
		return ca == nil && cb == nil
	}
	return *ca == *cb
}

// jsonEqual compares two JSON documents byte-for-byte after json.Compact;
// either failing to compact counts as different.
func jsonEqual(a, b json.RawMessage) bool {
	var ba, bb bytes.Buffer
	if json.Compact(&ba, a) != nil || json.Compact(&bb, b) != nil {
		return false
	}
	return bytes.Equal(ba.Bytes(), bb.Bytes())
}
