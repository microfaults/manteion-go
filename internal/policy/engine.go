package policy

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
	"manteion-go/internal/atrocontrol"
	"manteion-go/internal/conditions"
	"manteion-go/internal/model"
	"manteion-go/internal/promql"
	"manteion-go/internal/ruleconv"
	"manteion-go/internal/store"
)

const defaultTickInterval = 10 * time.Second

// Effect represents an action the engine wants the caller to apply.
// The engine decides what should happen; the caller (orchestrator loop)
// is responsible for executing effects via atrocontrol.
type Effect struct {
	ActionType string   // "push_rules" or "clear_rules"
	Service    string
	RuleIDs    []string // non-nil for push_rules; nil for clear_rules
}

// Decider evaluates policy rules against live metrics and returns a set of
// effects to apply. The Engine loop calls Decide each tick; callers execute
// the returned effects.
//
// The current Engine embeds a Decider implementation backed by PromQL +
// threshold comparison. A future OPA-backed implementation would accept the
// same (rules, metrics) inputs and return the same Effect slice, enabling a
// swap without changing the engine loop or the effect-execution layer.
type Decider interface {
	Decide(ctx context.Context, rules []*model.PolicyRule, metrics map[string]float64) ([]Effect, error)
}

// policyLister is the subset of store.PolicyRepo used by the engine.
type policyLister interface {
	ListEnabled(ctx context.Context) ([]*model.PolicyRule, error)
}

type Engine struct {
	policies   policyLister
	rules      *store.RuleRepo
	faults     *store.FaultRepo
	controller *atrocontrol.Controller
	prom       *promql.Client
	logger     *slog.Logger
	interval   time.Duration

	mu          sync.Mutex
	lastFiredAt map[string]time.Time // rule ID → last fire time
}

func New(
	policies policyLister,
	rules *store.RuleRepo,
	faults *store.FaultRepo,
	controller *atrocontrol.Controller,
	prom *promql.Client,
	logger *slog.Logger,
) *Engine {
	return &Engine{
		policies:    policies,
		rules:       rules,
		faults:      faults,
		controller:  controller,
		prom:        prom,
		logger:      logger,
		interval:    defaultTickInterval,
		lastFiredAt: make(map[string]time.Time),
	}
}

// Run starts the policy evaluation loop. Blocks until ctx is cancelled.
//
// The very first tick after process start is skipped to act as a cooldown
// grace window: lastFiredAt is in-memory, so on a restart every enabled rule
// whose condition is currently met would otherwise fire immediately, ignoring
// the cooldown that was respected before the crash. One missed evaluation is
// a small price for not double-applying fault rules across a restart.
func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	firstTick := true
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if firstTick {
				firstTick = false
				e.logger.Info("policy engine: first-tick grace; skipping evaluation",
					"interval", e.interval)
				continue
			}
			e.tick(ctx)
		}
	}
}

func (e *Engine) tick(ctx context.Context) {
	enabled, err := e.policies.ListEnabled(ctx)
	if err != nil {
		e.logger.Error("policy engine: list enabled rules failed", "error", err)
		return
	}
	for _, rule := range enabled {
		if err := e.evaluate(ctx, rule); err != nil {
			e.logger.Warn("policy engine: evaluate failed",
				"rule_id", rule.ID, "error", err)
		}
	}
}

// Decide implements Decider. It evaluates each rule's condition against the
// pre-fetched metrics and returns effects for rules whose conditions are met
// and whose cooldown has elapsed. Cooldown state is still tracked in-memory
// on the Engine instance.
//
// This is the same logic as the tick loop but factored for callers that want
// to supply their own metrics (e.g., an OPA-backed Decider that pre-fetches
// metrics once and evaluates multiple rules against the same snapshot).
func (e *Engine) Decide(_ context.Context, rules []*model.PolicyRule, metrics map[string]float64) ([]Effect, error) {
	var effects []Effect
	for _, rule := range rules {
		value, ok := metrics[rule.Condition.Metric]
		if !ok {
			continue
		}
		if !conditions.Met(value, rule.Condition.Operator, rule.Condition.Threshold) {
			continue
		}
		e.mu.Lock()
		last := e.lastFiredAt[rule.ID]
		if time.Since(last) < rule.Cooldown {
			e.mu.Unlock()
			continue
		}
		e.lastFiredAt[rule.ID] = time.Now()
		e.mu.Unlock()

		if rule.Action.PushRules == nil {
			continue
		}
		eff := Effect{
			ActionType: rule.Action.ActionType,
			Service:    rule.Action.PushRules.Service,
		}
		if rule.Action.ActionType == "push_rules" {
			eff.RuleIDs = rule.Action.PushRules.RuleIDs
		}
		effects = append(effects, eff)
	}
	return effects, nil
}

// Compile-time check that *Engine satisfies Decider.
var _ Decider = (*Engine)(nil)

func (e *Engine) evaluate(ctx context.Context, rule *model.PolicyRule) error {
	value, err := e.prom.QueryInstant(ctx, rule.Condition.Metric)
	if err != nil {
		// Prometheus unavailable — skip quietly, don't fire
		return fmt.Errorf("query metric %q: %w", rule.Condition.Metric, err)
	}

	if !conditions.Met(value, rule.Condition.Operator, rule.Condition.Threshold) {
		return nil
	}

	e.mu.Lock()
	last := e.lastFiredAt[rule.ID]
	if time.Since(last) < rule.Cooldown {
		e.mu.Unlock()
		return nil
	}
	e.lastFiredAt[rule.ID] = time.Now()
	e.mu.Unlock()

	e.logger.Info("policy engine: condition fired",
		"rule_id", rule.ID,
		"metric", rule.Condition.Metric,
		"value", value,
		"action", rule.Action.ActionType,
	)

	return e.executeAction(ctx, rule)
}

func (e *Engine) executeAction(ctx context.Context, rule *model.PolicyRule) error {
	switch rule.Action.ActionType {
	case "push_rules":
		if rule.Action.PushRules == nil {
			return fmt.Errorf("policy engine: rule %q: push_rules config is nil", rule.ID)
		}
		return e.doPushRules(ctx, rule.Action.PushRules)
	case "clear_rules":
		if rule.Action.PushRules == nil {
			return fmt.Errorf("policy engine: rule %q: push_rules config is nil", rule.ID)
		}
		_, err := e.controller.PushRules(ctx, rule.Action.PushRules.Service, nil)
		return err
	case "attack", "cachebox_mode_change":
		// Accepted by model validation but not yet wired to Zeus/atrocontrol.
		// Log at Info so an operator sees the rule fired — silent skips would
		// be a debugging trap.
		e.logger.Info("policy engine: action type not yet implemented; rule fired but had no effect",
			"rule_id", rule.ID,
			"action_type", rule.Action.ActionType)
		return nil
	default:
		e.logger.Warn("policy engine: unrecognized action type",
			"rule_id", rule.ID,
			"action_type", rule.Action.ActionType)
		return nil
	}
}

func (e *Engine) doPushRules(ctx context.Context, action *model.PushRulesAction) error {
	compiled, err := e.loadCompiledRules(ctx, action.RuleIDs)
	if err != nil {
		return err
	}
	_, err = e.controller.PushRules(ctx, action.Service, compiled)
	return err
}

// loadCompiledRules fetches rules by ID, resolves fault specs via ruleconv,
// and returns StaticRules ready for atrocontrol.PushRules.
func (e *Engine) loadCompiledRules(ctx context.Context, ruleIDs []string) ([]atroposdk.StaticRule, error) {
	modelRules := make([]*model.Rule, 0, len(ruleIDs))
	for _, id := range ruleIDs {
		r, err := e.rules.Get(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("get rule %q: %w", id, err)
		}
		modelRules = append(modelRules, r)
	}

	specResolver := &ruleconv.FuncResolver{Fn: e.faults.SpecResolver(ctx)}
	compResolver := &ruleconv.FuncCompositionResolver{Fn: e.faults.CompositionResolver(ctx)}
	compiled, err := ruleconv.CompileRules(modelRules, specResolver, compResolver)
	if err != nil {
		return nil, fmt.Errorf("compile rules: %w", err)
	}

	static := make([]atroposdk.StaticRule, len(compiled))
	for i, cr := range compiled {
		static[i] = atroposdk.StaticRule{
			Name:   cr.Name,
			Point:  parseInjectionPoint(cr.InjectionPoint),
			Labels: cr.Labels,
			Decision: atroposdk.Decision{
				Name: cr.Name,
				Mode: parseMode(cr.Mode),
			},
		}
	}
	return static, nil
}

func parseInjectionPoint(s string) atroposdk.InjectionPoint {
	switch s {
	case "egress":
		return atroposdk.Egress
	case "transient":
		return atroposdk.Transient
	case "custom":
		return atroposdk.Custom
	default:
		return atroposdk.Ingress
	}
}

func parseMode(s string) atroposdk.Mode {
	if s == "background" {
		return atroposdk.Background
	}
	return atroposdk.Inline
}
