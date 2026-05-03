# Rule Engine (Policy Engine)

> **Date:** 2026-04-28
> **Scope:** manteion-go — atropos side only
> **Go version:** 1.25 stdlib + pgx/v5 (already in go.mod)
> **Prerequisite:** `internal/promql/client.go` must exist. If the experiment
> orchestration plan has already been implemented, `promql.Client` is already
> available. If implementing this standalone, complete Phase 4 of that plan first
> (it is self-contained and has no dependencies on the orchestrator).

---

## Context

This plan adds the **Policy Engine** — a background goroutine that evaluates
standing `PolicyRule` conditions against live Prometheus metrics on a fixed tick.
When a condition fires and the cooldown has elapsed, it executes the action via
`atrocontrol.Controller` (push or clear rules on target SDK instances).

The rule engine is independent of experiments — it keeps running even when no
experiment is active. An operator creates a `PolicyRule` saying "if this metric
condition holds, push these fault rules to this service" and the engine does the
rest automatically.

---

## What Already Exists — Do Not Rebuild

| What | Where |
|---|---|
| `model.PolicyRule`, `PolicyCondition`, `PolicyAction` | `internal/model/policy.go` |
| `store.PolicyRepo` — `Create`, `Get`, `List`, `ListEnabled`, `Delete` | `internal/store/policy_repo.go` |
| `atrocontrol.Controller` — `PushRules`, `ClearFault` | `internal/atrocontrol/` |
| Rule compilation (FK resolution → wire format) | `internal/ruleconv/ruleconv.go` |
| `internal/promql/client.go` — `QueryInstant`, `Healthy` | (from orchestration plan) |

---

## What `PolicyRule` Is

`model.PolicyRule` (already in `internal/model/policy.go`) is a standing
"if this, then that" rule evaluated against live metrics:

```
PolicyRule {
    Condition: PolicyCondition{
        Metric:    "histogram_quantile(0.99, ...)",  // PromQL query
        Operator:  "gt",                              // gt, gte, lt, lte, eq
        Threshold: 500000,                            // e.g. microseconds
    },
    Action: PolicyAction{
        ActionType: "push_rules",                     // see Phase 1
        PushRules: &PushRulesAction{
            Service: "checkout",
            RuleIDs: ["rule-abc123"],
        },
    },
    Cooldown: 5 * time.Minute,
}
```

---

## Architecture Overview

```
Operator
    │
    ▼
POST /api/v1/policies          (create standing PolicyRule, enabled=true)
    │
    ▼
store.PolicyRepo               (persisted to policy_rules table)
    │
    ▼
policy.Engine (background goroutine, 10s tick)
    │  loads enabled PolicyRules
    │  queries Prometheus for each condition
    │  if condition met + cooldown elapsed:
    │
    ├──► action=push_rules  → atrocontrol.PushRules(service, compiledRules)
    └──► action=clear_rules → atrocontrol.PushRules(service, nil)
```

---

## Phase 1: Add `push_rules` / `clear_rules` to `PolicyAction`

**File:** `internal/model/policy.go`

`PolicyAction.ActionType` currently accepts `"attack"` and
`"cachebox_mode_change"`. Add two new types for the rule engine:

```go
type PolicyAction struct {
    ActionType     string            `json:"action_type"`
    AttackTarget   *AttackTargetSpec `json:"attack_target,omitempty"`
    CacheBoxChange *CacheBoxConfig   `json:"cachebox_change,omitempty"`
    PushRules      *PushRulesAction  `json:"push_rules,omitempty"`   // NEW
}

// PushRulesAction identifies which rules to push to which service.
type PushRulesAction struct {
    Service string   `json:"service"`
    RuleIDs []string `json:"rule_ids"`
}

func (a *PushRulesAction) Validate() error {
    if a.Service == "" {
        return errors.New("push_rules: service required")
    }
    if len(a.RuleIDs) == 0 {
        return errors.New("push_rules: at least one rule_id required")
    }
    return nil
}
```

Update `PolicyAction.Validate()` to handle the new action types:

```go
case "push_rules":
    if a.PushRules == nil {
        return errors.New("action: push_rules required for action_type=push_rules")
    }
    return a.PushRules.Validate()
case "clear_rules":
    if a.PushRules == nil || a.PushRules.Service == "" {
        return errors.New("action: push_rules.service required for action_type=clear_rules")
    }
    return nil
```

No DB migration needed — `PolicyAction` is stored as JSONB.

---

## Phase 2: Add `SetEnabled` to `PolicyRepo`

**File:** `internal/store/policy_repo.go`

```go
// SetEnabled enables or disables a policy rule.
func (r *PolicyRepo) SetEnabled(ctx context.Context, id string, enabled bool) error {
    res, err := r.db.ExecContext(ctx,
        `UPDATE policy_rules SET enabled = $2 WHERE id = $1`, id, enabled)
    if err != nil {
        return fmt.Errorf("set policy enabled: %w", err)
    }
    n, _ := res.RowsAffected()
    if n == 0 {
        return ErrNotFound
    }
    return nil
}
```

---

## Phase 3: Policy Engine

**New package:** `internal/policy/`

### 3a. `internal/policy/engine.go`

```go
package policy

import (
    "context"
    "fmt"
    "log/slog"
    "sync"
    "time"

    atroposdk "atropos-go"
    "manteion-go/internal/atrocontrol"
    "manteion-go/internal/model"
    "manteion-go/internal/promql"
    "manteion-go/internal/ruleconv"
    "manteion-go/internal/store"
)

const defaultTickInterval = 10 * time.Second

type Engine struct {
    policies   *store.PolicyRepo
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
    policies *store.PolicyRepo,
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
func (e *Engine) Run(ctx context.Context) {
    ticker := time.NewTicker(e.interval)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
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

func (e *Engine) evaluate(ctx context.Context, rule *model.PolicyRule) error {
    value, err := e.prom.QueryInstant(ctx, rule.Condition.Metric)
    if err != nil {
        // Prometheus unavailable — skip quietly, don't fire
        return fmt.Errorf("query metric %q: %w", rule.Condition.Metric, err)
    }

    if !conditionMet(value, rule.Condition.Operator, rule.Condition.Threshold) {
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
        return e.doPushRules(ctx, rule.Action.PushRules)
    case "clear_rules":
        _, err := e.controller.PushRules(ctx, rule.Action.PushRules.Service, nil)
        return err
    default:
        // "attack" and "cachebox_mode_change" are handled by zeus / future plans
        e.logger.Debug("policy engine: skipping unhandled action type",
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
// Pattern: loop ruleIDs → e.rules.Get → ruleconv.CompileRules → decode StaticRules
// (see internal/api/sdk_handler.go handlePollRules for the existing pattern)
func (e *Engine) loadCompiledRules(ctx context.Context, ruleIDs []string) ([]atroposdk.StaticRule, error) {
    panic("implement me")
}
```

### 3b. `internal/policy/conditions.go`

```go
package policy

func conditionMet(value float64, operator string, threshold float64) bool {
    switch operator {
    case "gt":  return value > threshold
    case "gte": return value >= threshold
    case "lt":  return value < threshold
    case "lte": return value <= threshold
    case "eq":  return value == threshold
    }
    return false
}
```

---

## Phase 4: Policy API Handlers

**New file:** `internal/api/policy_handler.go`

Routes to add to `server.go`:

```
POST   /api/v1/policies              create PolicyRule
GET    /api/v1/policies              list all
GET    /api/v1/policies/{id}         get by ID
DELETE /api/v1/policies/{id}         delete
PATCH  /api/v1/policies/{id}/enable  set enabled=true  → store.PolicyRepo.SetEnabled
PATCH  /api/v1/policies/{id}/disable set enabled=false → store.PolicyRepo.SetEnabled
```

All handlers follow the same shape as existing handlers in `internal/api/`.
The policy handler does not need a reference to the engine — CRUD goes directly
to `store.PolicyRepo`. The engine picks up changes on its next tick.

**Update `internal/api/server.go`:** register the new routes in `routes()`.
No new field needed on `Server` — `policyRepo` is already wired in if
`store.PolicyRepo` is passed to the constructor.

---

## Phase 5: Wire in `cmd/manteion/main.go`

```go
// promql.Client — reuse the one wired for the orchestrator, or add here:
promClient := promql.NewClient(envOr("PROMETHEUS_URL", "http://prometheus:9090"))

policyEngine := policy.New(policyRepo, ruleRepo, faultRepo, controller, promClient, logger)

// Start as background goroutine alongside the HTTP server
go policyEngine.Run(ctx)
```

---

## Implementation Order

Execute in this order — each step compiles independently before the next:

- [ ] **Step 1** — `internal/model/policy.go`: add `PushRulesAction` struct + `push_rules`/`clear_rules` cases in `PolicyAction.Validate()`. `go build ./internal/model/...`
- [ ] **Step 2** — `internal/store/policy_repo.go`: add `SetEnabled`. `go build ./internal/store/...`
- [ ] **Step 3** — `internal/policy/engine.go` + `conditions.go`: implement `Engine`, `Run`, `tick`, `evaluate`, `executeAction`, `doPushRules`, `loadCompiledRules`, `conditionMet`. `go build ./internal/policy/...`
- [ ] **Step 4** — `internal/api/policy_handler.go`: CRUD + enable/disable handlers. Register routes in `server.go`. `go build ./internal/api/...`
- [ ] **Step 5** — `cmd/manteion/main.go`: wire `policy.Engine`, start goroutine. `go build ./cmd/manteion`
- [ ] **Step 6** — `go build ./...` + `go vet ./...` + `go test ./...`

---

## Testing

### Unit Tests — `internal/policy/engine_test.go`

Use `httptest.NewServer` to fake Prometheus returning controlled metric values.
Use a mock or stub `atrocontrol.Controller`.

```go
// Test: condition fires when metric exceeds threshold → PushRules called
// Test: condition does not fire when below threshold → PushRules not called
// Test: cooldown prevents double-fire within cooldown window
// Test: "clear_rules" action calls PushRules with nil (empty rule set)
// Test: Prometheus unavailable → engine logs warning, does not panic
```

### Manual Integration Test

With manteion + Postgres running (`docker compose up -d`):

**1. Create a fault spec + rule:**
```bash
SPEC=$(curl -s -X POST localhost:8080/api/v1/faults/specs \
  -H 'Content-Type: application/json' \
  -d '{"name":"latency","category":"inline","fault_type":"latency","config":{"delay_ms":500}}')
SPEC_ID=$(echo $SPEC | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

RULE=$(curl -s -X POST localhost:8080/api/v1/rules \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"slow-checkout\",\"service\":\"checkout\",\"enabled\":true,\"mode\":\"inline\",\"fault_spec_id\":\"$SPEC_ID\"}")
RULE_ID=$(echo $RULE | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
```

**2. Create a standing PolicyRule that fires when `up == 1` (always true — easy to test):**
```bash
curl -s -X POST localhost:8080/api/v1/policies \
  -H 'Content-Type: application/json' \
  -d "{
    \"name\": \"auto-inject-on-load\",
    \"enabled\": true,
    \"condition\": {\"metric\": \"up\", \"operator\": \"eq\", \"threshold\": 1},
    \"action\": {
      \"action_type\": \"push_rules\",
      \"push_rules\": {\"service\": \"checkout\", \"rule_ids\": [\"$RULE_ID\"]}
    },
    \"cooldown\": 30000000000
  }"
# Within 10s: watch manteion logs for "policy engine: condition fired"
# SDK poll should show the latency rule active on checkout
```

**3. Disable the rule and verify it stops firing:**
```bash
POLICY_ID=<id from step 2>
curl -s -X PATCH localhost:8080/api/v1/policies/$POLICY_ID/disable
# Engine skips disabled rules — no further pushes after next tick
```

**4. Test clear_rules:**
```bash
curl -s -X POST localhost:8080/api/v1/policies \
  -H 'Content-Type: application/json' \
  -d "{
    \"name\": \"clear-on-recovery\",
    \"enabled\": true,
    \"condition\": {\"metric\": \"up\", \"operator\": \"eq\", \"threshold\": 1},
    \"action\": {
      \"action_type\": \"clear_rules\",
      \"push_rules\": {\"service\": \"checkout\"}
    },
    \"cooldown\": 30000000000
  }"
# SDK poll should show empty rules on checkout within 10s
```

---

## Key Constraints

- **No external Go dependencies** — engine uses only packages already in `go.mod`.
- **Prometheus is optional** — if Prometheus is unreachable, `QueryInstant` returns
  an error. The engine logs a warning and skips that rule for this tick. It does
  not crash manteion.
- **Cooldown is in-memory** — `lastFiredAt` is not persisted to the DB. If manteion
  restarts, rules may fire immediately once before the cooldown window is reestablished.
  This is acceptable for the current scope.
- **`clear_rules` uses `PushRules(nil)`** — not `ClearFault`. The distinction:
  `ClearFault` removes a one-shot fault injection; pushing a nil/empty rule set
  replaces the rule evaluator's rule list with nothing, which is what we want here.
- **`"attack"` and `"cachebox_mode_change"` action types** are logged and skipped —
  they are handled by zeus/Archer, not manteion.
