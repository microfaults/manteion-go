# Experiment Orchestration

> **Date:** 2026-04-28
> **Scope:** manteion-go — atropos side only (frontend calls zeus separately)
> **Go version:** 1.25 stdlib + pgx/v5 (already in go.mod)

---

## Context

This plan adds the **Experiment Orchestrator** to manteion. When a run starts,
it pushes phase 0 fault rules to atropos SDK instances and spawns a condition
watcher goroutine. When the transition condition holds for a configured window,
it advances to the next phase. When the final phase completes, it marks the run
finished and clears all rules.

---

## What Already Exists — Do Not Rebuild

| What | Where |
|---|---|
| `model.Experiment`, `model.ExperimentRun`, results types | `internal/model/experiment.go` |
| `store.ExperimentRepo` — full CRUD + results | `internal/store/experiment_repo.go` |
| `atrocontrol.Controller` — `PushRules`, `InjectFault`, `ClearFault` | `internal/atrocontrol/` |
| `atropos.Client` — HTTP calls to SDK admin endpoints | `internal/atropos/` |
| Rule compilation (FK resolution → wire format) | `internal/ruleconv/ruleconv.go` |

---

## Architecture Overview

```
Operator (curl / UI)
    │
    ▼
POST /api/v1/experiments/{id}/runs/{runId}/start
    │
    ▼
Orchestrator.StartRun()
    │  reads PhaseRules + TransitionCond from ExperimentRun
    │
    ├──► atrocontrol.PushRules(service, phase0Rules)   ← pushes to SDK /admin/rules
    │
    └──► background goroutine: watchPhase
              │  every 10s: query Prometheus
              │  if condition holds for Window duration: advance phase
              │
              └──► atrocontrol.PushRules(service, phase1Rules)
                   → when all phases done: mark run completed, clear rules
```

---

## Phase 1: Model Changes to `ExperimentRun`

**File:** `internal/model/experiment.go`

Experiment runs advance through phases when a metric condition holds for a
sustained window. Add three fields to `ExperimentRun`:

```go
// PhaseTransition defines when a run should advance to its next phase.
type PhaseTransition struct {
    Metric    string        `json:"metric"`    // PromQL query
    Operator  string        `json:"operator"`  // gt, gte, lt, lte, eq
    Threshold float64       `json:"threshold"`
    Window    time.Duration `json:"window_ns"` // condition must hold this long
}

// PhaseRuleSet is the fault rules active during one phase of a run.
type PhaseRuleSet struct {
    Phase       int      `json:"phase"`       // 0 = baseline, 1 = fault injection, etc.
    Description string   `json:"description"`
    RuleIDs     []string `json:"rule_ids"`    // IDs of rules to push when entering phase
}

// Add to ExperimentRun:
type ExperimentRun struct {
    // ... existing fields unchanged ...
    PhaseRules     []PhaseRuleSet   `json:"phase_rules,omitempty"`
    TransitionCond *PhaseTransition `json:"transition_condition,omitempty"`
    CurrentPhase   int              `json:"current_phase"`
}
```

`PhaseRules` and `TransitionCond` are stored as JSONB. `CurrentPhase` is a
plain integer column. Update `ExperimentRun.Validate()`:
- If `TransitionCond` is non-nil, validate `Metric` non-empty, `Operator` in
  `{gt, gte, lt, lte, eq}`, `Window > 0`.
- `PhaseRules` phases must be numbered sequentially from 0.

---

## Phase 2: DB Migration v5

**File:** `internal/db/migrations.go`

Append to the migrations slice:

```go
{5, "add experiment_run phase columns", `
    ALTER TABLE experiment_runs
        ADD COLUMN IF NOT EXISTS phase_rules        JSONB    NOT NULL DEFAULT '[]',
        ADD COLUMN IF NOT EXISTS transition_cond    JSONB,
        ADD COLUMN IF NOT EXISTS current_phase      INTEGER  NOT NULL DEFAULT 0;
`},
```

---

## Phase 3: Update `ExperimentRepo`

**File:** `internal/store/experiment_repo.go`

`CreateRun`, `GetRun`, and `ListRunsByExperiment` must include the three new
columns in their SQL and scan/marshal them:

```go
// In CreateRun INSERT:
phaseRulesJSON, _ := jsonbMarshal(run.PhaseRules)
transCondJSON, _  := jsonbMarshal(run.TransitionCond)
// append $12 = phaseRulesJSON, $13 = transCondJSON, $14 = run.CurrentPhase

// In GetRun / ListRunsByExperiment SELECT + Scan:
// add phase_rules, transition_cond, current_phase to column list and scan targets
```

Add a new method for phase advancement:

```go
// UpdateRunPhase atomically updates current_phase and status.
func (r *ExperimentRepo) UpdateRunPhase(ctx context.Context, id string, phase int, status string) error {
    _, err := r.db.ExecContext(ctx,
        `UPDATE experiment_runs SET current_phase = $2, status = $3 WHERE id = $1`,
        id, phase, status)
    return err
}
```

---

## Phase 4: Prometheus Client

**New file:** `internal/promql/client.go`

Simple stdlib-only HTTP client for Prometheus instant queries.

```go
package promql

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "net/url"
    "strconv"
    "strings"
    "time"
)

type Client struct {
    baseURL    string
    httpClient *http.Client
}

func NewClient(baseURL string) *Client {
    return &Client{
        baseURL:    strings.TrimRight(baseURL, "/"),
        httpClient: &http.Client{Timeout: 5 * time.Second},
    }
}

// QueryInstant executes a PromQL instant query and returns the scalar result.
// Returns an error if the query returns no data.
func (c *Client) QueryInstant(ctx context.Context, query string) (float64, error) {
    u := c.baseURL + "/api/v1/query?query=" + url.QueryEscape(query)
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
    if err != nil {
        return 0, err
    }
    resp, err := c.httpClient.Do(req)
    if err != nil {
        return 0, fmt.Errorf("prometheus query failed: %w", err)
    }
    defer resp.Body.Close()

    var result struct {
        Status string `json:"status"`
        Data   struct {
            Result []struct {
                Value [2]json.RawMessage `json:"value"` // [timestamp, value_string]
            } `json:"result"`
        } `json:"data"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return 0, fmt.Errorf("decode prometheus response: %w", err)
    }
    if result.Status != "success" {
        return 0, fmt.Errorf("prometheus returned status %q", result.Status)
    }
    if len(result.Data.Result) == 0 {
        return 0, fmt.Errorf("prometheus query %q returned no data", query)
    }

    var valStr string
    if err := json.Unmarshal(result.Data.Result[0].Value[1], &valStr); err != nil {
        return 0, err
    }
    return strconv.ParseFloat(valStr, 64)
}

// Healthy returns true if Prometheus is reachable.
func (c *Client) Healthy(ctx context.Context) bool {
    ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/-/healthy", nil)
    if err != nil {
        return false
    }
    resp, err := c.httpClient.Do(req)
    if err != nil {
        return false
    }
    resp.Body.Close()
    return resp.StatusCode == http.StatusOK
}
```

**Environment variable:** `PROMETHEUS_URL` (default `http://prometheus:9090`).

---

## Phase 5: Experiment Orchestrator

**New package:** `internal/orchestrator/`

### 5a. `internal/orchestrator/orchestrator.go`

```go
package orchestrator

import (
    "context"
    "fmt"
    "log/slog"
    "sync"
    "time"

    "manteion-go/internal/atrocontrol"
    "manteion-go/internal/model"
    "manteion-go/internal/promql"
    "manteion-go/internal/store"
)

type Orchestrator struct {
    experiments *store.ExperimentRepo
    rules       *store.RuleRepo
    faults      *store.FaultRepo
    controller  *atrocontrol.Controller
    prom        *promql.Client
    logger      *slog.Logger

    mu      sync.Mutex
    running map[string]context.CancelFunc // run ID → cancel watcher
}

func New(
    experiments *store.ExperimentRepo,
    rules *store.RuleRepo,
    faults *store.FaultRepo,
    controller *atrocontrol.Controller,
    prom *promql.Client,
    logger *slog.Logger,
) *Orchestrator {
    return &Orchestrator{
        experiments: experiments,
        rules:       rules,
        faults:      faults,
        controller:  controller,
        prom:        prom,
        logger:      logger,
        running:     make(map[string]context.CancelFunc),
    }
}

// StartRun transitions a run from "pending" to "running" and begins
// phase execution. Returns error if the run is not in "pending" state.
func (o *Orchestrator) StartRun(ctx context.Context, runID string) error {
    run, err := o.experiments.GetRun(ctx, runID)
    if err != nil {
        return err
    }
    if run.Status != "pending" {
        return fmt.Errorf("run %q is not pending (status=%s)", runID, run.Status)
    }

    if err := o.enterPhase(ctx, run, 0); err != nil {
        return fmt.Errorf("enter phase 0: %w", err)
    }

    now := time.Now()
    run.StartedAt = &now
    if err := o.experiments.UpdateRunStatus(ctx, runID, "running"); err != nil {
        return err
    }

    if len(run.PhaseRules) > 1 && run.TransitionCond != nil {
        watchCtx, cancel := context.WithCancel(context.Background())
        o.mu.Lock()
        o.running[runID] = cancel
        o.mu.Unlock()
        go o.watchPhase(watchCtx, run)
    }

    o.logger.Info("orchestrator: run started", "run_id", runID, "phase", 0)
    return nil
}

// StopRun halts an active run, clears rules from SDK instances, and
// marks the run with the given status ("completed" or "failed").
func (o *Orchestrator) StopRun(ctx context.Context, runID string, status string) error {
    o.mu.Lock()
    cancel, active := o.running[runID]
    if active {
        cancel()
        delete(o.running, runID)
    }
    o.mu.Unlock()

    run, err := o.experiments.GetRun(ctx, runID)
    if err != nil {
        return err
    }

    for _, svc := range o.collectServices(run) {
        if _, err := o.controller.PushRules(ctx, svc, nil); err != nil {
            o.logger.Warn("orchestrator: clear rules failed on stop",
                "service", svc, "error", err)
        }
    }

    return o.experiments.UpdateRunStatus(ctx, runID, status)
}
```

### 5b. `internal/orchestrator/runner.go`

```go
package orchestrator

import (
    "context"
    "time"

    "manteion-go/internal/model"
)

const watchInterval = 10 * time.Second

func (o *Orchestrator) watchPhase(ctx context.Context, run *model.ExperimentRun) {
    cond := run.TransitionCond
    ticker := time.NewTicker(watchInterval)
    defer ticker.Stop()

    var condHeldSince time.Time
    condHeld := false

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            value, err := o.prom.QueryInstant(ctx, cond.Metric)
            if err != nil {
                o.logger.Warn("orchestrator: condition query failed",
                    "run_id", run.ID, "error", err)
                condHeld = false
                condHeldSince = time.Time{}
                continue
            }

            if conditionMet(value, cond.Operator, cond.Threshold) {
                if !condHeld {
                    condHeld = true
                    condHeldSince = time.Now()
                } else if time.Since(condHeldSince) >= cond.Window {
                    o.advancePhase(ctx, run)
                    return
                }
            } else {
                condHeld = false
                condHeldSince = time.Time{}
            }
        }
    }
}

func (o *Orchestrator) advancePhase(ctx context.Context, run *model.ExperimentRun) {
    nextPhase := run.CurrentPhase + 1
    o.logger.Info("orchestrator: advancing phase",
        "run_id", run.ID, "from", run.CurrentPhase, "to", nextPhase)

    if nextPhase >= len(run.PhaseRules) {
        o.StopRun(ctx, run.ID, "completed")
        return
    }

    if err := o.enterPhase(ctx, run, nextPhase); err != nil {
        o.logger.Error("orchestrator: enter phase failed",
            "run_id", run.ID, "phase", nextPhase, "error", err)
        o.StopRun(ctx, run.ID, "failed")
        return
    }

    run.CurrentPhase = nextPhase
    o.experiments.UpdateRunPhase(ctx, run.ID, nextPhase, "running")

    if nextPhase+1 < len(run.PhaseRules) && run.TransitionCond != nil {
        watchCtx, cancel := context.WithCancel(context.Background())
        o.mu.Lock()
        o.running[run.ID] = cancel
        o.mu.Unlock()
        go o.watchPhase(watchCtx, run)
    }
}

// enterPhase pushes the rules for the given phase to all target services.
// An empty RuleIDs list pushes nil, clearing any previous rules on those services.
func (o *Orchestrator) enterPhase(ctx context.Context, run *model.ExperimentRun, phase int) error {
    var ruleIDs []string
    if phase < len(run.PhaseRules) {
        ruleIDs = run.PhaseRules[phase].RuleIDs
    }

    compiled, err := o.loadCompiledRules(ctx, ruleIDs)
    if err != nil {
        return err
    }

    for _, svc := range o.collectServices(run) {
        if _, err := o.controller.PushRules(ctx, svc, compiled); err != nil {
            o.logger.Warn("orchestrator: push rules failed",
                "service", svc, "phase", phase, "error", err)
        }
    }
    return nil
}

func (o *Orchestrator) collectServices(run *model.ExperimentRun) []string {
    seen := make(map[string]struct{})
    var svcs []string
    for _, fs := range run.FrozenServices {
        if _, ok := seen[fs.Service]; !ok {
            seen[fs.Service] = struct{}{}
            svcs = append(svcs, fs.Service)
        }
    }
    return svcs
}

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

### 5c. `internal/orchestrator/rules.go`

```go
package orchestrator

import (
    "context"

    atroposdk "atropos-go"
    "manteion-go/internal/ruleconv"
)

// loadCompiledRules fetches rules by ID, resolves fault specs via ruleconv,
// and returns StaticRules ready for atrocontrol.PushRules.
// An empty or nil ruleIDs slice returns nil (clears rules on the target service).
func (o *Orchestrator) loadCompiledRules(ctx context.Context, ruleIDs []string) ([]atroposdk.StaticRule, error) {
    if len(ruleIDs) == 0 {
        return nil, nil
    }
    // Fetch each rule from DB, collect into slice, then compile via ruleconv.
    // Pattern: loop ruleIDs → o.rules.Get → append to []model.Rule
    // Then: ruleconv.CompileRules(rules, specResolver, compResolver)
    // Then: decode compiled rules into []atroposdk.StaticRule
    // (see internal/api/sdk_handler.go handlePollRules for the existing pattern)
    panic("implement me")
}
```

---

## Phase 6: Experiment API Handlers

**New file:** `internal/api/experiment_handler.go`

Routes to add to `server.go`:

```
POST   /api/v1/experiments                               create experiment
GET    /api/v1/experiments                               list experiments
GET    /api/v1/experiments/{id}                          get experiment
DELETE /api/v1/experiments/{id}                          delete experiment

POST   /api/v1/experiments/{id}/runs                     create run (status=pending)
GET    /api/v1/experiments/{id}/runs                     list runs for experiment
GET    /api/v1/experiments/{id}/runs/{runId}             get run
POST   /api/v1/experiments/{id}/runs/{runId}/start       → orchestrator.StartRun
POST   /api/v1/experiments/{id}/runs/{runId}/stop        → orchestrator.StopRun
```

**`handleCreateRun`** — creates an `ExperimentRun` with `status=pending`.
Before inserting, verify that each `RuleID` in every `PhaseRuleSet.RuleIDs`
exists in the `rules` table (handler-level check, not a DB FK).

**`handleStartRun`** — calls `orchestrator.StartRun(ctx, runID)`. Returns 409
Conflict if the run is not pending.

**`handleStopRun`** — calls `orchestrator.StopRun(ctx, runID, "completed")`.

**Update `internal/api/server.go`:** add `orch *orchestrator.Orchestrator`
field to `Server`, add it as a parameter to `NewServer`, register the new
routes in `routes()`.

---

## Phase 7: Wire in `cmd/manteion/main.go`

```go
// New env var
prometheusURL := envOr("PROMETHEUS_URL", "http://prometheus:9090")

// New clients + components
promClient := promql.NewClient(prometheusURL)

orch := orchestrator.New(experimentRepo, ruleRepo, faultRepo, controller, promClient, logger)

// Pass orchestrator into API server
srv := api.NewServer(...existing args..., orch)
```

---

## Implementation Order

Execute in this order — each step compiles independently before the next:

- [ ] **Step 1** — `internal/model/experiment.go`: add `PhaseRuleSet`, `PhaseTransition`, and three new fields to `ExperimentRun`. Update `Validate()`. `go build ./internal/model/...`
- [ ] **Step 2** — `internal/db/migrations.go`: append migration v5. `go build ./internal/db/...`
- [ ] **Step 3** — `internal/store/experiment_repo.go`: update `CreateRun`, `GetRun`, `ListRunsByExperiment` SQL + scan. Add `UpdateRunPhase`. `go build ./internal/store/...`
- [ ] **Step 4** — `internal/promql/client.go`: implement `NewClient`, `QueryInstant`, `Healthy`. `go build ./internal/promql/...`
- [ ] **Step 5** — `internal/orchestrator/orchestrator.go` + `runner.go` + `rules.go`: implement all methods. `go build ./internal/orchestrator/...`
- [ ] **Step 6** — `internal/api/experiment_handler.go`: CRUD + start/stop handlers. Update `server.go` routes. `go build ./internal/api/...`
- [ ] **Step 7** — `cmd/manteion/main.go`: add `PROMETHEUS_URL`, wire `promql.Client` and `orchestrator.Orchestrator`. `go build ./cmd/manteion`
- [ ] **Step 8** — `go build ./...` + `go vet ./...` + `go test ./...`

---

## Testing

### Unit Tests — `internal/orchestrator/orchestrator_test.go`

Use `httptest.NewServer` to fake Prometheus and a mock `atrocontrol.Controller`.

```go
// Test: StartRun pushes phase 0 rules immediately
// Test: watchPhase advances when condition holds for full window
// Test: watchPhase resets window when condition drops mid-window
// Test: StopRun pushes nil rules to all services + marks run completed
// Test: final phase completion → run marked completed automatically
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

**2. Create experiment + two-phase run:**
```bash
EXP=$(curl -s -X POST localhost:8080/api/v1/experiments \
  -H 'Content-Type: application/json' \
  -d '{"name":"checkout-attribution","primary_workload_id":"wl-placeholder","status":"planned"}')
EXP_ID=$(echo $EXP | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

RUN=$(curl -s -X POST localhost:8080/api/v1/experiments/$EXP_ID/runs \
  -H 'Content-Type: application/json' \
  -d "{
    \"run_type\": \"isolation\",
    \"run_index\": 0,
    \"status\": \"pending\",
    \"phase_rules\": [
      {\"phase\": 0, \"description\": \"baseline\", \"rule_ids\": []},
      {\"phase\": 1, \"description\": \"fault injection\", \"rule_ids\": [\"$RULE_ID\"]}
    ],
    \"transition_condition\": {
      \"metric\": \"up\",
      \"operator\": \"eq\",
      \"threshold\": 1,
      \"window_ns\": 15000000000
    }
  }")
RUN_ID=$(echo $RUN | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
```

**3. Start the run:**
```bash
curl -s -X POST localhost:8080/api/v1/experiments/$EXP_ID/runs/$RUN_ID/start
```

**4. Watch for phase advance (condition holds for 15s → phase 1 rules pushed):**
```bash
# Watch manteion logs for: "orchestrator: advancing phase"
# Poll SDK rules to see phase 1 fault appear:
watch -n 5 'curl -s "localhost:8080/api/v1/sdk/rules?service=checkout&version=0" | python3 -m json.tool'
```

**5. Stop the run:**
```bash
curl -s -X POST localhost:8080/api/v1/experiments/$EXP_ID/runs/$RUN_ID/stop
# Rules should be cleared — SDK poll returns empty rules
```

---

## Key Constraints

- **No external Go dependencies** — promql client is pure stdlib.
- **Phase 0 with empty `rule_ids`** is valid — calls `PushRules` with `nil`,
  effectively clearing any previous rules on the service (baseline = clean state).
- **`PushRules(ctx, svc, nil)`** is the correct way to clear rules from SDK
  instances — it posts an empty rule set via `POST /admin/rules []`.
- **Cache-box is out of scope** — `FrozenServices` can be populated but the
  orchestrator does not call `atrocontrol.FreezeService` yet.
- **Prometheus is optional** — if `PROMETHEUS_URL` is unset or unreachable,
  condition watcher logs a warning and resets its window. It does not crash manteion.
