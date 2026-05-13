# Manually-Triggered Persistent Faults — Implementation Plan (V5)

## Architecture

Manteion holds the authoritative deadline. The SDK maintains live faults via continuous reconciliation against Manteion's poll/register responses — no `ExpiresAt` wire field, no clock-skew dependency. A heartbeat watchdog auto-drops faults if the SDK loses contact with Manteion past a grace period. The Reaper is a DB-cleanup safety net.

```
UI / API Client
      │
      ▼
Manteion API ──► PostgreSQL (fault_configs table)
      │          fired_at + duration_ms ⇒ deadline (server-side only)
      │
      ▼
atrocontrol.InjectFault(req)           ── slot-aware, single path for
      │                                   both manual and experiment uses
      ├──► ServiceIntent.ActiveFaults[req.Category] = req
      └──► fanout(POST /admin/fault) to all live SDKs

SDK loop:
   poll/register → response includes ActiveFaults with DurationMs
                   recomputed-as-remaining server-side
   reconcile     → upsert slots in response; DROP local slots not in
                   response; bump LastConfirmedAt for matched slots
   watchdog      → if (now - LastConfirmedAt) > grace, drop slot

Reaper (every 5s): scans for active rows past their deadline
   → marks DB row 'completed' (purely a cleanup pass; SDKs already
     self-expired via DurationMs timer or watchdog reconciliation)
```

### Why DurationMs (and not ExpiresAt)

Earlier drafts sent an absolute `ExpiresAt` timestamp so late-joining pods could compute `time.Until(expires)`. Two problems:

1. **Clock skew.** Wall-clock comparison between Manteion and each SDK pod is only as good as NTP sync. A monotonic-clock-from-now (`DurationMs`) avoids this entirely.
2. **Redundant with reconciliation.** Manteion already computes `remaining = expires - now()` at every register/poll response. Sending that as the SDK's `DurationMs` is exactly equivalent for the late-joiner case, without exposing a wall-clock contract.

So the SDK now treats every register/poll response's `DurationMs` as authoritative: start (or restart) the local timer at the value Manteion just provided. Reconciliation is the cancel-propagation channel; the watchdog is the safety net for `duration_ms = 0` (infinite) faults when Manteion crashes.

### One slot per category (D, J)

The DemoEvaluator holds at most one fault per category (`inline`, `network`, `resource`). Per-request decision picks the first matching category in priority order `inline > network > resource`. The `Type` field after the colon is metadata; it does not key the slot. **Specificity must live in the rule's request-match conditions**, not in slot keying.

Trade-off: you cannot arm two faults of the same category simultaneously on the same service (e.g., `network:latency` on one path and `network:retransmit_delay` on another). To express that, define a `FaultComposition` referencing both atomic faults; the composition is resolved at fire time and emitted as one wire FaultRequest whose internal logic the SDK evaluates per-request. True chained-effects-within-a-single-decision (latency THEN error on the same response) is V6 work.

---

## Data Model

### `fault_configs` table (migration 12)

```sql
CREATE TABLE IF NOT EXISTS fault_configs (
    id                   TEXT        PRIMARY KEY,
    name                 TEXT        NOT NULL,
    description          TEXT        NOT NULL DEFAULT '',
    service              TEXT        NOT NULL,
    category             TEXT        NOT NULL CHECK (category IN ('inline','network','resource')),
    fault_type           TEXT        NOT NULL,  -- see model.validFaultTypes (includes 'disk')
    fault_request        JSONB,                  -- atomic fault; nullable when composition_id is set
    fault_composition_id TEXT        NULL REFERENCES fault_compositions(id) ON DELETE SET NULL,
    duration_ms          BIGINT      NOT NULL DEFAULT 0,  -- 0 = infinite (whitelisted types only; watchdog enforces)
    experiment_run_id    TEXT        NULL REFERENCES experiment_runs(id) ON DELETE SET NULL,
    status               TEXT        NOT NULL DEFAULT 'ready'
                         CHECK (status IN ('ready','active','completed','manually_cancelled','failed')),
    -- NOTE: 'scheduled' status reserved for V6 (fire-at-T scheduling); not implemented here.
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    fired_at             TIMESTAMPTZ,
    completed_at         TIMESTAMPTZ,
    CONSTRAINT fault_request_or_composition CHECK (
        (fault_request IS NOT NULL) OR (fault_composition_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_fault_configs_service
    ON fault_configs(service);
CREATE INDEX IF NOT EXISTS idx_fault_configs_status
    ON fault_configs(status);
CREATE INDEX IF NOT EXISTS idx_fault_configs_experiment_run
    ON fault_configs(experiment_run_id) WHERE experiment_run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fault_configs_reaper
    ON fault_configs(fired_at) WHERE status = 'active' AND duration_ms > 0;

-- One armed fault per (service, category). Slot key is category-only;
-- fault_type is metadata, not part of the conflict.
CREATE UNIQUE INDEX IF NOT EXISTS idx_fault_conflict
    ON fault_configs(service, category) WHERE status = 'active';
```

Notes:

- **`duration_ms = 0`** is allowed only for fault types in the operator-configured infinite-allowed whitelist. High-risk types (`inline:hang`, `network:blackhole`, `network:rst`) default to deny because they're DOS-shaped without a timer.
- **`experiment_run_id`** ties a fault back to an owning experiment run, NULL for manually-fired faults. Forward-compat for unification: when the experiment path migrates to use `fault_configs`, this column becomes the audit trail for "show all faults for run X."
- **`fault_composition_id`** allows a FaultConfig to reference an existing FaultComposition. At fire time, the composition is resolved via existing `ruleconv` machinery into a wire FaultRequest. Mutually exclusive with `fault_request` (CHECK constraint requires exactly one).
- **Denormalization caveat**: `service`, `category`, `fault_type` are stored both as columns (for indexable queries) and inside `fault_request` JSON (when present). Update paths must keep them aligned; consider Postgres generated columns in a follow-up if this becomes a bug source.

### Status state machine

```
ready ──fire──► active ──reaper / SDK timer / watchdog─► completed
  ▲                  └──cancel───────────────────────► manually_cancelled
  │                  └──all pods dead─────────────────► failed
  └── re-fire from: completed | manually_cancelled | failed
```

---

## Files Changed — Complete Map

### `atropos-go`

| File | Action | Summary |
|------|--------|---------|
| `admin.go` (FaultRequest) | Modify | Trim to `{Category, Type, DurationMs, Config}` (H) |
| `admin.go` (DemoEvaluator) | Modify | One slot per category; `Set` replaces in-category; `Confirm` bumps `LastConfirmedAt` |
| `admin.go` (HTTP) | Modify | `DELETE /admin/fault/{category}` (single segment) |
| `register.go` (RegisterResponse) | Modify | Drop singular `ActiveFault`; only `ActiveFaults []FaultRequest` |
| `register.go` (Apply) | Modify | Reconciliation: upsert slots in response, drop slots not in response |
| `register.go` (watchdog) | New | Goroutine drops slots with stale `LastConfirmedAt` |
| `manteion.go` | Modify | Wire watchdog goroutine alongside existing `pollLoopWithTrigger` |

### `manteion-go`

| File | Action | Summary |
|------|--------|---------|
| `internal/model/fault_config.go` | New | `FaultConfig` type incl. `ExperimentRunID`, `FaultCompositionID` |
| `internal/model/fault_config_test.go` | New | Table-driven model tests |
| `internal/db/migrations.go` | Modify | Append migration 12 |
| `internal/store/fault_config_repo.go` | New | Postgres repository |
| `internal/atropos/client_fault.go` | Modify | `DELETE /admin/fault/{category}` (single segment) |
| `internal/atrocontrol/fault.go` | Modify | `InjectFault` / `ClearFault` are slot-aware (no separate `*Slot` methods) |
| `internal/atrocontrol/intent.go` | Modify | `ServiceIntent.ActiveFaults map[string]*FaultRequest` keyed by category |
| `internal/api/fault_config_store.go` | New | Interface |
| `internal/api/fault_config_handler.go` | New | 7 HTTP handlers |
| `internal/api/server.go` | Modify | New fields and 7 routes |
| `internal/api/sdk_handler.go` | Modify | Register response computes remaining `duration_ms` per fault |
| `internal/reaper/reaper.go` | New | Reaper worker (5s cadence) |
| `cmd/manteion/main.go` | Modify | Wire repo, ctrl, Reaper |

---

## Per-File Change Detail

---

### `atropos-go` — FaultRequest field cleanup (H)

Today `FaultRequest` carries inline-fault-specific top-level fields (`Delay`, `Jitter`, `StatusCode`, `Message`) alongside a generic `Config` blob. That made the type a per-fault-type union by accident. Trim to the minimum surface; per-type config moves into `Config`.

```go
type FaultRequest struct {
    Category   string          `json:"category"`     // inline | network | resource
    Type       string          `json:"type"`         // see manteion model.validFaultTypes
    DurationMs int64           `json:"duration_ms"`  // 0 = infinite (whitelist enforced server-side)
    Config     json.RawMessage `json:"config,omitempty"` // type-specific JSON
}
```

Per-type `Config` schemas:

| `Category:Type` | `Config` shape |
|---|---|
| `inline:latency` | `{"delay":"300ms","jitter":"50ms"}` |
| `inline:error` | `{"status_code":500,"message":"injected"}` |
| `inline:hang` | `{}` (uses DurationMs only) |
| `network:latency` | `{"delay":"300ms","jitter":"50ms"}` |
| `network:throttle` | `{"rate_kbps":1024}` |
| `network:retransmit_delay` | `{"rate":0.1,"delay":"100ms"}` |
| `network:drip` | `{"bytes":64,"interval":"100ms"}` |
| `network:rst` / `network:blackhole` | `{}` |
| `resource:cpu/memory/io/disk` | `{"percent":80}` |

Migration cost: every fault constructor in atropos (`NewLatencyFault`, `NewErrorFault`, network `decodeNetworkToxic`, resource `decodeResourceFault`) reads from structured `Config` instead of top-level fields. Pre-v1 posture lets us rename in one PR; no deprecation.

---

### `atropos-go` — DemoEvaluator (D, J)

```go
type faultSlot struct {
    decision        *Decision
    req             *FaultRequest
    lastConfirmedAt time.Time
}

type DemoEvaluator struct {
    mu    sync.RWMutex
    slots map[string]*faultSlot  // key = Category (one slot per category)
}

// Set installs or replaces the slot for req.Category.
func (e *DemoEvaluator) Set(decision *Decision, req *FaultRequest) {
    e.mu.Lock()
    defer e.mu.Unlock()
    if e.slots == nil {
        e.slots = make(map[string]*faultSlot)
    }
    e.slots[req.Category] = &faultSlot{
        decision:        decision,
        req:             req,
        lastConfirmedAt: time.Now(),
    }
}

func (e *DemoEvaluator) ClearSlot(category string) {
    e.mu.Lock()
    defer e.mu.Unlock()
    delete(e.slots, category)
}

func (e *DemoEvaluator) Clear() {
    e.mu.Lock()
    defer e.mu.Unlock()
    e.slots = make(map[string]*faultSlot)
}

// Confirm bumps lastConfirmedAt to now. Called for every slot present in
// a fresh poll/register response.
func (e *DemoEvaluator) Confirm(category string) {
    e.mu.Lock()
    defer e.mu.Unlock()
    if s, ok := e.slots[category]; ok {
        s.lastConfirmedAt = time.Now()
    }
}

// ActiveCategories returns the categories of all currently-armed slots.
// Used by Apply() to compute which slots need to be dropped after reconciliation.
func (e *DemoEvaluator) ActiveCategories() []string {
    e.mu.RLock()
    defer e.mu.RUnlock()
    out := make([]string, 0, len(e.slots))
    for cat := range e.slots {
        out = append(out, cat)
    }
    return out
}

// StaleSlots returns categories whose lastConfirmedAt is older than maxAge.
// Used by the watchdog goroutine.
func (e *DemoEvaluator) StaleSlots(maxAge time.Duration) []string {
    e.mu.RLock()
    defer e.mu.RUnlock()
    cutoff := time.Now().Add(-maxAge)
    var stale []string
    for cat, s := range e.slots {
        if s.lastConfirmedAt.Before(cutoff) {
            stale = append(stale, cat)
        }
    }
    return stale
}

// Evaluate returns the first decision matching the request, in
// inline > network > resource priority. Specificity is the rule's job.
func (e *DemoEvaluator) Evaluate(r *http.Request) *Decision {
    e.mu.RLock()
    defer e.mu.RUnlock()
    for _, cat := range []string{"inline", "network", "resource"} {
        if slot, ok := e.slots[cat]; ok && slot.decision != nil {
            return slot.decision
        }
    }
    return nil
}
```

**Composed faults (J)**: per-request decision remains single. Manteion resolves a `FaultComposition` at fire time via `ruleconv` and emits one compiled FaultRequest. The SDK still sees a single FaultRequest per category slot; the composition's complexity is internal to `Config` and the runtime evaluator. Multi-fault-per-request execution (e.g., latency THEN error chained on the response) is V6.

---

### `atropos-go` — `register.go` reconciliation (G, #1)

```go
type RegisterResponse struct {
    Status       string         `json:"status"`
    Rules        []CompiledRule `json:"rules,omitempty"`
    ActiveFaults []FaultRequest `json:"active_faults,omitempty"`
    FreezeCfg    *DelayRequest  `json:"freeze_cfg,omitempty"`
}
```

(Note: singular `ActiveFault` is **removed**, not deprecated. Per fix F, no old SDK clients to support.)

```go
func Apply(resp *RegisterResponse, targets ApplyTargets) error {
    // ... existing rules + freeze handling unchanged ...

    if targets.DemoEval == nil {
        return nil
    }

    // Reconciliation: drop slots not in the response (cancel-propagation
    // path that previously relied on SSE alone and could miss pods).
    inResponse := make(map[string]bool, len(resp.ActiveFaults))
    for _, req := range resp.ActiveFaults {
        inResponse[req.Category] = true
    }
    for _, cat := range targets.DemoEval.ActiveCategories() {
        if !inResponse[cat] {
            targets.DemoEval.ClearSlot(cat)
        }
    }

    // Apply / refresh slots that ARE in the response.
    for _, req := range resp.ActiveFaults {
        if err := applyActiveFault(req, targets.DemoEval, targets.NetworkResolver); err != nil {
            return fmt.Errorf("apply active_faults[%s]: %w", req.Category, err)
        }
        targets.DemoEval.Confirm(req.Category)
    }
    return nil
}

func applyActiveFault(req FaultRequest, eval *DemoEvaluator, resolve NetworkResolver) error {
    // No ExpiresAt guard — DurationMs=0 means "infinite, watchdog-bounded";
    // DurationMs>0 means "self-expire after this many ms from now."
    f, err := buildFault(req, resolve)
    if err != nil {
        return err
    }
    eval.Set(&Decision{Fault: f}, &req)
    return nil
}
```

---

### `atropos-go` — heartbeat watchdog (new) (#1)

```go
// StartFaultWatchdog runs until ctx is cancelled. Every tick, drops any
// fault slot whose lastConfirmedAt is older than the grace period.
//
// Grace = max(3 * pollInterval, 30s). This is the only mechanism that
// protects against zombie faults when duration_ms = 0 (infinite) AND
// Manteion crashes.
func StartFaultWatchdog(ctx context.Context, eval *DemoEvaluator,
    pollInterval time.Duration, logger *slog.Logger) {

    grace := 3 * pollInterval
    if grace < 30*time.Second {
        grace = 30 * time.Second
    }
    t := time.NewTicker(1 * time.Second)
    defer t.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-t.C:
            for _, cat := range eval.StaleSlots(grace) {
                eval.ClearSlot(cat)
                logger.Warn("watchdog: dropped stale fault slot",
                    "category", cat, "grace", grace)
            }
        }
    }
}
```

Wired in `ConnectManteion` after `pollLoopWithTrigger` starts, sharing the same ctx.

---

### `atropos-go` — `admin.go` HTTP

```
POST   /admin/fault                  → eval.Set() (replaces in-category)
GET    /admin/fault                  → {"faults": [...]}
DELETE /admin/fault                  → eval.Clear() (all slots; backward compat)
DELETE /admin/fault/{category}       → eval.ClearSlot(category)
```

Path is single-segment `{category}` — slot key is category-only.

GET response shape (matching the trimmed `FaultRequest`):

```json
{
  "faults": [
    {
      "category": "network",
      "type": "latency",
      "duration_ms": 30000,
      "config": {"delay":"300ms","jitter":"50ms"},
      "last_confirmed_at": "2026-05-03T12:00:30Z"
    }
  ]
}
```

---

### `manteion-go` — `internal/atropos/client_fault.go`

Modify `DeleteFault` to accept a category path segment:

```go
// DeleteFault clears a single category's slot on one Atropos instance.
// DELETE {addr}/admin/fault/{category}
func (c *Client) DeleteFault(ctx context.Context, addr, category string) error {
    path := "/admin/fault/" + url.PathEscape(category)
    _, err := c.doExpectStatus(ctx, http.MethodDelete, addr+path, nil, http.StatusOK)
    return err
}

// DeleteAllFaults remains for the cluster-wide clear path.
func (c *Client) DeleteAllFaults(ctx context.Context, addr string) error {
    _, err := c.doExpectStatus(ctx, http.MethodDelete, addr+"/admin/fault", nil, http.StatusOK)
    return err
}
```

---

### `manteion-go` — `internal/atrocontrol/fault.go` (E)

Single slot-aware path. No `*Slot` variants. Existing experiment-path callers update to use these (they were already using `InjectFault` / `ClearFault` — only the storage and clear-by-category semantics change).

```go
// InjectFault fans out a fault to all instances and writes it into the
// service's intent slot keyed by category. Replaces any existing slot in
// the same category (last-write-wins). Used by both manual-fault firing
// and experiment runs.
func (c *Controller) InjectFault(
    ctx context.Context,
    service string,
    req atroposdk.FaultRequest,
    opts ...CallOption,
) (FanoutResult, error) {
    co := c.resolveCallOpts(opts)

    c.intent.SetFaultSlot(service, req.Category, &req)

    targets, err := c.resolveTargets(ctx, service, co.filter)
    if err != nil {
        return FanoutResult{}, err
    }

    result := fanout(ctx, targets, func(ctx context.Context, t target) error {
        _, err := c.tx.PostFault(ctx, t.address, req)
        return err
    }, co)

    c.logFanout("inject_fault", service, co.runID, result)
    return result, nil
}

// ClearFault fans out DELETE /admin/fault/{category} and clears the slot
// from intent.
func (c *Controller) ClearFault(
    ctx context.Context,
    service, category string,
    opts ...CallOption,
) (FanoutResult, error) {
    co := c.resolveCallOpts(opts)

    c.intent.ClearFaultSlot(service, category)

    targets, err := c.resolveTargets(ctx, service, co.filter)
    if err != nil {
        return FanoutResult{}, err
    }
    result := fanout(ctx, targets, func(ctx context.Context, t target) error {
        return c.tx.DeleteFault(ctx, t.address, category)
    }, co)
    c.logFanout("clear_fault", service, co.runID, result)
    return result, nil
}
```

`ClearFault` signature changes: was `(service)`, now `(service, category)`. Existing experiment-path callers that called `ClearFault(service)` need to either pass the category, or call a new `ClearAllFaults(service)` helper that iterates intent's `ActiveFaults` and clears each.

---

### `manteion-go` — `internal/atrocontrol/intent.go` (D, F)

```go
type ServiceIntent struct {
    Rules        []atroposdk.StaticRule
    ActiveFaults map[string]*atroposdk.FaultRequest // key = Category
    FreezeCfg    *atroposdk.DelayRequest
    AppliedAt    time.Time
    RunID        string
}

func (t *IntentTracker) SetFaultSlot(service, category string, req *atroposdk.FaultRequest) {
    t.mu.Lock()
    defer t.mu.Unlock()
    intent, ok := t.state[service]
    if !ok {
        intent = &ServiceIntent{}
        t.state[service] = intent
    }
    if intent.ActiveFaults == nil {
        intent.ActiveFaults = make(map[string]*atroposdk.FaultRequest)
    }
    intent.ActiveFaults[category] = req
    intent.AppliedAt = time.Now()
}

func (t *IntentTracker) ClearFaultSlot(service, category string) {
    t.mu.Lock()
    defer t.mu.Unlock()
    intent, ok := t.state[service]
    if !ok {
        return
    }
    delete(intent.ActiveFaults, category)
    intent.AppliedAt = time.Now()
}
```

The singular `ActiveFault *atroposdk.FaultRequest` field is **removed**. Per F, we assume no old SDK clients in the field.

---

### `manteion-go` — `internal/model/fault_config.go` (new) (#2, #3, J)

```go
package model

import (
    "encoding/json"
    "errors"
    "fmt"
    "time"
)

type FaultConfigStatus string

const (
    FaultConfigReady             FaultConfigStatus = "ready"
    FaultConfigActive            FaultConfigStatus = "active"
    FaultConfigCompleted         FaultConfigStatus = "completed"
    FaultConfigManuallyCancelled FaultConfigStatus = "manually_cancelled"
    FaultConfigFailed            FaultConfigStatus = "failed"
    // FaultConfigScheduled — reserved for V6 (fire-at-T scheduling); not used here.
)

type FaultConfig struct {
    ID                 string            `json:"id"`
    Name               string            `json:"name"`
    Description        string            `json:"description,omitempty"`
    Service            string            `json:"service"`
    Category           string            `json:"category"`
    FaultType          string            `json:"fault_type"`

    // Exactly one of FaultReq or FaultCompositionID must be set.
    FaultReq           json.RawMessage   `json:"fault_request,omitempty"`
    FaultCompositionID *string           `json:"fault_composition_id,omitempty"` // J

    DurationMs         int64             `json:"duration_ms"` // 0 = infinite (whitelisted types only)
    ExperimentRunID    *string           `json:"experiment_run_id,omitempty"`     // #2

    Status             FaultConfigStatus `json:"status"`
    CreatedAt          time.Time         `json:"created_at"`
    UpdatedAt          time.Time         `json:"updated_at"`
    FiredAt            *time.Time        `json:"fired_at,omitempty"`
    CompletedAt        *time.Time        `json:"completed_at,omitempty"`
}

func (f *FaultConfig) Validate() error {
    if f.ID == "" {
        return errors.New("fault config: id required")
    }
    if f.Name == "" {
        return errors.New("fault config: name required")
    }
    if f.Service == "" {
        return errors.New("fault config: service required")
    }
    types, ok := validFaultTypes[f.Category]
    if !ok {
        return fmt.Errorf("fault config: invalid category %q", f.Category)
    }
    valid := false
    for _, t := range types {
        if t == f.FaultType {
            valid = true
            break
        }
    }
    if !valid {
        return fmt.Errorf("fault config: invalid fault_type %q for category %q",
            f.FaultType, f.Category)
    }
    hasReq := len(f.FaultReq) > 0 && string(f.FaultReq) != "null"
    if hasReq == (f.FaultCompositionID != nil) {
        return errors.New("fault config: exactly one of fault_request or fault_composition_id required")
    }
    if f.DurationMs < 0 {
        return errors.New("fault config: duration_ms must be >= 0")
    }
    return nil
}

func (f *FaultConfig) CanFire() bool   { return f.Status != FaultConfigActive }
func (f *FaultConfig) CanEdit() bool   { return f.Status != FaultConfigActive }
func (f *FaultConfig) CanDelete() bool { return f.Status != FaultConfigActive }

// ConflictKey is the (category:type) tuple used as the human-facing slot
// identifier and as the lookup key for the infinite-allowed whitelist.
// The DB-level uniqueness is on (service, category) only.
func (f *FaultConfig) ConflictKey() string { return f.Category + ":" + f.FaultType }

// IsInfiniteAllowed returns true if duration_ms > 0, OR if duration_ms == 0
// and this fault type is in the operator-configured whitelist.
func (f *FaultConfig) IsInfiniteAllowed(allowlist map[string]bool) bool {
    if f.DurationMs > 0 {
        return true
    }
    return allowlist[f.ConflictKey()]
}

// validFaultTypes — atomic types implemented in atropos-go.
// Composed faults are referenced via FaultCompositionID instead.
var validFaultTypes = map[string][]string{
    "inline":   {"error", "hang", "latency"},
    "network":  {"blackhole", "drip", "latency", "retransmit_delay", "rst", "throttle"},
    "resource": {"cpu", "memory", "io", "disk"}, // 'disk' added (B fix)
}

// DefaultInfiniteAllowed — fault types safe to fire with duration_ms = 0.
// Operator config can override via a deployment-time map.
//
// Default-deny rationale:
//   inline:hang        — every matching request hangs forever; caller queue
//                        saturates in seconds.
//   network:blackhole  — same shape, network layer.
//   network:rst        — connection-reset storms; downstream retry
//                        amplification.
var DefaultInfiniteAllowed = map[string]bool{
    "inline:latency":   true,
    "inline:error":     true,
    "network:latency":  true,
    "network:throttle": true,
    "network:drip":     true,
    "network:retransmit_delay": true,
    "resource:cpu":     true,
    "resource:memory":  true,
    "resource:io":      true,
    "resource:disk":    true,
}
```

---

### `manteion-go` — `internal/store/fault_config_repo.go` (new)

```go
package store

import (
    "context"
    "manteion-go/internal/model"
)

type ListFaultConfigFilters struct {
    Service         string
    Status          model.FaultConfigStatus
    ExperimentRunID *string  // nil = no filter; "" = manual only (NULL); "x" = run-x only
}

type FaultConfigRepo struct{ db *pgxpool.Pool }

func NewFaultConfigRepo(db *pgxpool.Pool) *FaultConfigRepo

// Create — INSERT INTO fault_configs
func (r *FaultConfigRepo) Create(ctx context.Context, f *model.FaultConfig) error

// Get — SELECT * FROM fault_configs WHERE id=$1
func (r *FaultConfigRepo) Get(ctx context.Context, id string) (*model.FaultConfig, error)

// List — SELECT with optional WHERE on service / status / experiment_run_id,
//        ORDER BY created_at DESC
func (r *FaultConfigRepo) List(ctx context.Context, f ListFaultConfigFilters) ([]*model.FaultConfig, error)

// Update — UPDATE ... WHERE id=$1 AND status != 'active'
//          0 rows → ErrConflict
func (r *FaultConfigRepo) Update(ctx context.Context, f *model.FaultConfig) error

// Delete — DELETE WHERE id=$1 AND status != 'active'
//          distinguish ErrNotFound vs ErrConflict
func (r *FaultConfigRepo) Delete(ctx context.Context, id string) error

// MarkFired — UPDATE SET status='active', fired_at=now() WHERE id=$1 AND status != 'active'
//             0 rows → ErrConflict (concurrent fire won the race)
func (r *FaultConfigRepo) MarkFired(ctx context.Context, id string) error

// MarkCompleted, MarkManuallyCancelled, MarkFailed — same shape, idempotent on
// repeated calls (0 rows is not an error).
func (r *FaultConfigRepo) MarkCompleted(ctx context.Context, id string) error
func (r *FaultConfigRepo) MarkManuallyCancelled(ctx context.Context, id string) error
func (r *FaultConfigRepo) MarkFailed(ctx context.Context, id string) error

// HasActiveConflict — slot key is (service, category). fault_type is NOT in the key.
func (r *FaultConfigRepo) HasActiveConflict(ctx context.Context,
    service, category, excludeID string) (bool, error)

// ListExpired — SELECT * FROM fault_configs
//   WHERE status='active'
//     AND duration_ms > 0
//     AND fired_at + (duration_ms * INTERVAL '1 millisecond') <= now()
func (r *FaultConfigRepo) ListExpired(ctx context.Context) ([]*model.FaultConfig, error)
```

---

### `manteion-go` — `internal/api/fault_config_handler.go` (new)

#### Fire handler

```go
func (s *Server) handleFireFaultConfig(w http.ResponseWriter, r *http.Request) {
    id := r.PathValue("id")
    ctx := r.Context()

    cfg, err := s.faultConfigs.Get(ctx, id)
    if err != nil {
        if errors.Is(err, store.ErrNotFound) {
            writeError(w, http.StatusNotFound, "fault config not found")
            return
        }
        writeError(w, http.StatusInternalServerError, "get fault config failed")
        return
    }

    if !cfg.CanFire() {
        writeError(w, http.StatusConflict, "fault config is already active")
        return
    }

    // Same-category conflict on this service. fault_type is not part of the key.
    conflict, err := s.faultConfigs.HasActiveConflict(ctx, cfg.Service, cfg.Category, id)
    if err != nil {
        writeError(w, http.StatusInternalServerError, "conflict check failed")
        return
    }
    if conflict {
        writeError(w, http.StatusConflict, fmt.Sprintf(
            "a %s fault is already active on %s", cfg.Category, cfg.Service))
        return
    }

    // Infinite-allowed gate (#3). Only enforced when duration_ms == 0.
    if !cfg.IsInfiniteAllowed(s.infiniteAllowed) {
        writeError(w, http.StatusBadRequest, fmt.Sprintf(
            "fault type %q cannot be fired with duration_ms=0", cfg.ConflictKey()))
        return
    }

    // Build the wire FaultRequest. If composition referenced, resolve it.
    var req atroposdk.FaultRequest
    if cfg.FaultCompositionID != nil {
        req, err = s.ruleconv.CompileFaultComposition(ctx, *cfg.FaultCompositionID)
        if err != nil {
            writeError(w, http.StatusInternalServerError, "compose fault: "+err.Error())
            return
        }
    } else {
        if err := json.Unmarshal(cfg.FaultReq, &req); err != nil {
            writeError(w, http.StatusInternalServerError, "invalid stored fault_request")
            return
        }
    }
    req.DurationMs = cfg.DurationMs // 0 means infinite; SDK watchdog enforces

    // Single InjectFault path (E).
    result, err := s.ctrl.InjectFault(ctx, cfg.Service, req, atrocontrol.WithRunID(id))
    if err != nil {
        writeError(w, http.StatusInternalServerError, "fanout error: "+err.Error())
        return
    }

    if len(result.Targeted) > 0 && len(result.OK) == 0 {
        _ = s.faultConfigs.MarkFailed(ctx, id)
        s.broker.Broadcast(cfg.Service, sse.Event{
            Type: "fault_config_state_changed",
            Data: fmt.Sprintf(`{"id":%q,"status":"failed"}`, id),
        })
        writeJSON(w, http.StatusBadGateway, map[string]any{
            "error":  "all instances failed to receive the fault",
            "fanout": result,
        })
        return
    }

    if err := s.faultConfigs.MarkFired(ctx, id); err != nil {
        if errors.Is(err, store.ErrConflict) {
            writeError(w, http.StatusConflict, "concurrent fire request won the race")
            return
        }
        writeError(w, http.StatusInternalServerError, "mark fired failed")
        return
    }

    s.broker.Broadcast(cfg.Service, sse.Event{
        Type: "fault_config_state_changed",
        Data: fmt.Sprintf(`{"id":%q,"status":"active"}`, id),
    })
    writeJSON(w, http.StatusOK, map[string]any{
        "id":     id,
        "status": model.FaultConfigActive,
        "fanout": result,
    })
}
```

#### Cancel handler

```go
func (s *Server) handleCancelFaultConfig(w http.ResponseWriter, r *http.Request) {
    id := r.PathValue("id")
    ctx := r.Context()

    cfg, err := s.faultConfigs.Get(ctx, id)
    // ... 404 / 500 ...

    if cfg.Status != model.FaultConfigActive {
        writeError(w, http.StatusConflict, "fault config is not active")
        return
    }

    // Best-effort slot clear. Mark cancelled in DB regardless; SDK reconciles
    // on next poll (any pod that didn't get the DELETE will see it missing
    // from the next register response and drop it).
    result, _ := s.ctrl.ClearFault(ctx, cfg.Service, cfg.Category,
        atrocontrol.WithRunID(id))

    if err := s.faultConfigs.MarkManuallyCancelled(ctx, id); err != nil {
        writeError(w, http.StatusInternalServerError, "mark cancelled failed")
        return
    }

    s.broker.Broadcast(cfg.Service, sse.Event{
        Type: "fault_config_state_changed",
        Data: fmt.Sprintf(`{"id":%q,"status":"manually_cancelled"}`, id),
    })
    writeJSON(w, http.StatusOK, map[string]any{
        "id":     id,
        "status": model.FaultConfigManuallyCancelled,
        "fanout": result,
    })
}
```

#### Routes

```
POST   /api/v1/fault-configs                handleCreateFaultConfig
GET    /api/v1/fault-configs                handleListFaultConfigs   (?service= ?status= ?experiment_run_id=)
GET    /api/v1/fault-configs/{id}           handleGetFaultConfig
PUT    /api/v1/fault-configs/{id}           handleUpdateFaultConfig
DELETE /api/v1/fault-configs/{id}           handleDeleteFaultConfig
POST   /api/v1/fault-configs/{id}/fire      handleFireFaultConfig
POST   /api/v1/fault-configs/{id}/cancel    handleCancelFaultConfig
```

---

### `manteion-go` — `internal/api/sdk_handler.go`

In `handleRegister`, fold active fault state into the response. Manteion is authoritative on remaining time: every register recomputes `remaining_ms` per fault and emits it as `req.DurationMs`. SDK starts a fresh local timer.

```go
activeCfgs, err := s.faultConfigs.List(ctx, store.ListFaultConfigFilters{
    Service: registeredService,
    Status:  model.FaultConfigActive,
})
if err != nil {
    s.logger.Warn("register: failed to load active faults", "error", err)
    // non-fatal: pod registers without faults; reconciles on next poll
}

now := time.Now()
var activeFaults []atroposdk.FaultRequest
for _, cfg := range activeCfgs {
    var req atroposdk.FaultRequest
    if cfg.FaultCompositionID != nil {
        req, err = s.ruleconv.CompileFaultComposition(ctx, *cfg.FaultCompositionID)
        if err != nil {
            s.logger.Warn("register: compose fault failed",
                "id", cfg.ID, "error", err)
            continue
        }
    } else {
        if err := json.Unmarshal(cfg.FaultReq, &req); err != nil {
            s.logger.Warn("register: invalid fault_request",
                "id", cfg.ID, "error", err)
            continue
        }
    }

    if cfg.DurationMs > 0 && cfg.FiredAt != nil {
        deadline := cfg.FiredAt.Add(time.Duration(cfg.DurationMs) * time.Millisecond)
        remaining := deadline.Sub(now)
        if remaining <= 0 {
            continue // expired; Reaper will sweep the row
        }
        req.DurationMs = remaining.Milliseconds()
    }
    // Else: cfg.DurationMs == 0 → keep req.DurationMs == 0 (infinite, watchdog-bounded)

    activeFaults = append(activeFaults, req)
}

resp := atroposdk.RegisterResponse{
    Status:       "registered",
    Rules:        currentRules,
    ActiveFaults: activeFaults,
    FreezeCfg:    intent.FreezeCfg,
}
```

---

### `manteion-go` — `internal/reaper/reaper.go` (new)

5-second sweep cadence. Reaper's role with V5 architecture: pure DB cleanup. SDKs already self-expire via the `DurationMs` timer on each pod, and the watchdog handles SDK liveness. The Reaper exists so DB rows don't accumulate in `active` after their natural deadline.

```go
package reaper

import (
    "context"
    "fmt"
    "log/slog"
    "time"

    "manteion-go/internal/atrocontrol"
    "manteion-go/internal/model"
)

type ReaperStore interface {
    ListExpired(ctx context.Context) ([]*model.FaultConfig, error)
    MarkCompleted(ctx context.Context, id string) error
}

type SSEPublisher interface {
    Broadcast(service string, event any)
}

type Reaper struct {
    repo   ReaperStore
    ctrl   *atrocontrol.Controller
    broker SSEPublisher
    logger *slog.Logger
    tick   time.Duration
}

func New(repo ReaperStore, ctrl *atrocontrol.Controller,
    broker SSEPublisher, logger *slog.Logger, tick time.Duration) *Reaper {
    return &Reaper{repo: repo, ctrl: ctrl, broker: broker, logger: logger, tick: tick}
}

func (r *Reaper) Start(ctx context.Context) {
    r.sweep(ctx) // immediate sweep on startup catches post-crash orphans
    t := time.NewTicker(r.tick)
    defer t.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-t.C:
            r.sweep(ctx)
        }
    }
}

func (r *Reaper) sweep(ctx context.Context) {
    configs, err := r.repo.ListExpired(ctx)
    if err != nil {
        r.logger.Error("reaper: list expired failed", "error", err)
        return
    }
    for _, cfg := range configs {
        r.reapOne(ctx, cfg)
    }
}

func (r *Reaper) reapOne(ctx context.Context, cfg *model.FaultConfig) {
    // ClearFault is best-effort. For pods that self-expired via DurationMs
    // or watchdog, this is a no-op. For Manteion-recovered crash scenarios
    // it's the explicit cleanup.
    result, err := r.ctrl.ClearFault(ctx, cfg.Service, cfg.Category,
        atrocontrol.WithRunID(cfg.ID))
    if err != nil {
        r.logger.Warn("reaper: clear fault error", "id", cfg.ID, "error", err)
    }
    r.logger.Info("reaper: expired fault",
        "id", cfg.ID, "service", cfg.Service,
        "category", cfg.Category, "type", cfg.FaultType,
        "ok", len(result.OK), "failed", len(result.Failed),
    )

    if err := r.repo.MarkCompleted(ctx, cfg.ID); err != nil {
        r.logger.Error("reaper: mark completed failed", "id", cfg.ID, "error", err)
        return // leave row active; will retry next sweep
    }

    r.broker.Broadcast(cfg.Service, map[string]any{
        "type": "fault_config_state_changed",
        "data": fmt.Sprintf(`{"id":%q,"status":"completed"}`, cfg.ID),
    })
}
```

---

### `manteion-go` — `cmd/manteion/main.go`

```go
faultConfigRepo := store.NewFaultConfigRepo(dbPool)

ctrl := atrocontrol.New(atroposClient, instanceResolver,
    atrocontrol.WithDefaultTimeout(2*time.Second),
    atrocontrol.WithDefaultConcurrency(16),
)

r := reaper.New(faultConfigRepo, ctrl, broker, logger, 5*time.Second)
go r.Start(rootCtx)

server := api.NewServer(
    // ... existing params ...
    faultConfigRepo,
    ctrl,
    model.DefaultInfiniteAllowed, // operator config can override
)
```

---

## Error Handling Reference

| Scenario | Behaviour |
|----------|-----------|
| Fire when config is already active | 409 from `CanFire()` |
| Same-category already active on service | 409 from `HasActiveConflict()` (slot key is `(service, category)`) |
| Two concurrent fires race | Unique partial index on `(service, category) WHERE active` rejects the second `MarkFired` → 409 |
| `duration_ms = 0` for high-risk type | 400 from `IsInfiniteAllowed()` |
| All pods unreachable on fire | `MarkFailed`, 502 with fanout detail |
| Some pods fail on fire | Partial OK; missed pods reconcile on next register |
| Cancel SDK clear fails | DB marked `manually_cancelled` anyway; SDK reconciles on next poll (slot will be missing from `ActiveFaults`) |
| Pod misses cancel SSE | `Apply()` reconciliation drops the slot on next poll/register response |
| Pod joins after fault expired | Manteion's register response computes `remaining = 0` → fault not included → SDK never starts it |
| Manteion crashes mid-fault, `duration_ms > 0` | SDK runs to its local timer; row stays `active` in DB until Manteion recovers and Reaper sweeps |
| Manteion crashes mid-fault, `duration_ms = 0` | Watchdog drops fault locally at `3 × pollInterval` grace |
| Composed fault | `FaultCompositionID` resolved via `ruleconv.CompileFaultComposition` at fire time; emitted as one wire FaultRequest |
| Old SDK version | Not supported. Per fix F, we assume the cluster is uniformly on the new SDK |

---

## Overengineering removed (vs V4)

Documenting what got cut and why, for future readers re-tracing the design space:

- **`ExpiresAt` wire field**: removed. Wall-clock comparison was clock-skew-sensitive and redundant with reconciliation. Manteion sends recomputed `DurationMs` on every register; SDK's local monotonic timer is the source of truth.
- **`EffectiveDuration()` method**: gone with `ExpiresAt`.
- **Dual `ActiveFault` (singular) + `ActiveFaults` (plural) on `RegisterResponse`**: collapsed to plural only. No old SDKs to support.
- **`InjectFaultSlot` / `ClearFaultSlot` separate from `InjectFault` / `ClearFault`**: consolidated. One slot-aware path serves both manual-fault firing and experiment runs.
- **Multi-slot keying by `category:type`**: collapsed to category-only. One armed fault per category; specificity lives in rule match conditions. `FaultComposition` provides the escape hatch for combining same-category effects.
- **Reaper at 1 Hz**: 5 s. Watchdog handles SDK liveness; Reaper is purely DB cleanup.
- **`Apply()` additive-only behaviour**: now reconciles. Fixes the silent cancel-propagation bug where pods that missed an SSE cancel would hold the fault until restart.
- **`/admin/fault/{category}/{type}` two-segment DELETE**: collapsed to `/admin/fault/{category}`. Slot key is category-only.

---

## Open follow-ups (V6 candidates)

- **`scheduled` status** for fire-at-T scheduling. Status enum has the placeholder reserved but not wired.
- **Experiment-path migration to `fault_configs`**: today the experiment path uses `InjectFault` directly without writing a `fault_configs` row; in V6, experiments write rows tagged with `experiment_run_id` so the table is the single source of truth for "what's running right now."
- **Postgres generated columns** for `service`, `category`, `fault_type` instead of duplicated denormalized columns + JSONB. Reduces sync-burden risk.
- **Multi-fault-per-request execution** (e.g., latency THEN error chained on the same response). Today, a `FaultComposition` resolves to one wire FaultRequest whose `Config` carries the composition; multi-effect runtime evaluation in atropos is V6.
- **`FaultRequest.Config` per-type schema validation** at the API layer in Manteion. Today validation lives in atropos; pushing it earlier in the pipeline catches malformed configs before fanout.
