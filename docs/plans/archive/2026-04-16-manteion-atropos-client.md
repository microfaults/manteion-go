# Manteion → Atropos Admin Client: Design Doc

> **Status:** Implemented — retro-spec. Core packages landed in `feat/inital-setup` commit `f2e579c` (Apr 17); composition support in `5d68f63` (Apr 19). Atropos Option X is also done: `FaultRequest`, `FaultStatus`, `DelayRequest` are exported. Remaining gaps (atropos-side apply of register response, fault CRUD handlers, audit sink) are tracked in a separate follow-up plan.
> **Spans repos:** `manteion-go` (new packages) + `atropos-go` (additive exports).

**Goal:** Give manteion a clean, testable, refactor-resistant way to drive atropos SDK instances over HTTP — for experiment orchestration today, and for rule pushes tomorrow — without coupling orchestration logic to transport details and without pretending atropos has capabilities it doesn't.

---

## Context

Atropos-go exposes three HTTP admin handlers today:

| Endpoint | Purpose |
|---|---|
| `/admin/fault` (POST/GET/DELETE) | Activate/inspect/clear an injected fault on one instance |
| `/admin/rules` (POST/GET) | Replace/list the `StaticEvaluator`'s rule set |
| `/admin/cachebox` (GET, DELETE) + `/admin/cachebox/delay` (POST) | Cache-box stats, clear, configure delay source |

Each handler is mounted by the hosting service on whatever mux it uses. Each instance registers with manteion via `POST /api/v1/sdk/register` and renews via `POST /api/v1/sdk/poll`, so manteion already knows every instance's `service`, `instance_id`, and `address`.

What's missing is a manteion-side client. Today any code path that wants to drive atropos would have to hand-roll HTTP calls and address lookups inline. The first real consumer is the experiment orchestrator (not yet built), which needs to apply/clear faults and cache-box freezes across fleets of instances, repeatedly, across experiment phases. A second consumer is the in-parallel admin UI, which needs CRUD-style buttons ("force-apply rule X to service Y," "freeze productcatalog," "status of all faults on frontend").

This doc specifies the client design. Implementation will follow in a separate plan doc.

---

## Goals and non-goals

**Goals:**

1. Single source of truth for "how manteion talks to atropos." No inline HTTP anywhere else in manteion.
2. Transport layer and orchestration layer cleanly separated, each independently testable.
3. Client speaks only atropos-native types for fault/cachebox admin. For rules, manteion produces a JSON-safe `CompiledRule` wire format (atropos's native `StaticRule` contains a `fault.Fault` interface value that can't JSON-serialize and uses int enums) — the SDK decodes `CompiledRule` locally and builds its own `StaticRule` list. Conversion happens upstream of any I/O.
4. Fan-out addressing is the default (apply to all alive instances of a service); per-instance targeting is available for surgical operations.
5. Partial-success semantics that the orchestrator can act on.
6. Minimal-first handling of rolling deploys — new pods that register mid-experiment auto-join the current fault state.
7. Scaffolding for rule-push without replacing the existing poll model. The push path is real and tested; the wire-up from rule mutations is left dormant.
8. Addresses never appear as config literals anywhere in manteion. Self-registration + registry lookup is the only discovery path.

**Non-goals (deferred, each has a pointer to a follow-up plan section):**

- Persistent audit log of admin actions (design sketched, implementation deferred).
- Replacing poll with push (client supports push; call-sites dormant).
- Reconciliation goroutine that periodically re-asserts intent to all instances (not needed for MVP).
- Experiment orchestrator itself — this doc is about the client it will use, not the orchestrator.
- Policy engine integration — policies are stored but not evaluated; separate plan.
- DSB / polyglot support via sidecar proxy — acknowledged as future path; no structural accommodation here.

---

## Atropos-go change (additive, minimal)

Atropos's admin handlers are callable today, but three of the wire structs that shape their bodies are unexported. Any Go client has to re-declare them locally, creating drift risk the moment atropos adds a field.

**Change:** export three types in `atropos-go` root package.

| Before | After | File |
|---|---|---|
| `type faultRequest struct { ... }` | `type FaultRequest struct { ... }` | `admin.go` |
| `type faultStatus struct { ... }` | `type FaultStatus struct { ... }` | `admin.go` |
| `type delayRequest struct { ... }` | `type DelayRequest struct { ... }` | `cachebox_admin.go` |

Zero behavior change. Internal references updated in the same commit. No new files, no new subpackages, no API semantics change — these types *are* the public HTTP API shape, and giving them Go names makes atropos's admin surface consumable by Go clients without ad-hoc redeclaration. Tests in atropos already use the type literals via JSON; they need cosmetic renames only.

This lands in `atropos-go` before manteion-side work begins. It's a ~30-line diff landed in isolation on its own branch.

---

## Manteion package structure

Two new packages plus one pure-conversion package:

```
internal/atropos/                     TRANSPORT — thin wrapper over atropos HTTP admin
  client.go          Client, NewClient(httpClient, opts...), option types
  client_fault.go    PostFault / GetFault / DeleteFault
  client_rules.go    PostRules / GetRules
  client_cachebox.go PostCacheBoxDelay / GetCacheBoxStats / ClearCacheBox
  errors.go          HTTPError, TransportError
  client_test.go     httptest.Server fixtures

internal/atrocontrol/                 ORCHESTRATION — fan-out, intent, per-service policy
  controller.go      Controller; wires *atropos.Client + InstanceResolver + IntentTracker
  fanout.go          Generic fan-out primitive: targets → FanoutResult
  freeze.go          FreezeService, ClearService
  fault.go           InjectFault, ClearFault
  rules.go           PushRules (calls ruleconv.ToStaticRules, then transport PostRules per instance)
  status.go          StatusByService (fan-out GETs, aggregates)
  intent.go          IntentTracker + narrow IntentReader interface
  resolver.go        InstanceResolver interface + SDKRepo-backed default impl
  options.go         Call options: WithTimeout, WithRetry, WithConcurrency, WithInstanceFilter, WithRunID
  errors.go          FanoutResult, PerInstanceError
  controller_test.go Fake transport + fake resolver

internal/ruleconv/                    PURE CONVERSION (no transport, no HTTP)
  ruleconv.go        ToStaticRules(rules []model.Rule, specs FaultSpecResolver) ([]atropos.StaticRule, error)
  ruleconv_test.go
```

**Why this split:**

- **Transport is single-package, file-per-endpoint-family.** Mirrors `internal/zeus/client.go`. Each file is thin — ~50 lines of HTTP plumbing per endpoint family. No subpackage-per-endpoint: Shape-2 splitting would force fanout and retry helpers into either a grandparent utility package or duplication across peers.
- **Orchestration is single-package, file-per-capability.** Same rationale. `fanout.go` is shared by `freeze.go` / `fault.go` / `rules.go` / `status.go` without import gymnastics. The `Controller` type exposes the public methods; files organize the impl.
- **Transport vs. orchestration IS split** because they have different consumers, different dependencies, and different test shapes. Admin UI handlers will use transport directly for surgical per-instance operations. Orchestrator uses controller. Transport tests need `httptest.Server`; controller tests fake the transport.
- **Ruleconv stands alone** because it has one pure responsibility with a single resolver dependency. Easiest thing in the system to unit-test.

Atropos's own `internal/` is never touched. We consume only the root package's exported API.

---

## Transport layer: `internal/atropos`

### Responsibilities

- HTTP transport over one atropos instance at a time.
- Marshalling to/from atropos's exported wire types.
- Surfacing HTTP status + body as typed errors.
- Nothing else — no fan-out, no retries, no intent tracking, no resolution.

### Shape

```go
package atropos

import (
    "context"
    "net/http"
    atroposdk "github.com/microfaults/atropos-go"
)

type Client struct {
    httpClient *http.Client
    // (no state beyond the http client)
}

type Option func(*Client)

func WithHTTPClient(h *http.Client) Option { ... }

func NewClient(opts ...Option) *Client { ... }
```

### Methods

Each takes a `context.Context` + a full base URL (`http://10.0.3.4:9090`) + endpoint-specific args. No service names, no instance IDs — transport works on addresses.

```go
// Fault admin
func (c *Client) PostFault(ctx context.Context, addr string, req atroposdk.FaultRequest) (atroposdk.FaultStatus, error)
func (c *Client) GetFault(ctx context.Context, addr string) (atroposdk.FaultStatus, error)
func (c *Client) DeleteFault(ctx context.Context, addr string) error

// Rules admin
func (c *Client) PostRules(ctx context.Context, addr string, rules []atroposdk.StaticRule) error
func (c *Client) GetRules(ctx context.Context, addr string) ([]atroposdk.StaticRule, error)

// Cachebox admin
func (c *Client) PostCacheBoxDelay(ctx context.Context, addr string, req atroposdk.DelayRequest) error
func (c *Client) GetCacheBoxStats(ctx context.Context, addr string) (atroposdk.Stats, error)
func (c *Client) ClearCacheBox(ctx context.Context, addr string) error
```

### Errors

```go
// HTTPError: server responded with non-2xx. Body is included for debugging.
type HTTPError struct {
    Method   string
    URL      string
    Status   int
    Body     string // truncated to reasonable size
}

// TransportError: network-level failure before an HTTP response was received.
type TransportError struct {
    Method string
    URL    string
    Err    error
}
```

Both implement `error`. `errors.As` / `errors.Is` friendly. Upper layers distinguish "atropos said no" from "couldn't reach atropos."

### Testing

`client_test.go` uses `httptest.NewServer` to simulate atropos. One test per method per outcome (200/204/4xx/5xx/network-fail). No mocks — the real HTTP round-trip is cheap and catches JSON shape bugs that mocks would hide.

---

## Orchestration layer: `internal/atrocontrol`

### Responsibilities

- Expand service names → instance addresses via `InstanceResolver`.
- Fan out per-instance HTTP calls with bounded concurrency and per-call timeouts.
- Collect partial-success results and surface them as a typed `FanoutResult`.
- Maintain intent state so register-time reconcile can serve new pods.
- Convert manteion rule types → atropos wire types (delegates to `internal/ruleconv`).
- Emit structured logs for every fan-out.

### Controller shape

```go
package atrocontrol

import (
    "context"
    "time"

    "github.com/faults-lab/manteion-go/internal/atropos"
    atroposdk "github.com/microfaults/atropos-go"
)

type Controller struct {
    tx       *atropos.Client
    resolver InstanceResolver
    intent   *IntentTracker
    logger   *slog.Logger
    opts     controllerOpts // defaults for concurrency, timeout, instance filter
}

func New(tx *atropos.Client, resolver InstanceResolver, opts ...Option) *Controller { ... }

// For register handler to consult intent without the full Controller surface.
func (c *Controller) IntentReader() IntentReader { return c.intent }
```

### Public API

All service-level methods take a service name and return a `FanoutResult`. Instance-level variants exist for surgical use from the admin UI.

**Cache-box / freeze:**

```go
func (c *Controller) FreezeService(ctx context.Context, service string, cfg atroposdk.DelayRequest, opts ...CallOption) (FanoutResult, error)
func (c *Controller) ClearService(ctx context.Context, service string, opts ...CallOption) (FanoutResult, error)

// Surgical:
func (c *Controller) FreezeInstance(ctx context.Context, instanceID string, cfg atroposdk.DelayRequest, opts ...CallOption) error
func (c *Controller) ClearInstance(ctx context.Context, instanceID string, opts ...CallOption) error
```

**Fault injection:**

```go
func (c *Controller) InjectFault(ctx context.Context, service string, req atroposdk.FaultRequest, opts ...CallOption) (FanoutResult, error)
func (c *Controller) ClearFault(ctx context.Context, service string, opts ...CallOption) (FanoutResult, error)

// Surgical:
func (c *Controller) InjectFaultOnInstance(ctx context.Context, instanceID string, req atroposdk.FaultRequest, opts ...CallOption) error
func (c *Controller) ClearFaultOnInstance(ctx context.Context, instanceID string, opts ...CallOption) error
```

**Rules push (scaffolding — built but not wired to mutations yet):**

```go
// PushRules accepts manteion-native rules + resolvers, converts via ruleconv,
// and fans out the resulting []StaticRule to every alive instance of `service`.
func (c *Controller) PushRules(ctx context.Context, service string, rules []model.Rule, specs ruleconv.FaultSpecResolver, comps ruleconv.FaultCompositionResolver, opts ...CallOption) (FanoutResult, error)

// Surgical:
func (c *Controller) PushRulesToInstance(ctx context.Context, instanceID string, rules []atroposdk.StaticRule, opts ...CallOption) error
```

**Status (GETs, fan-out, aggregate):**

```go
type ServiceStatus struct {
    Service   string
    Instances []InstanceStatus
}
type InstanceStatus struct {
    InstanceID string
    Address    string
    Fault      *atroposdk.FaultStatus  // nil if not queried or not present
    Rules      []atroposdk.StaticRule
    CacheBox   *atroposdk.Stats
    Err        error                   // per-instance error for transparency
}

func (c *Controller) StatusByService(ctx context.Context, service string, opts ...CallOption) (ServiceStatus, error)
```

### Call options

Applied per call, override controller defaults:

```go
type CallOption func(*callOpts)

func WithTimeout(d time.Duration) CallOption           // per-instance HTTP deadline
func WithConcurrency(n int) CallOption                 // max in-flight requests during fan-out
func WithInstanceFilter(f InstanceFilter) CallOption   // which instances to target
func WithRunID(id string) CallOption                   // correlation ID for logs + intent
func WithRetry(n int, backoff time.Duration) CallOption // forward-compatible; default is 1 try
```

Defaults (set at Controller construction, overridable per-call):

| Option | Default |
|---|---|
| Per-instance timeout | 2s |
| Fan-out concurrency | 16 |
| Instance filter | alive + suspect |
| Retries | 1 try (no retry) |
| Run ID | empty |

### FanoutResult

```go
type FanoutResult struct {
    Targeted  []string             // instance IDs we tried
    OK        []string             // instance IDs that succeeded
    Failed    []PerInstanceError   // instance IDs that failed, with reason
    Duration  time.Duration
}

type PerInstanceError struct {
    InstanceID string
    Address    string
    Err        error   // HTTPError, TransportError, context deadline, etc.
}

func (r FanoutResult) AllSucceeded() bool { return len(r.Failed) == 0 }
func (r FanoutResult) AnySucceeded() bool { return len(r.OK) > 0 }
```

The controller returns `FanoutResult` as the primary channel for partial-success reporting. The accompanying `error` is non-nil only if the fan-out couldn't start (no instances found, resolver failure). Per-instance failures live in `Failed`.

### Fan-out primitive (fanout.go)

Generic over the per-instance operation:

```go
type target struct {
    instanceID string
    address    string
}

type opFn func(ctx context.Context, t target) error

// fanout invokes op on each target with bounded concurrency and per-target timeout,
// collecting successes and failures. Respects ctx cancellation.
func fanout(ctx context.Context, targets []target, op opFn, cfg callOpts) FanoutResult
```

Each capability-level method (`FreezeService`, `InjectFault`, etc.) is ~15 lines: resolve → construct op closure → call `fanout` → stamp intent → log → return.

---

## Rule conversion: `internal/ruleconv`

### Why separate

Rule conversion is pure (no I/O, no state, deterministic). It has one responsibility: given manteion's normalized rule model + resolvers for FK references, produce atropos's wire type. Keeping it in its own package:

- Makes it trivially unit-testable (pure functions, table-driven tests).
- Lets the admin UI and GET `/api/v1/sdk/rules` handler use it too (both currently serve unresolved rules — this closes that gap).
- Prevents atrocontrol from accreting business logic.

### Interface

Rule conversion produces a JSON-safe `CompiledRule` wire type (not `atropos.StaticRule`), because atropos's `StaticRule.Decision` holds a `fault.Fault` interface value that can't be serialized across HTTP. SDKs consume `CompiledRule` and build `StaticRule`s in-process. The wire type lives in this package.

```go
package ruleconv

import (
    "encoding/json"

    "manteion-go/internal/model"
)

type FaultSpecResolver interface {
    GetFaultSpec(id string) (*model.FaultSpec, error)
}

type FaultCompositionResolver interface {
    GetFaultComposition(id string) (*model.FaultComposition, error)
}

// CompiledRule is the wire format for resolved rules served to SDKs.
// Either Fault or Composition is non-nil; both nil means disabled/no-op.
type CompiledRule struct {
    Name           string             `json:"name"`
    InjectionPoint string             `json:"injection_point"`
    Labels         map[string]string  `json:"labels,omitempty"`
    Mode           string             `json:"mode"`
    Priority       int                `json:"priority"`
    Fault          *InlineFault       `json:"fault,omitempty"`
    Composition    *InlineComposition `json:"composition,omitempty"`
}

type InlineFault struct {
    Category   string          `json:"category"`
    FaultType  string          `json:"fault_type"`
    Config     json.RawMessage `json:"config"`
    DurationMs int64           `json:"duration_ms,omitempty"`
    RampUpMs   int64           `json:"ramp_up_ms,omitempty"`
    RampDownMs int64           `json:"ramp_down_ms,omitempty"`
}

type InlineComposition struct {
    Name          string                    `json:"name"`
    ExecutionMode string                    `json:"execution_mode"`
    Members       []InlineCompositionMember `json:"members"`
}

type InlineCompositionMember struct {
    Position    int                `json:"position"`
    Direction   string             `json:"direction,omitempty"`
    Fault       *InlineFault       `json:"fault,omitempty"`
    Composition *InlineComposition `json:"composition,omitempty"`
}

// CompileRules resolves FaultSpec/Composition references and produces wire-ready
// compiled rules. Returns an error if any referenced spec/composition is dangling
// or if composition nesting exceeds maxCompositionDepth (=3).
func CompileRules(
    rules []*model.Rule,
    specs FaultSpecResolver,
    comps ...FaultCompositionResolver,
) ([]CompiledRule, error)

func CompileRule(
    r *model.Rule,
    specs FaultSpecResolver,
    comps ...FaultCompositionResolver,
) (CompiledRule, error)
```

The variadic `comps` parameter keeps composition support optional — a rule with only FaultSpecID works without a composition resolver.

### What the conversion does

- For `rule.FaultSpecID`: look up the FaultSpec, inline its config into the StaticRule's fault section.
- For `rule.FaultCompositionID`: look up the composition, recursively resolve members into inlined configs. Composition depth validation is already enforced at create-time; conversion trusts that.
- Maps manteion enum strings (`"inline"`, `"network"`, `"resource"`) to atropos's numeric enums.
- Copies rule selectors / point / key verbatim.
- Returns a typed error if any FK is dangling.

### SDKRepo already has resolvers

`FaultRepo.SpecResolver()` and `FaultRepo.CompositionResolver()` already exist as closures. They'll satisfy the `FaultSpecResolver` / `FaultCompositionResolver` interfaces once we define those (Go structural typing; may need one thin adapter).

### Collateral benefit

The existing `handlePollRules` in `internal/api/sdk_handler.go` serves rules with raw FaultSpec IDs — SDKs can't build evaluators from that. Once ruleconv exists, that handler can call it before serving, closing the unresolved-rules gap identified in the earlier architecture review (action A4). The poll path and the push path share the same conversion logic.

---

## Intent tracking + register-time reconciliation

### Problem

A rolling deploy mid-experiment kills a pod with an active fault and replaces it with a new pod that has no fault. Measurement windows are polluted: some traffic hits faulted pods, some doesn't. The experiment is no longer measuring what it claims to measure.

### Strategy

Two-piece minimal design:

**1. Intent tracker (in-memory, in `atrocontrol`).**

```go
type ServiceIntent struct {
    Rules       []atroposdk.StaticRule
    ActiveFault *atroposdk.FaultRequest
    FreezeCfg   *atroposdk.DelayRequest
    AppliedAt   time.Time
    RunID       string  // which experiment run set this; empty if ad-hoc
}

type IntentTracker struct {
    mu    sync.RWMutex
    state map[string]*ServiceIntent // keyed by service name
}

func (t *IntentTracker) Set(service string, intent ServiceIntent)
func (t *IntentTracker) Clear(service string)
func (t *IntentTracker) Get(service string) (*ServiceIntent, bool)
func (t *IntentTracker) All() map[string]*ServiceIntent // snapshot for admin UI / diagnostics
```

Controller writes to the tracker **before** fan-out starts (so late-joining registrations see the target state). It writes again on Clear (to remove intent). It does not sweep — failures in fan-out don't roll back intent. Rationale: intent is "what the orchestrator wants," not "what's currently applied everywhere." Partial-success reporting via `FanoutResult` is the separate, honest signal for reality.

**2. Register-handler reconcile (in `internal/api/sdk_handler.go`).**

Modify the registration response shape:

```go
// Current response (roughly):
type registerResponse struct {
    InstanceID string `json:"instance_id"`
    PollAfter  int    `json:"poll_after_seconds"`
}

// Extended response (new optional fields):
type registerResponse struct {
    InstanceID  string                      `json:"instance_id"`
    PollAfter   int                         `json:"poll_after_seconds"`
    Rules       []atroposdk.StaticRule      `json:"rules,omitempty"`
    ActiveFault *atroposdk.FaultRequest     `json:"active_fault,omitempty"`
    FreezeCfg   *atroposdk.DelayRequest     `json:"freeze_cfg,omitempty"`
}
```

Handler logic after storing the registration:

```go
if intent, ok := controller.IntentReader().Get(req.Service); ok {
    resp.Rules       = intent.Rules
    resp.ActiveFault = intent.ActiveFault
    resp.FreezeCfg   = intent.FreezeCfg
}
```

Atropos SDK applies these on startup before serving its first request. Backward-compatible: older SDKs ignore unknown fields.

### Atropos-side TODO (not in this plan)

Atropos SDK needs to read `rules`, `active_fault`, `freeze_cfg` from its register response and apply them via existing in-process APIs (`SetRules`, `Configure(WithEvaluator(...))`, `SetDelaySource`). This is a follow-up atropos change, scoped and straightforward. Handle in its own branch. For the MVP of this client, manteion can write intent and serve it via the register response; atropos consuming it is the next plan.

### What this deliberately doesn't handle

- **Silent state loss on an existing instance** (panic-recovery, manual admin clear from a different actor). Not detected until next orchestrator action.
- **Race between FreezeService start and concurrent registration.** Benign: intent is written first, so F either sees nothing yet (pre-intent) or sees the target state (post-intent). Worst case: F applies fault once from register response and once from the fan-out — idempotent.
- **Intent survival across manteion restart.** If manteion crashes mid-experiment, tracker is gone. Acceptable — the experiment crashed too.

### Graduation triggers (when to add more)

- Frequent taint flags on measurement windows → add a reconciliation sweep every N seconds.
- Cross-restart experiment durability matters → persist IntentTracker to postgres.
- Silent drift becomes real → per-poll state verification.

None of this is MVP.

---

## Taint flagging (cheap complement to intent)

Orchestrator records the set of `instance_id`s at measurement-window start. At window end, re-query. If the set changed, stamp the run's metadata:

```go
run.Metadata["instance_set_changed"] = true
run.Metadata["instance_delta"] = {"added": [...], "removed": [...]}
```

Analysis layer can filter or weight tainted runs. ~10 lines in the orchestrator, separate from this client. Called out here because it composes with the intent tracker: intent prevents pollution for common rolling-deploy cases; taint flagging catches cases where pollution happened anyway.

---

## Instance discovery

### InstanceResolver interface

```go
type InstanceResolver interface {
    ForService(ctx context.Context, service string) ([]model.SDKInstance, error)
    ForInstance(ctx context.Context, instanceID string) (*model.SDKInstance, error)
}
```

Production impl wraps `store.SDKRepo`:

```go
type RepoResolver struct{ repo *store.SDKRepo }

func (r *RepoResolver) ForService(ctx context.Context, service string) ([]model.SDKInstance, error) {
    return r.repo.ForService(ctx, service)
}
func (r *RepoResolver) ForInstance(ctx context.Context, instanceID string) (*model.SDKInstance, error) {
    return r.repo.Get(ctx, instanceID)
}
```

Tests provide a fake resolver that returns canned instance lists.

### Liveness filter

```go
type InstanceFilter func(model.SDKInstance) bool

// Built-in filters:
func FilterAliveOnly(i model.SDKInstance) bool   // Status == "alive"
func FilterAliveOrSuspect(i model.SDKInstance) bool // Status in {"alive","suspect"} — DEFAULT
func FilterAll(i model.SDKInstance) bool         // everything
```

**Default: alive + suspect.** Rationale: a "suspect" pod may still be receiving traffic (just missed a heartbeat or two). Injecting fault there is correct. If truly dead, the per-instance call fails and surfaces in `FanoutResult.Failed` — visible, not silent.

Before the reaper goroutine (A7, separate plan) exists, all registered instances are effectively "alive." The default filter is forward-compatible: when reaper lands and starts transitioning statuses, the filter semantics just start taking effect.

### No DNS shortcut

We could let kube-proxy load-balance to a service DNS name. We don't, because experiments need *per-replica* addressing (e.g., inject fault on 3 of 5 replicas deterministically, not whichever kube-proxy picks). Self-registration gives stable per-pod targets; the `address` field is the only handle.

---

## Fan-out execution semantics

### Defaults

- **Sync, partial-success.** `FreezeService` blocks until all instances respond or timeout. Returns `FanoutResult` — caller decides per-policy whether partial success is acceptable.
- **Bounded concurrency.** Default 16 in-flight. Tunable per-call. Prevents thundering-herd on large services.
- **Per-instance timeout.** Default 2s. Tunable. Instance exceeding timeout appears in `Failed` with a deadline error.
- **One try per instance.** No retry by default. `WithRetry` option exists for forward-compat but defaults to 1.

### Why not stricter (all-or-nothing with auto-rollback)

Auto-rollback introduces a three-valued state (applied / cleared / partial rollback) and rollback-of-rollback failure modes. Orchestrator expressing tolerance in its own policy layer is simpler and more honest. If the orchestrator wants strict semantics, it inspects `FanoutResult.AllSucceeded()` and proceeds only on true; otherwise it fires its own rollback (`ClearService`) and aborts the run.

### Why not looser (fire-and-forget)

Experiment measurement windows need the fault to be active *before* the window starts. Fire-and-forget gives no signal; the orchestrator would measure first and check later, yielding unreliable data. Sync is the right default.

---

## Observability

### Now: structured logs

Every fan-out emits one structured log entry:

```
atrocontrol.fanout {
  action:      "freeze_service" | "clear_service" | "inject_fault" | ...
  service:     "productcatalog"
  run_id:      "run-42" (if WithRunID)
  targeted:    5
  ok:          4
  failed:      1
  duration_ms: 1820
  details:     [
    { instance: "pod-abc", status: "ok" },
    { instance: "pod-xyz", status: "failed", err: "connect refused" }
  ]
}
```

Logger is injected at Controller construction. Uses whatever logger manteion already uses (`slog` likely). No storage; operator reads via kubectl logs / aggregator.

### Later: persistent audit table

Design sketch for the follow-up plan. New table:

```sql
CREATE TABLE admin_actions (
  id           UUID PRIMARY KEY,
  timestamp    TIMESTAMPTZ NOT NULL DEFAULT now(),
  actor        TEXT,                    -- filled once auth exists; "system" for now
  run_id       TEXT,                    -- null for ad-hoc admin UI actions
  action       TEXT NOT NULL,           -- "freeze_service", "inject_fault", etc.
  service      TEXT,                    -- null for instance-level actions
  instance_id  TEXT,                    -- null for service-level actions
  request_body JSONB,                   -- atropos wire request
  result       JSONB,                   -- FanoutResult or per-instance status
  duration_ms INTEGER
);

CREATE INDEX idx_admin_actions_service_time ON admin_actions(service, timestamp DESC);
CREATE INDEX idx_admin_actions_run ON admin_actions(run_id) WHERE run_id IS NOT NULL;
```

Controller would gain an optional `AuditSink` dependency, called after every fan-out. Sink implementations: `NoopSink` (today), `PostgresSink` (follow-up). The admin UI queries this table for "what's been done to productcatalog?" and "what did run-42 do?"

Not implemented in this plan. Separate plan lands after the admin UI needs it.

---

## Rule push scaffolding

### Intent

Build the real push path now — real client method, real HTTP, real tests — but do not wire it into `RuleRepo` mutations. Call-sites are dormant except admin-UI "force-apply" buttons and direct orchestrator use for experiment phases.

### What gets built

- `Controller.PushRules(ctx, service, rules, specs, comps, opts...)` — resolves service to instances, calls `ruleconv.ToStaticRules`, fans out `PostRules` per instance, returns `FanoutResult`.
- `Controller.PushRulesToInstance(ctx, instanceID, []atroposdk.StaticRule, opts...)` — surgical variant.
- Full tests for both (real push against httptest.Server, fake resolver).

### What stays dormant

- `RuleRepo.Create` / `Update` / `Delete` do **not** call `PushRules`. They only write to postgres. The existing SDK poll path (`handlePollRules`) remains the runtime delivery mechanism for now.

### Migration path to push (not this plan)

When we flip poll→push:
1. Wire `RuleRepo.Create/Update/Delete` to call `Controller.PushRules` post-commit.
2. Optionally add transaction outbox pattern if we want push to be reliable across manteion restarts.
3. Remove `handlePollRules`, update atropos SDK to stop polling.

All of that is one commit of call-site changes in manteion plus an atropos-side SDK update. The push implementation itself is ready today.

### Admin UI uses push today

The admin UI's "force-apply rule X to service Y" button calls `Controller.PushRules` directly. This gives the UI an operational escape hatch without waiting for the full migration.

---

## Wiring: where does the Controller live?

```go
// In cmd/manteion/main.go or internal/api/server.go wiring:

txClient := atropos.NewClient(atropos.WithHTTPClient(&http.Client{Timeout: 5 * time.Second}))
resolver := atrocontrol.NewRepoResolver(sdkRepo)
controller := atrocontrol.New(txClient, resolver,
    atrocontrol.WithDefaultTimeout(2*time.Second),
    atrocontrol.WithDefaultConcurrency(16),
    atrocontrol.WithLogger(logger),
)

// Register handler uses IntentReader:
apiServer := api.NewServer(api.Config{
    // ... existing deps ...
    IntentReader: controller.IntentReader(),
})

// Orchestrator (future, separate plan) takes the full controller:
orchestrator := experiment.NewOrchestrator(controller, experimentRepo, runRepo, ...)
```

Controller is a singleton for the manteion process. Stateless aside from `IntentTracker` (in-memory map). No connection pool beyond `http.Client`'s default transport. Graceful shutdown: no special handling needed — HTTP client cancels in-flight requests on context cancellation.

---

## Testing strategy

**Transport (`internal/atropos/client_test.go`):**
- `httptest.NewServer` per test. Real HTTP round-trip.
- One test per method per outcome: success, 4xx, 5xx, malformed JSON, timeout, connection refused.
- No mocks. Real JSON marshalling catches atropos type drift.

**Orchestration (`internal/atrocontrol/controller_test.go`):**
- Fake `*atropos.Client` via interface alias or function-typed fields.
- Fake `InstanceResolver` returning canned lists.
- Tests cover: happy-path fan-out, partial success, all-failed, resolver error, context cancellation mid-fan-out, intent write-before-fan-out ordering, instance filter honored, surgical methods bypass fan-out.

**Rule conversion (`internal/ruleconv/ruleconv_test.go`):**
- Table-driven. Fake resolvers as in-memory maps.
- Cases: rule with FaultSpecID, rule with FaultCompositionID, nested composition, dangling FK (error), empty rule set.

**Integration (`internal/api/sdk_handler_test.go` extensions):**
- Register handler serves intent from tracker. End-to-end: orchestrator sets intent, new registration response contains it.

---

## Rolled-in concerns from earlier review

This client design either resolves or forward-compatibly defers several items from the architecture review of commits 340df30 + baac8c5:

| Review item | This plan |
|---|---|
| A3 — repository interfaces | Partially addressed (`InstanceResolver` is a new interface; controller depends on `*atropos.Client` concretely for now — a transport interface can be extracted if tests need it) |
| A4 — rule compilation for poll endpoint | Resolved via `ruleconv`. Poll handler can call `ruleconv.ToStaticRules` immediately; this plan ships that package. |
| A5 — in-memory cache for rules/version | Out of scope here. Once push is wired and poll retires, cache question reframes. |
| A7 — reaper goroutine | Out of scope. `InstanceFilter` default is forward-compatible with reaper-set statuses. |
| A13 — wire ValidateComposition into composition creation | Out of scope. Belongs in a fault-handler plan, but `ruleconv` will benefit once it's wired. |
| A14 — HTTP handlers for fault specs / compositions | Out of scope. Mentioned because rule push requires specs to exist; admin UI will depend on A14. |
| Content-Type bug in http.Error | Pinned per earlier request. Not addressed in this plan. |

---

## Open questions (to confirm before plan-writing)

1. **`internal/ruleconv` resolver adapter.** `FaultRepo.SpecResolver()` returns a `func(id string) (*FaultSpec, error)` closure. Do we adapt with a thin wrapper type, or change `FaultRepo` to return an interface type? Recommend wrapper (keeps FaultRepo untouched).

2. **Logger dependency.** Use `*slog.Logger`, `log/slog`'s default, or a manteion-specific logger? Check what server/wiring currently uses and match.

3. **Error wrapping for PerInstanceError.Err.** Do we flatten (store `.Error()` string) or keep the typed error (supports `errors.As` by callers)? Recommend keep typed; structured-logging layer flattens at emit time.

4. **Register-response field naming.** `active_fault` vs `fault`; `freeze_cfg` vs `cachebox_delay`. Defer to atropos-side field names where they exist, invent only where needed.

---

## Implementation status

All items below are landed on `feat/inital-setup`:

1. ✅ Atropos-go: `FaultRequest`, `FaultStatus`, `DelayRequest` exported.
2. ✅ Manteion: `internal/ruleconv` package + tests (FaultSpec + FaultComposition compile paths).
3. ✅ Manteion: `internal/atropos` transport + tests.
4. ✅ Manteion: `internal/atrocontrol` orchestration (controller, fan-out, freeze, fault, rules, status, intent, resolver, options, errors) + tests.
5. ✅ Manteion: register response extended with intent fields; poll handler uses `CompileRules`.
6. ✅ Manteion: controller wired into `cmd/manteion/main.go`.

**Tracked in the follow-up plan (see `docs/plans/2026-04-19-atropos-client-followups.md`):**

- Atropos-side SDK change to consume the extended register response and apply `rules` / `active_fault` / `freeze_cfg` on startup before serving traffic. Intent tracking on the manteion side is useful today (admin UI can inspect "what's intended for productcatalog") but only closes the rolling-deploy loop once atropos applies it.
- Composition validation wired into the composition creation path (A13 from the architecture review).
- HTTP handlers for fault spec / composition CRUD (A14).
- Persistent audit sink backing the structured logs (design sketched above; implementation when admin UI needs it).
