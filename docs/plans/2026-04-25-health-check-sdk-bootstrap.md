# Health Check & SDK Bootstrap Protocol

> **Date:** 2026-04-25
> **Supersedes:** `2026-04-22-health-check-sdk-bootstrap.md`
> **Status:** Proposed
> **Scope:** manteion-go (control plane), atropos-go (SDK), service-beds (integration)
> **Go version:** 1.25 stdlib — uses `sync.WaitGroup.Go`, `synctest.Test`,
> `slog.DiscardHandler`, `t.Context()`, `http.NewResponseController`

---

## Context

The atropos ecosystem has a hard startup dependency: each service's embedded
atropos SDK must confirm manteion is alive, register itself, and fetch initial
rules before the service can serve traffic. After boot, the SDK periodically
polls manteion for rule updates — and that same poll doubles as a liveness
heartbeat.

**What already exists (on `feat/inital-setup` + `feat/admin-endpoints`):**

| Component | What's done | What's missing |
|-----------|------------|----------------|
| Manteion `/healthz` | Always 200 (correct) | Nothing |
| Manteion `/readyz` | DB ping gate (200/503) | Nothing |
| Manteion `/api/v1/sdk/init` | Stub — always returns 200 | Needs DB + rule-store gates |
| Manteion `/api/v1/status` | Rules/instances/zeus | Missing `db_healthy` |
| `Register()` (atropos-go) | Fully implemented | Nothing |
| `Apply()` (atropos-go) | Fully implemented | Nothing |
| `CompiledRule` wire format | Both repos, with composition support | Nothing |
| `CompileRules` in `handlePollRules` | Working, serves compiled rules | Nothing |
| Atropos SDK init lifecycle | `atropos.Init()` sets up OTel only | No manteion awareness |
| SDK health reporting | None | Entire subsystem |
| Push channel (SSE) | None | Entire subsystem |

This plan covers only the **missing** column.

---

## Architecture: Rule Builder vs Rule Evaluator

**Core principle: Rule Builder → Manteion. Rule Evaluator → Atropos.**

```
┌───────────────────────────────────────────────────────────────────────┐
│                    Manteion (Control Plane)                           │
│                                                                       │
│  ┌─────────────┐    ┌────────────┐    ┌──────────────────────┐       │
│  │ Rule Builder │───▶│ Rule Store │───▶│ CompileRules          │       │
│  │ CRUD + Valid │    │ (Postgres) │    │ resolves FK → inlined │       │
│  │ + Composition│    │            │    │ CompiledRule payload  │       │
│  └─────────────┘    └────────────┘    └──────────┬───────────┘       │
│                                                   │                   │
└───────────────────────────────────────────────────┼───────────────────┘
                                                    │
                          GET /sdk/rules (pull)      │    SSE /sdk/events (push)
                                                    │
┌───────────────────────────────────────────────────┼───────────────────┐
│                     Atropos SDK (per-service)      │                   │
│                                                   ▼                   │
│  ┌─────────────────┐    ┌──────────────────┐    ┌──────────────┐     │
│  │ ManteionClient   │───▶│ DecodeCompiled    │───▶│ Interceptor   │     │
│  │ Poll + SSE       │    │ Rules()           │    │ Fault dispatch│     │
│  │ listener          │    │ → StaticEvaluator │    │ + CacheBox    │     │
│  └─────────────────┘    └──────────────────┘    └──────────────┘     │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

**What each side owns:**

### Manteion (rule builder)

- `Rule` CRUD, `FaultSpec` definitions, `FaultComposition` grouping
- Validation (incompatibility checks, mode constraints, depth cap)
- Versioning — monotonic `rule_version` counter bumped on every mutation
- Rule compilation — `ruleconv.CompileRules` resolves FK references into
  `CompiledRule` / `CompiledFault` / `CompiledComposition` wire format

### Atropos (rule evaluator)

- `StaticEvaluator` — runtime rule matching (injection point + label AND match)
- `DecodeCompiledRules` — converts `CompiledRule` wire format → `StaticRule` list
- `Interceptor` — fault dispatch, cache-box dispatch, OTel span creation
- Hot-swap — `SetRules()` atomically replaces rules without request interruption

**The SDK never calls the rule CRUD API.** It only consumes the read-only
`/sdk/rules` (poll) and `/sdk/events` (push) endpoints.

**Existing wire types — USE THESE, do not create alternatives:**

| Type | Package | Purpose |
|------|---------|---------|
| `CompiledRule` | both repos (`ruleconv` / root) | Top-level wire rule |
| `CompiledFault` | both repos | Resolved fault spec with config inlined |
| `CompiledComposition` | both repos | Resolved composition tree |
| `CompiledCompositionMember` | both repos | Leaf or nested member |
| `DecodeCompiledRules` | atropos-go root | Wire → `[]StaticRule` |
| `Register()` | atropos-go root | `POST /api/v1/sdk/register` |
| `Apply()` | atropos-go root | Install `RegisterResponse` onto SDK objects |

---

## Startup Dependency Chain

```
┌─────────────┐         ┌──────────────────┐         ┌─────────────────────┐
│  Manteion   │────────▶│  Atropos SDK     │────────▶│  Service Ready      │
│  /sdk/init  │         │  ConnectManteion │         │  /_healthz → 200    │
│  200 OK     │         │  Register+Rules  │         │  (only if SDK ok)   │
│  DB migrated│         │  Poll loop start │         │                     │
└─────────────┘         └──────────────────┘         └─────────────────────┘
```

### Sequence diagram

```
Manteion                    Atropos SDK                  Service
   │                            │                           │
   │ Pod starts                 │                           │
   │ Connect Postgres           │                           │
   │ Run migrations             │                           │
   │ Start HTTP server          │                           │
   │ /healthz → 200             │                           │
   │ /readyz → 200 (DB pinged)  │                           │
   │                            │                           │
   │                            │ Service pod starts        │
   │                            │ atropos.Init() — OTel     │
   │◀───────────────────────────│ GET /sdk/init             │
   │ (retry w/ backoff)         │ (wait for 200)            │
   │────────────────────────────▶ 200 {"status":"ready"}    │
   │                            │                           │
   │◀───────────────────────────│ POST /sdk/register        │
   │                            │ (uses existing Register())│
   │────────────────────────────▶ 201 + intent if any       │
   │                            │                           │
   │                            │ Apply(resp, targets)      │
   │                            │ (uses existing Apply())   │
   │                            │                           │
   │◀───────────────────────────│ GET /sdk/rules            │
   │                            │ ?service=X&version=0      │
   │────────────────────────────▶ 200 {version:1, rules:[…]}│
   │                            │                           │
   │                            │ DecodeCompiledRules →     │
   │                            │ evaluator.SetRules()      │
   │                            │ Start poll loop (10s)     │
   │                            │─────────────────────────▶ │
   │                            │                    Service starts listening
   │                            │                    /_healthz → 200 SERVING
   │                            │                           │
   │             ┌─ Every 10s ──┤                           │
   │◀────────────│              │ GET /sdk/rules            │
   │             │              │ ?service=X&version=N      │
   │             │              │ &instance_id=ID           │
   │─────────────┤              │                           │
   │  304 (no change)           │  (also: TouchPoll)        │
   │  OR                        │                           │
   │  200 (new rules)           │  → SetRules() hot-swap    │
   │             └──────────────┤                           │
```

---

## Phase 1: Manteion Health Gaps (manteion-go)

Only two changes remain. `handleReadyz` already gates on DB.

### 1a. `GET /api/v1/sdk/init` — add DB + rule-store gates

Current implementation is a stub (`health_handler.go` isn't involved —
it's in `sdk_handler.go:146`):

```go
// CURRENT — always returns 200
func (s *Server) handleInit(w http.ResponseWriter, r *http.Request) {
    writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
```

Replace with:

```go
func (s *Server) handleInit(w http.ResponseWriter, r *http.Request) {
    if err := s.db.PingContext(r.Context()); err != nil {
        s.logger.Warn("sdk/init: db unreachable", "error", err)
        writeJSON(w, http.StatusServiceUnavailable, map[string]string{
            "status": "not_ready",
            "reason": "database unreachable",
        })
        return
    }
    if _, err := s.rules.Version(r.Context()); err != nil {
        s.logger.Warn("sdk/init: rule store not initialized", "error", err)
        writeJSON(w, http.StatusServiceUnavailable, map[string]string{
            "status": "not_ready",
            "reason": "rule store not initialized",
        })
        return
    }
    writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
```

| Condition | Status | SDK behavior |
|-----------|--------|-------------|
| Manteion not started | Connection refused | SDK retries |
| DB unreachable | 503 | SDK retries |
| Migrations pending | 503 (`Version()` fails) | SDK retries |
| Fully ready | 200 | SDK proceeds to register |

### 1b. `GET /api/v1/status` — add `db_healthy`

One-line addition to `handleStatus`:

```go
status := map[string]any{
    "db_healthy":     s.db.PingContext(ctx) == nil,   // ← new
    "rules":          ruleCount,
    "instances":      instanceCount,
    "zeus_reachable": s.zeus.Healthy(ctx),
}
```

### Files to modify

| File | Change |
|------|--------|
| `internal/api/sdk_handler.go` | Replace `handleInit` stub with gated version |
| `internal/api/health_handler.go` | Add `db_healthy` to `handleStatus` |

---

## Phase 2: Atropos ManteionClient (atropos-go)

`ManteionClient` is a **lifecycle wrapper** around the existing `Register()` and
`Apply()` functions. It adds: startup readiness polling, background rule poll
loop, exponential backoff, degraded-state tracking, and graceful shutdown.

### Phase 2 prerequisite: add `RegisterWithClient` for `*http.Client` injection

The existing `Register()` in atropos-go's `register.go` hardcodes
`http.DefaultClient`, which is fine for the existing E2E test
(`feat/admin-endpoints`, commit `6b72307`). `ManteionClient` needs to
inject its own `*http.Client` (for timeouts, transport tuning, testing).

**Additive — do not break the existing signature.** Add a new variant
alongside the existing function:

```go
// Register — existing signature, unchanged. Wraps RegisterWithClient
// using http.DefaultClient. Existing callers and the E2E test on
// feat/admin-endpoints keep working without modification.
func Register(ctx context.Context, baseURL string, req RegisterRequest) (RegisterResponse, error) {
    return RegisterWithClient(ctx, http.DefaultClient, baseURL, req)
}

// RegisterWithClient calls /sdk/register using the provided HTTP client.
// Use this when you need explicit timeout/transport control (e.g. from
// ManteionClient).
func RegisterWithClient(ctx context.Context, httpClient *http.Client, baseURL string, req RegisterRequest) (RegisterResponse, error) {
    // ... existing body, with http.DefaultClient.Do replaced by httpClient.Do ...
}
```

`ManteionClient` calls `RegisterWithClient(ctx, c.httpClient, ...)`.

### Public API (new file: `manteion.go`)

```go
package atropos

// ConnectManteion connects this SDK to the manteion control plane.
//
// Blocks until manteion is ready, registers the instance, fetches initial
// rules, configures the evaluator, and starts a background poll loop.
//
// URL resolution: opts > MANTEION_URL env > offline mode.
//
// Returns (nil, nil) if no URL is set (offline mode — tracing-only). All
// public methods on *ManteionClient are nil-receiver safe, so callers can
// unconditionally `defer client.Close(ctx)` without branching.
//
// Returns a non-nil error if:
//   - WithApplyTargets is not set or ApplyTargets.Evaluator is nil
//   - manteion is unreachable past InitTimeout
//   - registration fails
func ConnectManteion(ctx context.Context, serviceName string, opts ...ManteionOption) (*ManteionClient, error)

type ManteionOption interface{ applyManteion(*manteionConfig) }

func WithManteionURL(url string) ManteionOption
func WithInstanceID(id string) ManteionOption
func WithInitTimeout(d time.Duration) ManteionOption  // default: 30s
func WithPollInterval(d time.Duration) ManteionOption  // default: 10s
func WithOfflineMode() ManteionOption
func WithApplyTargets(t ApplyTargets) ManteionOption   // REQUIRED — must include Evaluator
func WithHTTPClient(c *http.Client) ManteionOption     // default: 10s timeout transport
```

### Construction-time validation

`ConnectManteion` validates before any I/O:

```go
func ConnectManteion(ctx context.Context, serviceName string, opts ...ManteionOption) (*ManteionClient, error) {
    cfg := defaultManteionConfig(serviceName)
    for _, o := range opts {
        o.applyManteion(&cfg)
    }

    // Offline mode — no URL set. Return (nil, nil) so callers can
    // unconditionally `defer client.Close(ctx)` (nil-safe) without
    // branching on whether manteion was configured.
    if cfg.url == "" {
        return nil, nil
    }

    // The poll loop calls targets.Evaluator.SetRules() — nil would panic.
    if cfg.targets.Evaluator == nil {
        return nil, errors.New("ConnectManteion: WithApplyTargets must be set with a non-nil Evaluator")
    }

    // ... proceed with waitForReady, register, poll loop ...
}
```

### Defaults

| Option | Default | Source |
|---|---|---|
| `WithManteionURL` | `MANTEION_URL` env, else `""` (offline) | env |
| `WithInstanceID` | `${hostname}-${random8hex}` | generated at startup; falls back to `MANTEION_INSTANCE_ID` env if set |
| `WithInitTimeout` | 30s | const; overridable via `MANTEION_INIT_TIMEOUT` env |
| `WithPollInterval` | 10s | const |
| `WithHTTPClient` | `&http.Client{Timeout: 10 * time.Second}` | construction |

**`instanceID` rationale:** in Kubernetes, `os.Hostname()` returns the
pod name — already unique cluster-wide and human-readable in logs. The
8-hex-char random suffix is belt-and-suspenders for environments where
hostname isn't unique (local dev where hostname is `macbook`; bare metal
running multiple SDK instances per host). The ID is stable across
re-registrations within a single SDK lifetime, so a partition heal
followed by `go c.register(ctx)` lands on the same manteion row
(idempotent upsert). On pod restart, a fresh ID is generated and the
reaper (per `2026-04-01-sdk-liveness-reaper.md`) clears the old entry
after 120s.

### Client internals (new file: `manteion_client.go`)

```go
type ManteionClient struct {
    cfg         manteionConfig
    httpClient  *http.Client       // injected via WithHTTPClient; used by ALL HTTP calls
    targets     ApplyTargets       // existing type from register.go
    ruleVersion atomic.Uint64      // written by poll loop, read by Health()
    lastPollAt  atomic.Int64       // unix nanos of last successful poll; 0 = never
    status      atomic.Int32       // ManteionStatus enum
    cancel      context.CancelFunc
    wg          sync.WaitGroup     // poll loop goroutine; Close() waits on this
    logger      *slog.Logger
}

type ManteionStatus int32
const (
    ManteionDisconnected ManteionStatus = iota
    ManteionConnected
    ManteionDegraded
)

// Nil-receiver safe methods — callers can defer Close on a nil client.
func (c *ManteionClient) Close(ctx context.Context) error {
    if c == nil { return nil }
    // ... real implementation ...
}

func (c *ManteionClient) Status() ManteionStatus {
    if c == nil { return ManteionDisconnected }
    return ManteionStatus(c.status.Load())
}
```

### Startup: `waitForReady`

Exponential backoff: 500ms → 1s → 2s → 4s → 5s (cap), up to `InitTimeout`.

```go
func (c *ManteionClient) waitForReady(ctx context.Context) error {
    deadline := time.Now().Add(c.cfg.initTimeout)
    ctx, cancel := context.WithDeadline(ctx, deadline)
    defer cancel()

    backoff := 500 * time.Millisecond
    for attempt := 1; ; attempt++ {
        // NewRequestWithContext so the deadline ctx actually bounds the call
        // (httpClient.Get ignores context — only c.httpClient.Timeout would apply).
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.url+"/api/v1/sdk/init", nil)
        if err != nil {
            return fmt.Errorf("build init request: %w", err)
        }
        resp, err := c.httpClient.Do(req)
        if err == nil {
            io.Copy(io.Discard, resp.Body) // drain so the conn can be reused
            resp.Body.Close()
            if resp.StatusCode == http.StatusOK {
                c.logger.Info("manteion ready", "attempts", attempt)
                return nil
            }
        }

        select {
        case <-ctx.Done():
            return fmt.Errorf("manteion not ready after %d attempts (%v): %w",
                attempt, c.cfg.initTimeout, ctx.Err())
        case <-time.After(backoff):
        }
        backoff = min(backoff*2, 5*time.Second)
    }
}
```

### Registration — wraps existing `Register()` + `Apply()`

All HTTP calls go through `c.httpClient` — including `Register()`, which
now accepts `*http.Client` as its first argument (see prerequisite above).

```go
func (c *ManteionClient) register(ctx context.Context) error {
    resp, err := RegisterWithClient(ctx, c.httpClient, c.cfg.url, RegisterRequest{
        ID:      c.cfg.instanceID,
        Service: c.cfg.serviceName,
        Version: c.cfg.serviceVersion,
        Address: c.cfg.address,
    })
    if err != nil {
        return err
    }
    return Apply(resp, c.targets)
}
```

### Poll loop

Every poll to `GET /api/v1/sdk/rules` serves three purposes:

1. **Rule sync** — fetch latest rules if version changed
2. **Health heartbeat** — manteion's `TouchPoll` records the SDK is alive
3. **Connectivity check** — SDK knows if manteion is reachable

```go
func (c *ManteionClient) pollLoop(ctx context.Context) {
    ticker := time.NewTicker(c.cfg.pollInterval)
    defer ticker.Stop()

    consecutiveFailures := 0

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            err := c.fetchRules(ctx)

            if err != nil {
                consecutiveFailures++
                c.status.Store(int32(ManteionDegraded))

                backoff := c.cfg.pollInterval * time.Duration(1<<min(consecutiveFailures, 6))
                backoff = min(backoff, 60*time.Second)
                ticker.Reset(backoff)

                c.logger.Warn("manteion poll failed, continuing with stale rules",
                    "failures", consecutiveFailures,
                    "next_retry", backoff,
                    "error", err,
                )

                if consecutiveFailures == 6 {
                    go c.register(ctx)
                }
                continue
            }

            if consecutiveFailures > 0 {
                c.logger.Info("manteion connection restored",
                    "missed_polls", consecutiveFailures)
                // Force a full rule pull on the next tick: manteion may
                // have restarted, lost our registration to the reaper, or
                // accumulated changes we missed during the partition.
                // Re-registering with the same instanceID is an idempotent
                // upsert on manteion's side.
                c.ruleVersion.Store(0)
                go c.register(ctx)
            }
            consecutiveFailures = 0
            c.lastPollAt.Store(time.Now().UnixNano())
            c.status.Store(int32(ManteionConnected))
            ticker.Reset(c.cfg.pollInterval)
        }
    }
}
```

### `fetchRules` — uses existing `DecodeCompiledRules`

```go
func (c *ManteionClient) fetchRules(ctx context.Context) error {
    q := url.Values{
        "service":     {c.cfg.serviceName},
        "version":     {strconv.FormatUint(c.ruleVersion.Load(), 10)},
        "instance_id": {c.cfg.instanceID},
    }
    fullURL := c.cfg.url + "/api/v1/sdk/rules?" + q.Encode()

    req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
    if err != nil {
        return fmt.Errorf("build rules request: %w", err)
    }
    resp, err := c.httpClient.Do(req)
    if err != nil {
        return fmt.Errorf("poll failed: %w", err)
    }
    defer resp.Body.Close()

    switch resp.StatusCode {
    case http.StatusNotModified:
        return nil

    case http.StatusOK:
        var payload struct {
            Version uint64         `json:"version"`
            Rules   []CompiledRule `json:"rules"`
        }
        if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
            return fmt.Errorf("decode rules: %w", err)
        }

        staticRules, err := DecodeCompiledRules(payload.Rules)
        if err != nil {
            c.logger.Error("rule conversion failed, keeping stale rules",
                "error", err)
            return nil // manteion is alive, this is a data issue
        }

        c.targets.Evaluator.SetRules(staticRules)
        c.ruleVersion.Store(payload.Version)

        c.logger.Info("rules updated",
            "version", payload.Version,
            "count", len(staticRules))
        return nil

    case http.StatusServiceUnavailable:
        return fmt.Errorf("manteion not ready (503)")

    default:
        body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
        return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
    }
}
```

### Shutdown (nil-receiver safe)

```go
func (c *ManteionClient) Close(ctx context.Context) error {
    if c == nil {
        return nil // offline mode — ConnectManteion returned (nil, nil)
    }
    if c.cancel != nil {
        c.cancel()
    }
    // Wait for the poll loop goroutine to actually exit before tearing
    // down. Otherwise an in-flight poll could outlive Close and race with
    // process shutdown.
    c.wg.Wait()

    // Best-effort deregister — don't fail shutdown for this.
    ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()
    deregisterURL := c.cfg.url + "/api/v1/sdk/register/" + url.PathEscape(c.cfg.instanceID)
    req, err := http.NewRequestWithContext(ctx, http.MethodDelete, deregisterURL, nil)
    if err != nil {
        c.logger.Warn("build deregister request failed", "error", err)
        return nil
    }
    resp, err := c.httpClient.Do(req)
    if err != nil {
        c.logger.Warn("deregister failed", "error", err)
        return nil
    }
    resp.Body.Close()
    return nil
}
```

### Files to create/modify

| File | Action | Phase |
|------|--------|-------|
| `manteion.go` | NEW — `ConnectManteion`, `ManteionOption` types | 2 |
| `manteion_client.go` | NEW — client internals | 2 |
| `health.go` | NEW — SDK health reporting (see below) | 2 |

---

## Phase 3: SDK Health Reporting (atropos-go)

### New SDK health API (new file: `health.go`)

```go
package atropos

// HealthStatus reports the SDK's readiness for service health checks.
type HealthStatus struct {
    Status               string    `json:"status"`
    RuleVersion          uint64    `json:"rule_version"`
    RuleCount            int       `json:"rule_count"`
    ManteionURL          string    `json:"manteion_url,omitempty"`
    LastSuccessfulPollAt time.Time `json:"last_successful_poll_at,omitempty"`
    // StaleFor is the human-readable duration since LastSuccessfulPollAt
    // (e.g. "12s", "2m4s"). Empty if never polled. Useful for ops
    // dashboards — RuleVersion alone doesn't tell you whether the
    // DEGRADED state is 5 seconds old or 5 hours old.
    StaleFor             string    `json:"stale_for,omitempty"`
}

// Health returns the current SDK health status.
func Health() HealthStatus

// Ready returns true if the service should accept traffic.
//   - connected:    yes (normal)
//   - degraded:     yes (stale rules, still functional)
//   - offline:      yes (no manteion configured, dev mode)
//   - disconnected: NO (manteion configured but never connected)
func Ready() bool

// HealthHandler returns an http.Handler that reports SDK health as JSON.
func HealthHandler() http.Handler
```

### Service-side integration

Each service's health handler changes:

```go
func (cs *checkoutService) handleHealth(w http.ResponseWriter, r *http.Request) {
    sdkHealth := atropos.Health()

    status := "SERVING"
    code := http.StatusOK

    switch sdkHealth.Status {
    case "disconnected":
        status = "NOT_READY"
        code = http.StatusServiceUnavailable
    case "degraded":
        status = "DEGRADED"
    }

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(code)
    json.NewEncoder(w).Encode(map[string]any{
        "status":  status,
        "atropos": sdkHealth,
    })
}
```

**Service health state machine:**

```
Service starts
    │
    ▼
/_healthz → 503 NOT_READY  (SDK connecting to manteion)
    │
    │  ConnectManteion succeeds
    ▼
/_healthz → 200 SERVING    (SDK connected, rules loaded)
    │
    │  Manteion becomes unreachable
    ▼
/_healthz → 200 DEGRADED   (SDK using stale rules, still works)
    │
    │  Manteion comes back
    ▼
/_healthz → 200 SERVING    (re-registered, rules refreshed)
```

DEGRADED still returns 200 because the service is functional with stale
rules. Only DISCONNECTED (never connected) returns 503.

---

## Phase 4: Service Integration (service-beds)

> **Pre-requisite:** Verify current state of service-beds before scoping
> this phase. This plan assumes the OTel-only `atropos.Init()` pattern is
> still in place.

Reference integration for one service (`checkoutservice`):

| File | Change |
|------|--------|
| `checkoutService/main.go` | Add `ConnectManteion`, update `handleHealth` |

---

## Phase 5: Push Channel (SSE) — manteion-go

### Design: Hybrid Pull + Push

```
┌──────────────────────────────────────────────────────────────┐
│                  Primary: Pull (always on)                    │
│                                                               │
│  GET /sdk/rules?service=X&version=N every 10s                │
│                                                               │
│  Triple duty:                                                 │
│  1. Rule sync (304 fast-path when no changes)                │
│  2. Health heartbeat (TouchPoll records liveness)            │
│  3. Connectivity check (SDK knows if manteion is up)         │
│                                                               │
│  Works through proxies, LBs, firewalls. No state to manage.  │
├──────────────────────────────────────────────────────────────┤
│              Optional: Push (SSE)                             │
│                                                               │
│  GET /sdk/events?service=X — long-lived event stream         │
│                                                               │
│  Events:                                                      │
│  • "rules_changed"  → SDK triggers immediate poll            │
│  • "cachebox_mode"  → instant mode switch (no 10s delay)     │
│  • "kill_switch"    → disable all rules immediately          │
│                                                               │
│  Push does NOT deliver rule payloads — lightweight            │
│  notifications that tell the SDK "poll now." Keeps push       │
│  simple (no ordering, no exactly-once) and lets poll handle  │
│  the actual data.                                             │
│                                                               │
│  On disconnect: silent fallback to poll-only.                │
└──────────────────────────────────────────────────────────────┘
```

### SSE event format

```
event: rules_changed
data: {"version": 43}

event: cachebox_mode
data: {"service": "frontend", "mode": "replay", "key_strategy": "exact"}

event: kill_switch
data: {"service": "frontend", "reason": "emergency disable"}
```

### EventBroker — per-service fan-out (new file: `internal/api/sse_broker.go`)

```go
package api

import "sync"

type Event struct {
    Type string
    Data string
}

// EventBroker fans out events to per-service SSE subscribers.
// Drop semantics: slow clients miss events rather than blocking the broker.
type EventBroker struct {
    mu      sync.RWMutex
    clients map[string]map[chan Event]struct{} // service → set of channels
}

func NewEventBroker() *EventBroker {
    return &EventBroker{
        clients: make(map[string]map[chan Event]struct{}),
    }
}

func (b *EventBroker) Subscribe(service string) chan Event {
    ch := make(chan Event, 16) // buffered to absorb bursts
    b.mu.Lock()
    if b.clients[service] == nil {
        b.clients[service] = make(map[chan Event]struct{})
    }
    b.clients[service][ch] = struct{}{}
    b.mu.Unlock()
    return ch
}

func (b *EventBroker) Unsubscribe(service string, ch chan Event) {
    b.mu.Lock()
    defer b.mu.Unlock()
    subs, ok := b.clients[service]
    if !ok {
        return
    }
    if _, exists := subs[ch]; !exists {
        return // already unsubscribed; ch is already closed
    }
    delete(subs, ch)
    if len(subs) == 0 {
        delete(b.clients, service)
    }
    // Close under the write lock so it serializes against Broadcast's
    // RLock — no Broadcast can be mid-send when we close here. Broadcast's
    // `select default` already handles slow clients, so no drain loop is
    // needed (the previous draft's `for range ch` would have hung
    // forever, leaking the caller's goroutine).
    close(ch)
}

// Broadcast sends an event to all subscribers for a service.
// Non-blocking: slow clients with full buffers miss the event.
func (b *EventBroker) Broadcast(service string, e Event) {
    b.mu.RLock()
    defer b.mu.RUnlock()
    for ch := range b.clients[service] {
        select {
        case ch <- e:
        default: // drop for slow clients
        }
    }
}

// ClientCount returns the total number of active SSE connections.
func (b *EventBroker) ClientCount() int {
    b.mu.RLock()
    defer b.mu.RUnlock()
    n := 0
    for _, subs := range b.clients {
        n += len(subs)
    }
    return n
}
```

### SSE handler (new file: `internal/api/sse_handler.go`)

Uses `http.NewResponseController` (not `http.Flusher` type assertion).
Sends heartbeat comments every 30s to prevent proxy timeout.
Sets `X-Accel-Buffering: no` for nginx compatibility.

```go
package api

import (
    "fmt"
    "net/http"
    "time"
)

func (s *Server) handleSSEEvents(w http.ResponseWriter, r *http.Request) {
    service := r.URL.Query().Get("service")
    if service == "" {
        writeError(w, http.StatusBadRequest, "service query param required")
        return
    }

    rc := http.NewResponseController(w)

    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")
    w.Header().Set("X-Accel-Buffering", "no") // nginx: disable response buffering
    w.WriteHeader(http.StatusOK)
    if err := rc.Flush(); err != nil {
        return
    }

    ch := s.broker.Subscribe(service)
    defer s.broker.Unsubscribe(service, ch)

    heartbeat := time.NewTicker(30 * time.Second)
    defer heartbeat.Stop()

    for {
        select {
        case <-r.Context().Done():
            return
        case event := <-ch:
            fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, event.Data)
            if err := rc.Flush(); err != nil {
                return
            }
        case <-heartbeat.C:
            fmt.Fprintf(w, ": heartbeat\n\n")
            if err := rc.Flush(); err != nil {
                return // client disconnected
            }
        }
    }
}
```

### Wiring the broker to rule mutations

After a rule CRUD operation commits successfully, broadcast a notification.
`RuleRepo.Create`/`Update`/`Delete` already call `bumpVersion` internally;
the handler reads the new version via `s.rules.Version(ctx)` post-commit:

```go
// In rule handler, after successful Create/Update/Delete:
newVersion, _ := s.rules.Version(r.Context())
s.broker.Broadcast(rule.Service, Event{
    Type: "rules_changed",
    Data: fmt.Sprintf(`{"version":%d}`, newVersion),
})
```

SDKs listening on SSE receive the event and trigger an immediate poll,
bypassing the 10s interval. SDKs not connected to SSE continue on the
normal poll cadence — no behavior change for them.

### Files to create/modify

| File | Action | Phase |
|------|--------|-------|
| `internal/api/sse_broker.go` | NEW — EventBroker | 5 |
| `internal/api/sse_handler.go` | NEW — SSE handler | 5 |
| `internal/api/server.go` | Add `broker *EventBroker` field; register route | 5 |
| `internal/api/rule_handler.go` | Broadcast after mutation | 5 |
| `manteion_client.go` (atropos) | SSE listener alongside poll loop | 5 |

---

## Edge Cases & Failure Handling

### During startup

| Scenario | Behavior | Recovery |
|----------|----------|---------|
| Manteion not started yet | `waitForReady` retries with backoff up to `InitTimeout` (30s) | Pod CrashLoopBackOff → K8s restarts → manteion likely ready |
| Manteion up, DB unreachable | `/sdk/init` → 503, SDK retries | Clears once DB connects |
| Manteion up, migrations pending | `rules.Version()` fails → 503 | Clears once migrations run (<1s) |
| `MANTEION_URL` not set | `ConnectManteion` returns nil (offline mode) | Service runs with tracing only |
| DNS resolution fails | HTTP call fails → retry with backoff | Clears once DNS resolves |
| InitTimeout exceeded | `ConnectManteion` returns error → `log.Fatal` | Pod restarts |

### During healthcheck / poll

> Rows marked *(reaper)* depend on the SDK instance reaper goroutine
> (`docs/plans/2026-04-01-sdk-liveness-reaper.md`), which is planned but
> not yet implemented. Until then, stale instances remain in the registry.

| Scenario | Service /_healthz | SDK behavior | Manteion behavior |
|----------|------------------|-------------|-------------------|
| Normal | 200 SERVING | Poll 10s, 304 fast-path | TouchPoll records liveness |
| Manteion unreachable | 200 DEGRADED | Exponential backoff, stale rules | *(reaper)* marks stale → dead |
| Manteion restarts | 200 DEGRADED → SERVING | Re-registers on first success | Fresh registration |
| Network partition | 200 DEGRADED | Stale rules, backoff | *(reaper)* marks stale after 30s |
| Partition heals | 200 SERVING | Re-registers, fetches latest | Fresh registration + TouchPoll |
| Malformed rules | 200 SERVING | Logs error, keeps last-good rules | Bug in manteion |
| DB fails after boot | 200 DEGRADED | Poll gets 500 → retry | `/readyz` → 503, K8s stops routing |
| Pod killed (no graceful shutdown) | N/A | No deregister sent | *(reaper)* deregisters after 120s |
| Pod graceful shutdown | N/A | `Close()` → DELETE /sdk/register | Immediate removal |
| Clock skew | No impact | All times from manteion DB clock | N/A |
| Stale intent for churned service | N/A | New pods get intent from `Apply()` | IntentTracker has state but no live instances; *(reaper)* clears after 120s |

### During rule updates

| Scenario | Behavior |
|----------|----------|
| Rule CRUD in manteion | `bumpVersion()` increments; next SDK poll gets new version |
| Push channel connected | `rules_changed` SSE event → immediate poll |
| Push channel disconnected | Silent fallback to poll-only |
| Concurrent rule edits | Version is monotonic; SDK gets consistent snapshot per poll |
| Rule references bad FaultSpec | Manteion validates at write time → HTTP 400; never reaches SDK |
| Incompatible composition | Manteion validates at write time (`composition_validate.go`) |
| Zero rules for a service | Empty array → evaluator has no rules → no faults injected |

---

## Testing Strategy

### Go 1.24/1.25 features to use

| Feature | Where | Replaces |
|---------|-------|----------|
| `slog.DiscardHandler` | All test loggers | `slog.New(slog.NewTextHandler(io.Discard, nil))` |
| `t.Context()` | Test contexts needing auto-cancel | Manual `context.WithCancel` in tests |
| `sync.WaitGroup.Go(fn)` | EventBroker fan-out, poll loop tests | `wg.Add(1); go func() { defer wg.Done(); ... }()` |
| `synctest.Test(t, fn)` | Poll loop backoff tests, ManteionClient lifecycle | Real `time.Sleep`-based tests that are slow and flaky |
| `t.Output()` | Plugging test loggers into slog | Custom io.Writer wiring |

### Phase 1 tests (manteion-go)

```bash
go test ./internal/api/... -v -run TestHandleInit
```

- `TestHandleInit_AllGatesPass` — DB + rule version OK → 200
- `TestHandleInit_DBFails` — DB ping fails → 503
- `TestHandleInit_RuleStoreUninit` — `Version()` fails → 503

### Phase 2 tests (atropos-go)

```bash
go test ./ -run TestManteion -v
```

- `TestManteionClient_WaitForReady_Success` — httptest returns 200
- `TestManteionClient_WaitForReady_Retry` — 503 three times then 200
- `TestManteionClient_WaitForReady_Timeout` — never ready → error
- `TestManteionClient_PollLoop_304` — no change → 304 path
- `TestManteionClient_PollLoop_RuleUpdate` — new version → evaluator swapped
- `TestManteionClient_PollLoop_BackoffOnFailure` — verify exponential backoff (**use `synctest.Test` with fake time**)
- `TestManteionClient_PollLoop_Recovery` — server returns → re-registers

### Phase 3 tests (atropos-go)

- `TestHealth_Status_AllStates` — connected, degraded, disconnected, offline

### Phase 5 tests (manteion-go)

- `TestEventBroker_Subscribe_Receive` — single subscriber gets event
- `TestEventBroker_Broadcast_MultipleSubscribers` — fan-out to N clients
- `TestEventBroker_SlowClient_Dropped` — full buffer → event dropped, not blocked
- `TestEventBroker_Unsubscribe_Cleanup` — no goroutine leak after unsubscribe
- `TestSSEHandler_StreamsEvents` — httptest with event verification
- `TestSSEHandler_Heartbeat` — receives heartbeat within 30s (**use `synctest.Test`**)
- `TestSSEHandler_ClientDisconnect` — context cancellation cleans up

### Manual integration test

1. `docker-compose up -d` → start postgres
2. `go run ./cmd/manteion` → start manteion
3. Verify health:
   - `curl localhost:8080/healthz` → 200
   - `curl localhost:8080/readyz` → 200
   - `curl localhost:8080/api/v1/sdk/init` → 200 with `{"status":"ready"}`
   - `curl localhost:8080/api/v1/status` → includes `db_healthy: true`
4. Kill postgres:
   - `curl localhost:8080/healthz` → 200 (still alive)
   - `curl localhost:8080/readyz` → 503
   - `curl localhost:8080/api/v1/sdk/init` → 503
5. Start checkoutService with `MANTEION_URL=http://localhost:8080`
6. Verify logs: "manteion ready", "rules updated"
7. `curl localhost:5050/_healthz` → `{"status":"SERVING","atropos":{"status":"connected"}}`
8. Kill manteion, wait 15s:
   `curl localhost:5050/_healthz` → `{"status":"DEGRADED","atropos":{"status":"degraded"}}`
9. Restart manteion → checkoutService logs "connection restored" + re-registers
10. `curl localhost:5050/_healthz` → back to SERVING

---

## Implementation Phases Summary

| Phase | Repo | New files | Modified files | Depends on |
|-------|------|-----------|----------------|------------|
| 1 | manteion-go | — | `sdk_handler.go`, `health_handler.go` | — |
| 2 | atropos-go | `manteion.go`, `manteion_client.go` | — | Phase 1 |
| 3 | atropos-go | `health.go` | — | Phase 2 |
| 4 | service-beds | — | `checkoutService/main.go` | Phase 2 + 3 |
| 5 | manteion-go + atropos-go | `sse_broker.go`, `sse_handler.go` | `server.go`, `rule_handler.go`, `manteion_client.go` | Phase 2 |

---

## Open Questions

1. **Fatal vs. warn on manteion boot failure?**
   - **(A)** `log.Fatal` — no service runs without centralized control (recommended for prod)
   - **(B)** `log.Warn` — resilient but runs blind (current OTel pattern)
   - Proposal: **(A)** for production, **(B)** available via `WithOfflineMode()`

2. **InitTimeout default — 30s enough?**
   - Cold start (Skaffold dev loop): Postgres + manteion takes 15-20s
   - CI with resource limits: might be tighter
   - Proposal: 30s default, configurable via `MANTEION_INIT_TIMEOUT` env

3. **SSE push — MVP or Phase 5?**
   - Poll-based has up to 10s latency. Acceptable for rule changes.
   - Experiment mode changes (cache-box switch) may need sub-second delivery.
   - Proposal: defer to Phase 5; poll with 10s interval is fine for MVP

---

## References

- `manteion-go/docs/plans/2026-04-16-manteion-atropos-client.md` — Admin client design (implemented)
- `manteion-go/docs/plans/2026-04-19-atropos-client-followups.md` — Followups implementation (completed)
- `atropos-go/register.go` — `Register()`, `Apply()`, `ApplyTargets`
- `atropos-go/compiled_rule.go` — `CompiledRule`, `DecodeCompiledRules`
- `manteion-go/internal/ruleconv/ruleconv.go` — `CompileRules`
- `manteion-go/internal/api/sdk_handler.go` — `handlePollRules`, `handleInit`
