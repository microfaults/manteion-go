# Health Check & SDK Bootstrap Protocol

> **Date:** 2026-04-22
> **Status:** Proposed
> **Scope:** manteion-go (control plane), atropos-go (SDK), service-beds (integration)

## Table of Contents

- [Context](#context)
- [Ecosystem Overview](#ecosystem-overview)
- [Architecture: Rule Builder vs Rule Evaluator](#architecture-rule-builder-vs-rule-evaluator)
- [Startup Dependency Chain](#startup-dependency-chain)
- [Manteion Health Endpoints](#manteion-health-endpoints)
- [Atropos SDK — ManteionClient](#atropos-sdk--manteionclient)
- [Service Health Integration](#service-health-integration)
- [Push vs Pull Rule Delivery](#push-vs-pull-rule-delivery)
- [Hydrated Rule Format](#hydrated-rule-format)
- [Edge Cases & Failure Handling](#edge-cases--failure-handling)
- [Implementation Phases](#implementation-phases)
- [Files to Modify/Create](#files-to-modifycreate)
- [Verification Plan](#verification-plan)
- [Open Questions](#open-questions)

---

## Context

The atropos ecosystem has a hard startup dependency: each service's embedded
atropos SDK must confirm manteion is alive, register itself, and fetch initial
rules before the service can serve traffic. After boot, the SDK periodically
polls manteion for rule updates — and that same poll doubles as a liveness
heartbeat.

**Today the scaffolding has the right endpoints but lacks:**

1. **Manteion**: real readiness semantics (the handlers always return 200)
2. **Atropos SDK**: any manteion-aware init logic (Init only sets up OTel)
3. **Service health**: no dependency on atropos SDK state
4. **Edge-case handling**: nothing for manteion restarts, partitions, DB failures

This document specifies the complete protocol for all three layers.

---

## Ecosystem Overview

This plan sits within the broader project structure:

| Component | Responsibility | Status |
|-----------|---------------|--------|
| **zeus** (end-to-end) | Load generation (k6 + vegeta via Archer) | Scaffolded |
| **manteion** — atrocontrol | SDK registration, health tracking, rule building, push/pull config | **This plan** |
| **manteion** — experiment orch | Baseline/isolation/combination run coordination | Future |
| **atropos** — cache fidelity | Cache-box mode correctness, replay validation | Implemented |
| **atropos** — response recorder | Record/replay for cache-box experiments | Implemented |
| **atropos** — rule engine + evaluator + gating | Runtime rule evaluation, fault dispatch, startup gating | **This plan** |
| **manteion-ui** | UI/UX for rule management, experiment control, QA | Future |
| **grafana + prometheus** | Experiment observability, repeatability, result storage | Future |
| **atropos** — faulting fidelity | Inline/network/resource fault correctness | Implemented |

---

## Architecture: Rule Builder vs Rule Evaluator

**Core principle: Rule Builder → Manteion. Rule Evaluator → Atropos.**

```
┌───────────────────────────────────────────────────────────────────────┐
│                    Manteion (Control Plane)                           │
│                                                                       │
│  ┌─────────────┐    ┌────────────┐    ┌──────────────────────┐       │
│  │ Rule Builder │───▶│ Rule Store │───▶│ Hydrator              │       │
│  │ CRUD + Valid │    │ (Postgres) │    │ Join Rule + FaultSpec │       │
│  │ + Composition│    │            │    │ into SDK-ready payload│       │
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
│  │ Manteion Client  │───▶│ Rule Evaluator    │───▶│ Interceptor   │     │
│  │ Poll + Push      │    │ StaticEvaluator   │    │ Fault dispatch│     │
│  │ listener          │    │ .SetRules()       │    │ + CacheBox    │     │
│  └─────────────────┘    └──────────────────┘    └──────────────┘     │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

**What each side owns:**

### Manteion (rule builder)

- `Rule` CRUD — create, read, update, delete rules
- `FaultSpec` definitions — atomic fault configurations
- `FaultComposition` — parallel/sequential grouping with depth limits
- `MatchCriteria` — service, injection point, label predicates
- Validation — incompatibility checks, mode constraints
- Versioning — monotonic `rule_version` counter bumped on every mutation
- **Hydration** — resolves `fault_spec_id` references into inline configs
  before sending to the SDK

### Atropos (rule evaluator)

- `StaticEvaluator` — runtime rule matching (injection point + label AND match)
- `Interceptor` — fault dispatch, cache-box dispatch, OTel span creation
- Fault instantiation — converts `HydratedFault` config into concrete `Fault`
  interface implementations (`inline.Latency`, `inline.Error`, etc.)
- Hot-swap — `SetRules()` atomically replaces rules without request interruption

**The SDK never calls the rule CRUD API.** It only consumes the read-only
`/sdk/rules` (poll) and `/sdk/events` (push) endpoints.

---

## Startup Dependency Chain

```
┌─────────────┐         ┌──────────────────┐         ┌─────────────────────┐
│  Manteion   │────────▶│  Atropos SDK     │────────▶│  Service Ready      │
│  /healthz   │         │  ConnectManteion │         │  /_healthz → 200    │
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
   │────────────────────────────▶ 201 registered            │
   │                            │                           │
   │◀───────────────────────────│ GET /sdk/rules            │
   │                            │ ?service=X&version=0      │
   │────────────────────────────▶ 200 {version:1, rules:[…]}│
   │                            │                           │
   │                            │ Configure(evaluator)      │
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

## Manteion Health Endpoints

### Current state

| Endpoint | Current | Problem |
|----------|---------|---------|
| `GET /healthz` | Always 200 | ✅ Correct for liveness |
| `GET /readyz` | Always 200 | ❌ Should check DB |
| `GET /api/v1/sdk/init` | Always 200 | ❌ Should verify DB + rule store |
| `GET /api/v1/status` | Basic counts | ⚠️ Missing DB health, instance breakdown |

### Proposed changes

#### `GET /healthz` — Liveness (no change)

Kubernetes liveness probe. Returns 200 if the process can serve HTTP —
that's all it should check. If the process is stuck, K8s restarts it.

```json
{"status": "ok"}
```

#### `GET /readyz` — Readiness (add DB check)

Kubernetes readiness probe. Gates when K8s routes traffic to this pod.

```go
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
    if err := s.db.PingContext(r.Context()); err != nil {
        s.logger.Warn("readyz: db unreachable", "error", err)
        writeJSON(w, http.StatusServiceUnavailable, map[string]string{
            "status": "not_ready",
            "reason": "database unreachable",
        })
        return
    }
    writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
```

- 200 → DB is reachable, pod can serve API traffic
- 503 → DB unreachable, K8s removes pod from Service endpoints

#### `GET /api/v1/sdk/init` — SDK startup gate (add DB + rule store)

This is the endpoint the atropos SDK hits before registering. It needs to
verify that manteion is fully initialized, not just listening.

```go
func (s *Server) handleInit(w http.ResponseWriter, r *http.Request) {
    // Gate 1: Database connection alive
    if err := s.db.PingContext(r.Context()); err != nil {
        writeJSON(w, http.StatusServiceUnavailable, map[string]string{
            "status": "not_ready",
            "reason": "database unreachable",
        })
        return
    }

    // Gate 2: Rule store tables exist and are queryable (migrations ran)
    if _, err := s.rules.Version(r.Context()); err != nil {
        writeJSON(w, http.StatusServiceUnavailable, map[string]string{
            "status": "not_ready",
            "reason": "rule store not initialized",
        })
        return
    }

    writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
```

**Failure scenarios:**

| Condition | Status | SDK Behavior |
|-----------|--------|-------------|
| Manteion not started | Connection refused | SDK retries |
| Manteion started, DB unreachable | 503 | SDK retries |
| Manteion started, DB up, migrations pending | 503 | SDK retries |
| Manteion fully ready | 200 | SDK proceeds to register |

#### `GET /api/v1/status` — Operational overview (enriched)

```go
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()

    dbHealthy := s.db.PingContext(ctx) == nil

    ruleCount := 0
    if rules, err := s.rules.List(ctx); err == nil {
        ruleCount = len(rules)
    }

    instanceCount := 0
    if count, err := s.sdk.Count(ctx); err == nil {
        instanceCount = count
    }

    writeJSON(w, http.StatusOK, map[string]any{
        "status":         "ok",
        "db_healthy":     dbHealthy,
        "rules":          ruleCount,
        "instances":      instanceCount,
        "zeus_reachable": s.zeus.Healthy(ctx),
    })
}
```

### Server struct change

The `Server` struct needs a `*sql.DB` reference for direct DB health checks:

```go
type Server struct {
    logger      *slog.Logger
    db          *sql.DB        // <-- new: for health checks
    rules       *store.RuleRepo
    faults      *store.FaultRepo
    sdk         *store.SDKRepo
    experiments *store.ExperimentRepo
    workloads   *store.WorkloadRepo
    policies    *store.PolicyRepo
    traces      *store.TraceRepo
    zeus        *zeus.Client
}
```

`NewServer` gains a `db *sql.DB` parameter. `cmd/manteion/main.go` passes the
`database` variable through.

---

## Atropos SDK — ManteionClient

### Design goal: one-line integration

```go
// Current pattern (no manteion awareness)
shutdown, _ := atropos.Init(ctx, atropos.WithServiceName("checkout"))
defer shutdown(ctx)

// New pattern (manteion-aware)
shutdown, _ := atropos.Init(ctx, atropos.WithServiceName("checkout"))
defer shutdown(ctx)

mc := atropos.ConnectManteion(ctx, "checkout")
defer mc.Close(ctx)
// ↑ Handles: waitForReady → register → fetchRules → Configure(evaluator) → poll loop
```

### Public API (new file: `manteion.go`)

```go
package atropos

// ConnectManteion connects this SDK to the manteion control plane.
//
// Blocks until manteion is ready, registers the instance, fetches initial
// rules, configures the evaluator, and starts a background poll loop.
//
// URL resolution: opts > MANTEION_URL env > nil (offline mode).
// Returns nil if no URL is set (backward compatible, tracing-only mode).
func ConnectManteion(ctx context.Context, serviceName string, opts ...ManteionOption) *ManteionClient

// ManteionOption configures ConnectManteion.
type ManteionOption interface { applyManteion(*manteionConfig) }

func WithManteionURL(url string) ManteionOption     // override URL
func WithInstanceID(id string) ManteionOption        // override hostname
func WithInitTimeout(d time.Duration) ManteionOption // default: 30s
func WithPollInterval(d time.Duration) ManteionOption // default: 10s
func WithOfflineMode() ManteionOption                // skip manteion entirely
```

### Client internals (new file: `manteion_client.go`)

```go
type ManteionClient struct {
    cfg         manteionConfig
    httpClient  *http.Client
    evaluator   *StaticEvaluator  // hot-swappable evaluator
    ruleVersion uint64
    status      atomic.Int32      // 0=disconnected, 1=connected, 2=degraded
    cancel      context.CancelFunc
    logger      *slog.Logger
}

type ManteionStatus int
const (
    ManteionDisconnected ManteionStatus = iota  // never connected or init failed
    ManteionConnected                            // healthy, rules up to date
    ManteionDegraded                             // poll failing, using stale rules
)
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
        resp, err := c.httpClient.Get(c.cfg.url + "/api/v1/sdk/init")
        if err == nil {
            resp.Body.Close()
            if resp.StatusCode == http.StatusOK {
                c.logger.Info("manteion ready", "attempts", attempt)
                return nil
            }
            // 503 = alive but not ready (DB still connecting) → keep retrying
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

### Registration

```go
func (c *ManteionClient) register(ctx context.Context) error {
    body := map[string]string{
        "id":      c.cfg.instanceID,
        "service": c.cfg.serviceName,
        "version": c.cfg.serviceVersion,
        "address": c.cfg.address,
    }
    payload, _ := json.Marshal(body)

    resp, err := c.httpClient.Post(
        c.cfg.url+"/api/v1/sdk/register",
        "application/json",
        bytes.NewReader(payload),
    )
    if err != nil {
        return fmt.Errorf("register: %w", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusCreated {
        return fmt.Errorf("register: unexpected status %d", resp.StatusCode)
    }
    return nil
}
```

### Poll loop (background goroutine)

Every poll to `GET /api/v1/sdk/rules` serves **three purposes simultaneously**:

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

                // Exponential backoff: 10s → 20s → 40s → ... → 60s cap
                backoff := c.cfg.pollInterval * time.Duration(1<<min(consecutiveFailures, 6))
                backoff = min(backoff, 60*time.Second)
                ticker.Reset(backoff)

                c.logger.Warn("manteion poll failed, continuing with stale rules",
                    "failures", consecutiveFailures,
                    "next_retry", backoff,
                    "error", err,
                )

                // After ~10 min of failures, try re-registering
                // (manteion may have restarted and lost our registration)
                if consecutiveFailures == 6 {
                    go c.register(ctx)
                }
                continue
            }

            // Success — restore normal state
            if consecutiveFailures > 0 {
                c.logger.Info("manteion connection restored",
                    "missed_polls", consecutiveFailures)
                go c.register(ctx) // re-register after outage
            }
            consecutiveFailures = 0
            c.status.Store(int32(ManteionConnected))
            ticker.Reset(c.cfg.pollInterval)
        }
    }
}
```

### `fetchRules` — dual-purpose poll

```go
func (c *ManteionClient) fetchRules(ctx context.Context) error {
    url := fmt.Sprintf("%s/api/v1/sdk/rules?service=%s&version=%d&instance_id=%s",
        c.cfg.url, c.cfg.serviceName, c.ruleVersion, c.cfg.instanceID)

    req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
    resp, err := c.httpClient.Do(req)
    if err != nil {
        return fmt.Errorf("poll failed: %w", err)
    }
    defer resp.Body.Close()

    switch resp.StatusCode {
    case http.StatusNotModified:
        // Rules unchanged — manteion is alive, heartbeat recorded
        return nil

    case http.StatusOK:
        var payload struct {
            Version uint64         `json:"version"`
            Rules   []HydratedRule `json:"rules"`
        }
        if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
            return fmt.Errorf("decode rules: %w", err)
        }

        // Convert manteion's hydrated rules → atropos StaticRules
        staticRules, err := convertHydratedRules(payload.Rules)
        if err != nil {
            // Bad rules from manteion — log but don't discard good stale rules
            c.logger.Error("rule conversion failed, keeping stale rules",
                "error", err)
            return nil // not a poll failure (manteion is alive)
        }

        // Atomic swap — evaluator is safe for concurrent reads
        c.evaluator.SetRules(staticRules)
        c.ruleVersion = payload.Version

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

### Shutdown

```go
func (c *ManteionClient) Close(ctx context.Context) error {
    // Stop poll loop
    if c.cancel != nil {
        c.cancel()
    }

    // Best-effort deregister
    req, _ := http.NewRequestWithContext(ctx, http.MethodDelete,
        c.cfg.url+"/api/v1/sdk/register/"+c.cfg.instanceID, nil)
    resp, err := c.httpClient.Do(req)
    if err != nil {
        c.logger.Warn("deregister failed", "error", err)
        return nil // don't fail shutdown for this
    }
    resp.Body.Close()
    return nil
}
```

---

## Service Health Integration

**Key insight: each service's healthcheck should cascade atropos SDK state.**

Currently every service's `/_healthz` always returns 200. After this change,
the handler reflects the SDK's manteion connectivity.

### New SDK health API (new file: `health.go`)

```go
package atropos

// HealthStatus reports the SDK's readiness for service health checks.
type HealthStatus struct {
    Status      string `json:"status"`        // connected, degraded, disconnected, offline
    RuleVersion uint64 `json:"rule_version"`
    RuleCount   int    `json:"rule_count"`
    ManteionURL string `json:"manteion_url,omitempty"`
}

// Health returns the current SDK health status.
func Health() HealthStatus

// Ready returns true if the service should accept traffic.
//   - connected: yes (normal)
//   - degraded:  yes (stale rules, still functional)
//   - offline:   yes (no manteion configured, dev mode)
//   - disconnected: NO (manteion configured but never connected)
func Ready() bool

// HealthHandler returns an http.Handler that reports SDK health as JSON.
func HealthHandler() http.Handler
```

### Service-side integration

Each service's health handler changes:

```go
// BEFORE (current)
func (cs *checkoutService) handleHealth(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"status": "SERVING"})
}

// AFTER (atropos-aware)
func (cs *checkoutService) handleHealth(w http.ResponseWriter, r *http.Request) {
    sdkHealth := atropos.Health()

    status := "SERVING"
    code := http.StatusOK

    switch sdkHealth.Status {
    case "disconnected":
        // Manteion was configured but SDK never connected
        status = "NOT_READY"
        code = http.StatusServiceUnavailable
    case "degraded":
        // Manteion unreachable, using stale rules — operational but warn
        status = "DEGRADED"
        // code stays 200 — service CAN handle traffic
    case "offline":
        // No manteion configured (dev mode)
        status = "SERVING"
    case "connected":
        status = "SERVING"
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

The key point: **DEGRADED still returns 200** because the service is
functional with stale rules. Only DISCONNECTED (never connected) returns 503.

---

## Push vs Pull Rule Delivery

### Analysis

| | **Pull (polling)** | **Push (SSE)** |
|---|---|---|
| Mechanism | `GET /sdk/rules?version=N` every 10s | `GET /sdk/events` — long-lived stream |
| Latency | Up to 10s for rule changes | Sub-second |
| Complexity | Simple — stateless HTTP, 304 fast path | Medium — connection management, reconnect |
| Firewall-friendly | Yes | Yes (SSE is HTTP, no WebSocket upgrade) |
| Failure mode | SDK retries next tick | Falls back to polling on disconnect |
| When needed | Always (baseline) | Experiment mode changes, kill switch |

### Recommended: hybrid approach

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
│              Optional: Push (SSE, Phase 5)                    │
│                                                               │
│  GET /sdk/events?service=X — long-lived event stream         │
│                                                               │
│  Events:                                                      │
│  • "rules_changed"  → SDK triggers immediate poll            │
│  • "cachebox_mode"  → instant mode switch (no 10s delay)     │
│  • "kill_switch"    → disable all rules immediately          │
│                                                               │
│  Push does NOT deliver rule payloads — it sends lightweight  │
│  notifications that tell the SDK "poll now." This keeps push │
│  simple (no ordering, no exactly-once) and lets poll handle  │
│  the actual data.                                             │
│                                                               │
│  On disconnect: silent fallback to poll-only.                │
└──────────────────────────────────────────────────────────────┘
```

### SSE event format (Phase 5)

```
event: rules_changed
data: {"version": 43}

event: cachebox_mode
data: {"service": "frontend", "mode": "replay", "key_strategy": "exact"}

event: kill_switch
data: {"service": "frontend", "reason": "emergency disable"}
```

### SSE handler sketch (manteion-side)

```go
func (s *Server) handleSSEEvents(w http.ResponseWriter, r *http.Request) {
    service := r.URL.Query().Get("service")
    flusher, ok := w.(http.Flusher)
    if !ok {
        writeError(w, http.StatusInternalServerError, "streaming not supported")
        return
    }

    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")
    w.WriteHeader(http.StatusOK)
    flusher.Flush()

    ch := s.ruleNotifier.Subscribe(service)
    defer s.ruleNotifier.Unsubscribe(ch)

    for {
        select {
        case <-r.Context().Done():
            return
        case event := <-ch:
            fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, event.Data)
            flusher.Flush()
        }
    }
}
```

The SSE push channel is deferred to Phase 5. The poll-based approach is
sufficient for MVP — experiment orchestration can tolerate 10s latency.

---

## Hydrated Rule Format

This is the bridge between manteion's rule builder and atropos's evaluator.
Manteion resolves all `fault_spec_id` references and sends flat, self-contained
rule objects.

### Wire format (JSON)

```json
{
  "version": 7,
  "rules": [
    {
      "id": "rule-abc123",
      "name": "latency-on-checkout-ingress",
      "service": "checkoutservice",
      "enabled": true,
      "priority": 10,
      "match": {
        "injection_point": "ingress",
        "labels": {"http.path": "/placeorder"}
      },
      "mode": "inline",
      "fault": {
        "category": "inline",
        "fault_type": "latency",
        "config": {"delay_ms": 500, "jitter_ms": 100},
        "duration_ms": 600
      }
    },
    {
      "id": "rule-def456",
      "name": "cachebox-replay-frontend-egress",
      "service": "frontend",
      "priority": 5,
      "match": {
        "injection_point": "egress",
        "labels": {"http.host": "productcatalogservice:3550"}
      },
      "mode": "inline",
      "cachebox": "replay"
    }
  ]
}
```

### Go type (atropos-go side)

```go
// HydratedRule is the SDK-consumable format sent by manteion.
type HydratedRule struct {
    ID       string        `json:"id"`
    Name     string        `json:"name"`
    Service  string        `json:"service"`
    Priority int           `json:"priority"`
    Match    HydratedMatch `json:"match"`
    Mode     string        `json:"mode"`                // "inline" | "background"
    Fault    *HydratedFault `json:"fault,omitempty"`
    CacheBox string        `json:"cachebox,omitempty"` // "passthrough"|"replay"|"replay_with_delay"
}

type HydratedMatch struct {
    InjectionPoint string            `json:"injection_point"` // ingress|egress|transient|custom|""
    Labels         map[string]string `json:"labels"`
}

type HydratedFault struct {
    Category   string          `json:"category"`    // inline|network|resource
    FaultType  string          `json:"fault_type"`  // latency|error|hang|blackhole|cpu|...
    Config     json.RawMessage `json:"config"`      // type-specific parameters
    DurationMs int64           `json:"duration_ms"`
}
```

### Conversion function

```go
func convertHydratedRules(rules []HydratedRule) ([]StaticRule, error) {
    out := make([]StaticRule, 0, len(rules))
    for _, r := range rules {
        sr := StaticRule{
            Name:   r.Name,
            Point:  parseInjectionPoint(r.Match.InjectionPoint),
            Labels: r.Match.Labels,
        }
        sr.Decision.Mode = parseMode(r.Mode)
        sr.Decision.CacheBox = parseCacheBoxAction(r.CacheBox)

        if r.Fault != nil {
            f, err := buildFaultFromConfig(r.Fault)
            if err != nil {
                return nil, fmt.Errorf("rule %q: %w", r.Name, err)
            }
            sr.Decision.Fault = f
        }
        out = append(out, sr)
    }
    return out, nil
}

func buildFaultFromConfig(hf *HydratedFault) (Fault, error) {
    dur := time.Duration(hf.DurationMs) * time.Millisecond

    switch hf.Category + ":" + hf.FaultType {
    case "inline:latency":
        var cfg struct {
            DelayMs  int64 `json:"delay_ms"`
            JitterMs int64 `json:"jitter_ms"`
        }
        json.Unmarshal(hf.Config, &cfg)
        return NewLatencyFault(
            time.Duration(cfg.DelayMs)*time.Millisecond,
            time.Duration(cfg.JitterMs)*time.Millisecond,
        ), nil

    case "inline:error":
        var cfg struct {
            StatusCode int    `json:"status_code"`
            Message    string `json:"message"`
        }
        json.Unmarshal(hf.Config, &cfg)
        return NewErrorFault(cfg.StatusCode, cfg.Message), nil

    case "inline:hang":
        return NewHangFault(dur), nil

    // Add network/resource fault constructors as they're implemented

    default:
        return nil, fmt.Errorf("unknown fault type: %s:%s", hf.Category, hf.FaultType)
    }
}
```

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

| Scenario | Service /_healthz | SDK behavior | Manteion behavior |
|----------|------------------|-------------|-------------------|
| Normal | 200 SERVING | Poll 10s, 304 fast-path | TouchPoll records liveness |
| Manteion unreachable | 200 DEGRADED | Exponential backoff, stale rules | Reaper marks stale → dead |
| Manteion restarts | 200 DEGRADED → SERVING | Re-registers on first success | Fresh registration |
| Network partition | 200 DEGRADED | Stale rules, backoff | Reaper marks stale after 30s |
| Partition heals | 200 SERVING | Re-registers, fetches latest | Fresh registration + TouchPoll |
| Malformed rules | 200 SERVING | Logs error, keeps last-good rules | Bug in manteion |
| DB fails after boot | 200 DEGRADED | Poll gets 500 → retry | `/readyz` → 503, K8s stops routing |
| Pod killed (no graceful shutdown) | N/A | No deregister sent | Reaper deregisters after 120s |
| Pod graceful shutdown | N/A | `Close()` → DELETE /sdk/register | Immediate removal |
| Clock skew | No impact | All times from manteion DB clock | N/A |

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

## Implementation Phases

### Phase 1: Manteion health (manteion-go) — No breaking changes

| # | File | Change |
|---|------|--------|
| 1 | `internal/api/server.go` | Add `db *sql.DB` to Server struct, update `NewServer` |
| 2 | `internal/api/health_handler.go` | Real DB ping in `handleReadyz` and `handleInit`; enriched `handleStatus` |
| 3 | `cmd/manteion/main.go` | Pass `database` to `NewServer` |

### Phase 2: Atropos ManteionClient (atropos-go) — New files, no breaking changes

| # | File | Change |
|---|------|--------|
| 4 | `manteion.go` [NEW] | `ConnectManteion`, `ManteionOption` types |
| 5 | `manteion_client.go` [NEW] | Client internals: waitForReady, register, pollLoop, fetchRules, Close |
| 6 | `manteion_types.go` [NEW] | `HydratedRule`, `HydratedFault`, `convertHydratedRules`, `buildFaultFromConfig` |
| 7 | `health.go` [NEW] | `Health()`, `HealthHandler()`, `Ready()` |

### Phase 3: Manteion rule hydration (manteion-go) — Enriched poll response

| # | File | Change |
|---|------|--------|
| 8 | `internal/api/sdk_handler.go` | Hydrate rules with FaultSpec in `handlePollRules` |
| 9 | `internal/store/rule_repo.go` | Add `ForServiceHydrated()` — JOINs rules ↔ fault_specs |

### Phase 4: Service integration (service-beds) — Reference service

| # | File | Change |
|---|------|--------|
| 10 | `checkoutService/main.go` | Add `ConnectManteion`, update `handleHealth` |

### Phase 5: Push channel + experiment readiness (future)

| # | File | Change |
|---|------|--------|
| 11 | `internal/api/sse.go` [NEW] | SSE push handler + RuleNotifier |
| 12 | `manteion_client.go` | SSE listener alongside poll loop |
| 13 | Experiment pre-flight gate | Check alive instances before launching runs |

---

## Files to Modify/Create

### manteion-go

| File | Action | Phase |
|------|--------|-------|
| `internal/api/server.go` | MODIFY — add `db *sql.DB` field | 1 |
| `internal/api/health_handler.go` | MODIFY — real health checks | 1 |
| `cmd/manteion/main.go` | MODIFY — pass `database` | 1 |
| `internal/api/sdk_handler.go` | MODIFY — hydrated poll response | 3 |
| `internal/store/rule_repo.go` | MODIFY — add `ForServiceHydrated()` | 3 |
| `internal/api/sse.go` | NEW — SSE push handler | 5 |

### atropos-go

| File | Action | Phase |
|------|--------|-------|
| `manteion.go` | NEW — public API (ConnectManteion) | 2 |
| `manteion_client.go` | NEW — client internals | 2 |
| `manteion_types.go` | NEW — HydratedRule, conversion | 2 |
| `health.go` | NEW — SDK health reporting | 2 |

### service-beds

| File | Action | Phase |
|------|--------|-------|
| `checkoutService/main.go` | MODIFY — ConnectManteion + health | 4 |

---

## Verification Plan

### Automated tests

**Manteion-go (Phase 1):**

```bash
go build ./...
go vet ./...
go test ./internal/api/... -v
```

- `TestHandleReadyz_DBHealthy` — mock DB ping success → 200
- `TestHandleReadyz_DBUnreachable` — mock DB ping failure → 503
- `TestHandleInit_AllGatesPass` — DB + rule version OK → 200
- `TestHandleInit_DBFails` — DB ping fails → 503
- `TestHandleInit_RuleStoreUninit` — Version() fails → 503
- `TestHandleStatus_Enriched` — verify all fields present

**Atropos-go (Phase 2):**

```bash
go build ./...
go test ./ -run TestManteion -v
```

- `TestManteionClient_WaitForReady_Success` — httptest returns 200
- `TestManteionClient_WaitForReady_Retry` — 503 three times then 200
- `TestManteionClient_WaitForReady_Timeout` — never ready → error
- `TestManteionClient_PollLoop_304` — no change → 304 path
- `TestManteionClient_PollLoop_RuleUpdate` — new version → evaluator swapped
- `TestManteionClient_PollLoop_BackoffOnFailure` — verify exponential backoff
- `TestManteionClient_PollLoop_Recovery` — server returns → re-registers
- `TestConvertHydratedRules_AllTypes` — latency, error, hang, cachebox
- `TestHealth_Status_AllStates` — connected, degraded, disconnected, offline

### Manual integration test

1. Start Postgres: `docker-compose up -d` (manteion-go)
2. Start manteion: `go run ./cmd/manteion`
3. Create a rule via curl:
   ```bash
   curl -X POST localhost:8080/api/v1/rules \
     -d '{"name":"test","service":"checkout","enabled":true,...}'
   ```
4. Verify health endpoints:
   - `curl localhost:8080/healthz` → 200
   - `curl localhost:8080/readyz` → 200
   - `curl localhost:8080/api/v1/sdk/init` → 200
   - `curl localhost:8080/api/v1/status` → 200 with `db_healthy: true`
5. Kill Postgres:
   - `curl localhost:8080/healthz` → 200 (still alive)
   - `curl localhost:8080/readyz` → 503
   - `curl localhost:8080/api/v1/sdk/init` → 503
6. Start checkoutService with `MANTEION_URL=http://localhost:8080`
7. Verify logs: "manteion ready", "rules updated"
8. `curl localhost:5050/_healthz` → `{"status":"SERVING","atropos":{"status":"connected"}}`
9. Kill manteion, wait 15s:
   `curl localhost:5050/_healthz` → `{"status":"DEGRADED","atropos":{"status":"degraded"}}`
10. Restart manteion → checkoutService logs "connection restored" + re-registers
11. `curl localhost:5050/_healthz` → back to SERVING

---

## Open Questions

1. **Fatal vs. warn on manteion boot failure?**
   - **(A)** `log.Fatal` — no service runs without centralized control (recommended for prod)
   - **(B)** `log.Warn` — resilient but runs blind (current OTel pattern)
   - Proposal: **(A)** for production, **(B)** available via `WithOfflineMode()`

2. **Hydrated compositions — how deep?**
   - **(A)** Flatten compositions to a single effective fault config (simpler for SDK)
   - **(B)** Preserve composition tree in response (SDK needs composition-aware evaluator)
   - Proposal: **(A)** for MVP

3. **SSE push — MVP or Phase 5?**
   - Poll-based has up to 10s latency. Acceptable for rule changes.
   - Experiment mode changes (cache-box switch) may need sub-second delivery.
   - Proposal: defer to Phase 5; poll with 10s interval is fine for MVP

4. **InitTimeout default — 30s enough?**
   - Cold start (Skaffold dev loop): Postgres + manteion takes 15-20s
   - CI with resource limits: might be tighter
   - Proposal: 30s default, configurable via `MANTEION_INIT_TIMEOUT` env

---

## References

- `manteion-go/docs/plans/2026-03-30-scaffolding.md` — API structure
- `manteion-go/docs/plans/2026-03-30-relational-schemas.md` — Data model
- `manteion-go/docs/plans/2026-04-01-sdk-liveness-reaper.md` — Instance reaper
- `atropos-go/internal/evaluator/static.go` — StaticEvaluator + SetRules
- `atropos-go/internal/interceptor/` — Interceptor, middleware, cache-box dispatch
- `atropos-go/VISION.md` — Research vision, cache-box design
