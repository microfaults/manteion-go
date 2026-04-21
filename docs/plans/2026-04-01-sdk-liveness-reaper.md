# SDK Liveness Detection & Instance Reaper

## Context

Once an atropos SDK instance registers with manteion, there is no mechanism to detect if it goes down. The existing `LastPollAt` timestamp on each instance record provides a passive signal -- if an SDK stops polling, the gap grows -- but nothing acts on it. This matters for experiment orchestration: starting a run that depends on a dead SDK wastes time waiting for a timeout. It also matters for operational visibility -- operators need to know which services have healthy atropos coverage.

The design leverages the existing poll-based communication model (SDK calls manteion, not the other way around) and adds a reaper goroutine on the manteion side plus recovery logic on the SDK side.

## Design

### Constants

| Name | Default | Description |
|------|---------|-------------|
| `PollInterval` | 10s | How often SDK polls `GET /sdk/rules` |
| `StaleThreshold` | 30s | 3x poll interval -- instance marked stale |
| `DeadThreshold` | 120s | 12x poll interval -- instance deregistered |
| `ReaperInterval` | 15s | How often manteion sweeps the registry |

### Instance States

```
ALIVE  --(miss 3 polls)-->  STALE  --(miss 12 polls)-->  DEAD
  ^                            |                            |
  +----(poll received)---------+                            |
                                                            v
                                                       deregistered
```

- **ALIVE**: `now - LastPollAt < StaleThreshold`
- **STALE**: `StaleThreshold <= now - LastPollAt < DeadThreshold`
- **DEAD**: `now - LastPollAt >= DeadThreshold` -- removed from registry

### Instance Record Changes

Add a `Status` field to the existing `Instance` struct in `internal/sdk/registry.go`:

```go
type Instance struct {
    ID           string
    Service      string
    Version      string
    Address      string
    RegisteredAt time.Time
    LastPollAt   time.Time
    Status       string    // "alive", "stale", "dead"
}
```

`Status` is computed by the reaper, not set by the SDK. On registration and on each poll, `Status` resets to `"alive"`.

### Manteion-Side: Reaper Goroutine

New file: `internal/sdk/reaper.go`

```go
type Reaper struct {
    registry       *Registry
    staleThreshold time.Duration
    deadThreshold  time.Duration
    interval       time.Duration
    logger         *slog.Logger
}
```

Lifecycle:
- Started in `cmd/manteion/main.go` alongside the HTTP server
- Runs in its own goroutine, stopped via context cancellation
- Each tick iterates all instances, transitions states, removes dead entries

Reaper loop pseudocode:

```
ticker := time.NewTicker(ReaperInterval)

for now := range ticker.C:
    for _, inst := range registry.All():
        gap := now.Sub(inst.LastPollAt)

        switch {
        case gap >= DeadThreshold:
            registry.Remove(inst.ID)
            log dead instance, emit sdk_instances_reaped_total

        case gap >= StaleThreshold:
            if inst.Status != "stale":
                inst.Status = "stale"
                registry.Update(inst)
                log stale instance, emit sdk_instances_stale gauge

        default:
            if inst.Status != "alive":
                inst.Status = "alive"
                registry.Update(inst)
            }
        }
```

### SDK-Side: Poll Recovery

This is guidance for the atropos-go SDK (not implemented in manteion-go), documented here for protocol completeness.

SDK poll loop behavior:

1. Poll `GET /sdk/rules?service=X&version=N` every `PollInterval`
2. On failure: increment `consecutiveFailures`, backoff = `min(PollInterval * 2^failures, 60s)`
3. Continue injecting faults with last-known rules during manteion outage
4. On first success after failures: `POST /sdk/register` to re-register (manteion may have restarted and lost in-memory state), reset `consecutiveFailures`
5. Emit `manteion_poll_failures_total` counter metric on each failure

### Experiment Pre-flight Gate

Before launching an `ExperimentRun`, the orchestrator checks that all required services have at least one alive instance:

```go
func (o *Orchestrator) preflight(run ExperimentRun) error {
    for _, svc := range run.requiredServices() {
        instances := registry.ByService(svc)
        alive := filter(instances, func(i Instance) bool {
            return i.Status == "alive"
        })
        if len(alive) == 0 {
            return fmt.Errorf(
                "no alive instances for service %q (%d stale, %d total)",
                svc, len(instances)-len(alive), len(instances),
            )
        }
    }
    return nil
}
```

This fails fast rather than starting a run that will time out.

### API Changes

No new endpoints. Existing endpoints gain status information:

`GET /api/v1/sdk/instances` response adds `"status"` field:

```json
[
  {
    "id": "frontend-pod-abc123",
    "service": "frontend",
    "version": "1.0.0",
    "address": "10.0.1.2:5000",
    "registered_at": "2026-04-01T09:00:00Z",
    "last_poll_at": "2026-04-01T10:15:32Z",
    "status": "alive"
  }
]
```

`GET /api/v1/status` response adds instance health breakdown:

```json
{
  "rules_count": 42,
  "instances_count": 13,
  "instances_alive": 11,
  "instances_stale": 2,
  "zeus_reachable": true
}
```

### Observability

| Metric | Type | Description |
|--------|------|-------------|
| `sdk_instances_total` | gauge | Current registered instances, labeled by status |
| `sdk_instances_reaped_total` | counter | Cumulative reap events |
| `manteion_reaper_sweep_duration_seconds` | histogram | Time to complete one sweep |

SDK-side (atropos-go, for reference):

| Metric | Type | Description |
|--------|------|-------------|
| `manteion_poll_failures_total` | counter | Consecutive poll failures |

## Edge Cases

| Scenario | Behavior |
|----------|----------|
| SDK pod killed (no graceful shutdown) | Reaper catches it after `DeadThreshold` (120s) |
| SDK pod graceful shutdown | `DELETE /sdk/register/{id}` -- immediate removal |
| Manteion restarts, loses registry | SDKs re-register on next successful poll |
| Network partition | SDK keeps injecting with stale rules; manteion marks stale then reaps; SDK re-registers when partition heals |
| Clock skew between pods | Irrelevant -- `LastPollAt` set by manteion's clock, reaper uses manteion's clock |

## Implementation Order

1. Add `Status` field to `Instance` struct, default to `"alive"` on register/poll
2. Implement `Reaper` in `internal/sdk/reaper.go` with tests
3. Wire reaper into `cmd/manteion/main.go` (start/stop with context)
4. Update `GET /sdk/instances` and `GET /status` responses to include status info
5. Add experiment pre-flight check (when orchestrator is implemented)

## Files to Modify

- `internal/sdk/registry.go` -- add `Status` field, update on poll
- `internal/sdk/reaper.go` -- new file, reaper goroutine
- `internal/sdk/reaper_test.go` -- new file, reaper tests
- `internal/api/sdk_handler.go` -- include status in instance list response
- `internal/api/health_handler.go` -- add alive/stale counts to status
- `cmd/manteion/main.go` -- start reaper goroutine

## Not In Scope

- **Push channel** (WebSocket/gRPC stream) for latency-sensitive rule delivery -- poll lag of 10s is acceptable for MVP
- **Quorum awareness** ("are enough instances alive?") -- only "at least one" check for now
- **Graceful drain** (SDK signals "draining, stop sending experiment traffic") -- future concern
