# Experiment Orchestration FSM — Implementation Record

> **Date:** 2026-04-29
> **Branch:** `feature/experiment-orchestration-fsm`
> **Scope:** manteion-go — orchestrator layer only; no changes to atropos-go or zeus-go
> **Prerequisite:** `2026-04-28-experiment-orchestration.md` and `2026-04-28-cache-preload-zeus.md` fully implemented

---

## Context

This document records the changes made in the `feature/experiment-orchestration-fsm` branch.
The prior experiment orchestration plan (`2026-04-28-experiment-orchestration.md`) built the
bones: schema, model types, phase-based rule pushing, PromQL phase transitions, cache preload,
and Zeus attack start/stop. Six capabilities were missing from that initial implementation and
are addressed here.

---

## What Was Missing Before This Branch

| Feature | Gap |
|---|---|
| Zeus status polling | Orchestrator never checked whether a Zeus attack was still running; a crashed or fixed-duration attack would leave the run stuck in `running` forever |
| Multi-workflow per experiment | `startZeusAttack` hardcoded a single attack from `Experiment.PrimaryWorkloadID`; no way to drive N flows simultaneously in one run |
| Result harvesting | No code read attack metrics back from Zeus after completion; `workflow_run_results` rows were never written |
| Run pause/resume | Run FSM was `pending → running → completed/failed`; no way to suspend a run mid-experiment and continue it later |
| Experiment auto-transition | After the last run finished, the parent experiment row stayed at `running` indefinitely; operators had to update it manually |
| Auto-start from "planned" | The only way to start all runs was to call `/start` on each run individually; no single-shot "begin this experiment" operation |

---

## Changes Per Area

### 1. Model (`internal/model/experiment.go`)

**Before:** `validRunStatuses` contained `pending | running | completed | failed`.
Two new fields were added to `ExperimentRun` as JSONB/TEXT columns:

```
ZeusAttackID  string   // single attack ID (existing, migration 6)
```

**After:** `paused` added to valid statuses. Two new fields added:

```go
// WorkloadIDs lists workloads to drive for this run.
// Falls back to Experiment.PrimaryWorkloadID when empty.
WorkloadIDs []string `json:"workload_ids,omitempty"`

// ZeusAttackIDs holds all attack IDs for multi-workflow runs.
ZeusAttackIDs []string `json:"zeus_attack_ids,omitempty"`
```

`ZeusAttackID` is preserved for backward compatibility (stores `ZeusAttackIDs[0]`).

FSM documented on the struct:

```
pending → running → completed
running → paused  → running (resume)
running → failed
```

---

### 2. DB Migrations (`internal/db/migrations.go`)

Two migrations appended after the existing v6:

| Version | Description |
|---|---|
| 7 | Drops and recreates `experiment_runs_status_check` to include `'paused'` |
| 8 | Adds `workload_ids JSONB` and `zeus_attack_ids JSONB` to `experiment_runs` |

**Before v7:** `status IN ('pending','running','completed','failed')` — a `paused` write would be rejected at the DB level.

**After v7:** `status IN ('pending','running','paused','completed','failed')`.

---

### 3. Store (`internal/store/experiment_repo.go`)

| Method | Before | After |
|---|---|---|
| `CreateRun` | Stored 14 columns | Stores 16 (adds `workload_ids`, `zeus_attack_ids`) |
| `GetRun` | Scanned 15 columns | Scans 17 (adds new columns) |
| `ListRunsByExperiment` | Inline scan loop | Delegates to `scanRun` helper (DRY) |
| `UpdateRunStatus` | `SET status = $2` only | Also sets `started_at` on first `running` transition; sets `completed_at` on `completed` or `failed` — **timestamps were silently not persisted before** |
| `UpdateStatus` (experiments) | `SET status = $2` only | Also sets `started_at` / `completed_at` with same CASE logic |
| `UpdateRunZeusAttack` | Set single TEXT column | Replaced by `UpdateRunZeusAttacks(ids []string)` — sets both `zeus_attack_id` (first) and `zeus_attack_ids` (all) |
| `ListPendingRuns` | Did not exist | Returns all runs with `status = 'pending'` for a given experiment |
| `ListWorkflowResults` | Did not exist | Returns `workflow_run_results` rows for a run |
| `scanRun` helper | Did not exist | Shared scan logic called by `GetRun`, `ListRunsByExperiment`, `ListPendingRuns` |

---

### 4. Zeus Client (`internal/zeus/client.go`)

Two new methods added for polling and result retrieval:

```go
// GetAttack fetches current status (pending / running / completed / stopped).
func (c *Client) GetAttack(ctx context.Context, attackID string) (*AttackInfo, error)

// GetAttackResult fetches final vegeta metrics after attack completion.
// Returns ErrAttackResultNotReady (wraps 404) when not yet available.
func (c *Client) GetAttackResult(ctx context.Context, attackID string) (*AttackResultInfo, error)
```

`AttackResultInfo` fields: `TotalRequests`, `DurationMs`, `RateActual`, `SuccessRate`,
`LatencyP50Us`, `LatencyP90Us`, `LatencyP95Us`, `LatencyP99Us`, `ThroughputRPS`.

**Before:** Zeus was write-only from manteion's perspective — start attack, stop attack, no reads.

**After:** Manteion can observe attack lifecycle and pull results when finished.

---

### 5. Orchestrator — New Files

#### `internal/orchestrator/poller.go`

Background goroutine `pollZeusStatus` started alongside `watchPhase` for every active run.

```
every 15s:
  for each attack ID in run.ZeusAttackIDs:
    zeus.GetAttack(id) → status
    if status == "completed" || "stopped": completedCount++
  if completedCount == len(attackIDs):
    HarvestResults(run)
    StopRun("completed")
```

**Before:** A fixed-duration Zeus attack that finished on its own would leave the manteion run
in `running` indefinitely. The Zeus poller closes that loop.

#### `internal/orchestrator/harvest.go`

`HarvestResults(ctx, run)` iterates all Zeus attack IDs, calls `GetAttackResult`, and writes
one `WorkflowRunResult` row per attack to Postgres.

- Non-fatal: missing or not-yet-ready results are logged and skipped.
- `Workflow` field is set to the attack's `Service` name.
- `LatencyP999Us` is approximated as `LatencyP99Us` (vegeta does not report p999).
- Called automatically by `StopRun` when `status == "completed"`.

**Before:** `workflow_run_results` table existed but was never populated by the orchestrator.

---

### 6. Orchestrator — Modified (`internal/orchestrator/orchestrator.go`)

#### `StartRun`

| Before | After |
|---|---|
| Called `startZeusAttack` (single attack, primary workload only) | Calls `startZeusAttacks` (N attacks, one per `WorkloadIDs`; falls back to `PrimaryWorkloadID`) |
| Only spawned `watchPhase` goroutine when `len(PhaseRules) > 1` | Always spawns `pollZeusStatus`; spawns `watchPhase` conditionally as before |
| Stored single `zeus_attack_id` via `UpdateRunZeusAttack` | Stores all IDs via `UpdateRunZeusAttacks` |
| Did not set `started_at` in the DB (set on in-memory struct only — **bug**) | `UpdateRunStatus("running")` now sets `started_at` via SQL |

#### `StopRun`

| Before | After |
|---|---|
| Stopped single `zeus_attack_id` | Iterates `ZeusAttackIDs`; falls back to `ZeusAttackID` |
| Called `UpdateRunStatus` and returned | Also calls `HarvestResults` (when `status == "completed"`) then `checkExperimentStatus` (in goroutine) |

#### New Methods

**`PauseRun(ctx, runID)`**
- Validates `status == "running"`.
- Cancels the watch/poll goroutines via the stored `context.CancelFunc`.
- Stops all Zeus attacks.
- Transitions to `status = "paused"`.

**`ResumeRun(ctx, runID)`**
- Validates `status == "paused"`.
- Re-enters `CurrentPhase` (re-pushes phase rules to SDK instances).
- Starts fresh Zeus attacks; updates attack IDs.
- Spawns new `watchPhase` (if more phases remain) and `pollZeusStatus`.
- Transitions to `status = "running"`.

**`StartExperiment(ctx, experimentID)`**
- Validates `status == "planned"`.
- Transitions experiment to `status = "running"`.
- Calls `ListPendingRuns` and calls `StartRun` on each.

**`checkExperimentStatus(ctx, experimentID)`**
- Lists all runs for the experiment.
- If any run is still `pending`, `running`, or `paused`: returns (experiment stays `running`).
- If all terminal and any `failed`: transitions experiment to `failed`.
- If all `completed`: transitions experiment to `completed`.
- Called as a goroutine by `StopRun` after every terminal transition.

#### `startZeusAttacks` / `startOneAttack` (replaces `startZeusAttack`)

`startZeusAttacks` iterates `run.WorkloadIDs` (or falls back to
`Experiment.PrimaryWorkloadID`) and calls `startOneAttack` for each. All returned
attack IDs are accumulated and stored together. Per-workload failures are logged
but do not abort the start sequence.

---

### 7. API (`internal/api/`)

#### New Routes

| Method | Path | Handler | Description |
|---|---|---|---|
| `POST` | `/api/v1/experiments/{id}/start` | `handleStartExperiment` | `planned → running`; starts all pending runs |
| `POST` | `.../runs/{runId}/pause` | `handlePauseRun` | `running → paused` |
| `POST` | `.../runs/{runId}/resume` | `handleResumeRun` | `paused → running` |
| `GET` | `.../runs/{runId}/results` | `handleListRunResults` | Returns `WorkflowRunResult[]` for a run |
| `GET` | `/api/v1/experiments/{id}/contributions` | `handleListContributions` | Returns `ContributionResult[]` for an experiment |

#### `Server` struct (`server.go`)

`policies *store.PolicyRepo` field added so `policy_handler.go` can compile.
`NewServer` signature gains one trailing `*store.PolicyRepo` parameter.

---

### 8. `cmd/manteion/main.go`

**Conflict resolved.** The stash/upstream conflict was about which parameters to pass to
`api.NewServer`:

- Upstream (develop) dropped `orch` and `cs` to avoid a dependency cycle during a mid-refactor.
- Stash had `policyRepo` where `autoRuleRepo` was expected.

Resolution:
- `autoRuleRepo` kept (upstream — correct type per `Server` struct).
- `policyRepo := store.NewPolicyRepo(database)` added before the `policy.New(...)` call that uses it (it was referenced before declaration — compile error).
- `orch, cs, policyRepo` all passed to `NewServer`.

---

## Before / After — Experiment Lifecycle

### Before this branch

```
Operator creates experiment (status="planned")
Operator creates N runs (status="pending")

For each run:
  POST /experiments/{id}/runs/{runId}/start
    → pushes phase 0 rules
    → starts Zeus attack (single workload only)
    → spawns watchPhase goroutine (if multi-phase)

Run finishes via phase watcher → StopRun("completed")
  → clears rules
  → stops Zeus attack
  → sets run status = "completed"

Operator must:
  - manually set experiment status = "completed" (no auto-transition)
  - manually read Zeus attack results (no harvesting)
  - call /start on every run individually (no experiment-level start)
  - accept that a paused/suspended run is impossible
  - accept that run.started_at / run.completed_at were never written to DB
```

### After this branch

```
Operator creates experiment (status="planned")
Operator creates N runs (status="pending")

POST /experiments/{id}/start
  → experiment: planned → running
  → all pending runs auto-started (phase rules pushed, Zeus attacks fired)

Each run lifecycle:
  running → [PromQL phase watcher advances phases]
  running → [Zeus poller detects fixed-duration attack complete]
              → harvests WorkflowRunResult rows from Zeus
              → run: running → completed
  running → [operator calls /pause]
              → Zeus attacks stopped, goroutines cancelled
              → run: running → paused
  paused  → [operator calls /resume]
              → phase rules re-pushed, Zeus attacks restarted
              → run: paused → running
  running/paused → [operator calls /stop]
              → rules cleared, Zeus stopped, results harvested
              → run: running → completed

After every run terminal transition:
  checkExperimentStatus (goroutine):
    all completed → experiment: running → completed
    any failed    → experiment: running → failed

Results readable via:
  GET /experiments/{id}/runs/{runId}/results  → WorkflowRunResult[]
  GET /experiments/{id}/contributions          → ContributionResult[]
```

---

## Key Design Decisions

**Zeus polling is per-run, not global.** Each active run gets its own `pollZeusStatus`
goroutine sharing the same `context.CancelFunc` as `watchPhase`. When `PauseRun` or
`StopRun` cancels the context, both goroutines stop cleanly.

**`UpdateRunStatus` timestamp tracking is SQL-side.** The previous implementation set
`run.StartedAt` on the Go struct but never persisted it (`UpdateRunStatus` only wrote
`status`). Fixing it at the SQL level is safer because it is atomic with the status
change and requires no coordination in Go caller code.

**Harvest is non-fatal.** If Zeus has not yet computed the result (or the result
endpoint returns 404), `harvestOne` logs and returns nil. The run still transitions to
`completed`. This avoids a blocking dependency on Zeus result latency at run completion
time.

**Multi-workflow uses `WorkloadIDs` on the run, not the experiment.** This allows
different runs within the same experiment to drive different sets of flows — e.g., a
baseline run drives all flows, while an isolation run drives only the primary flow to
reduce cost.

**`checkExperimentStatus` is always a goroutine.** It is called from `StopRun` which
may itself be called from inside a goroutine (e.g., from `watchPhase` → `advancePhase`
→ `StopRun`). Spawning it as a goroutine avoids a deadlock on `o.mu` and keeps
`StopRun`'s return path synchronous for the caller.

---

## Files Changed

| File | Change Type |
|---|---|
| `cmd/manteion/main.go` | Modified — conflict resolution + policyRepo wire-up |
| `internal/model/experiment.go` | Modified — `paused` status, `WorkloadIDs`, `ZeusAttackIDs` |
| `internal/db/migrations.go` | Modified — migrations 7 and 8 |
| `internal/store/experiment_repo.go` | Modified — new columns, timestamp fix, new methods |
| `internal/zeus/client.go` | Modified — `GetAttack`, `GetAttackResult` |
| `internal/api/server.go` | Modified — `policies` field, new routes |
| `internal/api/experiment_handler.go` | New — all experiment + run handlers |
| `internal/orchestrator/orchestrator.go` | New — `StartRun`, `StopRun`, `PauseRun`, `ResumeRun`, `StartExperiment`, `checkExperimentStatus`, `startZeusAttacks` |
| `internal/orchestrator/runner.go` | New — `watchPhase`, `advancePhase`, `enterPhase`, `collectServices` |
| `internal/orchestrator/rules.go` | New — `loadCompiledRules` |
| `internal/orchestrator/harvest.go` | New — `HarvestResults`, `harvestOne` |
| `internal/orchestrator/poller.go` | New — `pollZeusStatus`, `checkAttackStatuses` |
