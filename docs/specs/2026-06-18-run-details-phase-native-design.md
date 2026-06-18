# Run-details backend (phase-native, epoch-2) — design

**Status:** approved design, pre-implementation · **Date:** 2026-06-18 · **Repo:** manteion-go

## Context

The manteion-ui Runs surface (`src/lib/api/runs.ts` → a top-level `/runs` screen) calls
`/api/v1/runs[/{id}/faults|steps|events|events/history|resources|live-metrics|pause|resume|stop]`.
**No deployable backend serves these** (audit gap #2): they exist only on the abandoned
`feat/run-details-page-rewrite` branch, a *parallel pre-epoch-2 phase rewrite* built on a `run`
abstraction (tables `run_stats_snapshots`, `run_fault_events`, an alternate `model/experiment.go`)
that the epoch-2 consolidation deliberately removed. That branch is **reference only** — it cannot
be ported onto the current model. This feature re-implements run-details **natively on epoch-2**,
where the **phase is the run unit** (`experiments → experiment_phases → phase_workflows`, results in
`phase_workflow_results` / `phase_service_latency` / `phase_service_cache`), building on the
phase-aware orchestrator FSM (`internal/orchestrator`, branch `feat/phase-fsm-port`).

## Decisions (settled in brainstorming)

1. **Phase-native contract** — the API is redesigned around phases; the UI is updated to match
   (a "run" = a phase). No run-shaped compatibility shim.
2. **Hybrid data sourcing** — persist only what must survive a completed phase and isn't already
   stored: one new table, `phase_fault_events`. Everything else comes from existing stores
   (final stats) or, in v2, zeus/prometheus proxies (live).
3. **Core-first v1** — v1 ships the fully-DB-backed surface (list, detail, faults+audit, lifecycle);
   the live-streaming panels (steps/events/resources) are v2.
4. **Native enum** `fault_event_source` (`rule | cachebox | fault_config`), matching epoch-2's
   "native PG enums for stable vocabularies" philosophy (+ `enum_parity_test` + `model.EnumValues`).
5. **Backend-only plan** — this spec/plan covers manteion-go; the manteion-ui rewrite of
   `runs.ts` + run-detail components to the phase-native contract is a documented follow-on (it
   touches in-flight `feat/run-details-page` work).

## Goals / non-goals

**Goals (v1):** serve a coherent, fully-DB-backed phase-detail API — cross-experiment phase list,
phase detail (per-workflow results + aggregates), phase faults (frozen services + rules + a real
apply→clear fault-event audit trail), and phase lifecycle (pause/resume/stop) at a top-level,
phase-addressable URL surface.

**Non-goals (v1):** live step/histogram stats, the event timeline + history, resource snapshots
(all v2, via zeus-attack-stats + prometheus proxies); the `/live-metrics` endpoint (no UI consumer —
dropped); the manteion-ui rewrite (follow-on); re-introducing any `run`/`run_stats_snapshots`
table.

## Endpoint surface (top-level, phase-addressable)

`phaseId` is globally unique (`phase-<uuidv7>`), so reads/actions need no experiment segment. The
existing experiment-scoped phase routes (`/api/v1/experiments/{id}/phases/{phaseId}/…`) stay for the
builder flow.

| v1 (DB-backed) | Handler | Source |
|---|---|---|
| `GET /api/v1/phases?page=&limit=` | `handleListPhases` | `experiment_phases` ⨝ `experiments` (name) + `phase_workflows` (workflow_ids), paginated like `experiments` |
| `GET /api/v1/phases/{phaseId}` | `handleGetPhaseDetail` | phase row + `phase_workflow_results[]` + summable aggregates + `frozen_services` + `phase_rules` |
| `GET /api/v1/phases/{phaseId}/faults` | `handleGetPhaseFaults` | `frozen_services` + `phase_rules` + `phase_fault_events[]` |
| `POST /api/v1/phases/{phaseId}/pause` | `handlePausePhaseFlat` | orchestrator `PausePhase` |
| `POST /api/v1/phases/{phaseId}/resume` | `handleResumePhaseFlat` | orchestrator `StartPhase` (resumes from `paused`) |
| `POST /api/v1/phases/{phaseId}/stop` | `handleStopPhaseFlat` | orchestrator `StopPhase` |
| **v2 (deferred)** | | |
| `GET …/{phaseId}/steps` | — | zeus attack stats (per-step + histograms) |
| `GET …/{phaseId}/events`, `…/events/history` | — | zeus attack/run events |
| `GET …/{phaseId}/resources` | — | prometheus proxy (also closes audit gap #4) |

## DTOs (phase-native)

```go
// PhaseListItem — one row of GET /api/v1/phases.
type PhaseListItem struct {
    ID                 string     `json:"id"`
    ExperimentID       string     `json:"experiment_id"`
    ExperimentName     string     `json:"experiment_name"`
    Name               string     `json:"name"`
    Position           int        `json:"position"`
    WorkflowIDs        []string   `json:"workflow_ids"`
    FrozenServiceCount int        `json:"frozen_service_count"`
    Status             string     `json:"status"`
    StartedAt          *time.Time `json:"started_at,omitempty"`
    CompletedAt        *time.Time `json:"completed_at,omitempty"`
}

// PhaseDetail — GET /api/v1/phases/{phaseId}.
// No averaged percentiles: per-workflow rows + summable totals only (percentile-aggregation caveat).
type PhaseDetail struct {
    *model.ExperimentPhase                                   // id, experiment_id, name, position, status, frozen_services, persist_cache, timestamps
    ExperimentName    string                       `json:"experiment_name"`
    Workflows         []model.PhaseWorkflow         `json:"workflows"`
    WorkflowResults   []*model.PhaseWorkflowResult  `json:"workflow_results"`
    TotalRequestCount int64                         `json:"total_request_count"`
    TotalErrorCount   int64                         `json:"total_error_count"`
    OverallErrorRate  float64                       `json:"overall_error_rate"`
}

// PhaseFaults — GET /api/v1/phases/{phaseId}/faults.
type PhaseFaults struct {
    FrozenServices []model.CacheBoxConfig    `json:"frozen_services"`
    PhaseRules     []model.PhaseRule          `json:"phase_rules"`
    FaultEvents    []*model.PhaseFaultEvent   `json:"fault_events"`
}
```

## New persistence: `phase_fault_events`

Append migration (the only new schema). `fault_event_source` is a new native enum.

```sql
CREATE TYPE fault_event_source AS ENUM ('rule', 'cachebox', 'fault_config');

CREATE TABLE phase_fault_events (
    id         TEXT PRIMARY KEY,                              -- fevt-<uuidv7>
    phase_id   TEXT NOT NULL REFERENCES experiment_phases(id) ON DELETE CASCADE,
    source     fault_event_source NOT NULL,
    service    TEXT NOT NULL,
    kind       TEXT NOT NULL,                                 -- "cachebox:replay", "inline:latency", …
    detail     JSONB NOT NULL DEFAULT '{}',
    started_at TIMESTAMPTZ NOT NULL,
    ended_at   TIMESTAMPTZ                                    -- NULL while active
);
CREATE INDEX idx_phase_fault_events_phase ON phase_fault_events(phase_id);
```

- `model.PhaseFaultEvent` + `Validate()`; `FaultEventSourceValues = {"rule","cachebox","fault_config"}`
  added to `model.EnumValues` and `enum_parity_test`.
- `store.PhaseFaultEventRepo`: `Create(ctx, *PhaseFaultEvent)`, `EndOpenForPhase(ctx, phaseID, endedAt)`
  (stamps `ended_at` on the phase's still-open events), `ListForPhase(ctx, phaseID)`.
- `id` minted via `internal/id` with a new `fevt` prefix.

## Orchestrator wiring (the audit trail)

The FSM already applies and clears these; it just doesn't record them. Inject
`*store.PhaseFaultEventRepo` into `Orchestrator` (new `New(...)` arg + field). Best-effort writes —
a failure logs, never fails the phase (same posture as harvest).

- **`enterPhase`** (`runner.go`): after `pushPhaseRules`, `Create` one open event per pushed rule
  (`source=rule`, `service=rule.Service`, `kind=category:fault_type` via the existing rule→spec
  lookup, `started_at=now`). After `freezeServices`, `Create` one per frozen service
  (`source=cachebox`, `service=fs.Service`, `kind="cachebox:"+fs.Mode`, `detail={key_strategy}`,
  `started_at=now`).
- **`finishPhase`** (`runner.go`): after clearing rules / `thawServices`, call
  `EndOpenForPhase(ctx, phaseID, now)` to close the phase's open events.
- **`fault_config` source:** manual long-running faults already carry a `phase_id` FK; when one is
  fired against a phase, record a `source=fault_config` event (fire → open, cancel/reap → close).
  This hooks the existing `handleFireFaultConfig`/`handleCancelFaultConfig`/reaper paths (which now
  also broadcast via `bumpAndBroadcast`). *v1 records rule + cachebox reliably; fault_config wiring
  is included but lower-risk to defer if it complicates the fire path.*

## Files

| File | Change |
|---|---|
| `internal/db/migrations.go` | + migration: `fault_event_source` enum, `phase_fault_events` table + index |
| `internal/model/enums.go` | + `FaultEventSourceValues`, register in `EnumValues` |
| `internal/model/experiment.go` | + `PhaseFaultEvent` struct + `Validate()` |
| `internal/store/phase_fault_event_repo.go` (new) | `Create` / `EndOpenForPhase` / `ListForPhase` |
| `internal/store/experiment_repo.go` | + `ListPhasesPaged` (cross-experiment, with experiment_name + workflow_ids) |
| `internal/id/id.go` | + `fevt` prefix |
| `internal/api/phase_detail_handler.go` (new) | the 6 v1 handlers + DTOs |
| `internal/api/server.go` | + 6 routes; refresh the experiment-routes comment |
| `internal/orchestrator/{orchestrator,runner}.go` | inject repo; record/close events in enter/finish |
| `cmd/manteion/main.go` | construct `PhaseFaultEventRepo`, thread into orchestrator + server |
| `internal/store/enum_parity_test.go` | covered by `EnumValues` registration |
| CLAUDE.md | API surface row for `/api/v1/phases` |

## Error handling

- 404 on unknown `phaseId` (`store.ErrNotFound`); 400 on bad pagination; lifecycle errors surface
  the orchestrator's messages (409-style conflicts for bad-state transitions), matching the
  experiment lifecycle handlers.
- Fault-event writes are best-effort and never block phase progression.
- `GET /phases` paginates with the shared `Page`/`writePage` envelope used by `experiments`.

## Testing

- **Store (DB-gated):** `phase_fault_event_repo_test.go` — create, list-for-phase ordering,
  `EndOpenForPhase` closes only open rows; `ListPhasesPaged` returns experiment_name + workflow_ids
  and paginates.
- **Orchestrator (DB-gated, fake zeus):** extend the FSM suite — a completed attack-driven phase
  produces an **open→closed** cachebox event for each frozen service and a rule event per phase
  rule; a paused phase leaves events open.
- **API (httptest):** list/detail/faults shape; 404s; lifecycle routes call through to the
  orchestrator.
- Enum parity stays green via `EnumValues` registration.

## v1 / v2 split & UI follow-on

- **v1** = this spec (sections above): DB-backed phase list/detail/faults + lifecycle.
- **v2** = live panels — `…/{phaseId}/steps|events|events/history|resources` over zeus-attack-stats
  + a `/api/v1/prometheus` proxy (closes audit gap #4). Separate spec.
- **UI follow-on** (manteion-ui): rewrite `runs.ts` + the run-detail route/components to the
  phase-native contract; the live components (`live-event-tail`, `resource-metrics`,
  `step-breakdown-tree`, `run-histogram`) render a "live view lands in v2" state until then.

## Risks / open notes

- **Percentile honesty:** `PhaseDetail` returns per-workflow rows + summable totals, never an
  averaged p99 — consistent with `RecomputeExperimentResults`' worst/best framing.
- **Migration ordering:** epoch-2 `Migrate` is append-only and refuses DBs ahead of the binary;
  this adds one new step, applied cleanly to the deployed epoch-2 head.
- **`fault_config` event wiring** is the one piece touching a non-orchestrator path (the fire/cancel
  handlers); if it complicates that path it can ship a beat later without blocking v1's rule +
  cachebox trail.
