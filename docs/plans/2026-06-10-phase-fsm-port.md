# Phase-Aware Orchestrator FSM Port Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the rich experiment-run FSM (pause/resume/cancel, zeus attack supervision, pollers, result harvesting, crash recovery) — shelved during the epoch-2 schema consolidation — phase-aware on the current `experiments → experiment_phases → phase_workflows` model.

**Architecture:** The phase replaces the old `ExperimentRun` as the FSM unit. Race safety comes from the same three primitives the old FSM used (commits e81fdb0, 5153d5f): compare-and-swap status transitions in SQL, a per-experiment advance lock, and a single terminal path (`finishPhase`) whose CAS winner runs all side effects exactly once. A per-phase poller goroutine supervises zeus attacks; harvest writes the epoch-2 `phase_workflow_results` / `phase_service_latency` / `phase_service_cache` rows and recomputes the experiment rollup.

**Tech Stack:** Go 1.22+, database/sql + pgx, zeus typed client (`internal/zeus`), atrocontrol fanout controller, cachestore, httptest fake-zeus for integration tests (DB-gated via `MANTEION_TEST_DB`).

---

## Design decisions (the spec)

1. **Phase is the FSM unit.** Statuses: `pending → running ⇄ paused → completed | failed | skipped`.
   Experiment statuses: `planned → running → completed | failed | cancelled`.
2. **Experiment-level pause is derived state.** The `experiment_status` enum has no
   `paused` label (deliberate in epoch-2; `phase_status` has it). `PauseExperiment`
   pauses the running phase(s) and leaves the experiment `running`; `ResumeExperiment`
   resumes paused phases and re-walks the scheduler. No schema change on a
   freshly-deployed epoch.
3. **Sequential scheduler** (`advanceExperiment`, serialized per experiment):
   - Gate: experiment must be `running`; any `running`/`paused` phase → nothing to do.
   - A `failed` phase marks all later `pending` phases `skipped` and finalizes the
     experiment `failed` (sequential phases are dependent by construction — port of the
     old DAG failure cascade).
   - No pending left → finalize: `failed` if any phase failed, else `completed`
     (CAS `running → terminal` so a concurrent cancel wins).
   - Else start the first `pending` phase.
4. **Race-safe transitions.** New repo methods `TransitionExperiment` /
   `TransitionPhase(ctx, id, to, from...) (bool, error)` — single UPDATE with
   `status = ANY(from)` guard, stamping `started_at`/`completed_at` like the existing
   `UpdateStatus`/`UpdatePhaseStatus`.
5. **Phase enter sequence** (`startPhase`, fresh start from `pending`):
   cache preload (non-baseline only) → freeze frozen_services → push phase rules →
   materialize workflows into zeus → start attacks → spawn poller. Resume from
   `paused` repeats everything except the preload.
6. **Attacks:** one zeus attack per `phase_workflows` row that has a `target_url`
   (manteion treats workflow DSL as opaque — it cannot derive a target). Rate =
   `int(rate_rps)`, falling back to `vus` (1 rps/VU approximation, logged).
   `atk-` id is minted client-side and persisted via `UpdatePhaseWorkflowZeusAttack`
   *before* `StartAttack` (crash-recovery ordering, port of `startOneAttack`); if zeus
   returns a different id, the row is overwritten with zeus's. `RunRef` and
   `MetaTraceID` carry the phase id; `WorkflowLabel` the workflow id.
7. **Auto-complete:** a running phase with zero attacks has no driver; it is
   completed immediately (`autoComplete` flag, off in FSM unit tests — port of the
   old behavior).
8. **Pause** stops the poller and attacks but keeps rules and freeze in place;
   **resume** re-pushes rules (idempotent), re-freezes, starts fresh attacks (the
   single `zeus_attack_id` column tracks the *latest* attack; harvest reflects the
   last segment — documented limitation), respawns the poller.
9. **`finishPhase(ctx, phaseID, status, from...)`** is the only terminal path:
   CAS first; the winner cancels the poller, clears rules (push nil to the union of
   rule-target services and frozen services), thaws frozen services, stops attacks,
   harvests (only when `status == "completed"`), recomputes the experiment rollup,
   then async-advances the experiment.
10. **Harvest** (per completed phase):
    - per attack: `GetAttackResult` with not-ready backoff (1s/2s/4s) →
      `UpsertWorkflowResult` (`error_count = round((1-success_rate)*total)`,
      `latency_p999_us = latency_p99_us` — vegeta reports no p999; raw JSON kept);
      when the result names a service, also `UpsertServiceLatency(phase, service,
      workflow)` (no service-wide `workflow_id=''` rows — aggregating percentiles
      across workflows would be wrong; see the percentile-aggregation caveat).
    - per frozen service: `StatusByService` → sum cache-box `Hits`/`Misses` across
      instances → `UpsertServiceCache`. `cache_exact_match` equals the hit rate
      (every hit is an exact key match under the `exact*` strategies);
      `cache_staleness` is 0.0 — the SDK exposes no staleness counter yet.
11. **Recover** (process restart): for each `running` experiment, reconcile each
    `running` phase's attacks via `GetAttack` (3 retries, 100ms/500ms/2s backoff —
    port of `reconcileOneAttack`); all-lost → phase `failed`; survivors → respawn
    poller; no attacks configured → auto-complete. `paused` phases stay paused.
    Finally `advanceExperiment` (covers a crash between phases).
12. **Freeze mapping** (port of branch commit 3df896b): `CacheBoxConfig.
    SyntheticDelay.FitMu/FitSigma → atroposdk.DelayRequest{Mu, Sigma}`;
    `FreezeService` persists intent so re-registering SDKs inherit it; thaw =
    `ClearService`.
13. **Preload:** a phase with frozen services preloads from the latest `completed`
    phase of the same experiment with `persist_cache = true` and no frozen services
    (the baseline); cache files are already keyed by phase id
    (`cachestore.Read(phaseID, service)` — the ingest handler writes them that way).
14. **Poller:** interval default 15s (`WithPollInterval` override for tests);
    safety-net deadline = `max(maxPollDuration, max(phase workflow duration)+5min)`;
    `completed`/`stopped` attack statuses count as done, `pending`/`running` as in
    flight, anything else as failure.
15. **HTTP:** `POST /api/v1/experiments/{id}/pause|resume|cancel` return (404 on
    missing, 409 on bad state — same envelope as the retired e81fdb0 handlers).
    Existing `/start`, `/stop`, phase `/start`/`/stop` keep their signatures.
16. **Policy engine stays frozen** (docs/decisions/2026-06-policy-engine-freeze.md):
    metric-triggered automation is *not* in scope here; this port deliberately ships
    without promql-driven transitions so the freeze decision can be revisited
    separately.

## File structure

| File | Responsibility |
|---|---|
| `internal/store/experiment_repo.go` (modify) | + `TransitionExperiment`, `TransitionPhase` CAS methods |
| `internal/orchestrator/orchestrator.go` (rewrite) | Struct/options, experiment lifecycle (Start/Pause/Resume/Cancel/Stop), `advanceExperiment`, `Recover` |
| `internal/orchestrator/runner.go` (rewrite) | Phase machinery: `StartPhase`/`PausePhase`/`StopPhase`, enter sequence (preload/freeze/rules/materialize/attacks), `finishPhase`, thaw/teardown |
| `internal/orchestrator/poller.go` (rewrite) | Per-phase zeus poll loop + attack status aggregation |
| `internal/orchestrator/harvest.go` (rewrite) | Result harvest into phase_* tables + rollup recompute |
| `internal/orchestrator/rules.go` (keep) | `loadCompiledRules` (already epoch-2-ready) |
| `internal/orchestrator/orchestrator_test.go` (rewrite) | DB-gated integration suite + fake zeus (httptest) |
| `internal/api/experiment_handler.go` (modify) | + pause/resume/cancel handlers |
| `internal/api/server.go` (modify) | + 3 routes; refresh stale route comment |
| `CLAUDE.md` (modify) | API surface table row for the new endpoints |

## Tasks

### Task 1: CAS transition repo methods
- [x] Add `TransitionExperiment` / `TransitionPhase` to `internal/store/experiment_repo.go`
- [x] `go build ./...`

### Task 2: Orchestrator core
- [x] Rewrite `orchestrator.go` (struct, options, experiment FSM, scheduler, recover)
- [x] Rewrite `runner.go` (phase FSM + zeus/atrocontrol side effects)
- [x] Rewrite `poller.go`
- [x] Rewrite `harvest.go`
- [x] `go build ./... && go vet ./...`

### Task 3: HTTP endpoints
- [x] Add pause/resume/cancel handlers + routes
- [x] `go build ./...`

### Task 4: Integration tests
- [x] Fake zeus httptest server (attacks lifecycle + results)
- [x] FSM tests: start→auto-complete→advance→finalize; pause/resume; cancel;
      double-start CAS; failure cascade (skip remaining); recover (lost attacks → failed; paused untouched)
- [x] Harvest test: attack-driven phase writes workflow/service rows + rollup
- [x] `go test ./...` (unit) and `make integration` against local Postgres

### Task 5: Docs + commit
- [x] CLAUDE.md endpoint table
- [x] Commit(s) on `feat/phase-fsm-port`
