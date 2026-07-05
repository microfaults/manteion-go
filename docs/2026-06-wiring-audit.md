# manteion-go wiring & consistency audit — 2026-06

> **Produced:** 2026-06-16, from a read-only flow-wiring + consistency cross-check over
> manteion-go (`feat/phase-fsm-port` @`17a0b9b`) and manteion-ui (`feat/run-details-page`),
> part of the [cross-repo checkpoint](../../CHECKPOINT-2026-06.md). Findings are actionable
> engineering follow-ons to the phase-FSM port — none block the cleanup, but most should be
> resolved before merging the FSM port to `develop`.

## Closed seams (verified good)

- **Result harvest is now wired.** `feat/phase-fsm-port`'s `internal/orchestrator/harvest.go`
  calls the previously caller-less `ExperimentRepo.UpsertWorkflowResult`,
  `UpsertServiceLatency`, and `UpsertServiceCache`. On `develop`, `harvest.go` is a stub and
  these were dead; the FSM port closes them. The dropped `phase_service_resources` table leaves
  no dangling resources method.
- **Policy-engine freeze is clean.** `MANTEION_POLICY_ENGINE=off` gates the single call site
  (`policyEngine.Run` in `main.go`); CRUD handlers + `PolicyRepo` stay fully wired; no dangling
  references. (See `docs/decisions/2026-06-policy-engine-freeze.md`.)

## Dead vertical slices (defined-but-uncalled)

- **`TraceRepo` / `trace_anchors`.** `store.TraceRepo` (`Create` + `ListByPhase`),
  `model.TraceAnchor`, and the `trace_anchors` table have **zero production callers**. `main.go`
  constructs `traceRepo` and threads it into `api.Server` (`s.traces`), but no handler or
  orchestrator code reads/writes it. The meta-trace correlation it was meant to provide
  (anchoring zeus attack runs / harvested results to OTel trace ids) is never wired into
  `harvest.go`. **Decide:** wire trace-anchor capture into the harvest path (the
  `attack_results` table already has a `meta_trace_id` column), or remove the repo + table +
  model to stop carrying an unwired dependency through the server constructor.
- **`WorkloadRepo` attack-persistence layer.** The FSM port routes per-attack results through
  `phase_workflow_results` (via `UpsertWorkflowResult`), leaving `CreateAttack`,
  `CreateAttackResult`, `DeleteAttack`, `ListAttacks`, `ListResultsForAttack`,
  `ListResultsForPhase` with **zero callers**. The orchestrator receives `workloadRepo` and
  stores it as `o.workloads`, but the field is **write-only** — never read in
  `internal/orchestrator`. The `attacks` / `attack_results` tables are consequently orphaned
  (no writer), and the standalone-attack REST surface (`atk-`/`atkres-` ids) is unrouted.
  **Decide:** drop `o.workloads` + the unused methods + the two tables (epoch-2 already dropped
  `phase_service_resources`, so this is in-pattern), or restore the standalone-attack feature
  and wire `CreateAttack`/`CreateAttackResult` into a handler.

## UI ↔ backend route gaps (live calls to routes no deployable backend serves)

| UI caller | Calls | Served by | Effect |
|---|---|---|---|
| `src/lib/api/runs.ts` (Runs screen, top-level `/runs` nav, no `NotWiredYet` guard) | `GET/POST /api/v1/runs[/{id}/faults\|steps\|events\|events/history\|resources\|pause\|resume\|stop]` | **only** manteion-go `feat/run-details-page-rewrite` (`run_flat_handler.go`, `zeus_callback_handler.go`, `phase_handler.go`, prometheus live-metrics) | entire Runs UI 404s on `feat/phase-fsm-port` (merge candidate) and `develop` (deployed) |
| `src/components/services/service-detail-panel.tsx` | `GET /sdk/instances/{id}`, `POST .../kill-switch`, `POST .../cachebox` | nothing (backend serves only the `/sdk/instances` collection) | buttons fail at runtime; `atrocontrol` has `FreezeInstance`/`PushRulesToInstance`/`ClearInstance` but no HTTP route — half-built |
| `src/lib/prometheus.ts` (dashboard, observability cards) | `GET/POST /api/v1/prometheus/query[_range]` | nothing; also ignores the `vite.config.ts` `/prometheus/api` proxy + `VITE_PROMETHEUS_URL` | dashboard observability dead in dev and prod |
| `src/components/phase-hover-card.tsx` | `GET /experiments/{id}/phase/{name}/status` (name-addressed) | backend is id-addressed `GET /experiments/{id}/phases/{phaseId}` (+ `/results`) | hover-card status fetch 404s; epoch-2 alignment miss |

**Recommendations:** treat `feat/run-details-page-rewrite` as a hard cross-repo dependency of the
Runs UI — port its flat `/runs` handler layer onto `feat/phase-fsm-port` before merging, or land
both backend branches together. For service-detail and prometheus, either add the missing routes
(atrocontrol per-instance methods exist; prometheus needs a reverse-proxy route to
`PROMETHEUS_URL`) or gate/repoint the UI. For phase-hover-card, switch to id-addressed phases.

## No UI driver for the FSM keystone

`feat/phase-fsm-port` wires `POST /experiments/{id}/start|pause|resume|cancel|stop`, phase CRUD,
and the results endpoints — but **no UI calls them** (`experiments/$experimentId.tsx` is a
`NotWiredYet` stub). The keystone deliverable is operator-curl-only. Likewise `GET
/api/v1/faults/catalog` (the authoritative fault vocabulary + param metadata) has no UI consumer —
the fault editor uses a hardcoded `src/lib/faults.ts` switch that can silently drift from the
backend catalog. **Track** the experiment-detail / phase-lifecycle UI as the explicit FSM-port
follow-on, and switch the fault editor to fetch the catalog.

## Wire-contract / dependency findings

- **`go.mod` pins a fictional atropos-go version (HIGH).** `require
  git.ucsc.edu/microfaults/atropos-go v0.0.8-0.20260518024008-a1ba3ac6313f` (commit `a1ba3ac`)
  but `git ls-tree a1ba3ac` has **no `faultparams/` directory**; that package (imported by
  `internal/ruleconv`, `internal/faultcatalog`, `internal/model`, …) was introduced only at
  atropos-go `develop` tip `fd494e8` (the breaking `feat!: unify fault wire schema` commit). The
  build works **only** via `replace => ../atropos-go`. The deployed binary (`705437e`, built on
  VM1 where the local atropos clone is `fd494e8`) is fine; the risk is purely the recorded
  contract — any consumer/CI that drops the replace or re-derives the pin fails to compile.
  **Fix:** `go get git.ucsc.edu/microfaults/atropos-go@fd494e8 && go mod tidy` (keep the replace),
  in the same change that merges the FSM port; optionally tag atropos-go `fd494e8` as `v0.0.9`.
- **zeus `AttackConfig` ↔ manteion `AttackRequest` is consistent (INFO).** All JSON tags match
  (`id`, `target{url,method,headers}`, `rate`, `duration_s`, `timeout_s`, `max_connections`,
  `max_body_bytes`, `redirects`, `dedup_bypass{strategy,source}`, `meta_trace_id`,
  `experiment_id`, `run_ref`, `workflow_label`). Only asymmetry: zeus `TargetSpec` has an extra
  `body []byte` (omitempty) manteion omits — benign for GET-style attacks. Note both sides
  hand-mirror the JSON (no shared Go type) — a future field add must touch both structs.
- **Go/dep skew is benign.** All three modules declare `go 1.25.5`; atropos↔manteion wire deps
  are identical (otel v1.43.0, grpc v1.80.0, protobuf v1.36.11, x/net v0.52.0). zeus-go skews
  older (protobuf 1.36.8, x/net 0.43.0) but shares no Go types (JSON/HTTP) — optionally refresh
  for x/net security fixes, no contract coupling.

## SSE fast-path is half-wired (LOW)

`broadcastRulesChanged` fires only from the 3 rule-mutation handlers
(`rule_handler.go:48,134,174`). Three other paths bump `rule_version` **without** broadcasting:
`handleFireFaultConfig` + `handleCancelFaultConfig` (`fault_config_handler.go:101,138`) and the
fault-config reaper (`fault_config_reaper.go:45`); the orchestrator's `PushRules`/`FreezeService`
intent changes likewise never broadcast (no broker reference in `internal/orchestrator`). So a
fired/cancelled/expired manual fault or an experiment intent change gives subscribed SDKs no SSE
nudge — they converge only on the next poll. Degraded, not broken. **Fix:** route every
`rule_version` bump through one bump+broadcast helper, or document the SSE channel as rules-only.

## Parallel-but-dead fault path (INFO)

`atrocontrol.Controller` exposes `InjectFault`/`ClearFault` (+ per-instance variants) that push
faults to SDKs, but they have **no production caller** — manual long-running faults are delivered
via the poll `active_faults` desired-state set instead (`handleFireFaultConfig` → `MarkFired` +
`BumpVersion`; `activeFaultsForService` unions them into the poll/register response). Two parallel
delivery mechanisms exist; only the poll one is wired (by design per `CLAUDE.md`). Keep the
`controller_test` coverage so the unused push path stays wire-compatible with the poll path's
`FaultRequest` shape, or delete it to remove a second source of truth.
