# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
go build ./...

# Run (requires PostgreSQL)
MANTEION_ADDR=:8080 \
MANTEION_DATABASE_URL=postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable \
ZEUS_URL=http://archer:8080 \
go run ./cmd/manteion

# Vet & test
go vet ./...
go test ./...
go test -race ./...

# Single package
go test ./internal/model/...
go test ./internal/ruleconv/...
go test ./internal/atrocontrol/...

# Docker
docker build -t manteion .

# Kubernetes (skaffold)
skaffold run   # one-off deploy
skaffold dev   # live reload
```

## Architecture

Manteion is the **central rule oracle and experiment orchestrator** for the microfaults platform. It sits between operators, the atropos-go SDK instances, and zeus-go/Archer.

```
Operator (curl / UI)
    │  POST /api/v1/rules, /faults/specs, /faults/compositions
    ▼
manteion-go  ──────────────────► zeus-go / Archer  (proxy: /api/v1/zeus/*)
    │                                     ▲
    │  GET /api/v1/sdk/rules?service=X    │ GET /api/v1/zeus/workflows, /runs, /datasets
    │  POST /api/v1/sdk/register          │
    ▼                                     │
atropos-go SDK (in each service pod) ─────┘
    ▲ fidelity pulls via atrocontrol (rules, freeze and replay
    │ sets reach the SDK through its poll, never a push)
manteion-go (Controller)
```

### Packages

| Package | Role |
|---|---|
| `internal/model/` | Domain types: `Rule`, `FaultSpec`, `FaultComposition`, `FaultConfig`, `Experiment`, `ExperimentPhase`, `Workflow`, `Attack`/`AttackResult`, `PolicyRule`, `SDKInstance`, `TraceAnchor`. All `Validate()` methods live here; `enums.go` is the Go source of truth for the native PG enum vocabularies. |
| `internal/store/` | PostgreSQL repositories — one `*Repo` per entity, backed by `database/sql` + pgx driver. |
| `internal/db/` | Connection pool init (`db.Open`) and sequential schema migrations (`db.Migrate`). Migrations append-only in `migrations.go` — **schema epoch 2** (2026-06) reset the history to one consolidated definition; epoch-1 databases must be dropped (Migrate refuses them). |
| `internal/ruleconv/` | Resolves `model.Rule` + `FaultSpec`/`FaultComposition` into the wire format for SDK polling. The wire structs themselves (`CompiledRule`, unified `FaultRequest`, `DelayRequest`) are **imported from atropos-go** so both ends share one contract; the poll, register and status bodies are atropos-go's own `RuleSync`, `RegisterResponse` and `StatusReport` (`internal/api/sdk_wire.go`), so a shape drift fails the build; `model.ReplaySetRef` stays the persistence type (an int64 count) and converts to the SDK's at that boundary with a bounds check; the register answer for an unmaterializable document is the SDK's own empty `RegisterResponse` (the contract's degraded body reads "`etag` absent or empty", and the SDK's `omitempty` drops an empty one). A user-authored cache-box rule compiles with the context of the running phase `RuleRepo.ForService` serves it through (`model.Rule.ServedPhaseID`/`ServedExperimentID`, `json:"-"`: the first running phase it is attached to, in `ListRunningPhases` order; the key strategy is the rule's own), and without one while it is attached to no phase; it must key like its service's frozen-service config in the experiment (same resolved strategy, no `key_headers` on the config), checked at attach and at experiment/phase start (422 `validation`); the orchestrator's own cache-box rule is synthesized (`SynthesizeCacheBoxRule`). Max composition depth = 3. |
| `internal/desired/` | Desired-state materializer (protocol v1). `Generations`: per-service generation counter (`service_generations`, absent row = 1) plus the wake-up fabric — every bump wakes held long-polls (`Wait`) and broadcasts SSE `rules_changed {"generation":G}` (`Broker`). `Materializer`: per `(service, instance)`, phase-aware rules, active fault configs and the synthesized cache-box rule from the database, plus `freeze_cfg` and `replay_set` derived from the running phase that freezes the service and the artifact assigned to its `(phase, service)` (`PhaseSources`: one value in both `Materializer.Freeze` and `Materializer.ReplaySets`; only a host with a cache-box gets them), filtered by the instance's capabilities (`FilterRules`/`FilterFaults`), with ETag `"<sha256[:16] of the canonical JSON>"`: the content hash alone, never the generation, which rides `X-Atropos-Generation` and the body's `version`. |
| `internal/atrocontrol/` | SDK fleet reader. `Controller` resolves a service's live instances (`InstancesForService`: the orchestrator's convergence barrier, drain gate and verdict), reads instance status over the admin endpoints (`StatusByService`) and pulls fidelity snapshots (`FetchFidelity`). It pushes nothing: rules, faults, `freeze_cfg` and replay sets reach an instance through the desired state it polls (`internal/desired/`). `FilterLive` keeps `alive` and `stale` instances (`stale` pods owe drain records); the convergence barrier waits on `alive` ones only. |
| `internal/atropos/` | HTTP client for atropos-go SDK instances' admin endpoints: the status reads (`GetRules`, `GetFault`, `GetCacheBoxStats`) and `GetCacheBoxFidelity`. It is read-only: the push-side write methods went with the push paths, so nothing in manteion can write an instance's rules, faults or cache-box. |
| `internal/zeus/` | HTTP client for zeus-go/Archer REST API. |
| `internal/api/` | HTTP server. `server.go` wires routes; one handler file per domain group. |
| `internal/faultcatalog/` | Single source of truth for supported faults: vocabulary + typed params validation (backed by `atropos-go/faultparams`) + UI form metadata served at `GET /api/v1/faults/catalog`. |
| `internal/id/` | Entity id minting: `{prefix}-{uuidv7}` TEXT (`exp- phase- rule- spec- comp- wf- atk- atkres- fc- policy- opa- pdec-`). |
| `internal/opaharness/` | OPA harness (`docs/opa-harness.md`): compiles Rego v1 policies (`package atropos`, sandboxed, 5 s timeout), evaluates `data.atropos.mutations` over `input` `{now, now_ns, inputs, rules, services, instances}` on each policy's interval, and applies the in-scope mutations (`upsert_rule`, `set_enabled`, `set_params`, `delete_rule`; idempotent) through `store.RuleRepo`, so they reach instances through the generations. Cooldown after a changing apply, read back from the decision log; every evaluation, dry runs included, is one `policy_decisions` row. Runs only with `MANTEION_POLICY_ENGINE=rego|on`. |
| `internal/logql/` | Loki instant metric-query client (`GET /loki/api/v1/query`), returning `promql.Sample`s for harness inputs. |

### Key design choices

- **Go 1.22+ routing** — route patterns use `"METHOD /path"` syntax; path params via `r.PathValue("id")`.
- **`rule.Rule.Fault` is opaque** — stored as `json.RawMessage`; `ruleconv` resolves the pointer to FaultSpec/Composition at poll time so the SDK receives fully inlined config.
- **Unified fault wire shape** — fault specs, fault configs, and compiled rules all serialize faults as atropos-go's `FaultRequest` ({category, fault_type, duration_ms, ramp_*_ms, network?, params}); params schemas live in `atropos-go/faultparams` and are validated by `internal/faultcatalog`.
- **Workflows are manteion-owned** — the `workflows` table stores the full zeus DSL v2 document (`dsl JSONB`); zeus validates documents (stateless `POST /workflows/validate`) at create/update and executes after manteion materializes the definition (register-with-overwrite) at run/phase start.
- **Native PG enums** for the 14 stable vocabularies; `fault_type` stays TEXT (catalog-validated). `internal/store/enum_parity_test.go` pins pg_enum labels to `model.EnumValues`.
- **Desired state: generations + ETag** — each service has a generation (`service_generations`), bumped with every input change: in the same transaction for rule CRUD (the kill switch's rule updates included), right after the write for fault-config fire/cancel/delete and a re-register with other capabilities, once by the kill switch after its last write when it changed anything, by the fault-config reaper when it completes an expired config, and by the orchestrator on every phase enter, pause, drain and finish for each service the phase touches. A phase entry or resume then waits for convergence: every `alive` instance of those services reports `observed_generation >=` its bump and, on a frozen service, holds the assigned replay set committed in a status report applied at the bump or later (`applied_generation >=` it: artifact ids are content hashes that recur across phases, so a replay-set state from an older generation's report is no evidence, and an older `failed` does not fail the entry fast either), within `MANTEION_CONVERGE_TIMEOUT` (default 30 s); a timeout fails the entry naming the lagging instances (a timed-out resume leaves the phase paused), `stale` instances are named as suspect but not waited on (never counted as live for convergence), a frozen service with no `alive` instance fails the entry at once, and exit and drain wait the same bound without failing. Plan-1 exceptions move the document's content hash without a bump (a non-held poll sees them at once, a held one when its wait elapses or the next bump arrives, so within one wait): a fault config's time-based expiry, which changes the document up to one reaper sweep (30 s) before the reaper's bump (`docs/long-running-faults.md`); a fault-spec edit (`PUT /api/v1/faults/specs/{id}`), which recompiles the rules using it without a bump; attaching or detaching a phase's rules (`AttachPhaseRules` on experiment and phase create, `UpdatePhase` on a phase edit), since `ForService` serves an attached rule only while its phase runs (a standalone rule attached to a pending phase goes dark, a detached one live again); and deleting a phase or an experiment, which cascades to `phase_rules` with the same effect. The poll (`GET /api/v1/sdk/rules?service=&instance_id=&version=&wait=`) records the instance's `version` as `observed_generation` first, materializes the instance's document and answers 304 when `If-None-Match` names its ETag, else 200 with the document (`ETag`, `X-Atropos-Generation` on both); with `wait` (≤ 60 s) the poll is held only when `version` is the current generation and the ETag matches, until the generation moves, or the wait elapses, or the server begins shutting down. A matching ETag with an older `version` (a bump left the instance's document unchanged) is answered 304 at once with the current generation, which the instance adopts without re-applying. An `instance_id` manteion does not know (deregistered, purged, a database reset) is answered 404, on which the SDK registers again, so no unregistered pod applies rules unseen by the convergence barrier. Register answers with the same document; `POST /api/v1/sdk/status` stores what the instance applied: the report is a full snapshot written in one statement, its `applied_generation` (migration 14, `sdk_instances.applied_generation`) beside the replay-set id and state it describes, and it only raises `observed_generation`; a report applied below the stored `applied_generation` is a stale snapshot, answered 200 (logged at Debug) with nothing written, and register resets the column to 0 with the rest of the observed state. The single-row `rule_version` counter is still bumped by rule CRUD but only backs the `/sdk/init` readiness probe; no poll decision reads it.
- **Desired state, no pushes** — manteion never pushes to an SDK instance: everything an instance applies is in the document it polls, from the database, `freeze_cfg` and `replay_set` from the running phase that freezes the service and the artifact assigned to it (`desired.PhaseSources`). Phase entry assigns the replay-set artifacts (`buildReplayArtifacts`), bumps each service once and waits for the fleet (`waitConverged`, the barrier above); exit records the fidelity verdict, then bumps. Replay sets are pulled by the instance (`GET /api/v1/cache/artifacts/{id}`). The opt-in policy engine acts through rule CRUD (`push_rules` enables the listed rules, `clear_rules` disables the service's enabled ones; each `Update` bumps in-tx). Cache ingest dedupes on `(experiment, phase, instance, epoch, batch_seq)` (in memory, single replica): `epoch` is the SDK instance's start time in unix nanoseconds, so a restarted instance (new `epoch`, `batch_seq` back at 1) never collides with its previous process's batches, and a body without `epoch` is `0`, the old key.
- **Fault composition max depth = 3** (atoms → groups → top-level) enforced both in `model.ValidateComposition` at write time and in `ruleconv.resolveComposition` at read time.
- **Dataset per phase workflow** — `phase_workflows.dataset_id` (migration 10; `workflows[].dataset_id` on the create DTOs, `PhaseWorkflow.DatasetID`) names the zeus dataset that row's k6 run reads; empty falls back to `MANTEION_ZEUS_DATASET_ID` in `startPhaseRuns` (attack legs carry none). Phase read-outs (`phaseDetail`, `PhaseDetail`) derive a read-only `dataset_ids` — set-union of the phase's workflow dataset ids, first-seen order, `[]` never null. Both create handlers (`POST /experiments`, `POST ./{id}/phases`) and both start handlers (`POST ./{id}/start`, `POST ./{id}/phases/{phaseId}/start`) preflight the plan against zeus `GET /datasets` (`internal/api/dataset_preflight.go`): every referenced id must exist, else **422** `{"error", "code": "dataset_missing", "dataset_ids": [...]}`; each must be kept past now + Σ per-phase max `duration_sec` + 5 min unless `ttl_s == 0`, else **422** `{"error", "code": "dataset_expiring", "dataset_ids": [...]}`; zeus unreachable → **502** `{"error": "zeus unreachable: ...", "code": "zeus_unreachable"}`. Plans with no dataset ids skip the zeus call. The `error` string stays in every envelope.
- **Failure reasons** — `experiment_phases.failure_reason` / `experiments.failure_reason` (migration 11) say why a row is `failed`; NULL otherwise, and every read-out omits the key then. The reason is written in the same CAS statement as the failed transition (`store.FailPhase` / `FailExperiment`), and the orchestrator has one fail entry point (`failPhase`) that requires it — `finishPhase` is for completed/skipped only. Covered fail paths: the enter sequence (preload gate — the replay-set build's refusals —, converge gate — a failed bump, a frozen service with no live instance or one whose host has no cache-box, a failed replay set, or the timeout naming each lagging instance —, workflow materialisation incl. zeus register errors, and "no load driver could be started (N configured): …" listing each driver zeus refused — `zeus rejected run <run> for workflow <wf>: status 422: <body>` via `zeus.ResponseError` — or could not be reached for), the poller (a run zeus reports `rejected`/`failed` with its reason, an attack in an unexpected state, the safety-net deadline), crash recovery (`recovery: all N zeus load drivers lost after restart: …`, and `entry interrupted by a manteion restart; create a new phase to retry` for a running phase with configured workflows but no driver handle recorded — an entry the restart caught in its convergence wait or before zeus took its first driver; a running phase with nothing configured to start is still auto-completed), and an operator `stop?status=failed` (`stopped by operator as failed`, on the phase or the experiment). An experiment failed by the phase cascade names the phase: `phase <name> failed: <phase reason>`. Surfaced as `failure_reason` on `phases[]` in experiment detail, `GET …/phases/{phaseId}`, `GET /api/v1/phases[/{phaseId}]` (list query included), and experiment detail/list — via the model json tags; nothing is set by hand in the handlers.
- **Phase-level pause** — `experiment_status` has no `paused` label (deliberate); pausing an experiment pauses its running phase(s) while the experiment row stays `running`, and the orchestrator's scheduler (`advanceExperiment`) refuses to start new phases while any phase is `paused`. A paused phase is not running: its rules, cache-box rule, `freeze_cfg` and `replay_set` leave the materialized desired state (pause bumps the services, so pause thaws), and resume serves them again, bumping and waiting like a phase entry; a resume whose fleet does not converge leaves the phase paused. The orchestrator FSM (`internal/orchestrator`) is phase-aware: CAS transitions (`store.TransitionPhase`/`TransitionExperiment`), one terminal path (`finishPhase`) that stops the load, records the verdict, bumps the phase's rules and freeze out of the documents and harvests exactly once (pause, drain and the terminal CAS read the phase's drivers and cancel its poller inside the CAS's critical section, under the phase lock, and stop that snapshot outside it; the terminal winner sweeps again after its CAS), a per-phase zeus poller, and crash recovery (`Recover`) that reconciles persisted `zeus_attack_id` handles.

### HTTP API surface

| Group | Endpoints |
|---|---|
| Health | `GET /healthz`, `GET /readyz`, `GET /api/v1/status` |
| Rules | `POST/GET /api/v1/rules`, `GET/PUT/DELETE /api/v1/rules/{id}` |
| Fault specs | `POST/GET /api/v1/faults/specs`, `GET/DELETE /api/v1/faults/specs/{id}` |
| Fault compositions | `POST/GET /api/v1/faults/compositions`, `GET/DELETE /api/v1/faults/compositions/{id}` |
| SDK | `POST /api/v1/sdk/register` (answers with the materialized desired state), `DELETE /api/v1/sdk/register/{id}`, `GET /api/v1/sdk/instances` (rows carry `host`, `capabilities` and the observed state), `GET ./instances/{id}` (row + `active_rule_ids` + `recent_run_ids`), `POST ./instances/{id}/kill-switch` (disables every enabled rule for the instance's service, one rule-version and generation bump per rule, and cancels its active fault configs, then, when anything changed, bumps the generation once; it ends no phase, so a running phase's synthesized cache-box rule, `freeze_cfg` and `replay_set` stay; answers `disabled_rule_ids` and `cancelled_fault_config_ids`, never null), `GET /api/v1/sdk/rules` (long-poll, ETag/304), `POST /api/v1/sdk/status`, `GET /api/v1/sdk/init`, `GET /api/v1/sdk/events` (SSE `rules_changed`) |
| Experiments | `POST/GET /api/v1/experiments`, `GET/PUT/DELETE ./{id}`, `POST ./{id}/start`, `POST ./{id}/pause`, `POST ./{id}/resume`, `POST ./{id}/cancel`, `POST ./{id}/stop?status=`, `GET ./{id}/results`; phases: `POST ./{id}/phases`, `GET/PUT/DELETE ./{id}/phases/{phaseId}`, `POST ./{id}/phases/{phaseId}/start`, `POST ./{id}/phases/{phaseId}/stop?status=`, `GET ./{id}/phases/{phaseId}/results`. PUT edits are plan-only (409 otherwise): the experiment must be `planned`, and a phase must also be `pending`; omitted body fields keep their value. |
| Phases (run-details) | `GET /api/v1/phases`, `GET ./{phaseId}`, `GET ./{phaseId}/faults`, `POST ./{phaseId}/pause\|resume\|stop`. A "run" = a phase; live panels (steps/events/resources) are the v2 follow-on. |
| OPA policies | `POST/GET /api/v1/opa/policies`, `GET/PUT/DELETE ./{id}`, `GET ./{id}/decisions` (paged), `POST ./{id}/evaluate?apply=true\|false` (dry run by default) |
| Zeus proxy | `POST/GET /api/v1/zeus/workflows`, `GET/DELETE ./{id}`, `POST ./{id}/validate`, `POST/GET ./{id}/runs`; `GET /api/v1/zeus/runs`, `GET/DELETE ./{run_id}`, `GET ./{run_id}/events`, `GET ./{run_id}/stats`; `POST/GET /api/v1/zeus/datasets`, `GET/DELETE ./{id}`, `POST ./{id}/upload`, `GET ./{id}/sample` → Archer. Attacks NOT proxied (orchestrator-managed). |

**Error envelope.** Every handler returns `{"error": "<message>"}` on 4xx/5xx (`api.ErrorResponse`; expctl and the UI's generic path read `error`). The experiment and phase handlers (both route groups above) add a machine-readable `code` plus code-specific fields — `internal/api/error_response.go` (`writeErrorCode`, `errorEnvelope`). Rules / faults / SDK handlers still emit the bare envelope.

| Status | `code` | When | Extra fields |
|---|---|---|---|
| 400 | `bad_json` | body is not valid JSON | — |
| 404 | `not_found` | unknown experiment / phase id (`store.ErrNotFound`) | — |
| 409 | `conflict` | unique collision: experiment id, phase name or position (`store.ErrConflict`) | — |
| 409 | `invalid_state` | lifecycle refusal — not planned / not running / phase not pending or paused / already terminal / plan-only PUT (`orchestrator.ErrInvalidState`) | `status`: the current experiment or phase status |
| 409 | `service_overlap` | admission control (INV-5): the footprint overlaps a running experiment (`orchestrator.ErrServiceOverlap`) | `running_experiment_id`, `services` (sorted) |
| 422 | `validation` | well-formed but rejected: empty name, vus/duration ≤ 0, a phase that both sets `persist_cache` and freezes services (record in one phase, freeze in a later one: a recording phase's verdict is pulled after its drain has thawed it), cache-box strategy disagreement (between phases, or a user cache-box rule against its service's freeze), unknown workflow/rule id, mutation_policy, no phases, unknown `?status=` (`store.ErrValidation`, `orchestrator.ErrValidation`) | — |
| 502 | `zeus_unreachable` | transport failure talking to zeus while starting/resuming a phase (`orchestrator.ErrZeusUnreachable`); the phase is finalized `failed` | — |
| 504 | `not_converged` | a phase start or resume whose fleet did not observe its bump within `MANTEION_CONVERGE_TIMEOUT` (`orchestrator.ErrNotConverged`); a started phase is finalized `failed`, a resumed one stays `paused`. An experiment start reports its first phase's entry on the phase, as it does every entry failure. The start, resume, pause, cancel and stop handlers run the orchestrator detached from the request (`context.WithoutCancel`), so a client hanging up does not abort a wait | `lagging`, `suspect` (one line per instance) |
| 500 | `internal` | anything unclassified | — |

Typed errors wrap with `%w` and keep their historical log text; classify with `errors.As` / `errors.Is`, never by parsing `error`.

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `MANTEION_ADDR` | `:8080` | Listen address |
| `MANTEION_DATABASE_URL` | `postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable` | PostgreSQL DSN |
| `MANTEION_POLICY_ENGINE` | `off` | `rego` (or `on`) starts the OPA harness; `threshold` the frozen PromQL loop |
| `MANTEION_LOKI_URL` | `http://loki:3100` | Loki base URL for harness inputs |
| `MANTEION_CONVERGE_TIMEOUT` | `30s` | Convergence barrier bound: fatal at phase entry and resume, logged at exit and drain. Must be a positive duration of at most 1 h, as must `MANTEION_DRAIN_TIMEOUT` (startup refuses anything else); the HTTP write timeout is 2 × it + `MANTEION_DRAIN_TIMEOUT` + 30 s |
| `MANTEION_DRAIN_TIMEOUT` | `30s` | Drain-barrier wait for every expected instance's drain report before gap classification; a positive duration of at most 1 h |
| `ZEUS_URL` | `http://archer:8080` | Archer base URL |
| `MANTEION_ZEUS_DATASET_ID` | (empty) | Fallback zeus dataset for phase workflow rows without a `dataset_id` of their own |
