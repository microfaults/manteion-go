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
    ▲ push rules / freeze via atrocontrol
    │
manteion-go (Controller)
```

### Packages

| Package | Role |
|---|---|
| `internal/model/` | Domain types: `Rule`, `FaultSpec`, `FaultComposition`, `FaultConfig`, `Experiment`, `ExperimentPhase`, `Workflow`, `Attack`/`AttackResult`, `PolicyRule`, `SDKInstance`, `TraceAnchor`. All `Validate()` methods live here; `enums.go` is the Go source of truth for the native PG enum vocabularies. |
| `internal/store/` | PostgreSQL repositories — one `*Repo` per entity, backed by `database/sql` + pgx driver. |
| `internal/db/` | Connection pool init (`db.Open`) and sequential schema migrations (`db.Migrate`). Migrations append-only in `migrations.go` — **schema epoch 2** (2026-06) reset the history to one consolidated definition; epoch-1 databases must be dropped (Migrate refuses them). |
| `internal/ruleconv/` | Resolves `model.Rule` + `FaultSpec`/`FaultComposition` into the wire format for SDK polling. The wire structs themselves (`CompiledRule`, unified `FaultRequest`, `RuleSync`) are **imported from atropos-go** so both ends share one contract. Max composition depth = 3. |
| `internal/atrocontrol/` | Experiment orchestration controller. `Controller` fans out push-rules and cache-box freeze commands to all live SDK instances of a service. `IntentTracker` stores last-applied state so late-joining registrations receive it immediately. |
| `internal/atropos/` | HTTP client for talking to atropos-go SDK instances (`PostRules`, `PostCacheBoxDelay`, `ClearCacheBox`, `PostFault`). |
| `internal/zeus/` | HTTP client for zeus-go/Archer REST API. |
| `internal/api/` | HTTP server. `server.go` wires routes; one handler file per domain group. |
| `internal/faultcatalog/` | Single source of truth for supported faults: vocabulary + typed params validation (backed by `atropos-go/faultparams`) + UI form metadata served at `GET /api/v1/faults/catalog`. |
| `internal/id/` | Entity id minting: `{prefix}-{uuidv7}` TEXT (`exp- phase- rule- spec- comp- wf- atk- atkres- fc- policy-`). |

### Key design choices

- **Go 1.22+ routing** — route patterns use `"METHOD /path"` syntax; path params via `r.PathValue("id")`.
- **`rule.Rule.Fault` is opaque** — stored as `json.RawMessage`; `ruleconv` resolves the pointer to FaultSpec/Composition at poll time so the SDK receives fully inlined config.
- **Unified fault wire shape** — fault specs, fault configs, and compiled rules all serialize faults as atropos-go's `FaultRequest` ({category, fault_type, duration_ms, ramp_*_ms, network?, params}); params schemas live in `atropos-go/faultparams` and are validated by `internal/faultcatalog`.
- **Workflows are manteion-owned** — the `workflows` table stores the full zeus DSL v2 document (`dsl JSONB`); zeus validates documents (stateless `POST /workflows/validate`) at create/update and executes after manteion materializes the definition (register-with-overwrite) at run/phase start.
- **Native PG enums** for the 14 stable vocabularies; `fault_type` stays TEXT (catalog-validated). `internal/store/enum_parity_test.go` pins pg_enum labels to `model.EnumValues`.
- **Rule version in PostgreSQL** — `rule_version` table (single row, version=1 always) tracks a monotonic counter bumped on every mutation. SDK polling returns 304 when `clientVersion == currentVersion`.
- **Intent tracker** — `atrocontrol.IntentTracker` is an in-memory map so `POST /api/v1/sdk/register` can piggyback current rules/freeze config in the response without a DB round-trip.
- **Fanout concurrency** — `atrocontrol.fanout` uses a semaphore (default 16) with per-target timeouts (default 2 s); failures are collected, not fatal.
- **Fault composition max depth = 3** (atoms → groups → top-level) enforced both in `model.ValidateComposition` at write time and in `ruleconv.resolveComposition` at read time.
- **Phase-level pause** — `experiment_status` has no `paused` label (deliberate); pausing an experiment pauses its running phase(s) while the experiment row stays `running`, and the orchestrator's scheduler (`advanceExperiment`) refuses to start new phases while any phase is `paused`. The orchestrator FSM (`internal/orchestrator`) is phase-aware: CAS transitions (`store.TransitionPhase`/`TransitionExperiment`), one terminal path (`finishPhase`) that tears down rules/freezes/attacks and harvests exactly once, a per-phase zeus poller, and crash recovery (`Recover`) that reconciles persisted `zeus_attack_id` handles.

### HTTP API surface

| Group | Endpoints |
|---|---|
| Health | `GET /healthz`, `GET /readyz`, `GET /api/v1/status` |
| Rules | `POST/GET /api/v1/rules`, `GET/PUT/DELETE /api/v1/rules/{id}` |
| Fault specs | `POST/GET /api/v1/faults/specs`, `GET/DELETE /api/v1/faults/specs/{id}` |
| Fault compositions | `POST/GET /api/v1/faults/compositions`, `GET/DELETE /api/v1/faults/compositions/{id}` |
| SDK | `POST /api/v1/sdk/register`, `DELETE /api/v1/sdk/register/{id}`, `GET /api/v1/sdk/instances`, `GET ./instances/{id}` (row + `active_rule_ids` + `recent_run_ids`), `POST ./instances/{id}/kill-switch` (disables every enabled rule for the instance's service; one version bump per rule), `GET /api/v1/sdk/rules`, `GET /api/v1/sdk/init` |
| Experiments | `POST/GET /api/v1/experiments`, `GET/DELETE ./{id}`, `POST ./{id}/start`, `POST ./{id}/pause`, `POST ./{id}/resume`, `POST ./{id}/cancel`, `POST ./{id}/stop?status=`, `GET ./{id}/results`; phases: `POST ./{id}/phases`, `GET/DELETE ./{id}/phases/{phaseId}`, `POST ./{id}/phases/{phaseId}/start`, `POST ./{id}/phases/{phaseId}/stop?status=`, `GET ./{id}/phases/{phaseId}/results` |
| Phases (run-details) | `GET /api/v1/phases`, `GET ./{phaseId}`, `GET ./{phaseId}/faults`, `POST ./{phaseId}/pause\|resume\|stop`. A "run" = a phase; live panels (steps/events/resources) are the v2 follow-on. |
| Zeus proxy | `POST/GET /api/v1/zeus/workflows`, `GET/DELETE ./{id}`, `POST ./{id}/validate`, `POST/GET ./{id}/runs`; `GET /api/v1/zeus/runs`, `GET/DELETE ./{run_id}`, `GET ./{run_id}/events`, `GET ./{run_id}/stats`; `POST/GET /api/v1/zeus/datasets`, `GET/DELETE ./{id}`, `POST ./{id}/upload`, `GET ./{id}/sample` → Archer. Attacks NOT proxied (orchestrator-managed). |

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `MANTEION_ADDR` | `:8080` | Listen address |
| `MANTEION_DATABASE_URL` | `postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable` | PostgreSQL DSN |
| `MANTEION_POLICY_ENGINE` | `off` | Opt-in for the WIP-frozen policy evaluation loop (`on` to enable) |
| `ZEUS_URL` | `http://archer:8080` | Archer base URL |
