# manteion-go

Central coordination controller for the atropos fault-injection ecosystem.

## What It Does

Manteion sits between atropos-go SDKs (embedded in application services) and zeus-go (load generation). It provides:

- **Rule management** — stores fault-injection rules and serves them to SDK instances via version-based polling (304 on no-change).
- **Fault vocabulary** — persists atomic fault specs (latency / error / hang, blackhole, throttle, CPU / memory / I-O stressors) and depth-capped composition trees.
- **SDK registration & reconciliation** — SDKs register on startup and receive any in-progress experiment intent (rules, active fault, cache-box freeze config).
- **Experiment scaffolding** — data model for baseline + isolation + combination runs used in the cache-box performance-attribution protocol.
- **Zeus proxy** — pass-through to zeus-go Archer for workload and attack lifecycle.

Full research context lives in the `atropos-go` repo under `VISION.md`.

## Ecosystem

```
atropos-go  ──►  manteion-go  ◄──  zeus-go
 (SDK:            (rules,            (k6 + vegeta
  inject, trace)   experiments,       load gen)
                   policy)
```

## Architecture

- Go 1.25 with `net/http` method-path routing.
- PostgreSQL via pgx/v5. Schema managed by embedded migrations in `internal/db/`.
- `internal/model/` — domain types with `Validate()` methods.
- `internal/store/` — repositories (rules, faults, SDK, experiments, workloads, policies, trace anchors).
- `internal/api/` — HTTP handlers; store interfaces are defined at the consumer site for testability.
- `internal/ruleconv/` — compiled rule wire format for SDK polling.
- `internal/atrocontrol/` — intent reader driving register-time reconciliation.

## HTTP Surface

- `/api/v1/rules` — full CRUD
- `/api/v1/faults/specs` — full CRUD
- `/api/v1/faults/compositions` — full CRUD; runs `model.ValidateComposition` (depth ≤ 3, network-direction rules, pair- and wildcard-incompatibilities)
- `/api/v1/sdk/register`, `/api/v1/sdk/rules` (version polling), `/api/v1/sdk/init`, `/api/v1/sdk/instances`
- `/api/v1/zeus/*` — workload / attack / policy pass-through to Archer
- `/healthz`, `/readyz`, `/api/v1/status`

## Ongoing Development

Work currently in progress or queued next:

- **Rule compilation cache** — the SDK poll endpoint resolves FK references on every request; an in-memory cache keyed by `rule_version` is planned.
- **SDK liveness reaper** — `SDKInstance.Status` is modelled but the reaper goroutine from `docs/plans/2026-04-01-sdk-liveness-reaper.md` is not yet wired.
- **Experiment orchestrator** — domain types and repos exist, but no driver chains baseline → isolation → combination runs and computes contribution deltas.
- **Policy evaluation engine** — `PolicyRule` storage exists; the tick-based evaluator from zeus-go Archer has not yet been ported.
- **Rule resolution on poll** — rules carry FK references to fault specs today; the poll response will resolve those into inlined configs so SDKs can build evaluators without a second round-trip.

## Not Yet Implemented

Deferred to later phases:

- HTTP handlers for flows, personas, workloads, experiments, results, policies, trace anchors (repos exist, routes do not).
- In-memory SDK-instance registry (current storage writes Postgres on every heartbeat).
- Network and resource fault decoding on the SDK side (`DecodeCompiledRules` errors on these categories).
- SDK-side composition evaluator (compositions serialize on the wire but are rejected on decode).
- Per-member direction default and per-member timing / offsets for sequential compositions.
- Incompatibilities as data (currently hard-coded `DefaultIncompatibilities`).
- Composition-level timeout and cancellation semantics.
- Persistent audit sink for intent transitions.
- Periodic reconciliation sweep (register-time reconcile is the current MVP).
- Index on `trace_anchors(experiment_run_id)` and typed-per-backend `QueryHint` validation.
- `ListAttacksByWorkload` stub completion, `handleReadyz` DB ping, `RuleRepo.Count()`.

## Development

Requirements: Go 1.25, Docker (for Postgres).

```bash
# Start local Postgres (postgres:17-alpine)
docker compose up -d postgres

# Build / lint / test
go build ./...
go vet ./...
go test ./...

# Run
export MANTEION_ADDR=:8080
export MANTEION_DATABASE_URL=postgres://manteion:manteion@localhost:5432/manteion
go run ./cmd/manteion
```

Health checks: `GET /healthz`, `GET /readyz`. Status summary: `GET /api/v1/status`.

## Companion Repos

- **atropos-go** — fault injection + OTel instrumentation SDK embedded in each service.
- **zeus-go** — load generation (k6 workloads, vegeta attacks).
- **service-beds** — Go recreations of Google's Online Boutique, instrumented with OTel and atropos.

## License

Internal research project of the UCSC Faults Lab (Peter Alvaro's group). Not currently licensed for external use.
