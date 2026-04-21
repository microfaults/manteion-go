## Project Intention

Manteion is the central coordination controller for the atropos ecosystem. It turns atropos from a per-service fault injection library into a distributed platform by providing centralized rule management, zeus-go workflow orchestration, cache-box experiment coordination, and SDK startup dependency enforcement.

## Ecosystem

- **atropos-go** — Fault injection + OTel instrumentation SDK embedded in each service. Core interface: `Evaluator.Evaluate(ctx, Request) *Decision`. Three fault categories: inline (error/hang/latency), network (blackhole/drip/latency/loss/rst/throttle), resource (cpu/memory/io). Always-on tracing with `atropos.*` span attributes.
- **zeus-go** — Load generation platform. Archer REST API orchestrates k6 workloads (broad traffic) and vegeta attacks (precision endpoint loads). After policy migration to manteion, Archer is a pure execution engine.
- **service-beds** — Go HTTP recreations of Google's Online Boutique (13 services) with OTel + atropos instrumentation. Deployed via Skaffold/Kubernetes.
- **manteion-go** (this repo) — Sits between SDKs (atropos-go) and load gen (zeus-go). Pushes evaluator rules and cache-box mode changes to SDK instances. Orchestrates experiments. Owns policy evaluation.

## Research Context

This project is part of the UCSC Faults Lab (Peter Alvaro's group) research on interventional performance attribution for microservices. See `atropos-go/VISION.md` for the full research vision.

**The problem:** When microservice workflows overlap on shared services, tail latency conflates three causally distinct phenomena — intrinsic per-service cost, application-level contention (queue/connection pool competition), and infrastructure-level coupling (CFS throttling, LLC evictions, veth queueing). These interact nonlinearly because shared services are queueing systems. Observational tools (Jaeger, Zipkin) cannot separate them. Chaos engineering tests binary correctness, not performance decomposition.

**Cache-box:** The core research primitive. Freeze a service at its SDK boundary to replay cached responses — zero CPU, zero queueing, but call graph structure preserved. Methodologically the dual of LDFI: LDFI asks "which faults break correctness?" by removing components; cache-box asks "which services cause latency?" by removing contention. Three modes: passthrough (record), replay (instant cached response), replay-with-delay (cached response + synthetic latency from observed distribution).

**Experiment protocol (manteion orchestrates this):**
1. Baseline run — all services live, measure cross-workflow interference
2. N isolation runs — freeze one shared service each via cache-box
3. Combination runs — freeze pairs to detect interaction effects
4. Compute: `delta_service = baseline_latency - isolated_latency`
5. Interaction detection: `delta_combined - sum(delta_individual)` — nonzero means superadditive interference

**Manteion's role in the roadmap:** Phase 2 of the atropos vision. Phase 1 (cache-box core in atropos-go SDK) provides the primitive. Manteion pushes mode changes and rules to SDK instances, coordinates experiment enrollment, and provides the central control plane. Phase 3 (analysis integration) builds automated latency decomposition on top.

**Key references:** LDFI, Coz (causal profiling), 3MileBeach (tracing + fault injection with baggage), Filibuster (boundary instrumentation), ShapleyIQ (game-theoretic attribution — observational, not interventional).

**Target venues:** ACM SoCC, USENIX ATC, ICPE.

## Architecture

- REST API using stdlib `net/http` with Go 1.22+ method-path routing (`mux.HandleFunc("METHOD /path", handler)`)
- PostgreSQL-backed persistence via pgx/v5 (`internal/db/` for connection management and migrations, `internal/store/` for domain repositories)
- HTTP proxy to zeus-go Archer for workload/attack operations
- Version-based SDK polling (304 Not Modified on no-change) via `rule_version` counter bumped atomically on every rule mutation
- Docker-compose for local development (postgres:17-alpine)

### Storage

- PostgreSQL for all persistent state: rules, fault specs/compositions, experiments, results, workloads, trace anchors, policy rules
- Schema managed by embedded migrations in `internal/db/migrations.go`
- Rule version counter (`rule_version` single-row table) enables efficient 304 polling
- TODO: in-memory caching layer for hot-path polling (rule version + compiled rules); SDK instances may move to in-memory store (ephemeral, high-frequency writes)

## Key Domains

- **Workloads & Attacks** — k6 workloads (Flow + Persona) with vegeta sub-attacks. Types in `internal/model/workload.go`.
- **Fault Rules** — Composable fault specs: atomic faults + composition trees (max depth 3, parallel or sequential). Network faults are direction-aware (upstream/downstream). Incompatibility validation at composition creation. Types in `internal/model/fault.go`.
- **Experiments** — Each experiment has runs (baseline + isolation + combination). Experiments are described by their runs' `FrozenServices` and each cache-box's `Mode`; there is no experiment-type discriminator. Results aggregated per (run, service, workflow). ContributionResult stores delta computation. Types in `internal/model/experiment.go`.
- **Trace Anchors** — Pointers into Jaeger/Prometheus/Tempo (time range, filters), not trace data itself. CacheBoxConfig describes frozen service state: `KeyStrategy` (`"exact"`, `"exact_with_host"`, `"exact_with_body"` — aligns with atropos SDK `cachebox.KeyStrategy` constants), `MutationPolicy`/`SafeMethods` (reproducibility metadata only, not enforced by SDK), and `SyntheticDelay`. Types in `internal/model/trace.go`.
- **Policy** — Metric-triggered actions: launch attacks or change cache-box modes. Migrated from zeus-go Archer. Types in `internal/model/policy.go`.

## HTTP API

All routes live on the main API mux (see `internal/api/server.go`). Responses are JSON; errors use `{"error": "..."}`. Successful creates return 201; lists return `[]` (never `null`) on empty; not-found returns 404 via `errors.Is(err, store.ErrNotFound)`.

### Fault Specs & Compositions

| Method | Path | Purpose |
|---|---|---|
| POST   | `/api/v1/faults/specs` | Create a fault spec |
| GET    | `/api/v1/faults/specs` | List all fault specs |
| GET    | `/api/v1/faults/specs/{id}` | Get a fault spec |
| DELETE | `/api/v1/faults/specs/{id}` | Delete a fault spec |
| POST   | `/api/v1/faults/compositions` | Create a composition (runs full `model.ValidateComposition` — depth ≤ 3, network-direction rules, incompatibilities) |
| GET    | `/api/v1/faults/compositions` | List compositions |
| GET    | `/api/v1/faults/compositions/{id}` | Get a composition |
| DELETE | `/api/v1/faults/compositions/{id}` | Delete a composition |

Create-spec: server assigns `id` if absent (`spec-…`) and always overwrites `created_at`. Validation failures return 400 (`Warn` log); store/DB failures return 500 (`Error` log).

Create-composition: same server-authoritative id/timestamp treatment. The handler resolves `FaultSpecID`/`FaultCompositionID` references against the store before `ValidateComposition`, so dangling references surface as 400 validation errors — not 500s.

## Code Style

- Go 1.25. External deps: pgx/v5 for PostgreSQL. Minimize further deps.
- Follow zeus-go patterns: `Server` struct with `Handler()`, `routes()`, JSON helpers (`writeJSON`, `writeError`, `readJSON`)
- String IDs for foreign keys, no ORM
- `json.RawMessage` for opaque/extensible fields (fault config, k6 steps, query hints)
- `Validate() error` methods on domain types
- Table-driven tests

## PR Instructions

- Title format: `[manteion-go] <Title>`
- Run `go build ./...` and `go vet ./...` before committing
- Run `go fmt` on changed files

## Reference Files

Plans:
- `docs/plans/2026-03-30-scaffolding.md` — API scaffolding (project structure, routes, types, implementation order). Note: storage layer superseded by postgres in baac8c5.
- `docs/plans/2026-03-30-relational-schemas.md` — Full data model (5 schema domains, fault incompatibility tables, design decisions)
- `docs/plans/2026-04-01-sdk-liveness-reaper.md` — SDK liveness detection and instance reaper design (not yet implemented)

Implementation:
- `internal/db/migrations.go` — Schema DDL and migration runner
- `internal/store/` — PostgreSQL-backed domain repositories (rule_repo, fault_repo, sdk_repo, experiment_repo, workload_repo, policy_repo, trace_repo)
- `internal/store/helpers.go` — Shared repo utilities (transactions, JSONB marshalling, NULL conversions)
- `internal/model/composition_validate.go` — Composition depth, direction, and incompatibility validation
- `docker-compose.yml` — Local development database (postgres:17-alpine)

Companion repos (patterns to follow):
- `zeus-go/internal/api/server.go` — Server struct, routes, JSON helpers
- `zeus-go/internal/workload/registry.go` — In-memory store with sync.RWMutex
- `zeus-go/cmd/archer/main.go` — Entry point, wiring, graceful shutdown
- `zeus-go/docs/workflow-dsl-v2.md` — Workflow DSL v2 spec (persona-key references in tree nodes)
- `atropos-go/internal/evaluator/evaluator.go` — Evaluator interface (what manteion rules configure)
- `atropos-go/internal/fault/` — Fault type implementations (what FaultSpec maps to)
- `atropos-go/VISION.md` — Full research vision, problem statement, roadmap phases
