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
- In-memory stores with `sync.RWMutex` (same pattern as zeus-go `workload.Registry`)
- HTTP proxy to zeus-go Archer for workload/attack operations
- Version-based SDK polling (304 Not Modified on no-change)
- Zero external Go dependencies — stdlib only

## Key Domains

- **Workloads & Attacks** — k6 workloads (Flow + Persona) with vegeta sub-attacks. Types in `internal/model/workload.go`.
- **Fault Rules** — Composable fault specs: atomic faults + composition trees (max depth 3, parallel or sequential). Network faults are direction-aware (upstream/downstream). Incompatibility validation at composition creation. Types in `internal/model/fault.go`.
- **Experiments** — 5 types: interference, isolation, attribution, scenario, cache_fidelity. Each experiment has runs (baseline + isolation + combination). Results aggregated per (run, service, workflow). ContributionResult stores delta computation. Types in `internal/model/experiment.go`.
- **Trace Anchors** — Pointers into Jaeger/Prometheus/Tempo (time range, filters), not trace data itself. CacheBoxConfig describes frozen service state (key strategy, mutation policy, synthetic delay). Types in `internal/model/trace.go`.
- **Policy** — Metric-triggered actions: launch attacks or change cache-box modes. Migrated from zeus-go Archer. Types in `internal/model/policy.go`.

## Code Style

- Go 1.25, stdlib only (no external deps for MVP)
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
- `docs/plans/2026-03-30-scaffolding.md` — API scaffolding (project structure, routes, types, implementation order)
- `docs/plans/2026-03-30-relational-schemas.md` — Full data model (5 schema domains, fault incompatibility tables, design decisions)

Companion repos (patterns to follow):
- `zeus-go/internal/api/server.go` — Server struct, routes, JSON helpers
- `zeus-go/internal/workload/registry.go` — In-memory store with sync.RWMutex
- `zeus-go/cmd/archer/main.go` — Entry point, wiring, graceful shutdown
- `atropos-go/internal/evaluator/evaluator.go` — Evaluator interface (what manteion rules configure)
- `atropos-go/internal/fault/` — Fault type implementations (what FaultSpec maps to)
- `atropos-go/VISION.md` — Full research vision, problem statement, roadmap phases
