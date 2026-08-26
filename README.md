# manteion-go

Control plane of the faults-lab interventional performance-attribution instrument. Manteion owns
the experiment/phase state machine, the rule oracle the atropos-go SDKs poll, the recorded-cache
store, and the zeus load driver: it freezes services, pushes recordings back into them, drives a
fixed load, and judges whether the resulting latency deltas are scientifically valid.

## The instrument in one paragraph

To measure a service's latency contribution, manteion runs an experiment as an ordered sequence of
phases. The **baseline** phase replays a fixed workflow (zeus → k6) while every service's atropos
SDK records its egress HTTP responses and pushes them to manteion (NDJSON batches with
per-`(experiment, phase)` accounting); the phase ends with a **drain barrier** — every expected SDK
instance must flush and account for its records before the phase may complete. An **isolation**
phase pushes the baseline recording *back into* the target service (staged `begin/chunk/commit`
preload verified by a W5 checksum), **freezes** it — its egress is answered from the installed
replay set with a synthetic delay, and misses fail closed (a counted synthetic 503, never a live
call) — replays the same load, and reads the latency delta. Every frozen phase gets a **fidelity
verdict** (`VALID | VALID_WITH_WARNINGS | INVALID`) computed from per-phase SDK counters, so a
leak, a mis-keyed record, or an incomplete recording surfaces as a verdict, not as silently skewed
numbers.

## Packages

| Package | Role |
|---|---|
| `internal/model/` | Domain types + `Validate()`; `enums.go` is the Go source of truth for the PG enum vocabularies. |
| `internal/store/` | Postgres repositories (pgx). CAS phase/experiment transitions live here (`TransitionPhase` with explicit from-sets). |
| `internal/db/` | Pool init + append-only migrations. **Schema epoch 2**: `Migrate` refuses a database whose history it doesn't recognize — resetting means dropping the database. |
| `internal/orchestrator/` | The experiment FSM. `enterPhase`: version bump → preload gate → freeze gate → phase rules → workflow materialization → zeus runs + additive attacks → poller. `finishPhase`: the single terminal path — CAS `running→draining→completed` for recording phases (drain barrier), fidelity verdict *before* thaw, harvest exactly once, then experiment advance. Phase-level pause (experiment pause is derived); `Recover` reconciles crash state at startup. |
| `internal/orchestrator/{drain,verdict,admission}.go` | Drain barrier + gap classification, per-phase verdict from W6 snapshots, refusal of overlapping concurrent experiments (the SDK is single-tenant). |
| `internal/cachestore/` | NDJSON cache store: batch-seq dedup, per-key accounting, collision stats, drain-report ledger, W5 checksum. |
| `internal/atrocontrol/` | SDK fanout controller: freeze/thaw, rule push, staged preload driver, fidelity pulls, intent tracker for late-joining instances. Liveness filter is `FilterLive` — `stale` (slow-polling) pods are live and owe drain records; only `dead` is excluded. |
| `internal/atropos/` | HTTP client for individual SDK instances (delay, preload, fidelity, clear). |
| `internal/zeus/` | HTTP client for zeus (workflows, runs, attacks, datasets). |
| `internal/ruleconv/` | Rule → wire compilation. Wire structs (`CompiledRule`, `FaultRequest`, `RuleSync`) are **imported from atropos-go** — one contract, two repos. Cache-box rules are **synthesized per running phase** (`SynthesizeCacheBoxRule`), never stored. |
| `internal/api/` | HTTP server; one handler file per domain group. Swagger covers a subset of routes — the code is authoritative. |
| `internal/faultcatalog/` | Fault vocabulary + typed param validation backed by `atropos-go/faultparams`. |
| `internal/policy/` | WIP-frozen behind `MANTEION_POLICY_ENGINE=off`. |

## Wire contracts (shared with atropos-go)

- **Poll** — `GET /api/v1/sdk/rules?service=&version=`: 304 on version match, else the **full desired
  state**; an empty rule array is authoritative (clears SDK rules and triggers the drain tracker).
- **Register** — `POST /api/v1/sdk/register`: instance identity + published routes; the response
  piggybacks current intent (rules, freeze config) for late joiners.
- **Ingest** — `POST /api/v1/cache/ingest`: recorded-entry batches, deduped by
  `(experiment, phase, instance, batch_seq)`; stays open while the phase drains.
- **Drain** — `POST /api/v1/sdk/cachebox/drain`: per-`(experiment, phase)` instance report; the
  barrier compares manteion-received counts against SDK-recorded counts, with a W6 fidelity-pull
  fallback for lost reports.
- **Preload** — SDK-side `POST /cachebox/preload/{begin,chunk,commit,abort}`; commit verifies the
  W5 checksum (implementation pinned to an identical cross-repo test vector in both repos).
- **SSE** — `GET /api/v1/sdk/events` (`rules_changed`) nudges pollers between intervals.

## HTTP surface (grouped)

Health (`/healthz`, `/readyz`, `/api/v1/status`) · Rules CRUD · Fault specs / compositions CRUD ·
Fault catalog · SDK (register / instances / rules / init / events) · Experiments
(CRUD + start/pause/resume/cancel/stop + results, nested phases with the same verbs) · Phases
run-details (`GET /api/v1/phases[/{id}[/faults]]`, `POST .../pause|resume|stop` — a "run" in the UI
sense is a phase) · Cache (ingest, drain) · Zeus proxy (`/api/v1/zeus/*` — workflows, runs,
datasets pass through; attacks are orchestrator-managed, not proxied).

## Environment

| Variable | Default | Purpose |
|---|---|---|
| `MANTEION_ADDR` | `:9090` | Listen address |
| `MANTEION_DATABASE_URL` | `postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable` | Postgres DSN |
| `ZEUS_URL` | `http://archer:8080` | zeus base URL |
| `PROMETHEUS_URL` | `http://prometheus:9090` | Prometheus instant-query client |
| `CACHE_DIR` | `/var/cache/manteion` | NDJSON cache store root |
| `MANTEION_PRELOAD_TIMEOUT` | `60s` | Per-instance staged-preload budget |
| `MANTEION_DRAIN_TIMEOUT` | `30s` | Drain-barrier wait before gap classification |
| `MANTEION_ALLOW_DEGRADED_BASELINE` | `false` | Permit isolation phases over a degraded baseline recording |
| `MANTEION_POLICY_ENGINE` | `off` | WIP-frozen policy loop |
| `MANTEION_SDK_PURGE_ENABLED` | `false` | Dead-instance purge sweep |

## SDK dependency

manteion requires `git.ucsc.edu/microfaults/atropos-go` **by tag** (see `go.mod`) with no
`replace` directive: builds are hermetic everywhere, including the VM image build
(`GOPRIVATE=git.ucsc.edu/*`; the Docker context is this repo alone). For local cross-repo
development against a sibling checkout, use an uncommitted workspace file:

```bash
go work init . ../atropos-go   # never commit go.work
```

## Development

```bash
docker compose up -d postgres          # postgres:17-alpine
go build ./... && go vet ./...
go test -race ./...                    # unit tests

# DB-gated integration suites (store, orchestrator, api) — need BOTH gates and -p 1
# (the suites share one Postgres and truncate tables between tests):
MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test -p 1 ./...
# or: make integration

go run ./cmd/manteion
```

Deploy runs on VM1 via skaffold (`skaffold build --default-repo=localhost:5000`, k3s pulls
`manteion:<git-sha>`). Remember the epoch guard: deploying a newer schema epoch over an old
database requires dropping the `manteion` database first; the binary re-migrates on startup.

## Companion repos

- **atropos-go** — the in-process SDK: cache-box record/replay, fault injection, OTel; polls manteion.
- **zeus-go** — execution plane: k6 workflow runs + vegeta attacks on command.
- **service-beds** — the 11-service Go online-boutique mesh the instrument measures.
- **manteion-ui** — React operator UI.

Internal research project of the UCSC Faults Lab (Peter Alvaro's group). Not licensed for external use.

## Project report

This component is documented in the UCSC master's project report
*Safe Evolution and Interventional Fault Attribution in Microservice
Meshes* (Pranay Mundra, 2026) — Part II, as the control plane of the
faults-lab instrument. The report experiments' expctl configs live in
`tools/expctl/examples/` (exp12-dose, exp13-blackhole).
