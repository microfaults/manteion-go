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
| `internal/orchestrator/` | The experiment FSM. `enterPhase`: generation bump → preload gate → freeze gate → phase rules → workflow materialization → zeus runs + additive attacks → poller. `finishPhase`: the single terminal path — CAS `running→draining→completed` for recording phases (drain barrier), fidelity verdict *before* thaw, harvest exactly once, then experiment advance. Every transition (enter, pause, drain, finish) bumps the generation of each service the phase touches. Phase-level pause (experiment pause is derived; a paused phase's rules and cache-box role leave the desired state until resume); `Recover` reconciles crash state at startup. |
| `internal/orchestrator/{drain,verdict,admission}.go` | Drain barrier + gap classification, per-phase verdict from W6 snapshots, refusal of overlapping concurrent experiments (the SDK is single-tenant). |
| `internal/cachestore/` | NDJSON cache store: batch-seq dedup, per-key accounting, collision stats, drain-report ledger, W5 checksum. |
| `internal/desired/` | Desired-state materializer (protocol v1): per-service generations (`service_generations`; a bump wakes held long-polls and SSE subscribers) and `Materializer`, which builds each `(service, instance)` document from the database — phase-aware rules, active fault configs, the synthesized cache-box rule, and `freeze_cfg` and `replay_set`, derived from the running phase that freezes the service and the artifact assigned to its `(phase, service)` (`PhaseSources`) — filtered by the instance's capabilities (a host without a cache-box gets neither of those two), with ETag `"<sha256[:16]>"`: the content hash alone (the generation rides `X-Atropos-Generation` and the body's `version`), so a bump that leaves an instance's document unchanged keeps its ETag. Every input change bumps the generation except, in plan 1: a fault config's time-based expiry, which changes the document before the reaper's bump (`docs/long-running-faults.md`); a fault-spec edit (`PUT /api/v1/faults/specs/{id}`); attaching or detaching a phase's rules (`AttachPhaseRules` on experiment and phase create, `UpdatePhase` on a phase edit: a standalone rule attached to a pending phase goes dark, a detached one live again); and deleting a phase or an experiment (the delete cascades to `phase_rules`). Their content hash still moves: a non-held poll sees them at once, a held one when its wait elapses or the next bump arrives, so within one wait. |
| `internal/atrocontrol/` | SDK fanout controller: freeze/thaw, rule push, staged preload driver, fidelity pulls. Its in-memory intent tracker is no longer read for the register and poll answers (everything in them, `freeze_cfg` and `replay_set` included, comes from the database) and goes with the push paths in plan 2. Tracker writes are field-scoped: freeze and thaw are the only writers of the tracked freeze config (rule pushes and fault injection leave it alone), and a restart loses it. Liveness filter is `FilterLive` — `stale` (slow-polling) pods are live and owe drain records; only `dead` is excluded. |
| `internal/atropos/` | HTTP client for individual SDK instances (delay, preload, fidelity, clear). |
| `internal/zeus/` | HTTP client for zeus (workflows, runs, attacks, datasets). |
| `internal/ruleconv/` | Rule → wire compilation. Wire structs (`CompiledRule`, `FaultRequest`, `DelayRequest`) are **imported from atropos-go** — one contract, two repos; the poll, register and status bodies are atropos-go's own `RuleSync`, `RegisterResponse` and `StatusReport` (`internal/api/sdk_wire.go`), so a shape drift fails the build; `model.ReplaySetRef` stays the persistence type and converts at that boundary. Cache-box rules are **synthesized per running phase** (`SynthesizeCacheBoxRule`), never stored. |
| `internal/api/` | HTTP server; one handler file per domain group. Swagger covers a subset of routes — the code is authoritative. |
| `internal/faultcatalog/` | Fault vocabulary + typed param validation backed by `atropos-go/faultparams`. |
| `internal/policy/` | WIP-frozen behind `MANTEION_POLICY_ENGINE=off`. |

## Wire contracts (shared with atropos-go)

- **Poll** — `GET /api/v1/sdk/rules?service=&instance_id=&version=&wait=` with `If-None-Match`:
  records `version` (the generation the instance holds) as its `observed_generation`, then answers
  200 with the **full desired state** — `version` is the per-service generation, `etag` is the
  content hash alone (`"<sha256[:16]>"`, never the generation), plus `rules`, `active_faults`,
  `freeze_cfg`, `replay_set` — or 304 when `If-None-Match` names the current ETag; both carry
  `ETag` and `X-Atropos-Generation`. With `wait` (seconds, ≤ 60) the poll is held only when
  `version` is the current generation and the ETag matches, until the service's generation moves
  or the wait elapses (a long-poll), then re-materialized; a matching ETag with an older `version`
  is answered 304 at once with the current generation, which the instance adopts without
  re-applying. The document is filtered by the instance's capabilities; an empty rule array is
  authoritative (clears SDK rules and triggers the drain tracker).
- **Register** — `POST /api/v1/sdk/register`: instance identity, published routes, `host` and
  `capabilities`; the response is `status` plus the materialized desired state (the 200 poll body),
  filtered by the advertised capabilities. Re-registering with other capabilities bumps the
  generation.
- **Status** — `POST /api/v1/sdk/status`: the instance's report after every apply and on each
  replay-set state change (`applied_generation`, `etag`, unsupported rules, errors, replay-set
  id/state), a full snapshot stored on the instance row (`observed_*`, `replay_set_*`), except
  that a report never lowers `observed_generation` (the poll is its authoritative writer).
- **Ingest** — `POST /api/v1/cache/ingest`: recorded-entry batches, deduped by
  `(experiment, phase, instance, batch_seq)`; stays open while the phase drains.
- **Drain** — `POST /api/v1/sdk/cachebox/drain`: per-`(experiment, phase)` instance report; the
  barrier compares manteion-received counts against SDK-recorded counts, with a W6 fidelity-pull
  fallback for lost reports.
- **Preload** — SDK-side `POST /cachebox/preload/{begin,chunk,commit,abort}`; commit verifies the
  W5 checksum (implementation pinned to an identical cross-repo test vector in both repos).
- **SSE** — `GET /api/v1/sdk/events?service=`: `rules_changed` with data `{"generation":G}` on every
  generation change (orchestrator transitions included) nudges the instance to poll at once; the
  long-poll is primary.

## HTTP surface (grouped)

Health (`/healthz`, `/readyz`, `/api/v1/status`) · Rules CRUD · Fault specs / compositions CRUD ·
Fault catalog · SDK (register / instances / rules / status / init / events) · Experiments
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

**Plan 1 runs no experiments with the desired-state SDK.** This plan-1 materializer must not run
experiments against the branch SDK (atropos `feat/atroposd`, plan C) until plan 2 lands; the older
SDK, which sends no `If-None-Match` and re-applies every 200, is not affected. A 200 without
`replay_set` clears the SDK's replay set, and none is served yet (nothing assigns replay-set artifacts until the
orchestrator builds them, in plan 2), so a frozen instance drops the set its preload just installed at the
first 200 after the freeze (within one wait). The phase-entry rule
push through `/admin/rules` replaces the rules the SDK applied from the document with the phase's
rules alone (no standalone rules, no cache-box rule) and leaves its held ETag in place, so later
polls answer 304 and never restore them. The phase-end nil push clears every rule on the phase's
services the same way, after the post-phase 200, and later 304s never restore them either.

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
