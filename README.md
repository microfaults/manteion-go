# manteion-go

Control plane of the faults-lab interventional performance-attribution instrument. Manteion owns
the experiment/phase state machine, the rule oracle the atropos-go SDKs poll, the recorded-cache
store, and the zeus load driver: it freezes services, serves their recordings back to them as replay
sets, drives a fixed load, and judges whether the resulting latency deltas are scientifically valid.

## The instrument in one paragraph

To measure a service's latency contribution, manteion runs an experiment as an ordered sequence of
phases. The **baseline** phase replays a fixed workflow (zeus → k6) while every service's atropos
SDK records its egress HTTP responses and pushes them to manteion (NDJSON batches with
per-`(experiment, phase)` accounting); the phase ends with a **drain barrier** — every expected SDK
instance must flush and account for its records before the phase may complete. An **isolation**
phase turns the baseline recording into an immutable replay-set artifact that the target service's
instances pull and verify (entry count and W5 checksum) before any load starts, **freezes** the
service — its egress is answered from the installed replay set with a synthetic delay, and misses
fail closed (a counted synthetic 503, never a live call) — replays the same load, and reads the
latency delta. Every frozen phase gets a **fidelity verdict** (`VALID | VALID_WITH_WARNINGS |
INVALID`) computed from per-phase SDK counters, so a leak, a mis-keyed record, or an incomplete
recording surfaces as a verdict, not as silently skewed numbers.

## Packages

| Package | Role |
|---|---|
| `internal/model/` | Domain types + `Validate()`; `enums.go` is the Go source of truth for the PG enum vocabularies. |
| `internal/store/` | Postgres repositories (pgx). CAS phase/experiment transitions live here (`TransitionPhase` with explicit from-sets). |
| `internal/db/` | Pool init + append-only migrations. **Schema epoch 2**: `Migrate` refuses a database whose history it doesn't recognize — resetting means dropping the database. |
| `internal/orchestrator/` | The experiment FSM. `enterPhase`: replay-set artifacts for a frozen phase (built from the baseline recording and assigned to `(phase, service)` on a fresh start, re-read on a resume) → one generation bump per service the phase touches → convergence barrier (every alive instance observes the bump and, on a frozen service, holds the assigned replay set committed, within `MANTEION_CONVERGE_TIMEOUT`; a timeout fails the phase naming the lagging instances, suspect (`stale`) ones are named but not waited on, and a frozen service with no alive instance fails it at once) → workflow materialization → zeus runs + additive attacks → poller. Nothing is pushed to instances: rules, freeze config and replay sets reach them through their polls. `finishPhase`: the single terminal path — CAS `running→draining→completed` for recording phases (drain barrier); stop load and pull the fidelity snapshots while the phase still holds its status (its frozen instances still hold the replay set), then the terminal CAS, the verdict, the exit bump that thaws and a non-fatal wait, harvest exactly once, then experiment advance. A pause, a drain and a terminal transition read the phase's drivers and cancel its poller inside the critical section of their status CAS and stop that snapshot outside it, so a resume landing meanwhile keeps its own load; the terminal winner sweeps once more after its CAS, so load an entry started during a stop is stopped too. The entry that starts the load records the phase's fault events (freeze, rules) unless an earlier entry of the phase already did. Every transition (enter, pause, drain, finish) bumps the generation of each service the phase touches. Phase-level pause (experiment pause is derived; a paused phase's rules, cache-box role, freeze config and replay set leave the desired state until resume, which bumps and waits like an entry and leaves the phase paused if the fleet does not converge); `Recover` reconciles crash state at startup. |
| `internal/orchestrator/{drain,verdict,admission}.go` | Drain barrier + gap classification, per-phase verdict from W6 snapshots, refusal of overlapping concurrent experiments (the SDK is single-tenant). |
| `internal/cachestore/` | NDJSON cache store: batch dedup on `(instance, epoch, batch_seq)`, per-key accounting, collision stats, drain-report ledger, W5 checksum. |
| `internal/desired/` | Desired-state materializer (protocol v1): per-service generations (`service_generations`; a bump wakes held long-polls and SSE subscribers) and `Materializer`, which builds each `(service, instance)` document from the database — phase-aware rules, active fault configs, the synthesized cache-box rule, and `freeze_cfg` and `replay_set`, derived from the running phase that freezes the service and the artifact assigned to its `(phase, service)` (`PhaseSources`) — filtered by the instance's capabilities (a host without a cache-box gets neither of those two), with ETag `"<sha256[:16]>"`: the content hash alone (the generation rides `X-Atropos-Generation` and the body's `version`), so a bump that leaves an instance's document unchanged keeps its ETag. Every input change bumps the generation except, in plan 1: a fault config's time-based expiry, which changes the document before the reaper's bump (`docs/long-running-faults.md`); a fault-spec edit (`PUT /api/v1/faults/specs/{id}`); attaching or detaching a phase's rules (`AttachPhaseRules` on experiment and phase create, `UpdatePhase` on a phase edit: a standalone rule attached to a pending phase goes dark, a detached one live again); and deleting a phase or an experiment (the delete cascades to `phase_rules`). Their content hash still moves: a non-held poll sees them at once, a held one when its wait elapses or the next bump arrives, so within one wait. |
| `internal/atrocontrol/` | SDK fleet reader: instance status over the admin endpoints (`StatusByService`), W6 fidelity pulls (`FetchFidelity`, the drain fallback and the verdict) and the live registered instances of a service (`InstancesForService`, read by the convergence barrier and the drain gate). It pushes nothing: rules, faults, the freeze and replay sets reach an instance through the desired state it polls. Liveness filter is `FilterLive` — `stale` (slow-polling) pods are live and owe drain records; only `dead` is excluded. The convergence barrier waits on `alive` instances only and names `stale` ones as suspect. |
| `internal/atropos/` | HTTP client for individual SDK instances' admin endpoints: status reads (rules, fault, cache-box stats) and fidelity snapshots. Its write methods (rules, fault, delay, clear, staged preload) have no caller outside its tests since the push paths were removed. |
| `internal/zeus/` | HTTP client for zeus (workflows, runs, attacks, datasets). |
| `internal/ruleconv/` | Rule → wire compilation. Wire structs (`CompiledRule`, `FaultRequest`, `DelayRequest`) are **imported from atropos-go** — one contract, two repos; the poll, register and status bodies are atropos-go's own `RuleSync`, `RegisterResponse` and `StatusReport` (`internal/api/sdk_wire.go`), so a shape drift fails the build; `model.ReplaySetRef` stays the persistence type and converts at that boundary. Cache-box rules reach an instance two ways. The orchestrator's own is **synthesized per running phase** (`SynthesizeCacheBoxRule`), never stored. A user-authored one (`action.type: cachebox`) compiles with the context (experiment, phase, key strategy) of the running phase it is served through, so what it records and replays belongs to that phase: `RuleRepo.ForService` names the phase (`model.Rule.ServedPhaseID`/`ServedExperimentID`, never on the wire: the first running phase the rule is attached to, in `ListRunningPhases` order) and the key strategy is the rule's own. A rule attached to no phase compiles without a context. It must key like the frozen-service config of its service anywhere in the experiment — the same resolved key strategy, and no `key_headers` on that config, since the rule carries none (`model.ValidateCacheBoxStrategyAgreement`): checked when it is attached (creating a plan, adding or editing a phase) and again when the experiment or a phase starts, 422 `validation` naming the rule; a phase the scheduler reaches with a disagreeing rule fails with that reason. |
| `internal/api/` | HTTP server; one handler file per domain group. Swagger covers a subset of routes — the code is authoritative. |
| `internal/faultcatalog/` | Fault vocabulary + typed param validation backed by `atropos-go/faultparams`. |
| `internal/opaharness/` | OPA harness: Rego policies over Prometheus/Loki inputs that mutate rules through the rule repo (`MANTEION_POLICY_ENGINE=rego`); see `docs/opa-harness.md`. |
| `internal/logql/` | Loki instant metric-query client (harness inputs). |
| `internal/policy/` | Frozen PromQL threshold loop, reachable only as `MANTEION_POLICY_ENGINE=threshold`. Acts through rule CRUD: `push_rules` enables the listed rules, `clear_rules` disables the service's enabled ones, each update bumping the service's generation in-tx. |

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
  authoritative (clears SDK rules and triggers the drain tracker). An `instance_id` manteion does
  not know (deregistered, purged, a database reset) is answered 404, on which the SDK registers
  again; a poll without `instance_id` gets the unfiltered document.
- **Register** — `POST /api/v1/sdk/register`: instance identity, published routes, `host` and
  `capabilities`; the response is `status` plus the materialized desired state (the 200 poll body),
  filtered by the advertised capabilities. Re-registering with other capabilities bumps the
  generation.
- **Status** — `POST /api/v1/sdk/status`: the instance's report after every apply and on each
  replay-set state change (`applied_generation`, `etag`, unsupported rules, errors, replay-set
  id/state), a full snapshot stored on the instance row (`observed_*`, `replay_set_*`), except
  that a report never lowers `observed_generation` (the poll is its authoritative writer).
- **Ingest** — `POST /api/v1/cache/ingest`: recorded-entry batches, deduped by
  `(experiment, phase, instance, epoch, batch_seq)`; stays open while the phase drains. `epoch` is
  the instance's start time in unix nanoseconds: a restarted instance restarts `batch_seq` at 1
  under a new one, so it never collides with its previous process's batches. A body without
  `epoch` is `0` (old clients) and keeps the old key.
- **Drain** — `POST /api/v1/sdk/cachebox/drain`: per-`(experiment, phase)` instance report; the
  barrier compares manteion-received counts against SDK-recorded counts, with a W6 fidelity-pull
  fallback for lost reports.
- **Replay sets** — immutable NDJSON artifacts at `GET /api/v1/cache/artifacts/{id}` (`HEAD`,
  `Range`), pulled by the instance when the poll's `replay_set` names one (id, the
  `(experiment, phase)` pair, count, W5 digest); the instance stages, verifies and commits the set
  and reports its state, which the convergence barrier reads (W5 implementation pinned to an
  identical cross-repo test vector in both repos).
- **Kill switch** — `POST /api/v1/sdk/instances/{id}/kill-switch` disables every enabled rule of the
  instance's service and cancels its active fault configs, then bumps the generation once when it
  changed anything. It ends no phase: a running phase's synthesized cache-box rule, `freeze_cfg`
  and `replay_set` stay in the service's document until that phase stops.
- **SSE** — `GET /api/v1/sdk/events?service=`: `rules_changed` with data `{"generation":G}` on every
  generation change (orchestrator transitions included) nudges the instance to poll at once; the
  long-poll is primary.

## HTTP surface (grouped)

Health (`/healthz`, `/readyz`, `/api/v1/status`) · Rules CRUD · Fault specs / compositions CRUD ·
Fault catalog · SDK (register / instances / rules / status / init / events) · Experiments
(CRUD + start/pause/resume/cancel/stop + results, nested phases with the same verbs) · Phases
run-details (`GET /api/v1/phases[/{id}[/faults]]`, `POST .../pause|resume|stop` — a "run" in the UI
sense is a phase; start, resume, pause, cancel and stop run detached from the request, and a phase
start or resume whose fleet does not converge is 504 `not_converged`) · Cache (ingest, drain,
artifacts) · OPA policies (`/api/v1/opa/policies` CRUD, `/{id}/decisions`,
`/{id}/evaluate?apply=`) · Zeus proxy (`/api/v1/zeus/*` — workflows, runs,
datasets pass through; attacks are orchestrator-managed, not proxied).

## Environment

| Variable | Default | Purpose |
|---|---|---|
| `MANTEION_ADDR` | `:9090` | Listen address |
| `MANTEION_DATABASE_URL` | `postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable` | Postgres DSN |
| `ZEUS_URL` | `http://archer:8080` | zeus base URL |
| `PROMETHEUS_URL` | `http://prometheus:9090` | Prometheus instant-query client |
| `MANTEION_LOKI_URL` | `http://loki:3100` | Loki base URL for harness inputs |
| `CACHE_DIR` | `/var/cache/manteion` | NDJSON cache store root |
| `MANTEION_CONVERGE_TIMEOUT` | `30s` | Convergence barrier: how long a phase entry or resume waits for every alive instance to observe its bump (and hold a frozen service's replay set committed) before failing; exit and drain wait as long and only log. Must be a positive duration of at most 1 h (startup refuses anything else). The HTTP write timeout is 2 × this + `MANTEION_DRAIN_TIMEOUT` + 30 s, so a stop that waits out a drain barrier and an exit wait still gets its answer |
| `MANTEION_DRAIN_TIMEOUT` | `30s` | Drain-barrier wait before gap classification. Must be a positive duration of at most 1 h (startup refuses anything else) |
| `MANTEION_ALLOW_DEGRADED_BASELINE` | `false` | Permit isolation phases over a degraded baseline recording |
| `MANTEION_POLICY_ENGINE` | `off` | `rego` (or `on`) starts the OPA harness; `threshold` the frozen PromQL loop |
| `MANTEION_SDK_PURGE_ENABLED` | `false` | Dead-instance purge sweep |

## SDK dependency

manteion requires `git.ucsc.edu/microfaults/atropos-go` **by tag** (see `go.mod`) with no
`replace` directive: builds are hermetic everywhere, including the VM image build
(`GOPRIVATE=git.ucsc.edu/*`; the Docker context is this repo alone). For local cross-repo
development against a sibling checkout, use an uncommitted workspace file:

```bash
go work init . ../atropos-go   # never commit go.work
```

**Desired state v1.** manteion pushes nothing to SDK instances: entry assigns the replay-set
artifacts, bumps each service once and waits for the fleet to converge, exit bumps after the
fidelity verdict, and the opt-in policy engine enables and disables rules through rule CRUD
(`docs/plans/2026-10-30-desired-state-manteion-2-orchestrator.md`).

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
