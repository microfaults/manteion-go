# manteion-go

Control plane of faults-lab (Go, Postgres). It stores rules, fault specs and experiments, serves every atropos SDK instance a desired-state document that the instance long-polls, runs experiment phases (freeze services, replay recordings, drive load through zeus), and records a fidelity verdict per frozen phase.

## Workspace

<!-- Shared by the AGENTS.md of all five faults-lab repos; keep the copies in sync. Last verified 2026-11-18. -->
Five sibling git repos are checked out side by side in one workspace folder, written `faults-lab/` below (the folder itself is not a repo):

- `atropos-go`: Go SDK embedded in each service (faults, cache-box record/replay, OTel), plus the `atroposd` sidecar and the `atropos-agent` node agent.
- `manteion-go`: control plane in Go with Postgres. Holds rules and experiments and runs experiment phases.
- `zeus-go`: load generator. Runs k6 workflows and vegeta attacks when manteion asks.
- `service-beds`: the measured system, a Go port of Google's Online Boutique with atropos embedded.
- `manteion-ui`: React operator console for manteion.

zeus sends load to the bed. Each bed service embeds atropos, and every atropos instance long-polls manteion for its desired state; manteion never pushes rules. manteion calls zeus to start and stop load, and dials instance addresses only to read status and cache-box fidelity. The UI calls manteion's API (and, today, Prometheus directly).

Default checkouts lag by months. Before reading code, run `git worktree list` and `git branch -a --sort=-committerdate` and work in the newest tree. On 2026-11-16 those were: atropos-go `feat/atroposd` in `atropos-go/.claude/worktrees/elated-shannon-771fa0`; manteion-go and service-beds `feat/atropos-daemon` in `worktrees/manteion-atropos-daemon` and `worktrees/service-beds-atropos-daemon`; zeus-go `develop` in its main checkout; manteion-ui `mock-mode` in `manteion-ui/.claude/worktrees/mock-mode`. None of them was merged yet.

The lab is a single-node k3s on a home-LAN box at `10.0.0.250`. Since 2026-11-18 the lab runs there in namespace `faults-lab`: manteion with Postgres, zeus, the lean Go bed (service-beds overlay `demo`), Prometheus and an OTel collector agent, deployed only with service-beds' `scripts/lab-up.sh`. zeus runs in the cluster, not on the owner's Mac; Tempo, Loki and Grafana are not deployed. ssh works as a normal user (`<user>@10.0.0.250`, key auth) with no sudo, and kubectl works for that user. The box also runs the owner's unrelated services; leave everything outside `faults-lab` alone. The UCSC VMs (vm1, vm2, `2262-cse115b-0*.be.ucsc.edu`) are retired, so docs, manifests and scripts that name them describe history. The box is unreachable off that LAN: run `nc -z -G 5 10.0.0.250 22` before any deploy step, and check on the host what is actually deployed.

The cross-repo knowledge base is `faults-lab/docs` (not under git). `research/` is still valid background. `ONBOARDING-TLDR.md`, `STATUS-2026-08-19.md`, `design/DESIGN-CALLS-cachebox-2026-07-30.md` and the generated `api/` pages predate the pull-based desired-state protocol and the new host; read them as history.

Remotes: `github` (github.com/microfaults, public) is the primary remote; `origin` (git.ucsc.edu) is legacy.

## Commands

```bash
make verify          # go build, gofmt -l (fails on output), go vet, go test
go test -race ./...
make integration     # MANTEION_INTEGRATION=1 MANTEION_TEST_DB=1 go test -count=1 -timeout 300s -p 1 ./...
make openapi-local   # regenerate docs/swagger.{yaml,json} on this Mac
go run ./cmd/manteion
```

- Plain `go test ./...` passes while skipping every database-backed test. Store and orchestrator suites need `MANTEION_INTEGRATION=1`; many api and orchestrator tests also need `MANTEION_TEST_DB=1`. `make integration` sets both. CI's `go-integration` job sets only `MANTEION_INTEGRATION`, so the `MANTEION_TEST_DB` tests skip there too.
- The DB tests need Postgres: `colima start`, then `docker compose up -d postgres` (postgres:18-alpine on 5432). The default DSN is `postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable`; set `MANTEION_DATABASE_URL` to use another. If another worktree's compose holds 5432, run a separate `postgres:18-alpine` on `127.0.0.1:55432` and point `MANTEION_DATABASE_URL` at it. Keep `-p 1`: the suites share one database and truncate every table between tests. Remove the container and volume afterwards.
- OpenAPI: use `make openapi-local`, then `git diff --exit-code -- docs/swagger.yaml docs/swagger.json` (that diff is what CI's `openapi-check` fails on). Plain `make openapi` fails here because swag v2.0.0-rc5 cannot parse Go 1.27's standard library and `go.work` declares go 1.27; `openapi-local` runs under go.mod's go1.25.5 through a temporary copy of `go.work`. Needs `swag` v2.0.0-rc5 and python3 with PyYAML (install notes in the Makefile header).
- The experiment and phase handlers carry no swag annotations, so the spec does not cover them; the code is the reference.
- `go run ./cmd/manteion` listens on `:9090` (`MANTEION_ADDR`). The Kubernetes manifest sets `:8080`, which is why other repos say `manteion:8080`.

## How this tree resolves atropos

manteion compiles against a different atropos-go depending on how it is built. Check which one before trusting a fault-catalog or wire behaviour.

- Local `go build` / `go test`: the untracked, gitignored `go.work` replaces atropos with `faults-lab/worktrees/atropos-pin`.
- Docker and skaffold: `go.mod`'s `replace => ../atropos-go`, which from this tree is `worktrees/atropos-go`, a symlink to `worktrees/atropos-beds-pin`. The Dockerfile and `skaffold.yaml` also assume the repo directory is named `manteion-go` beside `atropos-go` (`COPY manteion-go/`, `dockerfile: manteion-go/Dockerfile`), which is not true for this worktree. The lab image is therefore built from a staging directory made by `service-beds/scripts/lab-up.sh`: `git archive` copies of manteion-go and atropos-go as `manteion-go/` and `atropos-go/`, with modules vendored from a local file GOPROXY, cross-built for linux/amd64.
- CI clones atropos-go `develop`, a third version.
- The fault catalog validates through atropos `faultparams`, so `network/packet_loss` and `mechanism: netem` are accepted by a beds-pin build and rejected (422) by a `go.work` build.
- The README's "SDK dependency" section (atropos required by tag, no replace, `GOPRIVATE`) is wrong; `go.mod` has the replace.

## Where the code misleads

- Nothing is pushed to SDK instances. Rules, fault configs, `freeze_cfg` and replay sets reach them only through the polled document. `internal/atropos` is read-only (`GetRules`, `GetFault`, `GetCacheBoxStats`, `GetCacheBoxFidelity`); README line 35 still says write methods exist. `docs/2026-06-wiring-audit.md` and `docs/architecture-diagram.html` still draw pushes.
- Desired state needs the phase and its experiment both `running`: `RuleRepo.ForService` and `ExperimentRepo.ListRunningPhases` (the source of the cache-box rule, `freeze_cfg` and `replay_set`) join on the experiment's status. A cancel or stop that fails partway (a crash after the experiment's terminal CAS, or a `finishPhase` error) still leaves the phase `running` under an ended experiment. Nothing is served for it any more, but no exit bump follows, so held polls drop it only when their wait ends, and `Recover` only scans running experiments, so nothing revisits the phase, its load or its open fault events. On a cancel the experiment's CAS lands before `finishPhase` stops the load, so an instance whose held poll re-materializes in between drops the phase's rules and freeze while load still runs (a skipped phase is not harvested). A phase started through the phase-level API under an experiment that is not `running` is served nothing, and a frozen one cannot converge.
- An interrupted resume is completed, not failed. Pause leaves the old zeus handles on `phase_workflows`; if manteion restarts during a resume's convergence wait, `Recover` sees a running phase with drivers and completes it over pre-pause traffic. The "entry interrupted by a manteion restart" guard only catches fresh entries.
- The phase-level API bypasses the experiment state machine and admission. `POST /experiments/{id}/phases` adds phases to running or finished experiments, and `POST .../phases/{phaseId}/start` never checks the experiment's status or service overlap. Admission itself is check-then-act without a lock and passes when its reads fail.
- `DELETE` on an experiment or phase works in any status and tears nothing down: zeus load keeps running, the poller ticks until its safety-net deadline (at least 30 min), and there is no exit bump, verdict or harvest.
- Fault configs are not phase-scoped. `fault_configs.phase_id` is only a tag; a config with `duration_ms: 0` outlives its phase and experiment, and a config fired at a frozen service bypasses admission and the verdict.
- The kill switch (`POST /sdk/instances/{id}/kill-switch`) acts on the whole service, not the instance. It disables rules permanently (there is no un-kill) and cancels fault configs, but leaves a running phase, its load, its cache-box rule and freeze in place, and the OPA harness can re-enable rules on its next tick.
- Pausing a recording (`persist_cache`) phase loses records: the SDK's pause-triggered flush is answered `409 phase_not_recording`, which the SDK treats as terminal. The final drain then likely reports the baseline degraded.
- A frozen phase is set `running` before its replay-set artifact exists, so an instance polling in that window gets `freeze_cfg` without a replay set and fails closed (synthetic 503s).
- Convergence means "observed the generation", not "applied it". `observed_errors` and `unsupported` do not block, and `internal/desired/capabilities.go` drops the `netem` capability and filters by category only. A phase whose fault no instance can execute still converges and is recorded as a fault phase.
- Experiment start answers 202 even when the first phase failed to converge, after blocking up to `MANTEION_CONVERGE_TIMEOUT` (30 s). Only resume and phase start return `504 not_converged`. Experiment pause, resume and cancel wait behind an in-flight entry because they take the same per-experiment lock.
- Verdicts exist only for completed frozen phases. They check replay misses, uncommitted preload, degraded baseline, missing telemetry and collision rate; there is no dropped-iteration check, and `INVALID` is stored without refusing anything.
- `error_rate` in `phase_workflow_results` is `requests_dropped / requests_sent`, i.e. k6 dropped iterations (`internal/orchestrator/harvest.go`). A phase with half its requests failing with 5xx records about 0. HTTP failures are only in `raw_metrics`.
- zeus forgets runs and attacks when it restarts. A workflow run's `GET /runs/{id}` 404 (`zeus.ErrRunNotFound`) fails the phase on the next poll with "zeus no longer knows run ...", but an attack's `GET /attacks/{id}` 404 is still treated as transient, so a phase whose attack zeus lost hangs until `max(30 min, duration + 5 min)` and fails with a misleading "safety-net deadline" reason. Stopped runs get no zeus stats snapshot, so no `phase_workflow_results` row is written.
- The zeus events proxy shares the zeus client's 30 s total timeout and copies without flushing, so `/api/v1/zeus/runs/{id}/events` is cut at 30 s and arrives buffered.
- State lost on restart: cache-store dedup and received counts, collision stats, claims, pollers. SDK retries after a restart can append duplicate batches; the drain gate then usually times out as degraded. A crash after a phase's terminal CAS loses its verdict, harvest and fault-event closing for good. `Recover` reads at most 200 running experiments.
- Some document changes do not bump the generation: a fault config's time-based expiry (until the reaper's bump), a fault-spec edit, attaching or detaching phase rules, and deleting a phase or experiment. Held polls see them only when their wait ends, within one wait.
- An enabled rule attached to no phase is live immediately. Rules are created disabled; `tools/expctl` creates them disabled, attaches them, then enables them.
- `readJSON` ignores unknown fields and `MatchCriteria.Validate` is never called, so typos in request bodies are dropped silently. The UI's `match_ast` is dropped the same way.
- `docs/design/DESIGN-CALLS-cachebox-2026-07-30.md` in the knowledge base reads D4, D6 and D8 as adopted; none is implemented as written. Nothing marks a recording degraded on `Recover` (D4); degraded baselines are allowed only by the env var `MANTEION_ALLOW_DEGRADED_BASELINE`, not a per-run flag (D6); admission refuses overlapping service footprints, not a second experiment per mesh (D8).
- OPA harness (`MANTEION_POLICY_ENGINE=rego`): a policy cannot act while Prometheus or Loki is down, so a "safety brake" policy fails static. Its input has no experiment or phase state and mutations are only scope-checked by service, so a policy can toggle phase-attached rules mid-phase. The example queries in `docs/opa-harness.md` never match (wrong service name, log body and span name).
- Stale targets: every `tools/expctl/examples/*.yaml` targets the retired VM1, and `tools/expctl/README.md` port-forwards on vm1. `exp3-latwave.yaml` fails with 422 after creating its specs and rules, leaving them behind.

## Conventions

- Schema epoch 2: migrations in `internal/db/migrations.go` are append-only, and `Migrate` refuses a database whose history it does not recognize. Resetting means dropping the database.
- PG enums: `internal/store/enum_parity_test.go` pins the Postgres labels to `model.EnumValues`; a new enum value needs a migration and the model change together.
- Handler tests are DB-gated because `Server.experiments` is a concrete `*store.ExperimentRepo` with no fake. `experiment_phases_position_unique` is DEFERRABLE, so its violations surface at `Commit()`.
- Classify errors with `errors.As` / `errors.Is` on the typed errors, never by parsing the `error` string.
- When landing `feat/atropos-daemon`, bring in the fix "stop counting k6 dropped iterations as request errors": it exists only on git.ucsc.edu `origin/develop`, not on this branch or on GitHub's `develop`.
- Commits use conventional-commit subjects with a scope (`fix(orchestrator): ...`); one commit per request, files staged by name. PR titles use `[manteion-go] <Title>`. The owner manages branches and merges, so do not push or open pull or merge requests unless asked; when asked to push, use `github`.
- `docs/` describes current reality only. Specs and plans from brainstorming stay local under `docs/superpowers/`, which is not gitignored in this repo (one older spec is tracked): do not commit new files there. `docs/plans/` holds executed plans, kept as history.

## Read these

- `README.md`: packages, wire contracts, environment table (mostly current; its SDK-dependency notes are not). Deploy only through service-beds' `scripts/lab-up.sh`; skaffold and VM1 are history.
- `docs/long-running-faults.md` and `docs/decisions/` (policy-engine freeze, OPA harness, no multitenancy).
- The SDK contract: `atropos-go` worktree `docs/plans/2026-10-30-desired-state-protocol.md`.
