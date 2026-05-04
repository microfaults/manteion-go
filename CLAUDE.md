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
    │  GET /api/v1/sdk/rules?service=X    │ GET /api/v1/zeus/workloads, /attacks, /policies
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
| `internal/model/` | Domain types: `Rule`, `FaultSpec`, `FaultComposition`, `Experiment`, `ExperimentRun`, `Workload`, `Policy`, `SDKInstance`, `Trace`. All `Validate()` methods live here. |
| `internal/store/` | PostgreSQL repositories — one `*Repo` per entity, backed by `database/sql` + pgx driver. |
| `internal/db/` | Connection pool init (`db.Open`) and sequential schema migrations (`db.Migrate`). Migrations append-only in `migrations.go`. |
| `internal/ruleconv/` | Compiles `model.Rule` + `FaultSpec`/`FaultComposition` into `CompiledRule` wire format for SDK polling. Max composition depth = 3. |
| `internal/atrocontrol/` | Experiment orchestration controller. `Controller` fans out push-rules and cache-box freeze commands to all live SDK instances of a service. `IntentTracker` stores last-applied state so late-joining registrations receive it immediately. |
| `internal/atropos/` | HTTP client for talking to atropos-go SDK instances (`PostRules`, `PostCacheBoxDelay`, `ClearCacheBox`, `PostFault`). |
| `internal/zeus/` | HTTP client for zeus-go/Archer REST API. |
| `internal/api/` | HTTP server. `server.go` wires routes; one handler file per domain group. |

### Key design choices

- **Go 1.22+ routing** — route patterns use `"METHOD /path"` syntax; path params via `r.PathValue("id")`.
- **`rule.Rule.Fault` is opaque** — stored as `json.RawMessage`; `ruleconv` resolves the pointer to FaultSpec/Composition at poll time so the SDK receives fully inlined config.
- **Rule version in PostgreSQL** — `rule_version` table (single row, version=1 always) tracks a monotonic counter bumped on every mutation. SDK polling returns 304 when `clientVersion == currentVersion`.
- **Intent tracker** — `atrocontrol.IntentTracker` is an in-memory map so `POST /api/v1/sdk/register` can piggyback current rules/freeze config in the response without a DB round-trip.
- **Fanout concurrency** — `atrocontrol.fanout` uses a semaphore (default 16) with per-target timeouts (default 2 s); failures are collected, not fatal.
- **Fault composition max depth = 3** (atoms → groups → top-level) enforced both in `model.ValidateComposition` at write time and in `ruleconv.resolveComposition` at read time.

### HTTP API surface

| Group | Endpoints |
|---|---|
| Health | `GET /healthz`, `GET /readyz`, `GET /api/v1/status` |
| Rules | `POST/GET /api/v1/rules`, `GET/PUT/DELETE /api/v1/rules/{id}` |
| Fault specs | `POST/GET /api/v1/faults/specs`, `GET/DELETE /api/v1/faults/specs/{id}` |
| Fault compositions | `POST/GET /api/v1/faults/compositions`, `GET/DELETE /api/v1/faults/compositions/{id}` |
| SDK | `POST /api/v1/sdk/register`, `DELETE /api/v1/sdk/register/{id}`, `GET /api/v1/sdk/instances`, `GET /api/v1/sdk/rules`, `GET /api/v1/sdk/init` |
| Zeus proxy | `/api/v1/zeus/workloads`, `/api/v1/zeus/attacks`, `/api/v1/zeus/policies` → Archer |

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `MANTEION_ADDR` | `:8080` | Listen address |
| `MANTEION_DATABASE_URL` | `postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable` | PostgreSQL DSN |
| `ZEUS_URL` | `http://archer:8080` | Archer base URL |
