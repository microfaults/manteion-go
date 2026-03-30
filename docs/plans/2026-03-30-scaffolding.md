# Manteion-go: Central Controller Scaffolding

## Context

Manteion is the rule coordination oracle that turns atropos from a per-service library into a distributed platform. Currently every service needs static rule configuration. Manteion provides centralized rule management, zeus-go workflow orchestration, and startup dependency enforcement so SDKs initialize with the latest configuration before serving traffic.

## Architecture Overview

- **REST API** (stdlib `net/http`, Go 1.22+ method-path routing) -- matches zeus-go's Archer pattern
- **In-memory stores** for rules and SDK instances -- MVP, no external deps
- **HTTP proxy** to zeus-go Archer for workflow/attack/policy operations
- **Version-based polling** -- SDKs poll for rule changes, 304 on no-change
- **Zero external dependencies** -- stdlib only (`go.mod` has no `require` block)

## Project Structure

```
manteion-go/
├── cmd/manteion/main.go              # Entry point, dep wiring, graceful shutdown
├── internal/
│   ├── api/
│   │   ├── server.go                 # Server struct, routes(), JSON helpers
│   │   ├── rule_handler.go           # Rule CRUD (POST/GET/PUT/DELETE /api/v1/rules)
│   │   ├── sdk_handler.go            # SDK register, deregister, poll rules, init check
│   │   ├── zeus_handler.go           # Proxy pass-through to Archer
│   │   └── health_handler.go         # /healthz, /readyz, /api/v1/status
│   ├── rule/
│   │   └── store.go                  # Rule type, MatchCriteria, Store (sync.RWMutex)
│   ├── sdk/
│   │   └── registry.go              # Instance type, Registry (sync.RWMutex)
│   └── zeus/
│       └── client.go                 # HTTP client proxying to Archer
├── Dockerfile                        # Multi-stage build (golang:1.25-alpine -> alpine:3.21)
├── kubernetes-manifests/
│   ├── manteion.yaml                 # Deployment + Service
│   └── kustomization.yaml
├── skaffold.yaml
├── go.mod
└── go.sum
```

## API Routes

```
# Rule Management
POST   /api/v1/rules              Create rule
GET    /api/v1/rules              List all rules
GET    /api/v1/rules/{id}         Get rule by ID
PUT    /api/v1/rules/{id}         Update rule
DELETE /api/v1/rules/{id}         Delete rule

# SDK Registration & Polling
POST   /api/v1/sdk/register       Register SDK instance
DELETE /api/v1/sdk/register/{id}  Deregister SDK instance
GET    /api/v1/sdk/instances      List registered instances
GET    /api/v1/sdk/rules          Poll rules (?service=X&version=N) -- 304 if unchanged
GET    /api/v1/sdk/init           Startup readiness check for SDK init

# Zeus Proxy (pass-through to Archer)
POST   /api/v1/zeus/workloads              -> POST /api/v1/workloads
GET    /api/v1/zeus/workloads              -> GET /api/v1/workloads
DELETE /api/v1/zeus/workloads/{id}         -> DELETE /api/v1/workloads/{id}
POST   /api/v1/zeus/attacks                -> POST /api/v1/attacks
GET    /api/v1/zeus/attacks/{id}           -> GET /api/v1/attacks/{id}
DELETE /api/v1/zeus/attacks/{id}           -> DELETE /api/v1/attacks/{id}
POST   /api/v1/zeus/policies              -> POST /api/v1/policies
GET    /api/v1/zeus/policies              -> GET /api/v1/policies
DELETE /api/v1/zeus/policies/{id}         -> DELETE /api/v1/policies/{id}

# Health
GET    /api/v1/status             Status overview (rules count, instances count, zeus reachable)
GET    /healthz                   Liveness probe
GET    /readyz                    Readiness probe
```

## Key Types

### `internal/rule/store.go`

```go
Rule {
    ID        string            // unique rule ID
    Name      string            // display name
    Service   string            // target service (e.g., "productcatalog")
    Enabled   bool
    Priority  int               // higher = evaluated first
    Match     MatchCriteria     // when to apply
    Fault     json.RawMessage   // opaque fault spec -- SDK deserializes
    Mode      string            // "inline" or "background"
    CreatedAt time.Time
    UpdatedAt time.Time
}

MatchCriteria {
    InjectionPoint string            // "ingress","egress","transient","custom", "" = any
    Labels         map[string]string // all must match (AND semantics)
}

Store {
    mu      sync.RWMutex
    rules   map[string]*Rule
    version uint64              // monotonic, bumped on every mutation
}
```

Store methods: `NewStore`, `Add`, `Get`, `Update`, `Delete`, `List`, `ForService`, `Version`

### `internal/sdk/registry.go`

```go
Instance {
    ID           string    // e.g., "frontend-pod-abc123"
    Service      string    // service name
    Version      string    // service version
    Address      string    // for future health probes
    RegisteredAt time.Time
    LastPollAt   time.Time
}

Registry {
    mu        sync.RWMutex
    instances map[string]*Instance
}
```

Registry methods: `NewRegistry`, `Register` (upsert -- pods restart), `Get`, `Deregister`, `List`, `ForService`, `TouchPoll`

### `internal/zeus/client.go`

```go
Client {
    baseURL    string
    httpClient *http.Client  // 30s timeout
}
```

Client methods: `NewClient`, `Do` (generic proxy), `Status`, `Healthy`

## SDK Polling Protocol

1. SDK registers: `POST /api/v1/sdk/register` with `{id, service, version, address}`
2. SDK polls: `GET /api/v1/sdk/rules?service=frontend&version=42`
3. If store version == requested version: **304 Not Modified** (fast path)
4. Otherwise: **200** with `{version: 43, rules: [...]}`
5. SDK calls `atropos.Configure()` with an evaluator built from the received rules

## Startup Dependency

**Server-side**: `GET /api/v1/sdk/init` returns `{"status": "ready"}` when manteion is listening.

**Client-side** (described, not scaffolded -- this is an atropos-go change):
- SDK Init resolves `MANTEION_URL` env var
- Retry loop with backoff to `/api/v1/sdk/init`
- On success: register + fetch initial rules
- On failure after timeout: `Init` returns error -> service's `main()` calls `log.Fatal`

**Kubernetes**: manteion Deployment gets readiness probe on `/readyz`. Services can also use an initContainer as fallback: `until wget -qO- http://manteion:8080/api/v1/sdk/init; do sleep 2; done`

## Environment Variables

- `MANTEION_ADDR` -- listen address (default `:8080`)
- `ZEUS_URL` -- Archer base URL (default `http://archer:8080`)

## Implementation Order

1. `go.mod`
2. `internal/rule/store.go` -- no deps
3. `internal/sdk/registry.go` -- no deps
4. `internal/zeus/client.go` -- no deps
5. `internal/api/server.go` -- imports rule, sdk, zeus
6. `internal/api/health_handler.go`
7. `internal/api/rule_handler.go`
8. `internal/api/sdk_handler.go`
9. `internal/api/zeus_handler.go`
10. `cmd/manteion/main.go`
11. `Dockerfile`
12. `kubernetes-manifests/manteion.yaml`
13. `kubernetes-manifests/kustomization.yaml`
14. `skaffold.yaml`

## What's Left as TODO

| Area | Scaffolded | TODO (needs your input) |
|------|-----------|------------------------|
| Fault spec | `json.RawMessage` field | Concrete type (align with `faultRequest` or new `FaultSpec`) |
| Match evaluation | `MatchCriteria` struct | Actual matching logic (SDK-side concern) |
| Experiment orchestration | Zeus proxy | Baseline -> isolation -> combination protocol |
| SDK health probing | `Address` field on Instance | Active probe loop from manteion |
| Kill switch | Comment placeholder | Emergency disable-all-rules-for-service endpoint |
| Auth | None | mTLS, API keys, RBAC |
| Persistence | In-memory | Durable storage for rules |

## Reference Files

- `zeus-go/internal/api/server.go` -- Server struct, routes, JSON helpers pattern
- `zeus-go/internal/workload/registry.go` -- In-memory store with sync.RWMutex
- `zeus-go/cmd/archer/main.go` -- Entry point, wiring, graceful shutdown
- `atropos-go/internal/evaluator/evaluator.go` -- Evaluator interface and types
- `atropos-go/.claude/worktrees/great-brahmagupta/admin.go` -- faultRequest/DemoEvaluator pattern

## Verification

1. `go build ./...` compiles cleanly
2. `go vet ./...` passes
3. Manual curl tests:
   - `POST /api/v1/rules` with sample rule JSON -> 201
   - `GET /api/v1/rules` -> list with the created rule
   - `POST /api/v1/sdk/register` -> 201
   - `GET /api/v1/sdk/rules?service=X&version=0` -> 200 with rules
   - `GET /api/v1/sdk/rules?service=X&version=<current>` -> 304
   - `GET /api/v1/sdk/init` -> 200
   - `GET /healthz` -> 200
   - Zeus proxy routes return 502 (no Archer running) -- confirms proxy wiring
4. `docker build .` succeeds
