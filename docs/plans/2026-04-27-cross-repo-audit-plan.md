# Cross-Repo Audit & API Contracts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish OpenAPI 3.0 specs for `manteion-go`, `atropos-go`, and `zeus-go` via `swaggo/swag` v2; ship the approved atropos service-author DX extensions (all Tier 1 + `TestMatch`); wire `manteion-ui` to consume the generated TS types; bump `service-beds/checkoutService` to the new atropos SDK; delete zeus's redundant policies engine.

**Architecture:** Annotation-driven OpenAPI (swag v2) with a CI gate that fails on stale specs. Atropos DX extensions are pure additions to the existing public surface — no breaking changes to current `Init`/`Configure`/middleware paths. `TestMatch` is generic over a `MatchableRequest` interface so gRPC/Thrift adapters can land later without redesign. Zeus policies engine + manteion's `/api/v1/zeus/policies` proxy delete cleanly (pre-v1, no live consumers, no deprecation).

**Tech Stack:** Go 1.25, PostgreSQL, `swaggo/swag` v2 (OpenAPI 3.0), `openapi-typescript` v6+ (UI consumer), `net/http` with Go 1.22+ method-path patterns, GitHub Actions for CI.

**Source spec:** [docs/plans/2026-04-27-cross-repo-audit-design.md](2026-04-27-cross-repo-audit-design.md).

---

## File Structure Overview

**`manteion-go`**
- New: `Makefile`, `.github/workflows/openapi.yml`, `internal/api/error_response.go`, `internal/api/autorule_handler.go`, `internal/api/autorule_handler_test.go`, `docs/swagger.yaml`, `docs/swagger.json`, `docs/openapi-conventions.md`
- Modified: `cmd/manteion/main.go` (server constructor args), `internal/api/server.go` (annotations + AutoRule routes + drop `/api/v1/zeus/policies`), every `internal/api/*_handler.go` (annotations), `internal/api/zeus_handler.go` (drop policies switch arms)

**`atropos-go`**
- New: `Makefile`, `.github/workflows/openapi.yml`, `info.go`, `info_test.go`, `decision_context.go`, `decision_context_test.go`, `errors.go`, `errors_test.go`, `snapshot.go`, `snapshot_test.go`, `snapshot_admin.go`, `snapshot_admin_test.go`, `match.go`, `match_test.go`, `docs/swagger.yaml`, `docs/swagger.json`, `docs/openapi-conventions.md`
- Modified: `admin.go`, `cachebox_admin.go`, `rules_admin.go` (annotations), `atropos.go` (`ActiveRules`, `SDKInfo`, `Snapshot`, `TestMatch` exports), `middleware.go` (stash decision on ctx), `internal/cachebox/cachebox.go` (expose `Stats()` if not already), `internal/evaluator/static.go` (read-only access for `TestMatch`)

**`zeus-go`**
- New: `Makefile`, `.github/workflows/openapi.yml`, `docs/swagger.yaml`, `docs/swagger.json`
- Modified: every `internal/api/*_handler.go` (annotations), `docs/api-contract.md` (slim to overview)
- Deleted: `internal/policy/engine.go` and any policy-related routes/wiring

**`manteion-ui`**
- New: `scripts/gen-types.sh`, `src/types/manteion-api.ts` (generated, gitignored or committed — see Task 10)
- Modified: `package.json` (add `openapi-typescript` devDep + script), existing fetch wrappers to use generated types

**`service-beds/microservices-demo-go/src/checkoutService`**
- Modified: `go.mod`, `go.sum` (atropos bump), `main.go` (optionally adopt `atropos.Snapshot()` on `/healthz`)

---

## Task 1: Foundations — swag v2 toolchain + error envelope (manteion-go)

Establishes the OpenAPI generation pattern that Tasks 2–4 propagate. Defines the standard error envelope as a Go type so swag annotations can reference it.

**Files:**
- Create: `Makefile`, `.github/workflows/openapi.yml`, `internal/api/error_response.go`, `docs/openapi-conventions.md`, `docs/swagger.yaml`, `docs/swagger.json`
- Modify: `internal/api/server.go` (`writeError` to use new type + add `@title`/`@version` package-level annotations; annotate `handleListRules` as the reference example)
- Test: existing handler tests still pass

- [ ] **Step 1: Install `swag` v2 binary and module dep**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
go install github.com/swaggo/swag/v2/cmd/swag@latest
go get github.com/swaggo/swag/v2
swag --version  # expect 2.x
```

Expected: `swag version v2.x.x`.

- [ ] **Step 2: Create `Makefile` with `openapi` target**

```makefile
.PHONY: openapi openapi-check test build

openapi:
	swag init \
		--v3.1 \
		--generalInfo cmd/manteion/main.go \
		--dir cmd/manteion,internal/api,internal/model \
		--output docs \
		--outputTypes yaml,json \
		--parseDependency \
		--parseInternal

openapi-check: openapi
	@git diff --exit-code -- docs/swagger.yaml docs/swagger.json || \
		(echo "ERROR: swagger spec is stale. Run 'make openapi' and commit." && exit 1)

test:
	go test ./...

build:
	go build ./...
```

- [ ] **Step 3: Create `internal/api/error_response.go`**

```go
// Package api error envelope used across all manteion HTTP handlers.
// swag annotations reference api.ErrorResponse for 4xx/5xx documentation.
//
// Standard shape (subject to future migration to RFC 9457 problem+json
// per manteion-ui/docs/API-NEEDED.md §C.5).
package api

// ErrorResponse is the JSON body returned for all error responses.
//
// Example: {"error": "rule: invalid mode \"foo\""}
type ErrorResponse struct {
	Error string `json:"error" example:"validation failed"`
}
```

- [ ] **Step 4: Update `writeError` in `internal/api/server.go` to use `ErrorResponse`**

Replace the existing `writeError`:

```go
// writeError writes a JSON error response using the standard envelope.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}
```

- [ ] **Step 5: Add package-level swag annotations to `cmd/manteion/main.go`**

Insert above `func main()`:

```go
// @title           Manteion Control-Plane API
// @version         1.0
// @description     Central coordination controller for the atropos ecosystem.
// @description     Manages rules, faults, experiments, workflows, and SDK lifecycle.
// @host            localhost:8080
// @BasePath        /api/v1
// @schemes         http https
// @produce         json
// @accept          json
func main() {
```

- [ ] **Step 6: Annotate `handleListRules` as the reference example in `internal/api/rule_handler.go`**

Above the existing `handleListRules` function, add:

```go
// handleListRules returns all rules.
//
// @Summary      List rules
// @Description  Returns all configured rules, ordered by priority descending.
// @Tags         rules
// @Produce      json
// @Success      200  {array}   model.Rule
// @Failure      500  {object}  api.ErrorResponse  "internal error"
// @Router       /rules [get]
func (s *Server) handleListRules(w http.ResponseWriter, r *http.Request) {
```

- [ ] **Step 7: Run `make openapi`, verify spec generates**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
make openapi
ls docs/swagger.yaml docs/swagger.json
```

Expected: both files exist; `docs/swagger.yaml` contains a `paths./rules.get` block referencing `model.Rule` and `api.ErrorResponse`.

- [ ] **Step 8: Run existing tests to confirm `writeError` change doesn't break anything**

```bash
go test ./internal/api/...
```

Expected: PASS.

- [ ] **Step 9: Create `docs/openapi-conventions.md` (the style guide for Tasks 2–4)**

```markdown
# OpenAPI Annotation Conventions

All Go handlers in this repo use [swaggo/swag](https://github.com/swaggo/swag) v2 to generate OpenAPI 3.1.

## Required annotations on every handler

- `@Summary` — one-line imperative ("Create rule", "List workflows")
- `@Description` — one paragraph; UI renders this as JSDoc
- `@Tags` — one of: `rules`, `autorules`, `faults`, `sdk`, `experiments`, `workflows`, `zeus-proxy`, `health`, `admin`
- `@Produce json` (or `text/event-stream` for SSE)
- `@Accept json` (for routes that take a body)
- `@Success` and `@Failure` for every documented status code
- `@Router` with method in brackets

## Body and response types

- Use Go model types directly: `{object} model.Rule`, `{array} model.Workflow`
- Use `api.ErrorResponse` for all 4xx/5xx responses
- For path/query params, use `@Param name in type required "description"`

## Streaming endpoints (SSE)

- `@Produce text/event-stream`
- Event types listed in the description text
- See run-events handler (when added) for the canonical example

## Regenerating the spec

```
make openapi
```

Commit `docs/swagger.{yaml,json}` together with handler edits. CI fails if the spec is stale.
```

- [ ] **Step 10: Create CI workflow `.github/workflows/openapi.yml`**

```yaml
name: OpenAPI spec freshness

on:
  pull_request:
    paths:
      - 'cmd/**'
      - 'internal/api/**'
      - 'internal/model/**'
      - 'docs/swagger.*'
      - 'Makefile'
  push:
    branches: [main, develop]

jobs:
  spec-fresh:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.25'
      - name: Install swag v2
        run: go install github.com/swaggo/swag/v2/cmd/swag@latest
      - name: Verify spec is fresh
        run: make openapi-check
```

- [ ] **Step 11: Commit foundations**

```bash
git add Makefile .github/workflows/openapi.yml \
  internal/api/error_response.go internal/api/server.go \
  internal/api/rule_handler.go cmd/manteion/main.go \
  docs/openapi-conventions.md docs/swagger.yaml docs/swagger.json \
  go.mod go.sum
git commit -m "feat(openapi): swag v2 toolchain + error envelope + reference annotation"
```

---

## Task 2: Annotate manteion-go remaining routes

Apply the Task-1 pattern across every remaining handler. ~30 routes spanning rules (already started), faults, sdk, experiments, health, zeus-proxy. Each handler's annotation block is a one-time mechanical write.

**Files:** Modify every `internal/api/*_handler.go` (excluding `rule_handler.go:handleListRules` which is already done in Task 1, and excluding zeus's `/policies` arms which Task 12 deletes).

- [ ] **Step 1: Inventory all handler functions**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
rg -n "^func \(s \*Server\) handle" internal/api/
```

Expected: ~30 functions across `rule_handler.go`, `fault_handler.go`, `sdk_handler.go`, `health_handler.go`, `zeus_handler.go`. Save the list as the working checklist.

- [ ] **Step 2: Annotate `handleCreateRule`** (`internal/api/rule_handler.go`)

```go
// handleCreateRule creates a new rule.
//
// @Summary      Create rule
// @Description  Persist a new rule. Body must validate per model.Rule.Validate().
// @Tags         rules
// @Accept       json
// @Produce      json
// @Param        rule  body      model.Rule  true  "Rule definition"
// @Success      201   {object}  model.Rule
// @Failure      400   {object}  api.ErrorResponse  "validation error"
// @Failure      409   {object}  api.ErrorResponse  "id conflict"
// @Failure      500   {object}  api.ErrorResponse
// @Router       /rules [post]
func (s *Server) handleCreateRule(w http.ResponseWriter, r *http.Request) {
```

- [ ] **Step 3: Annotate remaining rule handlers** — `handleGetRule`, `handleUpdateRule`, `handleDeleteRule`. Pattern follows Step 2; tag `rules`; success codes per existing handler behavior.

- [ ] **Step 4: Annotate `fault_handler.go`** — `handleCreateFaultSpec`, `handleListFaultSpecs`, `handleGetFaultSpec`, `handleDeleteFaultSpec`, plus the four composition variants. Tag: `faults`. Use `model.FaultSpec` and `model.FaultComposition` as body/response types.

- [ ] **Step 5: Annotate `sdk_handler.go`** — `handleRegister`, `handleDeregister`, `handleListInstances`, `handlePollRules`, `handleInit`. Tag: `sdk`. The `handlePollRules` annotation must document the `If-None-Match` header for version-checked polling and 304 response.

```go
// handlePollRules returns the current compiled rule set if newer than the client's version.
//
// @Summary      Poll for rule updates
// @Description  SDKs send their last-known rule version via If-None-Match.
// @Description  Returns 304 if unchanged, 200 with full rule set if newer.
// @Tags         sdk
// @Produce      json
// @Param        If-None-Match  header  string  false  "Last known rule version (uint64)"
// @Success      200  {array}   ruleconv.CompiledRule
// @Success      304  "no rule changes since If-None-Match"
// @Failure      500  {object}  api.ErrorResponse
// @Router       /sdk/rules [get]
func (s *Server) handlePollRules(w http.ResponseWriter, r *http.Request) {
```

- [ ] **Step 6: Annotate `health_handler.go`** — `handleHealthz`, `handleReadyz`, `handleStatus`. Tag: `health`. Simple 200/503 responses.

- [ ] **Step 7: Annotate `zeus_handler.go` (workloads + attacks only)**. The proxy handles a small number of routes; tag: `zeus-proxy`. Body shapes are passthrough and not modeled in manteion — annotate as opaque.

```go
// handleZeusProxy forwards workload and attack requests to zeus-go.
//
// @Summary      Zeus passthrough
// @Description  Proxies the request to zeus-go's load generation API.
// @Description  Body and response shapes are defined by zeus-go; see its OpenAPI spec.
// @Tags         zeus-proxy
// @Accept       json
// @Produce      json
// @Success      200  "passthrough — see zeus-go spec for response shape"
// @Failure      502  {object}  api.ErrorResponse  "zeus unreachable"
// @Router       /zeus/workloads [post]
func (s *Server) handleZeusProxy(w http.ResponseWriter, r *http.Request) {
```

Repeat the `@Router` line per registered zeus route (workloads GET/POST/DELETE, attacks GET/POST/DELETE). Skip the `policies` arms — Task 12 deletes them.

- [ ] **Step 8: Regenerate spec and verify**

```bash
make openapi
git diff docs/swagger.yaml | head -50
go test ./...
```

Expected: spec contains entries for every annotated route; tests still pass.

- [ ] **Step 9: Commit**

```bash
git add internal/api/ docs/swagger.yaml docs/swagger.json
git commit -m "feat(openapi): annotate manteion-go handlers"
```

---

## Task 3: Annotate atropos-go admin handlers

Atropos exposes three admin handlers (`FaultAdminHandler`, `CacheBoxAdminHandler`, `RulesAdminHandler`) for host services to mount. Spec describes their endpoint shape with the recommended mount path `/atropos/admin/`.

**Files:**
- Create: `Makefile`, `.github/workflows/openapi.yml`, `docs/openapi-conventions.md`, `docs/swagger.yaml`, `docs/swagger.json`, package-level annotation file `atropos.go` additions
- Modify: `admin.go`, `cachebox_admin.go`, `rules_admin.go` (per-handler annotations)

- [ ] **Step 1: Install swag v2 in atropos-go module**

```bash
cd /Users/pronei/work/faults-lab/atropos-go
go install github.com/swaggo/swag/v2/cmd/swag@latest
go get github.com/swaggo/swag/v2
```

- [ ] **Step 2: Create `Makefile`**

```makefile
.PHONY: openapi openapi-check test build

openapi:
	swag init \
		--v3.1 \
		--generalInfo atropos.go \
		--dir . \
		--output docs \
		--outputTypes yaml,json \
		--parseDependency \
		--parseInternal \
		--exclude internal/cachebox/testdata,internal/evaluator/testdata

openapi-check: openapi
	@git diff --exit-code -- docs/swagger.yaml docs/swagger.json || \
		(echo "ERROR: swagger spec is stale. Run 'make openapi' and commit." && exit 1)

test:
	go test ./...

build:
	go build ./...
```

- [ ] **Step 3: Add package-level annotations at top of `atropos.go`**

```go
// @title           Atropos SDK Admin API
// @version         1.0
// @description     Admin handlers exposed by the atropos-go SDK. Host services
// @description     mount these handlers at a path of their choice; recommended
// @description     mount path is /atropos/admin/. Endpoints below are documented
// @description     relative to that mount path.
// @host            (host-defined)
// @BasePath        /atropos/admin
// @schemes         http https
// @produce         json
// @accept          json

// Package atropos provides the Go SDK for the atropos fault-injection control plane.
package atropos
```

- [ ] **Step 4: Annotate `FaultAdminHandler`** (`admin.go`)

Add above `func FaultAdminHandlerWith(...)`:

```go
// FaultAdminHandlerWith returns an http.Handler for ad-hoc per-request fault binding.
//
// @Summary      Bind a fault to the next request matching criteria
// @Description  POST registers a transient fault binding evaluated by the demo evaluator.
// @Description  GET returns current bindings.
// @Tags         admin
// @Accept       json
// @Produce      json
// @Param        binding  body  FaultRequest  false  "binding (POST only)"
// @Success      200  {array}   FaultStatus
// @Success      201  {object}  FaultStatus
// @Failure      400  {object}  ErrorResponse  "invalid binding"
// @Router       /faults [post]
// @Router       /faults [get]
func FaultAdminHandlerWith(eval *DemoEvaluator, resolve NetworkResolver) http.Handler {
```

Note: atropos doesn't have an `ErrorResponse` type yet. Define it now in `errors.go` (Task 7 expands this file with sentinels):

```go
// File: errors.go (initial form; Task 7 expands)
package atropos

// ErrorResponse is the JSON envelope returned for admin handler errors.
type ErrorResponse struct {
	Error string `json:"error" example:"invalid binding"`
}
```

- [ ] **Step 5: Annotate `CacheBoxAdminHandler`** (`cachebox_admin.go`)

```go
// CacheBoxAdminHandler returns an http.Handler for runtime cache-box control.
//
// @Summary      Inspect or change cache-box state
// @Description  GET returns current mode + stats. POST {mode} switches mode.
// @Description  POST {synthetic_delay} sets replay-with-delay parameters.
// @Tags         admin
// @Accept       json
// @Produce      json
// @Param        body  body  DelayRequest  false  "delay configuration (mode-set requests)"
// @Success      200  {object}  cachebox.Stats
// @Failure      400  {object}  ErrorResponse
// @Router       /cachebox [get]
// @Router       /cachebox [post]
func CacheBoxAdminHandler(cb *CacheBox) http.Handler {
```

(Task 8's `cachebox.Stats` type lives in `internal/cachebox`; if not yet exported, mark this annotation TODO and Task 8 wires it.)

- [ ] **Step 6: Annotate `RulesAdminHandler`** (`rules_admin.go`)

```go
// RulesAdminHandler returns an http.Handler for runtime rule management.
//
// @Summary      Manage SDK runtime rules
// @Description  GET returns the current rule list. POST replaces it atomically with the body.
// @Tags         admin
// @Accept       json
// @Produce      json
// @Param        rules  body  []StaticRule  false  "rules (POST only)"
// @Success      200  {array}  StaticRule
// @Success      204  "rules replaced"
// @Failure      400  {object}  ErrorResponse  "invalid rules"
// @Router       /rules [get]
// @Router       /rules [post]
func RulesAdminHandler(eval *StaticEvaluator) http.Handler {
```

- [ ] **Step 7: Copy `docs/openapi-conventions.md` from manteion-go (adjust intro for atropos-specific context)**

Same content as Task 1 Step 9, but the intro paragraph reads:

```markdown
All admin handlers in this repo are annotated with [swaggo/swag](...) v2 to
generate OpenAPI 3.1. Host services that mount these handlers can publish a
combined spec or reference this one directly.
```

- [ ] **Step 8: Generate spec, run tests**

```bash
make openapi
go test ./...
```

Expected: spec generated; tests pass.

- [ ] **Step 9: Add CI workflow `.github/workflows/openapi.yml`** (same as manteion-go's; only paths filter changes)

```yaml
name: OpenAPI spec freshness
on:
  pull_request:
    paths:
      - '*.go'
      - 'docs/swagger.*'
      - 'Makefile'
  push:
    branches: [main, develop]
jobs:
  spec-fresh:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.25'
      - run: go install github.com/swaggo/swag/v2/cmd/swag@latest
      - run: make openapi-check
```

- [ ] **Step 10: Commit**

```bash
git add Makefile .github/workflows/openapi.yml docs/ \
  errors.go atropos.go admin.go cachebox_admin.go rules_admin.go \
  go.mod go.sum
git commit -m "feat(openapi): swag v2 toolchain + admin handler annotations"
```

---

## Task 4: Port zeus-go api-contract.md → swag annotations

Zeus already documents its API in `docs/api-contract.md`. Port that content to handler-level annotations; the markdown shrinks to a high-level overview. Drift between the markdown and code surfaces during this step — record findings in commit messages.

**Files:**
- Create: `Makefile`, `.github/workflows/openapi.yml`, `docs/openapi-conventions.md`, `docs/swagger.yaml`, `docs/swagger.json`, `internal/api/error_response.go`
- Modify: every handler file under `internal/api/`, `docs/api-contract.md` (slim to overview)
- Excluded: any `policies` route — Task 12 deletes those

- [ ] **Step 1: Install swag v2 in zeus-go**

```bash
cd /Users/pronei/work/faults-lab/zeus-go
go install github.com/swaggo/swag/v2/cmd/swag@latest
go get github.com/swaggo/swag/v2
```

- [ ] **Step 2: Create `Makefile`** — same shape as manteion-go's, with `--generalInfo cmd/zeus/main.go` (or whatever the zeus main is — verify with `find cmd -name main.go`).

- [ ] **Step 3: Create `internal/api/error_response.go`** — same `ErrorResponse` struct as manteion-go's.

- [ ] **Step 4: Inventory zeus handlers and cross-check against `docs/api-contract.md`**

```bash
cd /Users/pronei/work/faults-lab/zeus-go
rg -n "^func.*handle|^func.*Handler" internal/api/
```

For each handler, note (a) the route registered in zeus's mux, (b) the corresponding section in `docs/api-contract.md`. Drift candidates: route exists in code but missing from contract, contract documents a route that doesn't exist, response shape differs.

- [ ] **Step 5: Add package-level annotations to `cmd/zeus/main.go`** — title `Zeus Load Generation API`, BasePath `/api/v1`, schemes `http https`. Reuse description text from `docs/api-contract.md`'s opening paragraph.

- [ ] **Step 6: Annotate run lifecycle handlers**

Per `api-contract.md`, runs have a state machine. Annotation example for `POST /runs`:

```go
// handleCreateRun starts a new run.
//
// @Summary      Start a run
// @Description  Creates a run with state=starting. Validates dataset binding before transitioning.
// @Description  See state machine in docs/api-contract.md (rejected/failed/completed terminal states).
// @Tags         runs
// @Accept       json
// @Produce      json
// @Param        run  body      RunRequest  true  "run definition"
// @Success      201  {object}  Run
// @Failure      400  {object}  api.ErrorResponse  "validation error"
// @Failure      422  {object}  api.ErrorResponse  "dataset binding rejected"
// @Router       /runs [post]
func (s *Server) handleCreateRun(w http.ResponseWriter, r *http.Request) {
```

- [ ] **Step 7: Annotate workflow CRUD** — `POST/GET/DELETE /workflows`. Tag `workflows`. Body type `WorkflowRequest` (the DSL v2 envelope per `api-contract.md`). 409 on name conflict if `overwrite != true`.

- [ ] **Step 8: Annotate dataset endpoints** — verify which exist; tag `datasets`. Note any drift in commit message.

- [ ] **Step 9: Annotate attack endpoints** — `POST /attacks`, `GET /attacks/{id}`, etc. Tag `attacks`. Body shapes match what `api-contract.md` describes.

- [ ] **Step 10: Skip annotating `policies` routes** — Task 12 deletes them. Leave them un-annotated.

- [ ] **Step 11: Slim `docs/api-contract.md`**

Replace endpoint catalog sections with a redirect:

```markdown
## Endpoint catalog

See `docs/swagger.yaml` for the full route reference. This file documents only:
- The run lifecycle state machine (below)
- Cross-cutting concerns (auth — when added — error envelope, dataset handshake semantics)
```

Keep the state machine section, the dataset rejection semantics, the error envelope description.

- [ ] **Step 12: Generate spec, test, copy CI workflow**

```bash
make openapi
go test ./...
```

Then create `.github/workflows/openapi.yml` (same shape as Tasks 1 and 3).

- [ ] **Step 13: Commit (note any drift discovered)**

```bash
git add Makefile .github/workflows/openapi.yml internal/api/ docs/ \
  cmd/ go.mod go.sum
git commit -m "feat(openapi): port api-contract.md to swag annotations

Drift discovered while porting:
- [list any routes/shapes that disagreed with api-contract.md]"
```

---

## Task 5: AutoRule HTTP handler in manteion-go

Manteion has `model.AutoRule` and `store.AutoRuleRepo` but no HTTP routes. Add CRUD following `rule_handler.go` pattern, annotate, wire into `server.go`.

**Files:**
- Create: `internal/api/autorule_handler.go`, `internal/api/autorule_handler_test.go`
- Modify: `internal/api/server.go` (route registrations), `cmd/manteion/main.go` (no change — `autoRuleRepo` already created)

- [ ] **Step 1: Write the failing handler test** (`internal/api/autorule_handler_test.go`)

```go
package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"manteion-go/internal/model"
)

func TestCreateAutoRule_201(t *testing.T) {
	srv := newTestServer(t)  // existing test helper; sets up sqlite/postgres + repos
	defer srv.Close()

	body := model.AutoRule{
		ID: "ar1", Name: "p99-attack", Enabled: true,
		Condition: model.AutoRuleCondition{Metric: "checkout_p99_us", Operator: "gt", Threshold: 50000},
		Action: model.AutoRuleAction{
			ActionType: "attack",
			AttackTarget: &model.AttackTargetSpec{
				URL: "http://x:1/y", Method: "GET", Rate: 100, DurationMs: 30000,
			},
		},
		Cooldown:  5 * time.Minute,
		CreatedAt: time.Now(),
	}
	buf, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/api/v1/autorules", bytes.NewReader(buf))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d: %s", rr.Code, rr.Body.String())
	}
}
```

If `newTestServer` doesn't yet exist as a test helper, write it as a parallel step pulling from the existing `rule_handler_test.go` setup pattern. Steps below assume it does.

- [ ] **Step 2: Run test to verify it fails**

```bash
go test ./internal/api/ -run TestCreateAutoRule_201 -v
```

Expected: FAIL with 404 (route not registered) or compile error if helper missing.

- [ ] **Step 3: Create `internal/api/autorule_handler.go`**

```go
package api

import (
	"errors"
	"net/http"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// handleCreateAutoRule creates a new auto-rule.
//
// @Summary      Create auto-rule
// @Description  Persist a new metric-triggered auto-rule. Body must validate per model.AutoRule.Validate().
// @Tags         autorules
// @Accept       json
// @Produce      json
// @Param        autorule  body      model.AutoRule  true  "AutoRule definition"
// @Success      201       {object}  model.AutoRule
// @Failure      400       {object}  api.ErrorResponse  "validation error"
// @Failure      409       {object}  api.ErrorResponse  "id conflict"
// @Router       /autorules [post]
func (s *Server) handleCreateAutoRule(w http.ResponseWriter, r *http.Request) {
	var rule model.AutoRule
	if err := readJSON(r, &rule); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := rule.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.autoRules.Create(r.Context(), &rule); err != nil {
		writeError(w, http.StatusInternalServerError, "create autorule: "+err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, rule)
}

// handleListAutoRules returns all auto-rules.
//
// @Summary      List auto-rules
// @Description  Returns all auto-rules ordered by created_at.
// @Tags         autorules
// @Produce      json
// @Success      200  {array}  model.AutoRule
// @Failure      500  {object}  api.ErrorResponse
// @Router       /autorules [get]
func (s *Server) handleListAutoRules(w http.ResponseWriter, r *http.Request) {
	rules, err := s.autoRules.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list autorules: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rules)
}

// handleGetAutoRule returns one auto-rule.
//
// @Summary      Get auto-rule
// @Tags         autorules
// @Produce      json
// @Param        id   path      string  true  "AutoRule ID"
// @Success      200  {object}  model.AutoRule
// @Failure      404  {object}  api.ErrorResponse
// @Router       /autorules/{id} [get]
func (s *Server) handleGetAutoRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rule, err := s.autoRules.Get(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "autorule not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get autorule: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rule)
}

// handleDeleteAutoRule deletes an auto-rule.
//
// @Summary      Delete auto-rule
// @Tags         autorules
// @Param        id  path  string  true  "AutoRule ID"
// @Success      204
// @Failure      404  {object}  api.ErrorResponse
// @Router       /autorules/{id} [delete]
func (s *Server) handleDeleteAutoRule(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := s.autoRules.Delete(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "autorule not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "delete autorule: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
```

- [ ] **Step 4: Register routes in `internal/api/server.go`**

In `routes()`, after the rule routes block, add:

```go
// AutoRule CRUD (metric-triggered actions, formerly PolicyRule).
mux.HandleFunc("POST /api/v1/autorules", s.handleCreateAutoRule)
mux.HandleFunc("GET /api/v1/autorules", s.handleListAutoRules)
mux.HandleFunc("GET /api/v1/autorules/{id}", s.handleGetAutoRule)
mux.HandleFunc("DELETE /api/v1/autorules/{id}", s.handleDeleteAutoRule)
```

- [ ] **Step 5: Run the test to verify it passes**

```bash
go test ./internal/api/ -run TestCreateAutoRule_201 -v
```

Expected: PASS.

- [ ] **Step 6: Add additional tests**: get-not-found, list-empty, list-after-create, delete, delete-not-found, validation-fails-400, id-conflict-409. Mirror `rule_handler_test.go`'s structure.

- [ ] **Step 7: Run full test suite**

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 8: Regenerate spec**

```bash
make openapi
git diff docs/swagger.yaml | grep -E "autorules|AutoRule" | head -20
```

Expected: spec contains `/autorules` paths and `model.AutoRule` schema.

- [ ] **Step 9: Commit**

```bash
git add internal/api/autorule_handler.go internal/api/autorule_handler_test.go \
  internal/api/server.go docs/swagger.yaml docs/swagger.json
git commit -m "feat(autorule): expose AutoRule CRUD HTTP routes"
```

---

## Task 6: atropos Tier 1 — `ActiveRules`, `SDKInfo`, `DecisionFromContext`

Three additive read-only exports that introspect SDK state.

**Files:**
- Create: `info.go`, `info_test.go`, `decision_context.go`, `decision_context_test.go`
- Modify: `atropos.go` (add `ActiveRules`, `SDKInfo` package-level funcs), `middleware.go` (stash decision on ctx), `init.go` (capture init params for `SDKInfo`)

- [ ] **Step 1: Write `info_test.go` first**

```go
package atropos

import (
	"context"
	"testing"
	"time"
)

func TestSDKInfo_AfterInit(t *testing.T) {
	// Reset package state; assumes Init records into a package-level *infoState.
	resetInfoForTest()

	shutdown, err := Init(context.Background(),
		WithService("checkoutservice"),
		WithManteionURL("http://manteion:8080"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown(context.Background())

	info := SDKInfo()
	if info.Service != "checkoutservice" {
		t.Errorf("Service: want checkoutservice, got %q", info.Service)
	}
	if info.ManteionURL != "http://manteion:8080" {
		t.Errorf("ManteionURL mismatch")
	}
	if info.StartedAt.IsZero() || time.Since(info.StartedAt) > time.Second {
		t.Errorf("StartedAt should be ~now")
	}
}

func TestSDKInfo_BeforeInit(t *testing.T) {
	resetInfoForTest()
	info := SDKInfo()
	if info.Service != "" || !info.StartedAt.IsZero() {
		t.Errorf("expect zero-value Info before Init, got %+v", info)
	}
}
```

- [ ] **Step 2: Run test, expect FAIL** (`SDKInfo` not defined)

```bash
go test -run TestSDKInfo -v
```

- [ ] **Step 3: Implement `info.go`**

```go
package atropos

import (
	"sync"
	"time"
)

// Info is a snapshot of SDK runtime state for service-author observability.
type Info struct {
	Service              string    `json:"service"`
	Version              string    `json:"version"`
	ManteionURL          string    `json:"manteion_url"`
	StartedAt            time.Time `json:"started_at"`
	LastPollAt           time.Time `json:"last_poll_at,omitempty"`
	LastAckedRuleVersion uint64    `json:"last_acked_rule_version,omitempty"`
	LastPollError        string    `json:"last_poll_error,omitempty"`
}

type infoState struct {
	mu                   sync.RWMutex
	service              string
	version              string
	manteionURL          string
	startedAt            time.Time
	lastPollAt           time.Time
	lastAckedRuleVersion uint64
	lastPollError        string
}

var info infoState

// SDKInfo returns the current SDK runtime state. Safe for concurrent use.
// Returns zero-value Info if Init has not been called.
func SDKInfo() Info {
	info.mu.RLock()
	defer info.mu.RUnlock()
	return Info{
		Service: info.service, Version: info.version,
		ManteionURL: info.manteionURL, StartedAt: info.startedAt,
		LastPollAt: info.lastPollAt, LastAckedRuleVersion: info.lastAckedRuleVersion,
		LastPollError: info.lastPollError,
	}
}

// Internal setters used by Init and the rule poller.
func setInfoOnInit(service, version, manteionURL string) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.service, info.version, info.manteionURL = service, version, manteionURL
	info.startedAt = time.Now()
}

func setLastPoll(at time.Time, ackedVersion uint64, pollErr error) {
	info.mu.Lock()
	defer info.mu.Unlock()
	info.lastPollAt, info.lastAckedRuleVersion = at, ackedVersion
	if pollErr != nil {
		info.lastPollError = pollErr.Error()
	} else {
		info.lastPollError = ""
	}
}

func resetInfoForTest() {
	info.mu.Lock()
	defer info.mu.Unlock()
	info = infoState{}
}
```

- [ ] **Step 4: Wire `setInfoOnInit` into `init.go`**

Inside `Init(...)`, after option parsing succeeds:

```go
setInfoOnInit(opts.Service, Version, opts.ManteionBaseURL)
```

(Use whatever names existing options actually have. `Version` is a package constant — define if missing.)

- [ ] **Step 5: Wire `setLastPoll` into the rule poller**

Locate the rule polling loop (likely in `register.go` or a `poller.go`). After each poll attempt, call `setLastPoll(time.Now(), respVersion, pollErr)`.

- [ ] **Step 6: Run tests**

```bash
go test -run TestSDKInfo -v
```

Expected: PASS.

- [ ] **Step 7: Implement `ActiveRules()` in `atropos.go`**

```go
// ActiveRules returns the rule set currently applied by the SDK.
// Returns nil if no StaticEvaluator is configured (custom evaluators may
// expose introspection differently).
func ActiveRules() []StaticRule {
	configMu.RLock()
	eval := config.evaluator
	configMu.RUnlock()
	if se, ok := eval.(*StaticEvaluator); ok {
		return se.Rules()
	}
	return nil
}
```

(The exact `configMu`/`config` accessor depends on the existing `Configure` implementation in `atropos.go`. If `configureState` is the package-private struct, expose a getter.)

- [ ] **Step 8: Add test for `ActiveRules`**

```go
// info_test.go (continues)
func TestActiveRules_StaticEvaluator(t *testing.T) {
	rules := []StaticRule{{Name: "r1"}, {Name: "r2"}}
	eval := NewStaticEvaluator(rules) // existing constructor
	Configure(WithEvaluator(eval))
	defer resetConfigForTest()

	got := ActiveRules()
	if len(got) != 2 || got[0].Name != "r1" {
		t.Errorf("ActiveRules mismatch: %+v", got)
	}
}
```

Add `resetConfigForTest()` to atropos.go if missing.

- [ ] **Step 9: Implement `decision_context.go`**

```go
package atropos

import "context"

type decisionCtxKey struct{}

// withDecision returns a derived context carrying the SDK's match decision.
// Called by IngressMiddleware after the evaluator runs.
func withDecision(ctx context.Context, d *Decision) context.Context {
	return context.WithValue(ctx, decisionCtxKey{}, d)
}

// DecisionFromContext returns the SDK's decision for the in-flight request,
// or nil if no decision was recorded (no middleware on this path, or the
// request didn't match any rule).
//
// Safe to call from service handler code that wraps atropos middleware.
func DecisionFromContext(ctx context.Context) *Decision {
	if d, ok := ctx.Value(decisionCtxKey{}).(*Decision); ok {
		return d
	}
	return nil
}
```

- [ ] **Step 10: Wire `withDecision` into `middleware.go`'s `IngressMiddleware`**

After the evaluator returns a decision (search for the existing call site), wrap the request:

```go
decision := evalResult // existing var
if decision != nil {
	r = r.WithContext(withDecision(r.Context(), decision))
}
next.ServeHTTP(w, r)
```

- [ ] **Step 11: Test `DecisionFromContext`**

```go
package atropos

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDecisionFromContext_PopulatedByMiddleware(t *testing.T) {
	rules := []StaticRule{{Name: "test-rule" /* match-everything for the test */}}
	Configure(WithEvaluator(NewStaticEvaluator(rules)))
	defer resetConfigForTest()

	var got *Decision
	handler := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = DecisionFromContext(r.Context())
	}), "test-svc")

	req := httptest.NewRequest("GET", "/anything", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if got == nil {
		t.Fatal("DecisionFromContext returned nil; expected a Decision from middleware")
	}
}
```

- [ ] **Step 12: Run full atropos test suite**

```bash
go test ./...
```

Expected: PASS. Existing tests must not regress.

- [ ] **Step 13: Commit**

```bash
git add atropos.go init.go middleware.go info.go info_test.go \
  decision_context.go decision_context_test.go
git commit -m "feat(atropos): SDKInfo, ActiveRules, DecisionFromContext

Tier 1 introspection extensions per cross-repo audit plan.
Pure additions — no behavior change to existing middleware/Configure paths."
```

---

## Task 7: atropos Tier 1 — typed errors + `cachebox.Stats()`

**Files:**
- Modify: `errors.go` (add sentinels), `internal/cachebox/cachebox.go` (export `Stats()`), call sites that wrap errors
- Create: `errors_test.go` (sentinel-presence tests)

- [ ] **Step 1: Define sentinels in `errors.go`**

```go
package atropos

import "errors"

// ErrManteionUnreachable is returned (typically wrapped) when the SDK's rule
// poller or registration call cannot reach manteion. Service code can branch
// on errors.Is(err, ErrManteionUnreachable).
var ErrManteionUnreachable = errors.New("atropos: manteion unreachable")

// ErrRuleVersionMismatch is returned when manteion's rule version is older
// than the SDK's last-acked version (suggests rollback or registry corruption).
var ErrRuleVersionMismatch = errors.New("atropos: rule version mismatch")

// ErrCacheBoxColdStart is returned by Seed when the cache-box has no entries
// after seeding completes. Service code may want to fail readiness probes
// in this case.
var ErrCacheBoxColdStart = errors.New("atropos: cachebox cold start (no entries seeded)")
```

(`ErrorResponse` from Task 3 stays in this file unchanged.)

- [ ] **Step 2: Wrap call sites**

Locate the rule poller's HTTP-error path (search `register.go`, `poller.go` if it exists, `init.go`):

```go
if errors.Is(err, syscall.ECONNREFUSED) || isNetErr(err) {
	return fmt.Errorf("%w: %v", ErrManteionUnreachable, err)
}
```

For `Seed` in `cache_seed.go`, after the seed loop:

```go
if seededCount == 0 {
	return seededCount, ErrCacheBoxColdStart
}
```

For version-mismatch detection in the poll handler, when `ackedVersion > respVersion`:

```go
return fmt.Errorf("%w: acked=%d resp=%d", ErrRuleVersionMismatch, ackedVersion, respVersion)
```

- [ ] **Step 3: Test sentinel propagation**

```go
package atropos

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSeed_ColdStart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204) // no body — empty cache
	}))
	defer srv.Close()

	store := newTestCacheStore() // existing test helper
	n, err := Seed(context.Background(), srv.URL, "svc", store)
	if !errors.Is(err, ErrCacheBoxColdStart) {
		t.Fatalf("want ErrCacheBoxColdStart, got %v (n=%d)", err, n)
	}
}

func TestPoll_ManteionUnreachable(t *testing.T) {
	// Bind a closed port to force connection refused.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	// ... attempt poll, assert errors.Is(err, ErrManteionUnreachable)
}
```

- [ ] **Step 4: Expose `cachebox.Stats()`**

In `internal/cachebox/cachebox.go` (or wherever the cache type lives), add or expose:

```go
// Stats is a snapshot of cache-box runtime counters.
type Stats struct {
	Mode             string  `json:"mode"`              // "passthrough" | "replay" | "replay_with_delay"
	Entries          int     `json:"entries"`
	Hits             int64   `json:"hits"`
	Misses           int64   `json:"misses"`
	StalenessAvgMs   float64 `json:"staleness_avg_ms"`
	WarmupRemainingMs int64  `json:"warmup_remaining_ms,omitempty"`
}

// Stats returns a snapshot of the cache-box's runtime state.
func (c *CacheBox) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return Stats{
		Mode: c.mode.String(),
		Entries: len(c.store),
		Hits: c.hits.Load(),
		Misses: c.misses.Load(),
		// ... existing internals
	}
}
```

(Adapt to actual internal field names — the implementer will look at the existing struct.)

- [ ] **Step 5: Re-export from top-level package**

In `atropos.go` (or `types.go`), add a type alias so service code can write `atropos.CacheBoxStats`:

```go
// CacheBoxStats is re-exported from the internal cachebox package.
type CacheBoxStats = cachebox.Stats
```

- [ ] **Step 6: Test `Stats()` returns sensible defaults**

```go
func TestCacheBoxStats_Empty(t *testing.T) {
	cb := cachebox.New(...)
	s := cb.Stats()
	if s.Entries != 0 || s.Hits != 0 || s.Misses != 0 {
		t.Errorf("expected zero-value stats on empty cachebox, got %+v", s)
	}
}
```

- [ ] **Step 7: Run all atropos tests**

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 8: Regenerate atropos spec**

```bash
make openapi
```

Verify `cachebox.Stats` is now resolvable in the spec where `CacheBoxAdminHandler` references it.

- [ ] **Step 9: Commit**

```bash
git add errors.go errors_test.go internal/cachebox/cachebox.go atropos.go \
  cache_seed.go register.go docs/swagger.yaml docs/swagger.json
git commit -m "feat(atropos): typed error sentinels + cachebox.Stats() export"
```

---

## Task 8: atropos `Snapshot()` aggregator + admin handler

Single struct + getter that consolidates Tier 1 outputs. Plus a host-mountable HTTP handler so service authors can wire one URL.

**Files:**
- Create: `snapshot.go`, `snapshot_test.go`, `snapshot_admin.go`, `snapshot_admin_test.go`
- Modify: `docs/swagger.yaml` (regen)

- [ ] **Step 1: Write `snapshot_test.go`**

```go
package atropos

import (
	"context"
	"testing"
)

func TestSnapshot_Aggregates(t *testing.T) {
	resetInfoForTest()
	rules := []StaticRule{{Name: "r1"}, {Name: "r2"}}
	Configure(WithEvaluator(NewStaticEvaluator(rules)))
	defer resetConfigForTest()
	shutdown, _ := Init(context.Background(), WithService("svc"), WithManteionURL("http://m"))
	defer shutdown(context.Background())

	snap := Snapshot()

	if snap.Info.Service != "svc" {
		t.Errorf("Info.Service: %q", snap.Info.Service)
	}
	if len(snap.ActiveRules) != 2 {
		t.Errorf("ActiveRules: %+v", snap.ActiveRules)
	}
	// CacheBoxStats is zero-value if no cache-box configured — that's fine
}
```

- [ ] **Step 2: Implement `snapshot.go`**

```go
package atropos

// Snapshot is the unified observability surface for service authors.
// One call returns SDK info, active rules, cache-box stats, and rule-poll status.
//
// Designed to back a single host-service handler (see SnapshotAdminHandler).
type Snapshot struct {
	Info          Info          `json:"info"`
	ActiveRules   []StaticRule  `json:"active_rules"`
	CacheBoxStats CacheBoxStats `json:"cachebox_stats"`
}

// Snapshot returns a unified snapshot of SDK state.
// Safe for concurrent use.
func TakeSnapshot() Snapshot {
	s := Snapshot{
		Info:        SDKInfo(),
		ActiveRules: ActiveRules(),
	}
	if cb := configuredCacheBox(); cb != nil {
		s.CacheBoxStats = cb.Stats()
	}
	return s
}
```

(Method name is `TakeSnapshot` not `Snapshot` to avoid collision with the type. If naming preference differs, rename to `GetSnapshot` or expose as a method on a singleton.)

Add `configuredCacheBox()` accessor in `atropos.go` if not already exported (returns the cachebox set via `WithCacheBoxCoordinator`).

- [ ] **Step 3: Run snapshot test**

```bash
go test -run TestSnapshot -v
```

Expected: PASS.

- [ ] **Step 4: Implement `snapshot_admin.go`**

```go
package atropos

import (
	"encoding/json"
	"net/http"
)

// SnapshotAdminHandler returns an http.Handler that serves the Snapshot as JSON on GET.
//
// @Summary      Get SDK snapshot
// @Description  Unified observability snapshot: SDK info + active rules + cachebox stats.
// @Description  Recommended mount path: /atropos/admin/snapshot
// @Tags         admin
// @Produce      json
// @Success      200  {object}  Snapshot
// @Failure      405  {object}  ErrorResponse
// @Router       /snapshot [get]
func SnapshotAdminHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(ErrorResponse{Error: "method not allowed"})
			return
		}
		_ = json.NewEncoder(w).Encode(TakeSnapshot())
	})
}
```

- [ ] **Step 5: Test the admin handler**

```go
func TestSnapshotAdminHandler_GET(t *testing.T) {
	resetInfoForTest()
	Configure(WithEvaluator(NewStaticEvaluator([]StaticRule{{Name: "r1"}})))
	defer resetConfigForTest()

	req := httptest.NewRequest("GET", "/snapshot", nil)
	rr := httptest.NewRecorder()
	SnapshotAdminHandler().ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var snap Snapshot
	if err := json.Unmarshal(rr.Body.Bytes(), &snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(snap.ActiveRules) != 1 {
		t.Errorf("ActiveRules: %+v", snap.ActiveRules)
	}
}

func TestSnapshotAdminHandler_405(t *testing.T) {
	req := httptest.NewRequest("POST", "/snapshot", nil)
	rr := httptest.NewRecorder()
	SnapshotAdminHandler().ServeHTTP(rr, req)
	if rr.Code != 405 {
		t.Errorf("want 405, got %d", rr.Code)
	}
}
```

- [ ] **Step 6: Run all tests**

```bash
go test ./...
```

- [ ] **Step 7: Regenerate spec**

```bash
make openapi
```

Verify `/snapshot` path appears in `docs/swagger.yaml`.

- [ ] **Step 8: Commit**

```bash
git add snapshot.go snapshot_test.go snapshot_admin.go snapshot_admin_test.go \
  atropos.go docs/swagger.yaml docs/swagger.json
git commit -m "feat(atropos): Snapshot() aggregator + SnapshotAdminHandler"
```

---

## Task 9: atropos `TestMatch[T MatchableRequest]` + adapters

Generic dry-run that lets service code (HTTP, gRPC, Thrift, …) verify what the SDK *would* decide for a given request, without the middleware actually running.

**Files:**
- Create: `match.go`, `match_test.go`
- Modify: `atropos.go` (re-exports if any), `internal/evaluator/static.go` (read-only `Evaluate` accessor if not already exported)

- [ ] **Step 1: Write `match_test.go` first** (red)

```go
package atropos

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTestMatch_HTTPAdapter(t *testing.T) {
	rules := []StaticRule{
		{Name: "match-checkout", /* matches /checkout */},
	}
	Configure(WithEvaluator(NewStaticEvaluator(rules)))
	defer resetConfigForTest()

	req := httptest.NewRequest("POST", "/checkout", nil)
	dec, evald, err := TestMatch(FromHTTP(req))

	if err != nil {
		t.Fatal(err)
	}
	if dec == nil {
		t.Fatal("expected non-nil Decision")
	}
	if len(evald) != 1 || !evald[0].Matched {
		t.Errorf("expected one matched EvaluatedRule, got %+v", evald)
	}
}

func TestTestMatch_NoMatch(t *testing.T) {
	rules := []StaticRule{{Name: "match-other", /* matches /other only */}}
	Configure(WithEvaluator(NewStaticEvaluator(rules)))
	defer resetConfigForTest()

	req := httptest.NewRequest("GET", "/checkout", nil)
	_, evald, err := TestMatch(FromHTTP(req))
	if err != nil {
		t.Fatal(err)
	}
	if len(evald) != 1 || evald[0].Matched {
		t.Errorf("expected one unmatched EvaluatedRule, got %+v", evald)
	}
}
```

- [ ] **Step 2: Run, expect FAIL**

```bash
go test -run TestTestMatch -v
```

- [ ] **Step 3: Implement `match.go`**

```go
package atropos

import (
	"net/http"
)

// MatchableRequest is the protocol-agnostic view that rule matching operates on.
// Adapters convert protocol-specific request types into this shape so TestMatch
// can dry-run rule evaluation across HTTP, gRPC, Thrift, and future transports.
type MatchableRequest interface {
	Method() string                    // HTTP verb, gRPC "POST", Thrift "call"
	Path() string                      // URL path, gRPC full method, Thrift function name
	Header(name string) string
	Headers() map[string]string
	Labels() map[string]string         // service-supplied labels for matching
}

// EvaluatedRule is one rule's outcome from a TestMatch evaluation.
type EvaluatedRule struct {
	Rule    StaticRule `json:"rule"`
	Matched bool       `json:"matched"`
	Reason  string     `json:"reason,omitempty"` // why matched / why not
}

// TestMatchOption customizes a TestMatch call.
type TestMatchOption func(*testMatchConfig)

type testMatchConfig struct {
	rules []StaticRule // override package-level rules for hermetic tests
}

// WithRules overrides the default rule set (the configured evaluator's rules)
// for this TestMatch call. Useful for asserting against rule sets that aren't
// installed in the running SDK.
func WithRules(rules ...StaticRule) TestMatchOption {
	return func(c *testMatchConfig) { c.rules = rules }
}

// TestMatch is a pure-function dry-run: given a request, it reports what the
// configured evaluator would decide and which rules participated. Does NOT
// apply the decision (no fault injection, no cache-box mode change).
func TestMatch[T MatchableRequest](req T, opts ...TestMatchOption) (*Decision, []EvaluatedRule, error) {
	cfg := &testMatchConfig{rules: ActiveRules()}
	for _, opt := range opts {
		opt(cfg)
	}
	return evaluateForTest(req, cfg.rules)
}

// HTTPRequest adapts *http.Request to MatchableRequest.
type HTTPRequest struct{ R *http.Request }

func (h HTTPRequest) Method() string         { return h.R.Method }
func (h HTTPRequest) Path() string           { return h.R.URL.Path }
func (h HTTPRequest) Header(n string) string { return h.R.Header.Get(n) }
func (h HTTPRequest) Headers() map[string]string {
	out := make(map[string]string, len(h.R.Header))
	for k, v := range h.R.Header {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}
func (h HTTPRequest) Labels() map[string]string { return nil } // services may inject via context

// FromHTTP wraps an *http.Request as a MatchableRequest.
func FromHTTP(r *http.Request) HTTPRequest { return HTTPRequest{R: r} }

// evaluateForTest runs the matcher logic in dry-run mode against an explicit ruleset.
// Implementation lives in internal/evaluator and is exposed via a thin shim so
// TestMatch doesn't need to know internal layout.
func evaluateForTest(req MatchableRequest, rules []StaticRule) (*Decision, []EvaluatedRule, error) {
	// Delegate to internal/evaluator's pure match function. If that doesn't
	// exist as a pure func today, add it: it's a refactor that pulls the
	// match logic out of *StaticEvaluator.Evaluate into a free function.
	return evaluator.MatchAll(req, rules)
}
```

(The `internal/evaluator/static.go` may need a small refactor to expose `MatchAll` as a free function. Implementer: extract the per-rule matching loop from `(*StaticEvaluator).Evaluate` into `MatchAll(req MatchableRequest, rules []StaticRule)`. Existing `Evaluate` becomes a thin wrapper.)

- [ ] **Step 4: Add adapter stubs for gRPC and Thrift (compile-only, deferred behavior)**

In `match.go`, add commented placeholders so future adopters know the shape:

```go
// FromGRPC adapts a gRPC method + metadata to MatchableRequest.
// Add this when atropos-go adds gRPC ingress middleware.
//
// func FromGRPC(method string, md metadata.MD) GRPCRequest { ... }
//
// FromThrift adapts a Thrift function call to MatchableRequest.
// Add this when atropos-go adopts Thrift.
//
// func FromThrift(fn string, headers map[string]string) ThriftRequest { ... }
```

(Don't actually import gRPC/Thrift — keep this comment-only.)

- [ ] **Step 5: Refactor `internal/evaluator/static.go` to expose `MatchAll`**

```go
// File: internal/evaluator/static.go (additions)

// MatchAll evaluates each rule against req and returns the first matching
// rule's Decision plus a per-rule evaluation trace. Used by atropos.TestMatch
// for service-side dry-runs.
//
// The req parameter is the public MatchableRequest interface (declared in
// the parent package); this package depends on the parent — fine because
// MatchableRequest has no behavioral coupling.
func MatchAll(req MatchableRequest, rules []StaticRule) (*Decision, []EvaluatedRule, error) {
	out := make([]EvaluatedRule, 0, len(rules))
	var winner *Decision
	for _, r := range rules {
		matched, reason := evaluateRule(req, r) // existing private fn, refactored to take MatchableRequest
		out = append(out, EvaluatedRule{Rule: r, Matched: matched, Reason: reason})
		if matched && winner == nil {
			d := r.Decision // or however the existing code constructs Decision
			winner = &d
		}
	}
	return winner, out, nil
}
```

(Note: `MatchableRequest` and `EvaluatedRule` need to be importable here. If circular import is a concern, define them in a small shared `internal/types` package and re-export from the top-level `atropos` package.)

- [ ] **Step 6: Run TestMatch tests**

```bash
go test -run TestTestMatch -v
```

Expected: PASS.

- [ ] **Step 7: Run full test suite**

```bash
go test ./...
```

Existing tests must still pass — the refactor of `evaluateRule` to take `MatchableRequest` should be transparent.

- [ ] **Step 8: Regenerate spec, commit**

```bash
make openapi
git add match.go match_test.go atropos.go internal/evaluator/static.go \
  docs/swagger.yaml docs/swagger.json
git commit -m "feat(atropos): TestMatch[T MatchableRequest] + HTTP adapter

Generic dry-run for service-author rule testing across protocols.
HTTP adapter (FromHTTP) ships now; gRPC/Thrift adapters are commented
placeholders pending those protocols being adopted by atropos-go."
```

---

## Task 10: UI consumer wiring (manteion-ui)

Generate TypeScript types from manteion-go's `docs/swagger.yaml` and replace existing hand-written types in fetch wrappers.

**Files:**
- Create: `scripts/gen-types.sh` (or pure npm script), `src/types/manteion-api.ts` (generated)
- Modify: `package.json` (devDep + script), existing fetch wrapper files (search for current API types and replace)

- [ ] **Step 1: Add `openapi-typescript` as a devDep**

```bash
cd /Users/pronei/work/faults-lab/manteion-ui
pnpm add -D openapi-typescript
```

- [ ] **Step 2: Add `gen-types` script to `package.json`**

```json
"scripts": {
  "gen-types": "openapi-typescript ../manteion-go/docs/swagger.yaml -o src/types/manteion-api.ts",
  "dev": "vite",
  ...
}
```

- [ ] **Step 3: Run the generator**

```bash
pnpm run gen-types
ls -la src/types/manteion-api.ts
```

Expected: file exists with `export type paths = { ... }` etc.

- [ ] **Step 4: Locate existing hand-written API types**

```bash
rg -n "interface.*Response|interface.*Request|type Rule|type Workflow|type Experiment" src/
```

Note all locations. They'll be migrated to `paths`/`components` from the generated file.

- [ ] **Step 5: Migrate one fetch wrapper as the reference**

Pick the rules wrapper. Pattern:

```ts
// Before (hand-written):
import type { Rule } from '../types/api';
async function listRules(): Promise<Rule[]> { ... }

// After (generated):
import type { components } from '../types/manteion-api';
type Rule = components['schemas']['model.Rule'];
async function listRules(): Promise<Rule[]> { ... }
```

- [ ] **Step 6: Run typecheck**

```bash
pnpm run typecheck
```

Expected: PASS for the migrated wrapper. Other wrappers still using old types continue to work until migrated.

- [ ] **Step 7: Migrate remaining wrappers** — workflows, experiments, faults, sdk, autorules. Same pattern.

- [ ] **Step 8: Delete the old hand-written types file** once all consumers migrated. Run `pnpm run typecheck && pnpm run lint && pnpm run test`.

- [ ] **Step 9: Decide tracking policy for `src/types/manteion-api.ts`**

Two options:
- **Commit it** — UI build doesn't require manteion-go checkout. Recommended for now.
- **Gitignore it** — UI build runs `gen-types` in CI. Cleaner long-term but adds a build step.

Default: commit. Add a comment at the top:

```ts
// THIS FILE IS GENERATED by openapi-typescript from manteion-go/docs/swagger.yaml.
// Do not edit by hand. Run `pnpm run gen-types` to regenerate.
```

- [ ] **Step 10: Commit**

```bash
git add package.json pnpm-lock.yaml src/types/manteion-api.ts \
  src/<migrated-wrapper-files>
git commit -m "feat(types): generate TS types from manteion-go OpenAPI spec"
```

---

## Task 11: service-beds checkoutService SDK bump

Pull the new atropos-go release (post Task 9) and verify checkoutService compiles + runs. Optionally adopt one Tier 1 introspection method.

**Files:**
- Modify: `src/checkoutService/go.mod`, `src/checkoutService/go.sum`, `src/checkoutService/main.go` (optional Snapshot endpoint)

- [ ] **Step 1: Determine the new atropos-go version**

After Tasks 6–9 land in atropos-go and a release is tagged (or via pseudo-version from main), capture the version string.

```bash
cd /Users/pronei/work/faults-lab/atropos-go
git log -1 --format='%H'
```

- [ ] **Step 2: Bump in checkoutService**

```bash
cd /Users/pronei/work/faults-lab/service-beds/microservices-demo-go/src/checkoutService
go get github.com/microfaults/atropos-go@<commit-or-tag>
go mod tidy
```

- [ ] **Step 3: Verify compile**

```bash
go build ./...
```

Expected: clean compile.

- [ ] **Step 4: Run existing tests**

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 5: (Optional) Adopt `atropos.Snapshot()` on the existing health/info handler**

If `main.go` registers a `/healthz` or `/info` endpoint, mount snapshot too:

```go
// In main.go, near where atropos.Init is called:
mux.Handle("/atropos/admin/snapshot", atropos.SnapshotAdminHandler())
```

- [ ] **Step 6: Manual smoke test** — bring up the service locally per the existing service-beds dev workflow, hit `/atropos/admin/snapshot`, verify JSON.

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum main.go
git commit -m "chore(checkoutservice): bump atropos-go to <version>; mount Snapshot handler"
```

---

## Task 12: Zeus policies deletion

Remove the redundant policies engine and the manteion-side proxy routes. Pre-v1, no live consumers, no deprecation pipeline.

**Files:**
- Modify (manteion-go): `internal/api/server.go` (remove `/api/v1/zeus/policies` route registrations), `internal/api/zeus_handler.go` (remove any policy-specific switch arms)
- Delete (zeus-go): `internal/policy/engine.go` (and any `internal/policy/*.go`), the policies route registrations in zeus's mux, any `policy_*` SQL files if zeus persists them

- [ ] **Step 1: Inventory zeus's policies surface**

```bash
cd /Users/pronei/work/faults-lab/zeus-go
rg -n "policy|policies" --type go internal/ cmd/ | grep -vi "test"
```

Save the file list as the deletion checklist.

- [ ] **Step 2: Inventory manteion's zeus-policies proxy surface**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
rg -n "zeus.*policies|zeus/policies" internal/ cmd/
```

- [ ] **Step 3: Delete zeus's policy package**

```bash
cd /Users/pronei/work/faults-lab/zeus-go
git rm -r internal/policy/
```

- [ ] **Step 4: Remove zeus's policy route registrations and any wiring**

In zeus's mux/server file (whatever lists routes), delete every `/policies` route. In any `Server` constructor that took a `policyRepo` or similar dependency, drop the parameter and the field. In `cmd/zeus/main.go`, drop the policy-repo construction.

- [ ] **Step 5: Verify zeus-go still builds**

```bash
go build ./... && go test ./...
```

Expected: PASS.

- [ ] **Step 6: Remove manteion's zeus-policies proxy routes**

In `internal/api/server.go`, delete:

```go
mux.HandleFunc("POST /api/v1/zeus/policies", s.handleZeusProxy)
mux.HandleFunc("GET /api/v1/zeus/policies", s.handleZeusProxy)
mux.HandleFunc("DELETE /api/v1/zeus/policies/{id}", s.handleZeusProxy)
```

If `zeus_handler.go` has any policy-specific path-rewriting logic, remove it (the proxy is generic so likely no specific code, but check).

- [ ] **Step 7: Verify manteion-go still builds**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
go build ./... && go test ./...
```

Expected: PASS.

- [ ] **Step 8: Regenerate manteion's spec**

```bash
make openapi
```

Verify `/zeus/policies` paths are no longer in the spec. Also run `make openapi` in zeus-go for the same.

- [ ] **Step 9: Commit (one cross-repo commit per repo)**

In zeus-go:

```bash
git add -A internal/ cmd/ docs/
git commit -m "refactor(zeus): delete policies engine

Functionally redundant with manteion's AutoRule. Pre-v1, no live consumers.
Manteion's /api/v1/zeus/policies proxy routes are deleted in a parallel commit
in manteion-go."
```

In manteion-go:

```bash
git add internal/api/server.go internal/api/zeus_handler.go \
  docs/swagger.yaml docs/swagger.json
git commit -m "refactor(zeus-proxy): remove /api/v1/zeus/policies routes

Zeus's policies engine has been deleted (see zeus-go commit). AutoRule
(/api/v1/autorules) is the manteion-resident replacement, exposed in Task 5."
```

---

## Verification across all tasks

After Task 12 lands, the workspace should satisfy:

- [ ] `make openapi-check` passes in all three repos.
- [ ] `manteion-ui` typechecks against generated types.
- [ ] `service-beds/checkoutService` builds and tests pass against the new atropos-go.
- [ ] No `policy_rules` or `PolicyRule` references in zeus-go or in manteion-go's API surface.
- [ ] `manteion-go/docs/swagger.yaml` paths include `/autorules` (Task 5) and exclude `/zeus/policies` (Task 12).
- [ ] `atropos-go/docs/swagger.yaml` paths include `/snapshot` (Task 8).
- [ ] CI workflows run `make openapi-check` and fail on stale specs.

## Open items deferred to follow-up plans

Per the source spec's "Open questions" and "Connection to Track B":

- Body matching across protocols (`MatchableRequest.Body()`) — wait for a use case.
- gRPC and Thrift adapter implementations (`FromGRPC`, `FromThrift`) — adopt when those protocols land.
- `problem+json` migration of the error envelope — single-file swap once decided.
- Auth / RBAC — separate plan.
- Track B (Prometheus + dataset stub) — uses the SSE convention, error envelope, and swag annotations established here.
