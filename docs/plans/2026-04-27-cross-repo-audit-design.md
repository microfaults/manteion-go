# Cross-Repo Audit & API Contracts — Design

| | |
|---|---|
| Date | 2026-04-27 |
| Track | A — Audit + OpenAPI |
| Source | brainstorm conversation 2026-04-27 |
| Feeds into | implementation plan via `superpowers:writing-plans` |
| Companion | Track B — Prometheus + dataset stub (separate plan) |

## Goals

Produce alignment across the three Go repos (`manteion-go`, `atropos-go`, `zeus-go`) and their downstream consumer (`service-beds/microservices-demo-go/checkoutService`) on:

1. What each currently exposes vs. what consumers expect.
2. Where surfaces drift, duplicate, or conflict.
3. What the atropos service-author API should grow toward.
4. How OpenAPI specs are generated and kept in sync.

## Non-goals

- Implementing audit findings, atropos extensions, or specs in this design — those land via the implementation plan that follows.
- Auth / RBAC / consistent error envelope (`API-NEEDED §C.5/C.10`) — recorded as future work.
- Polyglot SDKs (`atropos-go-polyglot`).
- Broader service-beds rollout to the other 10 services beyond `checkoutService`.
- Phase 2 schema-refactors implementation (referenced as a known coming change, not driven by this doc).

## Deliverable

Single markdown design doc (this file). The implementation plan that follows decomposes into ~7 PRs.

---

## Per-repo audit scope

Findings tagged: `{drift | gap | dead-code | duplicate | naming}` × `{blocker | followup | nit}`.

### manteion-go

**Audit against:** `manteion-ui/docs/API-NEEDED.md`, the Figma `2005:9` screen set, the schema-refactors plan, existing `internal/api/server.go` route table.

**Evidence to collect:** route ↔ doc cross-table; handler-to-store-to-model paths; handlers that compile but aren't wired (`API-NEEDED §C.9`).

**Status:** much of this audit was done conversationally in the lead-up to today; the implementation plan records existing findings (workflow/flow rename, hypothesis/created_by gap, match-criteria flat, AutoRule HTTP routes missing) plus anything fresh from a route-table walk during PR-1.

### atropos-go

**Audit against:** manteion's `internal/atropos/` client (what manteion expects), schema-refactors Phase 2.6 (`CompiledRule` additive `Action` field), the SDK admin handlers (`FaultAdminHandler`, `CacheBoxAdminHandler`, `rules_admin.go`) vs how manteion actually uses them.

**Evidence:** exported-symbol inventory; admin-handler route inventory; cross-reference between manteion's atropos client and the SDK admin endpoints; `compiled_rule.go` readiness for Phase 2 Action decoding.

**Output:** gap list ordered by service-author surface area. Drives the DX catalog scoping below.

### zeus-go

**Audit against:** zeus's own `docs/api-contract.md`, manteion's `internal/zeus/client.go` (currently a blind passthrough), the proxy routes in `manteion/internal/api/server.go` (`/api/v1/zeus/*`).

**Evidence:** zeus's `internal/api/` route table vs `api-contract.md` claims; what manteion's UI surface needs that the passthrough hides (`API-NEEDED §C.3`); the `policies` redundancy with manteion's renamed `AutoRule`.

**Decision (this brainstorm, 2026-04-27):** zeus's `policies` path is functionally redundant with manteion's `AutoRule`. Plan for **deletion** (not deprecation) — see Sequencing §7.

**Open question — confirm before implementation begins:** "zeus is not in use, remove code liberally" — does this apply to the policies engine specifically, or to the entire zeus-go service? This document assumes **only the policies engine + manteion's `/api/v1/zeus/policies` proxy routes**, with the rest of zeus (workloads, attacks, k6 driver) staying in scope. If broader retirement is intended, the audit's zeus chapter shrinks dramatically and manteion's zeus client + remaining proxy routes are also removable.

### service-beds — checkoutService only

**Audit against:** current pinned `atropos-go` version in `src/checkoutService/go.mod`; expected breakage from Phase 2 wire format (additive — should be none) and any DX extensions cataloged below.

**Evidence:** `go.mod` pin, `main.go` usage of atropos symbols (Init/Configure options, middleware/transport pattern), test coverage if any.

**Output:** SDK bump checklist + sanity check that the cataloged DX extensions actually help this consumer.

---

## Atropos client-side DX extension catalog

Service-author DX focus per the brainstorm. Each entry: motivation, sketch, cost. Tiers reflect value/effort assessment.

### Approved scope (this design)

- All Tier 1 items.
- `TestMatch(req)` from Tier 2 — see protocol-generic design notes below.

The other Tier 2/3/4 items remain catalogued for future reference but are not in this implementation track.

### Tier 1 — High value, low blast radius (APPROVED)

- **`atropos.ActiveRules() []StaticRule`** — service code introspects what the SDK applies. Motivation: `/info` endpoints, debugging "why did this match." Cost: expose what's already in memory.
- **`atropos.LastDecision(ctx) *Decision`** (or `DecisionFromContext`) — read the SDK's decision for the in-flight request. Motivation: services log observability with rule name attached. Cost: stash a `*Decision` on a context key; one new exported type.
- **`atropos.SDKInfo() Info`** — version, manteion base URL, last poll timestamp, last acked rule version. Motivation: services include this in `/healthz`. Cost: trivial.
- **Typed error sentinels** — `ErrManteionUnreachable`, `ErrRuleVersionMismatch`, `ErrCacheBoxColdStart`. Motivation: services branch on SDK failures. Cost: locate existing error sites, wrap them.
- **`cachebox.Stats() Stats`** — hits/misses/staleness/active mode. Motivation: services export cache-box metrics under their own service name. Cost: expose existing internal counters.
- **Cross-cutting `atropos.Snapshot() Snapshot`** — returns `{Info, ActiveRules, CacheBoxStats, RulePollStatus}` in one call. Motivation: a single `/atropos-status` handler in the host service. Cost: composition of the above.

### Tier 2 — High value for tests/research

**APPROVED:**

- **`atropos.TestMatch[T MatchableRequest](req T)`** — pure-function dry-run. See "TestMatch generic-request design" below.

**CATALOGUED (not approved):**

- `WithStaticRules(rules ...StaticRule) MiddlewareOption` — frozen ruleset for hermetic tests.
- Lifecycle hook `OnRuleApplied(func(EventRuleApplied))` — fire-after instrumentation.

### Tier 3 — Quality-of-life (catalogued, not approved)

- Fault builder ergonomics — chained options across the 7 `NewXFault` constructors.
- OTel context helpers: `atropos.WorkflowFromContext(ctx) string`, `atropos.WithWorkflow(ctx, name) context.Context`.
- `cachebox.OnModeChange(func(old, new Mode))`.

### Tier 4 — Future / research-flavored (catalogued, not approved)

- `atropos.Replay(record SDKRecord, rules []StaticRule) Decision`.
- `cachebox.Entries(prefix) iter.Seq[Entry]` / `KeysSample(n)`.
- Logger injection: `WithLogger(*slog.Logger) Option`.

### TestMatch — generic-request design

`TestMatch` must accept multiple request shapes: `*http.Request`, gRPC proto, Thrift, future protocols. The matcher only needs a small subset — method, path/operation name, headers/metadata, optionally labels. Approach:

```go
// MatchableRequest is the protocol-agnostic view the matcher operates on.
// Adapters convert protocol-specific request types into this shape.
type MatchableRequest interface {
    Method() string                    // HTTP verb, gRPC "POST", Thrift "call", etc.
    Path() string                      // URL path, gRPC full method, Thrift function name
    Header(name string) string
    Headers() map[string]string
    Labels() map[string]string         // service-supplied labels for matching
}

// Adapter constructors keep per-protocol details out of TestMatch.
func FromHTTP(r *http.Request) MatchableRequest                      { /* ... */ }
func FromGRPC(method string, md metadata.MD) MatchableRequest        { /* ... */ }
// FromThrift, FromMessage, etc. as protocols are adopted.

// TestMatch is the pure-function dry-run used by service tests.
func TestMatch[T MatchableRequest](req T, opts ...TestMatchOption) (Decision, []EvaluatedRule, error)
```

Notes:
- Atropos's existing `IngressMiddleware` is HTTP-only; this design does **not** add gRPC/Thrift ingress middleware. It only makes `TestMatch` work for them so service authors writing gRPC/Thrift code can unit-test rule matching.
- `Labels()` becomes the canonical injection point for protocol-specific metadata that doesn't fit the HTTP-shaped header model.
- `EvaluatedRule` (new exported type) carries `{rule_name, matched bool, reason string}` so tests can assert *why* something matched or didn't.

**Out of scope (intentional):**
- gRPC/Thrift ingress middleware — orthogonal feature, separate design.
- Body matching across protocols — content-type-specific, deferred until a use case appears.
- Auth/RBAC in admin handlers — deferred.
- Per-route disable APIs — flag if you want it considered.

---

## OpenAPI scaffolding plan

### Tooling

`swaggo/swag` v2, native OpenAPI 3.0 output (matters for discriminated unions like `RuleAction` and nullable fields). Per-repo `docs/swagger.yaml` and `docs/swagger.json`, both committed.

### Build & CI gate

- `make openapi` per repo runs `swag init` with the repo-specific config.
- CI step: `make openapi && git diff --exit-code -- docs/swagger.{yaml,json}`. Any unstaged change fails the build, forcing handler edits to re-run swag in the same PR.
- Local pre-commit hook is optional, not enforced.

### Annotation conventions (same in all three repos)

- Per-handler block comment using swag's standard syntax.
- Tag groups named for the UI sidebar: `rules`, `faults`, `sdk`, `experiments`, `workflows`, `autorules`, `zeus-proxy`, `admin`.
- Standard error envelope (`{error: string}`) registered once via `@Definitions`. Future migration to `problem+json` (`API-NEEDED §C.5`) is a single-place swap.
- `@Security` deferred (no auth yet).
- `@Description` required on every route — UI's TS types render this as JSDoc.

### Per-repo specifics

**manteion-go** — ~30 routes across `internal/api/*.go`, plus the new `workflow_handler.go` after Phase 1 schema-refactors lands. Zeus proxy routes (`/api/v1/zeus/*`) get a one-line annotation pointing at zeus's spec rather than duplicating; the `/policies` subset is removed entirely (see zeus retirement). AutoRule routes when exposed (no handler today; PR-3 below).

**atropos-go** — annotate the SDK-provided admin handlers. Spec describes endpoint shape regardless of where the host service mounts them. Tag: `admin`. Spec includes a `description` noting "served by host services that import atropos and mount these handlers; recommended mount path is `/atropos/admin/`."

**zeus-go** — port `docs/api-contract.md` content into swag annotations on `internal/api/` handlers. The porting exercise itself is half the audit — drift between contract and code surfaces here. After porting, `api-contract.md` becomes a high-level narrative + state-machine diagram, with route details in `docs/swagger.yaml`. **Policies routes are deleted, not annotated**, per the retirement decision.

### Consumer wiring

- `manteion-ui`: `openapi-typescript manteion-go/docs/swagger.yaml -o src/types/manteion-api.ts`. UI's existing fetch wrappers swap their hand-written types for generated ones.
- `service-beds/checkoutService`: no HTTP consumer of atropos (Go import); spec is reference-only.
- `zeus-go`: spec stands alone; future work could generate an Archer Go client from it.

### Annotation example

```go
// CreateWorkflow registers a new workflow.
//
// @Summary      Create workflow
// @Description  Create a new workflow definition. Body is the workflow DSL v2 document.
// @Tags         workflows
// @Accept       json
// @Produce      json
// @Param        workflow  body      model.Workflow  true  "Workflow definition"
// @Success      201       {object}  model.Workflow
// @Failure      400       {object}  api.ErrorResponse  "validation error"
// @Failure      409       {object}  api.ErrorResponse  "name conflict"
// @Router       /workflows [post]
func (s *Server) handleCreateWorkflow(w http.ResponseWriter, r *http.Request) { ... }
```

Per-handler cost: ~8 lines of comments. swag does the rest by reading `model.Workflow` directly.

### Naming clarification

Manteion's existing `/api/v1/zeus/policies` proxy is being deleted entirely (see Sequencing §7). The remaining zeus proxy routes (`/api/v1/zeus/workloads`, `/api/v1/zeus/attacks`) annotate with a one-line description: "Zeus's load generation surface; passthrough — see zeus-go OpenAPI for body shapes."

---

## Sequencing

The implementation plan that follows this design decomposes into the steps below. Order chosen so swag conventions land before things that need annotating, and so AutoRule HTTP routes exist before zeus's policies get deleted.

1. **Foundations** (1 PR, manteion-go). Add swag v2 toolchain + `make openapi` + CI gate. Define standard error envelope type and tag taxonomy. Annotate one or two existing routes as the reference style.
2. **Annotate existing routes** (3 PRs in parallel, one per repo). Manteion: rest of existing handlers. Atropos: SDK admin handlers. Zeus: port `api-contract.md` content into annotations on `internal/api/` handlers (excluding `/policies`).
3. **Manteion AutoRule HTTP handler** (1 PR, manteion-go). Currently no exposed route. CRUD following `rule_handler.go` pattern. Annotate as part of the same PR.
4. **Atropos DX extensions — Tier 1 + TestMatch** (1 PR, atropos-go). All Tier 1 items + `TestMatch[T MatchableRequest]` together. Includes the `Snapshot()` aggregator and the `MatchableRequest` interface + `FromHTTP` adapter (gRPC/Thrift adapters land when those protocols are adopted). Annotations updated for any new admin endpoints exposed by `Snapshot`.
5. **UI consumer wiring** (1 PR, manteion-ui). `openapi-typescript` against `manteion-go/docs/swagger.yaml`. Replace hand-written types in existing fetch wrappers.
6. **Service-beds checkoutService bump** (1 PR, service-beds). Pull new atropos-go version, verify compile + existing tests, opportunistically adopt one or two Tier 1 introspection methods if natural.
7. **Zeus policies deletion** (1 PR spanning manteion-go + zeus-go). Remove manteion's `/api/v1/zeus/policies` proxy routes. Remove `zeus-go/internal/policy/engine.go` and any associated routes/wiring. Verify tests still pass.

---

## Open questions

- **Zeus retirement scope.** "Zeus is not in use, remove code liberally" — assumed to apply to the policies engine specifically. Confirm whether the rest of zeus (workloads, attacks, k6 driver) stays in scope; if not, sequencing §2 zeus annotation PR drops, manteion's zeus client + remaining proxy routes also get deleted.
- **AutoRule route naming.** `/api/v1/autorules` vs `/api/v1/auto-rules`. Default to `/autorules` matching existing `/rules` convention; UI confirms.
- **Atropos admin spec mount path.** Recommended default `/atropos/admin/`; host services may override but spec defaults to this.
- **`cachebox.Stats()` shape.** Fixed Go struct (recommended) vs Prometheus collector. Services adapt to their Prom registry on their side.
- **`MatchableRequest` interface scope.** Minimal viable methods are `Method/Path/Header/Headers/Labels`. If body matching becomes a requirement, the interface needs `Body() io.ReadCloser` or similar; deferred.
- **Should manteion's UI generate types from zeus's spec** for the proxy fields it currently passes through? Default: no; manteion's spec describes the proxy as opaque payload. (Less relevant if zeus retires further.)

---

## Risk register

- swag annotation quality drifts across authors → mitigated by Foundations PR establishing the style with reference annotations.
- Atropos admin spec has no real "base URL" → mitigated by recommending the `/atropos/admin/` default mount path.
- Schema-refactors plan and this audit overlap (Phase 2.6 changes `compiled_rule.go`; this audit annotates it) → coordination note, not a blocker; different layers.
- swag v2 is newer than v1 — may have annotation gaps or edge-case bugs. Mitigation: Foundations PR validates the toolchain end-to-end before propagation.
- "Zeus is not in use" interpretation risk — see Open Questions.

---

## Connection to Track B (Prometheus + dataset stub)

Track B follows separately. This design hands off:

- **Streaming convention:** SSE, locked. Anything streamable from manteion (run events, trace fetch, future event tails) uses SSE with `Last-Event-ID` resume.
- **Spec conventions:** Track B's manteion endpoints get swag annotations from day one, using the conventions established in Foundations PR.
- **Error envelope:** same `{error: string}` envelope; future `problem+json` migration applies uniformly.
- **AutoRule endpoint surface:** independent of Track B.
- **Atropos:** does not appear in Track B unless the dataset-stretch pipeline needs SDK-side trace tagging — out of scope for now.
- **Naming:** Track B reserves `/api/v1/traces/*` and `/api/v1/datasets/*` namespaces; nothing in this audit collides.
