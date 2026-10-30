# OPA Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give manteion an OPA harness: Rego policies whose inputs are live Prometheus / Loki telemetry and whose outputs are idempotent rule mutations applied through `store.RuleRepo`, so they reach every instance by the normal desired-state path.

**Architecture:** A new `internal/opaharness` package owns the loop. Per enabled policy it fetches the declared inputs (`internal/promql`, new `internal/logql`), evaluates a prepared `data.atropos.mutations` query (OPA v1.19.1, Rego v1 syntax), refuses mutations outside the policy's service scope, applies the rest through a `RuleMutator` that wraps `store.RuleRepo` (plus `FaultRepo.UpdateSpec` for `set_params`, with the rule re-saved so the version bump happens), records one `policy_decisions` row per evaluation, and backs off `cooldown_s` after a change. Storage is one appended migration (`opa_policies`, `policy_decisions`). `internal/api` gets `/api/v1/opa/policies` CRUD, a decision-log reader and a dry-run evaluate. `cmd/manteion` starts the harness when `MANTEION_POLICY_ENGINE=rego` (or `on`) and keeps the frozen PromQL engine behind `threshold`.

**Tech Stack:** Go (`go 1.25.5` directive, toolchain go1.27.2 locally), `github.com/open-policy-agent/opa` v1.19.1 (`v1/rego`, `v1/ast`), pgx/v5 via `database/sql`, stdlib `net/http` + `httptest`, swag v2.0.0-rc5 (`make openapi`).

**Spec:** ADR-1 (§2) of `/Users/pronei/work/faults-lab/atropos-go/.claude/worktrees/elated-shannon-771fa0/docs/plans/2026-09-27-daemon-design-review-2.md`, resolved by the user on 2026-10-30 as "keep it simple": the per-request matcher in the SDK stays label-based; metric- and log-conditioned automation is central, as an OPA harness whose outputs are rule mutations. Current-state facts: `/private/tmp/claude-501/-Users-pronei-work-faults-lab-atropos-go--claude-worktrees-elated-shannon-771fa0/608f89fc-46c1-49c4-98bd-0b38b054c3ca/scratchpad/research-review-facts.md` §A. Sibling contract: `/Users/pronei/work/faults-lab/atropos-go/.claude/worktrees/elated-shannon-771fa0/docs/plans/2026-10-30-desired-state-protocol.md` (its manteion plan adds `internal/desired`, `service_generations`, and takes migration **12**).

Repo root for every relative path below: `/Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon` (branch `feat/atropos-daemon`, HEAD `a85ee9e`); line numbers are as of that commit. Run every `go` command as `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon ...` (the worktree carries an untracked `go.work` whose `replace` points at the atropos-go `feat/atroposd` worktree; workspace mode is what makes `go build ./...` resolve the SDK).

## Global Constraints

- The `go` directive stays `go 1.25.5` (`go.mod:3`). OPA is pinned to **v1.19.1**: it is the newest release whose own `go.mod` says `go 1.25.0`; v1.20.0 through v1.22.0 require `go 1.26.0`, which CI (`.gitlab-ci.yml`, `image: golang:1.25`) and the `Dockerfile` (`golang:1.25-alpine`) cannot build. Measured on a hello-world (darwin/arm64): linking `v1/rego` adds ≈ +29 MB unstripped, ≈ +23 MB with `-ldflags="-s -w"` (what the Dockerfile uses).
- Rego v1 syntax (OPA ≥ 1.0 default: `if` / `contains` keywords, no `import rego.v1` needed). Every policy declares `package atropos`; the harness queries `data.atropos.mutations`. `http.send`, `net.lookup_ip_addr` and `opa.runtime` are refused at compile time.
- Mutations touch stored rules only through `store.RuleRepo.Create/Update/Delete` (`internal/store/rule_repo.go:24-127`): that is where the in-transaction `rule_version` bump lives today and where the desired-state plan adds its per-service generation bump. `set_params` additionally calls `FaultRepo.UpdateSpec` and then re-saves the rule through `RuleRepo.Update` so pollers recompile the spec.
- Mutations are idempotent: a mutation describing the stored state reports `unchanged` and writes nothing. Cooldown starts only after an evaluation whose applied set changed something.
- A mutation whose `(service)` is not in the policy's `scope.services` is refused, never applied.
- Migration is appended as version **13** (`internal/db/migrations.go:103-115`), never edited after it lands. The epoch guard (`migrations.go:135-148`) compares `max(version)` with `len(migrations)`, so the list must stay contiguous: if this plan merges before the desired-state plan, renumber this migration to 12 (and that one to 13).
- Gate: `MANTEION_POLICY_ENGINE` = `off` (default) | `rego` or `on` (OPA harness) | `threshold` (the frozen `internal/policy` loop; `on` no longer means that engine).
- Loki base URL: `MANTEION_LOKI_URL`, default `http://loki:3100`; Prometheus stays `PROMETHEUS_URL` (`cmd/manteion/main.go:49`).
- API error envelope: the bare `{"error": "..."}` (`writeError`, `internal/api/server.go:301`) like the rules handlers; 404 via `errors.Is(err, store.ErrNotFound)`; lists never serialize as `null`; paginated reads use `parsePagination` / `writePage` (`internal/api/pagination.go`). swag tag for the new endpoints: `autorules` (already in the allowed list, `docs/openapi-conventions.md:16`).
- Unit tests never reach the network: fake providers, fake mutator, fake repos behind narrow interfaces, `httptest` for the HTTP clients, real Rego in-process. DB suites run only under `MANTEION_TEST_DB=1` (store) / `MANTEION_INTEGRATION=1` (`testutil.TestDB`).
- After every task: `gofmt -l .` prints nothing, `go vet ./...` clean, `go test ./...` green. Conventional commit messages; every commit message ends with the trailer line `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Do not edit `.claude/` or `vendor/` (there is no vendor tree in this repo).

## Review Focus

1. A mutation whose top-level `service` disagrees with `rule.service` (`upsert_rule`) must be refused rather than applied to either service — Task 5, `TestEnforceScope` ("key mismatch").
2. A `scalar` input whose query returns several series (the author forgot `sum(...)`) must fail the evaluation loudly, not silently take the first series — Task 7, `TestVectorProvider_Fetch` ("scalar rejects several series").
3. A policy row whose Rego no longer compiles (edited in the DB, or a stricter future OPA) must be recorded as an errored decision while the other policies keep running — Task 7, `TestTick_CompileErrorIsRecordedAndOthersRun`.
4. `set_params` against a fault spec that other rules also reference would change those rules too; it must be refused (dedicated spec required) — Task 6, `TestRuleMutator_SetParams` ("shared spec refused").
5. A policy that emits a non-object mutation (a string, a number) must produce a decode error in the decision log, never a panic — Task 5, `TestPrepared_Eval_RejectsNonObjectMutation`.

---

## File Structure

| Path | Responsibility |
|---|---|
| `go.mod`, `go.sum` (modify) | Add `github.com/open-policy-agent/opa v1.19.1`. |
| `internal/model/opapolicy.go` (create) | `OPAPolicy`, `PolicyInput`, `PolicyScope`, `PolicyDecision` + `Validate()`. |
| `internal/model/opapolicy_test.go` (create) | Validation table tests. |
| `internal/id/id.go:4-5` (modify) | Document the `opa-` and `pdec-` prefixes. |
| `internal/db/migrations.go:103-115, 658-661` (modify) | `migration13`: `opa_policies`, `policy_decisions`. |
| `internal/testutil/db.go:46-66` (modify) | Truncate the two new tables. |
| `internal/store/opa_policy_repo.go` (create) | `OPAPolicyRepo` CRUD, `ListEnabled`, unique-name → `ErrConflict`. |
| `internal/store/policy_decision_repo.go` (create) | `PolicyDecisionRepo`: `Record` (insert + prune), `List` (paged), `LastChangeAt`. |
| `internal/store/opa_policy_repo_test.go` (create) | One DB-gated round trip (exercises the migration). |
| `internal/promql/client.go:43-80` (modify) | `Sample`, `DecodeVector`, `QueryVector`; `QueryInstant` keeps its contract. |
| `internal/promql/client_test.go` (create) | Decoder + no-data contract tests. |
| `internal/logql/client.go` (create) | Loki instant metric query client (`/loki/api/v1/query`). |
| `internal/logql/client_test.go` (create) | httptest-backed tests. |
| `internal/opaharness/compile.go` (create) | `Compile`, `Prepared.Eval`, `SourceHash`, unsafe builtins. |
| `internal/opaharness/mutation.go` (create) | `Mutation`, `MutationResult`, ops/outcomes, `Validate`, `Key`, `enforceScope`. |
| `internal/opaharness/mutator.go` (create) | `Mutator` interface, `RuleMutator` over `RuleStore` + `SpecStore`. |
| `internal/opaharness/inputs.go` (create) | `InputProvider`, `VectorQuerier`, `VectorProvider`, `Providers`. |
| `internal/opaharness/harness.go` (create) | `Deps`, `Harness`, `Evaluation`, `Run`, `tick`, `EvaluateOnce`. |
| `internal/opaharness/{compile,mutation,mutator,inputs,harness}_test.go` (create) | Unit tests with fakes and real Rego. |
| `internal/api/opa_handler.go` (create) | Narrow store interfaces + seven handlers with swag annotations. |
| `internal/api/opa_handler_test.go` (create) | Handler tests with DB-free fakes through the real route table. |
| `internal/api/server.go:33-58, 60-106, 236-242` (modify) | Three new `Server` fields, `NewServer` params, routes. |
| `Makefile:27-35` (modify) | Add `internal/opaharness` to swag `--dir`. |
| `docs/swagger.yaml`, `docs/swagger.json` (regenerate) | `make openapi`. |
| `cmd/manteion/main.go:44-50, 90-111, 123-127, 137-144` (modify) | Loki URL, repos, harness, `NewServer` args, three-way gate. |
| `docs/opa-harness.md` (create) | Policy shape, input document, mutation ops, cooldown, example policy with expected mutations. |
| `docs/decisions/2026-10-opa-harness.md` (create) | Decision record superseding the "revisit" note of the 2026-06 freeze. |
| `README.md:39, 72-78`, `CLAUDE.md:36-50 (packages table), 76-85 (API table), 114-120 (env table)`, `AGENTS.md:54` (modify) | Documentation. |

---

### Task 1: Dependency — OPA v1.19.1

**Files:**
- Modify: `go.mod:5-10` (require block), `go.sum`

**Interfaces:**
- Produces: importable `github.com/open-policy-agent/opa/v1/rego` and `github.com/open-policy-agent/opa/v1/ast` for Tasks 5-7.

- [ ] **Step 1: Record the current binary size (for the docs in Task 9)**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon build -o /tmp/manteion-before ./cmd/manteion && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon build -ldflags="-s -w" -o /tmp/manteion-before-s ./cmd/manteion && ls -l /tmp/manteion-before /tmp/manteion-before-s`
Expected: two sizes printed (≈ 29.0 MB unstripped on darwin/arm64 at HEAD); note them.

- [ ] **Step 2: Verify the version pin**

Run: `GOWORK=off GOFLAGS=-mod=mod go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon list -m -versions github.com/open-policy-agent/opa | tr ' ' '\n' | tail -6`
Expected: a list ending in `v1.22.0` (newer may exist by execution time). Then confirm the go directive of the pinned release:
Run: `GOWORK=off go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon mod download -json github.com/open-policy-agent/opa@v1.19.1 | python3 -c 'import sys,json; print(open(json.load(sys.stdin)["GoMod"]).read().splitlines()[2])'`
Expected: `go 1.25.0`. (v1.20.0+ print `go 1.26.0`; do not pick them — see Global Constraints.)

- [ ] **Step 3: Add the requirement (no tidy yet)**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon get github.com/open-policy-agent/opa@v1.19.1 && grep -n "^go \|open-policy-agent" /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon/go.mod`
Expected: `go: added ...` lines, then `3:go 1.25.5` and `github.com/open-policy-agent/opa v1.19.1 // indirect` (in the second require block; nothing imports it yet). Do **not** run `go mod tidy` in this task: with no importer, tidy removes the requirement again (verified on a throwaway copy). Task 5 runs tidy once `compile.go` imports `v1/rego`, which makes the line direct and completes `go.sum`. Note: `go get` / `go mod tidy` ignore `go.work` and resolve `../atropos-go` through `go.mod`'s `replace`, i.e. `/Users/pronei/work/faults-lab/worktrees/atropos-go` (a symlink to the `atropos-refactor` checkout); that is fine for dependency resolution — the workspace build in Step 4 is what exercises the real SDK.

- [ ] **Step 4: Build, vet, test in workspace mode**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon build ./... && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon vet ./... && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./... && git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon status --short`
Expected: every package `ok` (same list as before: api, atrocontrol, atropos, cachestore, conditions, faultcatalog, id, model, orchestrator, policy, ruleconv, store, zeus); status shows only ` M go.mod` and ` M go.sum`. If the build reports `missing go.sum entry`, run `GOFLAGS=-mod=mod go -C ... build ./...` once (it writes the gitignored `go.work.sum`) and re-run.

- [ ] **Step 5: Commit**

```bash
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon add go.mod go.sum
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon commit -m "chore(deps): add github.com/open-policy-agent/opa v1.19.1 for the Rego harness

v1.19.1 is the newest OPA whose go.mod allows go 1.25 (v1.20+ require 1.26,
which CI and the Dockerfile images cannot build).

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Model — `OPAPolicy` and `PolicyDecision`

**Files:**
- Create: `internal/model/opapolicy.go`
- Test: `internal/model/opapolicy_test.go`
- Modify: `internal/id/id.go:4-5` (prefix list comment: add `opa-`, `pdec-`)

**Interfaces:**
- Produces:
  - `type OPAPolicy struct { ID, Name string; Enabled bool; Rego string; IntervalS, CooldownS int; Inputs []PolicyInput; Scope PolicyScope; CreatedAt, UpdatedAt time.Time }` with JSON tags `id, name, enabled, rego, interval_s, cooldown_s, inputs, scope, created_at, updated_at`.
  - `type PolicyInput struct { Name, Source, Query, Kind string }` (JSON `name, source, query, kind`).
  - `type PolicyScope struct { Services []string }` (JSON `services`); `func (s PolicyScope) Allows(service string) bool`.
  - `func (p *OPAPolicy) Validate() error` — defaults `IntervalS == 0 → 30` (`DefaultOPAIntervalS`) and `Kind == "" → "scalar"` in place, like `Rule.Validate` defaults `StartPolicy`.
  - Constants `InputSourcePrometheus = "prometheus"`, `InputSourceLoki = "loki"`, `InputKindScalar = "scalar"`, `InputKindVector = "vector"`, `DefaultOPAIntervalS = 30`.
  - `type PolicyDecision struct { ID, PolicyID string; EvaluatedAt time.Time; InputDigest string; Mutations json.RawMessage; Applied bool; Error string }` (JSON `id, policy_id, evaluated_at, input_digest, mutations, applied, error,omitempty`). `Mutations` holds a JSON array of `opaharness.MutationResult` (Task 5); the model stays raw so it does not import the harness.

- [ ] **Step 1: Write the failing tests**

`internal/model/opapolicy_test.go`:

```go
package model

import "testing"

func validOPAPolicy() OPAPolicy {
	return OPAPolicy{
		ID: "opa-1", Name: "checkout-hot", Enabled: true,
		Rego:      "package atropos\n\nmutations contains m if { false; m := {} }\n",
		IntervalS: 30, CooldownS: 120,
		Inputs:    []PolicyInput{{Name: "p99", Source: "prometheus", Query: "up", Kind: "scalar"}},
		Scope:     PolicyScope{Services: []string{"checkout"}},
	}
}

func TestOPAPolicy_Validate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*OPAPolicy)
		wantErr bool
	}{
		{"valid", nil, false},
		{"missing id", func(p *OPAPolicy) { p.ID = "" }, true},
		{"missing name", func(p *OPAPolicy) { p.Name = "" }, true},
		{"blank rego", func(p *OPAPolicy) { p.Rego = "  \n" }, true},
		{"negative interval", func(p *OPAPolicy) { p.IntervalS = -1 }, true},
		{"negative cooldown", func(p *OPAPolicy) { p.CooldownS = -1 }, true},
		{"input unknown source", func(p *OPAPolicy) { p.Inputs[0].Source = "tempo" }, true},
		{"input unknown kind", func(p *OPAPolicy) { p.Inputs[0].Kind = "matrix" }, true},
		{"input name not an identifier", func(p *OPAPolicy) { p.Inputs[0].Name = "p-99" }, true},
		{"input blank query", func(p *OPAPolicy) { p.Inputs[0].Query = " " }, true},
		{"duplicate input name", func(p *OPAPolicy) { p.Inputs = append(p.Inputs, p.Inputs[0]) }, true},
		{"no inputs is allowed", func(p *OPAPolicy) { p.Inputs = nil }, false},
		{"empty scope", func(p *OPAPolicy) { p.Scope.Services = nil }, true},
		{"blank scope service", func(p *OPAPolicy) { p.Scope.Services = []string{""} }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := validOPAPolicy()
			if tt.modify != nil {
				tt.modify(&p)
			}
			err := p.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestOPAPolicy_ValidateDefaults(t *testing.T) {
	p := validOPAPolicy()
	p.IntervalS = 0
	p.Inputs[0].Kind = ""
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if p.IntervalS != DefaultOPAIntervalS {
		t.Errorf("IntervalS = %d, want %d", p.IntervalS, DefaultOPAIntervalS)
	}
	if p.Inputs[0].Kind != InputKindScalar {
		t.Errorf("Kind = %q, want %q", p.Inputs[0].Kind, InputKindScalar)
	}
}

func TestPolicyScope_Allows(t *testing.T) {
	s := PolicyScope{Services: []string{"checkout", "cart"}}
	for svc, want := range map[string]bool{"checkout": true, "cart": true, "frontend": false, "": false} {
		if got := s.Allows(svc); got != want {
			t.Errorf("Allows(%q) = %v, want %v", svc, got, want)
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/model/ -run 'TestOPAPolicy|TestPolicyScope' -v`
Expected: `FAIL	manteion-go/internal/model [build failed]` with `undefined: OPAPolicy`.

- [ ] **Step 3: Implement `internal/model/opapolicy.go`**

Types and constants exactly as in Interfaces. `Validate` errors, in this order, all prefixed `opa policy: `: `id required`; `name required`; `rego required` (after `strings.TrimSpace`); `interval_s must be >= 1` (after defaulting 0); `cooldown_s must be non-negative`; per input `input[%d]: name %q must match ^[A-Za-z_][A-Za-z0-9_]*$`, `input[%d]: duplicate name %q`, `input[%d]: invalid source %q`, `input[%d]: query required`, `input[%d]: invalid kind %q`; `scope.services must name at least one service`; `scope.services[%d]: blank service`. Use a package-level `var inputNameRe = regexp.MustCompile(...)` and `setOf` from `enums.go:53` for the source/kind sets. `Allows` is a linear scan. Add `opa-` and `pdec-` to the prefix list in `internal/id/id.go:4-5`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/model/ -run 'TestOPAPolicy|TestPolicyScope' -v`
Expected: `--- PASS` for all three tests, `ok  	manteion-go/internal/model`.

- [ ] **Step 5: Commit**

```bash
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon add internal/model/opapolicy.go internal/model/opapolicy_test.go internal/id/id.go
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon commit -m "feat(model): OPAPolicy and PolicyDecision for the OPA harness

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Storage — migration 13 and the two repositories

**Files:**
- Modify: `internal/db/migrations.go:103-115` (slice) and after `:661` (new const)
- Modify: `internal/testutil/db.go:46-66`
- Create: `internal/store/opa_policy_repo.go`, `internal/store/policy_decision_repo.go`
- Test: `internal/store/opa_policy_repo_test.go` (DB-gated by the package `TestMain`, `internal/store/rule_repo_test.go:37-66`)

**Interfaces:**
- Consumes: `model.OPAPolicy`, `model.PolicyDecision` (Task 2); helpers `execTx`, `jsonbMarshal`, `jsonbScan`, `nullString`, `affectedOrNotFound`, `pgViolation`, `ErrConflict`, `ErrNotFound`, `Page` (`internal/store/helpers.go`).
- Produces:
  - `func NewOPAPolicyRepo(db *sql.DB) *OPAPolicyRepo`; methods `Create(ctx, *model.OPAPolicy) error`, `Get(ctx, id string) (*model.OPAPolicy, error)`, `Update(ctx, *model.OPAPolicy) error`, `Delete(ctx, id string) error`, `List(ctx) ([]*model.OPAPolicy, error)`, `ListEnabled(ctx) ([]*model.OPAPolicy, error)`.
  - `const DefaultDecisionRetention = 1000`; `func NewPolicyDecisionRepo(db *sql.DB) *PolicyDecisionRepo`; `func (r *PolicyDecisionRepo) WithRetention(keep int) *PolicyDecisionRepo`; methods `Record(ctx, *model.PolicyDecision) error`, `List(ctx, policyID string, p Page) ([]*model.PolicyDecision, int, error)` (newest first, `COUNT(*) OVER ()` total, clamps like `ListPhasesPaged` at `experiment_repo.go:686-695`), `LastChangeAt(ctx, policyID string) (time.Time, bool, error)`.

- [ ] **Step 1: Write the failing DB-gated test**

`internal/store/opa_policy_repo_test.go`:

```go
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"manteion-go/internal/model"
)

// TestOPAPolicyRepo_RoundTripAndDecisions exercises migration 13 end to end:
// policy CRUD incl. the unique-name conflict, the decision log's retention
// prune, LastChangeAt (the cooldown witness), and the ON DELETE CASCADE.
func TestOPAPolicyRepo_RoundTripAndDecisions(t *testing.T) {
	ctx := context.Background()
	policies := NewOPAPolicyRepo(testDB)
	decisions := NewPolicyDecisionRepo(testDB).WithRetention(2)
	tag := fmt.Sprintf("%d", time.Now().UnixNano())

	p := &model.OPAPolicy{
		ID: "opa-" + tag, Name: "rt-" + tag, Enabled: true,
		Rego:      "package atropos\n\nmutations contains m if { false; m := {} }\n",
		IntervalS: 15, CooldownS: 60,
		Inputs:    []model.PolicyInput{{Name: "p99", Source: "prometheus", Query: "up", Kind: "scalar"}},
		Scope:     model.PolicyScope{Services: []string{"checkout"}},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := policies.Create(ctx, p); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = policies.Delete(context.Background(), p.ID) })

	dup := *p
	dup.ID = "opa-dup-" + tag
	if err := policies.Create(ctx, &dup); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate name: err = %v, want ErrConflict", err)
	}

	got, err := policies.Get(ctx, p.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Name != p.Name || got.Rego != p.Rego || got.IntervalS != 15 || got.CooldownS != 60 || !got.Enabled ||
		len(got.Inputs) != 1 || got.Inputs[0].Query != "up" || len(got.Scope.Services) != 1 || got.Scope.Services[0] != "checkout" {
		t.Errorf("round trip = %+v, want %+v", got, p)
	}

	p.Enabled = false
	p.CooldownS = 90
	if err := policies.Update(ctx, p); err != nil {
		t.Fatalf("update: %v", err)
	}
	enabled, err := policies.ListEnabled(ctx)
	if err != nil {
		t.Fatalf("list enabled: %v", err)
	}
	for _, e := range enabled {
		if e.ID == p.ID {
			t.Errorf("disabled policy %s still listed by ListEnabled", p.ID)
		}
	}
	if got, _ = policies.Get(ctx, p.ID); got.CooldownS != 90 {
		t.Errorf("CooldownS after update = %d, want 90", got.CooldownS)
	}

	base := time.Now().Add(-time.Minute).Truncate(time.Microsecond)
	for i := 0; i < 3; i++ {
		d := &model.PolicyDecision{
			ID: fmt.Sprintf("pdec-%s-%d", tag, i), PolicyID: p.ID,
			EvaluatedAt: base.Add(time.Duration(i) * time.Second), InputDigest: "abc",
			Applied:   i == 1,
			Mutations: json.RawMessage(`[{"op":"upsert_rule","service":"checkout","name":"r","outcome":"created"}]`),
		}
		if err := decisions.Record(ctx, d); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	list, total, err := decisions.List(ctx, p.ID, Page{Limit: 10})
	if err != nil {
		t.Fatalf("list decisions: %v", err)
	}
	if total != 2 || len(list) != 2 || list[0].ID != fmt.Sprintf("pdec-%s-2", tag) || list[1].ID != fmt.Sprintf("pdec-%s-1", tag) {
		t.Errorf("retention 2: total %d, ids %v; want the two newest, newest first", total, []string{list[0].ID, list[1].ID})
	}
	last, ok, err := decisions.LastChangeAt(ctx, p.ID)
	if err != nil || !ok {
		t.Fatalf("LastChangeAt: ok=%v err=%v", ok, err)
	}
	if want := base.Add(time.Second); !last.Equal(want) {
		t.Errorf("LastChangeAt = %v, want %v (the only applied+changed row)", last, want)
	}

	if err := policies.Delete(ctx, p.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, total, _ = decisions.List(ctx, p.ID, Page{Limit: 10}); total != 0 {
		t.Errorf("decisions after policy delete = %d, want 0 (cascade)", total)
	}
	if _, err := policies.Get(ctx, p.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("get after delete: err = %v, want ErrNotFound", err)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/store/ -run TestOPAPolicyRepo -v`
Expected: `FAIL	manteion-go/internal/store [build failed]` with `undefined: NewOPAPolicyRepo` (the package does not compile, with or without a database).

- [ ] **Step 3: Add migration 13**

Append after `migration11` (`internal/db/migrations.go:661`) and register `{13, "opa_policies + policy_decisions (OPA harness; 12 is the desired-state plan's)", migration13}` as the last slice element at `:103-115`:

```go
// migration13 adds the OPA harness: opa_policies holds Rego policies whose
// inputs are live telemetry and whose outputs are rule mutations;
// policy_decisions is the decision log, one row per evaluation, pruned per
// policy to PolicyDecisionRepo's retention. Numbered 13 because the
// desired-state plan (service_generations, sdk_instances observed columns)
// takes 12; if this migration merges first, renumber it to 12 and that one to
// 13 — the epoch guard compares max(version) with len(migrations), so a gap
// makes every later start refuse the database.
const migration13 = `
CREATE TABLE opa_policies (
    id          TEXT PRIMARY KEY,                   -- 'opa-<uuidv7>'
    name        TEXT NOT NULL UNIQUE,
    enabled     BOOLEAN NOT NULL DEFAULT false,
    rego        TEXT NOT NULL,
    interval_s  INT NOT NULL DEFAULT 30,
    cooldown_s  INT NOT NULL DEFAULT 0,
    inputs      JSONB NOT NULL DEFAULT '[]',        -- [{name, source, query, kind}]
    scope       JSONB NOT NULL DEFAULT '{}',        -- {services: [...]}
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE policy_decisions (
    id           TEXT PRIMARY KEY,                  -- 'pdec-<uuidv7>'
    policy_id    TEXT NOT NULL REFERENCES opa_policies(id) ON DELETE CASCADE,
    evaluated_at TIMESTAMPTZ NOT NULL,
    input_digest TEXT NOT NULL DEFAULT '',
    mutations    JSONB NOT NULL DEFAULT '[]',       -- [opaharness.MutationResult]
    applied      BOOLEAN NOT NULL DEFAULT false,
    error        TEXT
);
CREATE INDEX idx_policy_decisions_policy_time ON policy_decisions(policy_id, evaluated_at DESC);
`
```

In `internal/testutil/db.go:46-66` insert `"policy_decisions",` and `"opa_policies",` before `"policy_rules",` (children first).

- [ ] **Step 4: Implement the two repositories**

`internal/store/opa_policy_repo.go`: column list `id, name, enabled, rego, interval_s, cooldown_s, inputs, scope, created_at, updated_at`; `Create` and `Update` call `p.Validate()` first, marshal `Inputs`/`Scope` with `jsonbMarshal`, and map SQLSTATE `23505` (via `pgViolation`) to `fmt.Errorf("opa policy name %q already exists: %w", p.Name, ErrConflict)` (pattern: `experiment_repo.go:64-66`); `Update` sets `updated_at = now()` and uses `affectedOrNotFound`; `Get` returns `ErrNotFound` on `sql.ErrNoRows`; `List` orders by `created_at`; `ListEnabled` adds `WHERE enabled`. One shared `scanOPAPolicy(scanner)` like `scanInstance` (`sdk_repo.go:96-113`).

`internal/store/policy_decision_repo.go`: `Record` runs in `execTx`: `INSERT` (nil `Mutations` → `[]byte("[]")`, `Error` via `nullString`) then the prune
`DELETE FROM policy_decisions WHERE policy_id = $1 AND id NOT IN (SELECT id FROM policy_decisions WHERE policy_id = $1 ORDER BY evaluated_at DESC, id DESC LIMIT $2)`.
`List`: `SELECT ..., COUNT(*) OVER () AS total_count ... WHERE policy_id = $1 ORDER BY evaluated_at DESC, id DESC LIMIT $2 OFFSET $3`, returning a non-nil empty slice when there are no rows. `LastChangeAt`:
`SELECT evaluated_at FROM policy_decisions WHERE policy_id = $1 AND applied AND EXISTS (SELECT 1 FROM jsonb_array_elements(mutations) m WHERE m->>'outcome' IN ('created','updated','deleted')) ORDER BY evaluated_at DESC LIMIT 1` → `(zero, false, nil)` on `sql.ErrNoRows`.

- [ ] **Step 5: Compile without a database, then run the gated test**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon vet ./internal/store/ ./internal/db/ ./internal/testutil/ && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/store/`
Expected: vet clean; `store integration tests skipped — set MANTEION_TEST_DB=1 to run` and `ok  	manteion-go/internal/store`.

Then, with Postgres up (`docker compose up -d postgres` in the repo root):
Run: `MANTEION_TEST_DB=1 MANTEION_INTEGRATION=1 go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test -count=1 -p 1 ./internal/store/ -run 'TestOPAPolicyRepo|TestEnumParity' -v`
Expected: `migration applied version=13 ...` in the log on first run, `--- PASS: TestOPAPolicyRepo_RoundTripAndDecisions`, `--- PASS: TestEnumParity` (no new enum types), `ok`.

- [ ] **Step 6: Commit**

```bash
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon add internal/db/migrations.go internal/testutil/db.go internal/store/opa_policy_repo.go internal/store/policy_decision_repo.go internal/store/opa_policy_repo_test.go
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon commit -m "feat(store,db): opa_policies + policy_decisions (migration 13) with repos

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Telemetry inputs — Prometheus vectors and a Loki client

**Files:**
- Modify: `internal/promql/client.go:43-80`
- Create: `internal/promql/client_test.go`, `internal/logql/client.go`, `internal/logql/client_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `type promql.Sample struct { Labels map[string]string "json:\"metric\""; Value float64 "json:\"value\"" }`
  - `func promql.DecodeVector(r io.Reader) ([]Sample, error)` — decodes the Prometheus instant-query envelope (which Loki metric queries share): `status != "success"` → `fmt.Errorf("query returned status %q: %s: %s", status, errorType, error)`; `resultType "vector"` → one `Sample` per series (empty vector → empty non-nil slice); `"scalar"` → one `Sample` with an empty label map; any other `resultType` → `fmt.Errorf("unsupported resultType %q (metric queries only)", t)`.
  - `func (c *promql.Client) QueryVector(ctx, query string) ([]Sample, error)` — `GET {base}/api/v1/query?query=`; `QueryInstant` is rewritten on top of it and keeps returning `prometheus query %q returned no data` for an empty vector and the first sample's value otherwise.
  - `package logql`: `type Client`, `type ClientOption func(*Client)`, `func WithTimeout(d time.Duration) ClientOption`, `func NewClient(baseURL string, opts ...ClientOption) *Client` (5 s default timeout, trailing `/` trimmed, mirrors `promql.NewClient`), `func (c *Client) QueryVector(ctx, query string) ([]promql.Sample, error)` — `GET {base}/loki/api/v1/query?query=`; a non-2xx status → `fmt.Errorf("loki returned status %d: %s", code, firstLineOfBody)`; body decoded with `promql.DecodeVector`, so a log (`streams`) query fails with the "metric queries only" error.

- [ ] **Step 1: Write the failing tests**

`internal/promql/client_test.go`:

```go
package promql

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestDecodeVector(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		want    []Sample
		wantErr string
	}{
		{"vector", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"service":"checkout"},"value":[1700000000,"1.5"]},{"metric":{"service":"cart"},"value":[1700000000.5,"2"]}]}}`,
			[]Sample{{Labels: map[string]string{"service": "checkout"}, Value: 1.5}, {Labels: map[string]string{"service": "cart"}, Value: 2}}, ""},
		{"empty vector", `{"status":"success","data":{"resultType":"vector","result":[]}}`, []Sample{}, ""},
		{"scalar", `{"status":"success","data":{"resultType":"scalar","result":[1700000000,"3"]}}`, []Sample{{Labels: map[string]string{}, Value: 3}}, ""},
		{"streams", `{"status":"success","data":{"resultType":"streams","result":[]}}`, nil, `resultType "streams"`},
		{"error status", `{"status":"error","errorType":"bad_data","error":"parse error at 1"}`, nil, "bad_data"},
		{"not json", `<html>`, nil, "decode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeVector(strings.NewReader(tt.body))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeVector: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("samples = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func fakeProm(t *testing.T, body string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" || r.URL.Query().Get("query") == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return NewClient(srv.URL)
}

func TestQueryVector(t *testing.T) {
	c := fakeProm(t, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"le":"+Inf"},"value":[1700000000,"42"]}]}}`)
	got, err := c.QueryVector(context.Background(), "up")
	if err != nil || len(got) != 1 || got[0].Value != 42 || got[0].Labels["le"] != "+Inf" {
		t.Fatalf("QueryVector = %+v, %v; want one sample 42 with le=+Inf", got, err)
	}
}

func TestQueryInstant_KeepsNoDataError(t *testing.T) {
	c := fakeProm(t, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	if _, err := c.QueryInstant(context.Background(), "up"); err == nil || !strings.Contains(err.Error(), "returned no data") {
		t.Fatalf("err = %v, want the historical 'returned no data' error", err)
	}
	c = fakeProm(t, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"600"]}]}}`)
	if v, err := c.QueryInstant(context.Background(), "up"); err != nil || v != 600 {
		t.Fatalf("QueryInstant = %v, %v; want 600", v, err)
	}
}
```

`internal/logql/client_test.go`:

```go
package logql

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func fakeLoki(t *testing.T, status int, body string) (*Client, *http.Request) {
	t.Helper()
	var got http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = *r
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return NewClient(srv.URL + "/"), &got
}

func TestQueryVector(t *testing.T) {
	c, got := fakeLoki(t, http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"42"]}]}}`)
	q := `sum(count_over_time({service_name="checkout"} |= "atropos.decision" [1m]))`
	samples, err := c.QueryVector(context.Background(), q)
	if err != nil {
		t.Fatalf("QueryVector: %v", err)
	}
	if got.URL.Path != "/loki/api/v1/query" || got.URL.Query().Get("query") != q {
		t.Errorf("request = %s %s, want GET /loki/api/v1/query with the query param", got.URL.Path, got.URL.RawQuery)
	}
	if len(samples) != 1 || samples[0].Value != 42 {
		t.Errorf("samples = %+v, want one sample 42", samples)
	}
}

func TestQueryVector_StreamsRejected(t *testing.T) {
	c, _ := fakeLoki(t, http.StatusOK, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
	if _, err := c.QueryVector(context.Background(), `{service_name="checkout"}`); err == nil || !strings.Contains(err.Error(), `"streams"`) {
		t.Fatalf("err = %v, want the unsupported resultType error", err)
	}
}

func TestQueryVector_HTTPError(t *testing.T) {
	c, _ := fakeLoki(t, http.StatusBadRequest, "parse error at line 1")
	if _, err := c.QueryVector(context.Background(), "nope"); err == nil || !strings.Contains(err.Error(), "status 400") {
		t.Fatalf("err = %v, want 'loki returned status 400'", err)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/promql/ ./internal/logql/`
Expected: `[build failed]` for promql (`undefined: DecodeVector`) and `no Go files` / build failure for logql.

- [ ] **Step 3: Implement**

`internal/promql/client.go`: add `Sample`, `DecodeVector` (decode into a struct with `Status, ErrorType, Error string` and `Data struct{ ResultType string; Result json.RawMessage }`, then switch on `ResultType`; the value pair is `[2]json.RawMessage` whose second element is a quoted float, parsed with `strconv.ParseFloat`), `QueryVector`, and rewrite `QueryInstant` as `QueryVector` + the empty check. `internal/logql/client.go`: the client shape of `promql.Client` (`client.go:16-41`), `QueryVector` reads the body once; on non-2xx it returns the status error with the first line of the body (max 200 bytes), else `promql.DecodeVector`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/promql/ ./internal/logql/ ./internal/policy/ ./internal/orchestrator/ -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: `--- PASS` for `TestDecodeVector` (6 subtests), `TestQueryVector` (both packages), `TestQueryInstant_KeepsNoDataError`, `TestQueryVector_StreamsRejected`, `TestQueryVector_HTTPError`; `ok` for all four packages (policy and orchestrator still consume `QueryInstant`).

- [ ] **Step 5: Commit**

```bash
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon add internal/promql internal/logql
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon commit -m "feat(promql,logql): vector instant queries for harness inputs

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Harness core — compiled policies and mutation shapes

**Files:**
- Create: `internal/opaharness/compile.go`, `internal/opaharness/mutation.go`
- Test: `internal/opaharness/compile_test.go`, `internal/opaharness/mutation_test.go`

**Interfaces:**
- Consumes: `model.Rule`, `model.PolicyScope` (`internal/model`), `github.com/open-policy-agent/opa/v1/rego` (`rego.New`, `rego.Query`, `rego.Module`, `rego.UnsafeBuiltins`, `rego.StrictBuiltinErrors`, `(*rego.Rego).PrepareForEval`, `rego.PreparedEvalQuery.Eval`, `rego.EvalInput`), `github.com/open-policy-agent/opa/v1/ast` (`ast.ParseModule`).
- Produces:
  - Ops `OpUpsertRule = "upsert_rule"`, `OpSetEnabled = "set_enabled"`, `OpSetParams = "set_params"`, `OpDeleteRule = "delete_rule"`; outcomes `OutcomeCreated = "created"`, `OutcomeUpdated = "updated"`, `OutcomeDeleted = "deleted"`, `OutcomeUnchanged = "unchanged"`, `OutcomeRefused = "refused"` (never reached the store: out of scope or malformed), `OutcomeFailed = "failed"` (the store rejected it).
  - `type Mutation struct { Op string "json:\"op\""; Service string "json:\"service,omitempty\""; Name string "json:\"name,omitempty\""; Rule *model.Rule "json:\"rule,omitempty\""; Enabled *bool "json:\"enabled,omitempty\""; Params json.RawMessage "json:\"params,omitempty\"" }`; `func (m Mutation) Key() (service, name string)` (for `upsert_rule` the rule's `Service`/`Name`; otherwise the top-level fields); `func (m Mutation) Validate() error`.
  - `type MutationResult struct { Op, Service, Name, Outcome string; Error string "json:\"error,omitempty\""; RuleID string "json:\"rule_id,omitempty\"" }` (JSON `op, service, name, outcome, error, rule_id`); `func (r MutationResult) Changed() bool` (created | updated | deleted).
  - `func enforceScope(scope model.PolicyScope, ms []Mutation) (allowed []Mutation, refused []MutationResult)`.
  - `type Prepared struct` (unexported fields `pq rego.PreparedEvalQuery`, `hash string`); `func (p *Prepared) Hash() string`; `func SourceHash(src string) string` (sha256 hex); `func Compile(src string) (*Prepared, error)`; `func (p *Prepared) Eval(ctx context.Context, input any) ([]Mutation, error)`; `const evalTimeout = 5 * time.Second`.

- [ ] **Step 1: Write the failing tests**

`internal/opaharness/compile_test.go`:

```go
package opaharness

import (
	"context"
	"strings"
	"testing"
)

const scalarRego = `package atropos

mutations contains {"op": "set_enabled", "service": "checkout", "name": "r", "enabled": false} if {
	input.inputs.p99 > 1
}
`

const upsertRego = `package atropos

mutations contains {"op": "upsert_rule", "rule": {
	"name": "checkout-egress-latency",
	"service": "checkout",
	"enabled": true,
	"priority": 100,
	"mode": "inline",
	"match": {"injection_point": "egress", "labels": {"atropos.http.path": "/cart/checkout"}},
	"action": {"type": "fault_spec", "fault_spec_id": "spec-egress-latency-200ms"},
}}
`

func TestCompile_Rejects(t *testing.T) {
	tests := []struct{ name, src, wantErr string }{
		{"parse error", "package atropos\nmutations contains x if { x := }", "rego_parse_error"},
		{"unsafe builtin", "package atropos\nmutations contains r if { r := http.send({\"method\": \"get\", \"url\": \"http://x\"}) }", "unsafe built-in"},
		{"wrong package", "package other\nmutations contains 1", "package atropos"},
		{"empty", "   ", "package atropos"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Compile(tt.src); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Compile err = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestCompile_Hash(t *testing.T) {
	p, err := Compile(scalarRego)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if p.Hash() != SourceHash(scalarRego) || len(p.Hash()) != 64 {
		t.Errorf("Hash() = %q, want sha256 hex of the source", p.Hash())
	}
}

func TestPrepared_Eval(t *testing.T) {
	p, err := Compile(scalarRego)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ctx := context.Background()
	hot, err := p.Eval(ctx, map[string]any{"inputs": map[string]any{"p99": 2.0}})
	if err != nil || len(hot) != 1 {
		t.Fatalf("hot eval = %+v, %v; want one mutation", hot, err)
	}
	if hot[0].Op != OpSetEnabled || hot[0].Service != "checkout" || hot[0].Name != "r" || hot[0].Enabled == nil || *hot[0].Enabled {
		t.Errorf("mutation = %+v, want set_enabled checkout/r enabled=false", hot[0])
	}
	for name, in := range map[string]any{
		"cold":       map[string]any{"inputs": map[string]any{"p99": 0.5}},
		"null input": map[string]any{"inputs": map[string]any{"p99": nil}},
		"no inputs":  map[string]any{},
	} {
		got, err := p.Eval(ctx, in)
		if err != nil || got == nil || len(got) != 0 {
			t.Errorf("%s: eval = %+v, %v; want an empty, non-nil slice", name, got, err)
		}
	}
}

func TestPrepared_Eval_DecodesRule(t *testing.T) {
	p, err := Compile(upsertRego)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	got, err := p.Eval(context.Background(), map[string]any{})
	if err != nil || len(got) != 1 || got[0].Rule == nil {
		t.Fatalf("eval = %+v, %v; want one upsert with a rule", got, err)
	}
	r := got[0].Rule
	if svc, name := got[0].Key(); svc != "checkout" || name != "checkout-egress-latency" {
		t.Errorf("Key() = %s/%s", svc, name)
	}
	if r.Priority != 100 || r.Mode != "inline" || r.Match.InjectionPoint != "egress" ||
		r.Match.Labels["atropos.http.path"] != "/cart/checkout" || r.Action.FaultSpecID != "spec-egress-latency-200ms" || !r.Enabled {
		t.Errorf("rule = %+v", r)
	}
}

func TestPrepared_Eval_RejectsNonObjectMutation(t *testing.T) {
	p, err := Compile("package atropos\n\nmutations contains \"boom\"\n")
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if _, err := p.Eval(context.Background(), map[string]any{}); err == nil || !strings.Contains(err.Error(), "decode mutations") {
		t.Fatalf("err = %v, want a decode error, not a panic", err)
	}
}
```

`internal/opaharness/mutation_test.go`:

```go
package opaharness

import (
	"encoding/json"
	"strings"
	"testing"

	"manteion-go/internal/model"
)

func TestEnforceScope(t *testing.T) {
	scope := model.PolicyScope{Services: []string{"checkout"}}
	on := true
	tests := []struct {
		name        string
		m           Mutation
		wantRefused string // substring of the refused result's error; "" = allowed
	}{
		{"upsert in scope", Mutation{Op: OpUpsertRule, Rule: &model.Rule{Name: "r", Service: "checkout"}}, ""},
		{"set_enabled in scope", Mutation{Op: OpSetEnabled, Service: "checkout", Name: "r", Enabled: &on}, ""},
		{"set_params in scope", Mutation{Op: OpSetParams, Service: "checkout", Name: "r", Params: json.RawMessage(`{"delay":"50ms"}`)}, ""},
		{"delete in scope", Mutation{Op: OpDeleteRule, Service: "checkout", Name: "r"}, ""},
		{"out of scope", Mutation{Op: OpSetEnabled, Service: "frontend", Name: "r", Enabled: &on}, "outside policy scope"},
		{"key mismatch", Mutation{Op: OpUpsertRule, Service: "frontend", Rule: &model.Rule{Name: "r", Service: "checkout"}}, "disagree"},
		{"unknown op", Mutation{Op: "nuke", Service: "checkout", Name: "r"}, "unknown op"},
		{"upsert without rule", Mutation{Op: OpUpsertRule, Service: "checkout", Name: "r"}, "rule required"},
		{"set_enabled without enabled", Mutation{Op: OpSetEnabled, Service: "checkout", Name: "r"}, "enabled required"},
		{"set_params without params", Mutation{Op: OpSetParams, Service: "checkout", Name: "r"}, "params required"},
		{"set_params with non-object params", Mutation{Op: OpSetParams, Service: "checkout", Name: "r", Params: json.RawMessage(`[1]`)}, "JSON object"},
		{"delete without name", Mutation{Op: OpDeleteRule, Service: "checkout"}, "name required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed, refused := enforceScope(scope, []Mutation{tt.m})
			if tt.wantRefused == "" {
				if len(allowed) != 1 || len(refused) != 0 {
					t.Fatalf("allowed %d refused %+v, want allowed", len(allowed), refused)
				}
				return
			}
			if len(allowed) != 0 || len(refused) != 1 {
				t.Fatalf("allowed %d refused %+v, want one refusal", len(allowed), refused)
			}
			if refused[0].Outcome != OutcomeRefused || !strings.Contains(refused[0].Error, tt.wantRefused) {
				t.Errorf("refusal = %+v, want outcome refused with %q", refused[0], tt.wantRefused)
			}
		})
	}
}

func TestEnforceScope_KeepsOrderAndSplits(t *testing.T) {
	scope := model.PolicyScope{Services: []string{"checkout"}}
	ms := []Mutation{
		{Op: OpDeleteRule, Service: "frontend", Name: "a"},
		{Op: OpDeleteRule, Service: "checkout", Name: "b"},
		{Op: OpDeleteRule, Service: "checkout", Name: "c"},
	}
	allowed, refused := enforceScope(scope, ms)
	if len(allowed) != 2 || allowed[0].Name != "b" || allowed[1].Name != "c" || len(refused) != 1 || refused[0].Name != "a" {
		t.Errorf("allowed %+v refused %+v", allowed, refused)
	}
}

func TestMutationResult_Changed(t *testing.T) {
	for outcome, want := range map[string]bool{OutcomeCreated: true, OutcomeUpdated: true, OutcomeDeleted: true, OutcomeUnchanged: false, OutcomeRefused: false, OutcomeFailed: false} {
		if got := (MutationResult{Outcome: outcome}).Changed(); got != want {
			t.Errorf("Changed(%s) = %v, want %v", outcome, got, want)
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/opaharness/`
Expected: `[build failed]` with `undefined: Compile` (and the other symbols).

- [ ] **Step 3: Implement `mutation.go`**

`Validate` errors (prefix `mutation: `): `unknown op %q`; `upsert_rule: rule required`; `upsert_rule: rule.service and rule.name required`; `upsert_rule: service/name disagree with rule (%s/%s vs %s/%s)` when a non-empty top-level field differs; `set_enabled: enabled required`; `set_params: params required`; `set_params: params must be a JSON object` (first non-space byte is not `{` or `!json.Valid`); and for the three keyed ops `service required` / `name required`. `enforceScope` runs `Validate` first, then `scope.Allows(service)` (`service %q outside policy scope`), producing `MutationResult{Op, Service, Name, Outcome: OutcomeRefused, Error}` for refusals and preserving order within each output.

- [ ] **Step 4: Implement `compile.go`**

```go
const (
	policyFilename = "policy.rego"
	policyPackage  = "data.atropos"
	mutationsQuery = "data.atropos.mutations"
	evalTimeout    = 5 * time.Second
)

// unsafeBuiltins are refused at compile time: a policy must never reach the
// network or inspect the process from inside the evaluator.
var unsafeBuiltins = map[string]struct{}{"http.send": {}, "net.lookup_ip_addr": {}, "opa.runtime": {}}
```

`Compile`: `ast.ParseModule(policyFilename, src)` (error returned verbatim — it carries `rego_parse_error`); reject `m.Package.Path.String() != policyPackage` with `policy must declare "package atropos" (got %s)`; blank source hits the same error (ParseModule on whitespace errors — wrap it so the message still contains `package atropos`); then `rego.New(rego.Query(mutationsQuery), rego.Module(policyFilename, src), rego.UnsafeBuiltins(unsafeBuiltins), rego.StrictBuiltinErrors(true)).PrepareForEval(ctx)` with a `context.WithTimeout(context.Background(), evalTimeout)` (the unsafe-builtin error surfaces here as `rego_type_error: unsafe built-in function calls`). `Eval`: `context.WithTimeout(ctx, evalTimeout)`; `rs, err := p.pq.Eval(ctx, rego.EvalInput(input))`; `len(rs) == 0 || len(rs[0].Expressions) == 0` → `[]Mutation{}` (undefined = nothing to do); otherwise `json.Marshal(rs[0].Expressions[0].Value)` (a set arrives as `[]any`) and `json.Unmarshal` into `[]Mutation`, wrapping failures as `decode mutations: %w`; a `nil` result becomes `[]Mutation{}`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/opaharness/ -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: `--- PASS` for `TestCompile_Rejects`, `TestCompile_Hash`, `TestPrepared_Eval`, `TestPrepared_Eval_DecodesRule`, `TestPrepared_Eval_RejectsNonObjectMutation`, `TestEnforceScope`, `TestEnforceScope_KeepsOrderAndSplits`, `TestMutationResult_Changed`; `ok  	manteion-go/internal/opaharness`. Then run `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon mod tidy && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon build ./...`: the OPA requirement moves into the direct `require` block (marker dropped) and `go.sum` gains the entries the build needs; `git status --short` shows only `go.mod`, `go.sum` and the new package.

- [ ] **Step 6: Commit**

```bash
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon add go.mod go.sum internal/opaharness
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon commit -m "feat(opaharness): compile Rego v1 policies into prepared mutation queries

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Rule mutator — idempotent mutations through the rule repository

**Files:**
- Create: `internal/opaharness/mutator.go`
- Test: `internal/opaharness/mutator_test.go`

**Interfaces:**
- Consumes: `Mutation`, `MutationResult`, ops/outcomes (Task 5); `model.Rule.Validate` (`internal/model/rule.go:102-130`), `model.FaultSpec.Validate` (`fault.go:96-113`, catalog-strict params); `store.ErrNotFound`; `id.New`.
- Produces:
  - `type RuleStore interface { List(ctx) ([]*model.Rule, error); Create(ctx, *model.Rule) error; Update(ctx, *model.Rule) error; Delete(ctx, id string) error }` — satisfied by `*store.RuleRepo` (`rule_repo.go:25, 82, 116, 130`).
  - `type SpecStore interface { GetSpec(ctx, id string) (*model.FaultSpec, error); UpdateSpec(ctx, *model.FaultSpec) error }` — satisfied by `*store.FaultRepo` (`fault_repo.go:62, 89`).
  - `type Mutator interface { Apply(ctx context.Context, policyID string, ms []Mutation) ([]MutationResult, error) }` — the returned error is infrastructure only (listing rules failed); per-mutation store failures are `OutcomeFailed` results. (The brief's `Apply(ctx, policyID, []Mutation) error` gains the result slice: the harness needs per-mutation outcomes for the decision log and the cooldown.)
  - `type RuleMutator struct` with unexported `rules RuleStore`, `specs SpecStore`, `newID func(string) string` (default `id.New`), `now func() time.Time` (default `time.Now`); `func NewRuleMutator(rules RuleStore, specs SpecStore) *RuleMutator`.

- [ ] **Step 1: Write the failing tests**

`internal/opaharness/mutator_test.go`:

```go
package opaharness

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

type fakeRules struct {
	rules                     []*model.Rule
	creates, updates, deletes int
}

func (f *fakeRules) List(_ context.Context) ([]*model.Rule, error) {
	out := make([]*model.Rule, 0, len(f.rules))
	for _, r := range f.rules {
		cp := *r
		out = append(out, &cp)
	}
	return out, nil
}

func (f *fakeRules) Create(_ context.Context, r *model.Rule) error {
	if err := r.Validate(); err != nil {
		return err
	}
	cp := *r
	f.rules = append(f.rules, &cp)
	f.creates++
	return nil
}

func (f *fakeRules) Update(_ context.Context, r *model.Rule) error {
	if err := r.Validate(); err != nil {
		return err
	}
	for i, existing := range f.rules {
		if existing.ID == r.ID {
			cp := *r
			f.rules[i] = &cp
			f.updates++
			return nil
		}
	}
	return store.ErrNotFound
}

func (f *fakeRules) Delete(_ context.Context, id string) error {
	for i, existing := range f.rules {
		if existing.ID == id {
			f.rules = append(f.rules[:i], f.rules[i+1:]...)
			f.deletes++
			return nil
		}
	}
	return store.ErrNotFound
}

func (f *fakeRules) get(service, name string) *model.Rule {
	for _, r := range f.rules {
		if r.Service == service && r.Name == name {
			return r
		}
	}
	return nil
}

type fakeSpecs struct {
	specs   map[string]*model.FaultSpec
	updates int
}

func (f *fakeSpecs) GetSpec(_ context.Context, id string) (*model.FaultSpec, error) {
	if s, ok := f.specs[id]; ok {
		cp := *s
		return &cp, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeSpecs) UpdateSpec(_ context.Context, s *model.FaultSpec) error {
	if err := s.Validate(); err != nil {
		return err
	}
	cp := *s
	f.specs[s.ID] = &cp
	f.updates++
	return nil
}

var fixedTime = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

func newTestMutator(rules *fakeRules, specs *fakeSpecs) *RuleMutator {
	m := NewRuleMutator(rules, specs)
	m.newID = func(prefix string) string { return prefix + "-fixed" }
	m.now = func() time.Time { return fixedTime }
	return m
}

func desiredRule() *model.Rule {
	return &model.Rule{
		Name: "checkout-egress-latency", Service: "checkout", Enabled: true, Priority: 100, Mode: "inline",
		Match:  model.MatchCriteria{InjectionPoint: "egress", Labels: map[string]string{"atropos.http.path": "/cart/checkout"}},
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: "spec-lat"},
	}
}

func latencySpec(id string) *model.FaultSpec {
	return &model.FaultSpec{ID: id, Name: id, Category: "inline", FaultType: "latency", Host: "process", Params: json.RawMessage(`{"delay":"200ms"}`)}
}

func apply(t *testing.T, m *RuleMutator, ms ...Mutation) []MutationResult {
	t.Helper()
	res, err := m.Apply(context.Background(), "opa-1", ms)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(res) != len(ms) {
		t.Fatalf("results = %d, want one per mutation (%d)", len(res), len(ms))
	}
	return res
}

func TestRuleMutator_UpsertCreatesThenUnchangedThenUpdates(t *testing.T) {
	rules := &fakeRules{}
	m := newTestMutator(rules, &fakeSpecs{})

	res := apply(t, m, Mutation{Op: OpUpsertRule, Rule: desiredRule()})
	if res[0].Outcome != OutcomeCreated || res[0].RuleID != "rule-fixed" || rules.creates != 1 {
		t.Fatalf("first upsert = %+v (creates %d), want created rule-fixed", res[0], rules.creates)
	}
	stored := rules.get("checkout", "checkout-egress-latency")
	if stored == nil || stored.StartPolicy != "deduplicate_by_rule" || !stored.CreatedAt.Equal(fixedTime) {
		t.Fatalf("stored rule = %+v, want defaults applied and timestamps set", stored)
	}

	res = apply(t, m, Mutation{Op: OpUpsertRule, Rule: desiredRule()})
	if res[0].Outcome != OutcomeUnchanged || rules.updates != 0 || res[0].RuleID != "rule-fixed" {
		t.Fatalf("second upsert = %+v (updates %d), want unchanged, no write", res[0], rules.updates)
	}

	changed := desiredRule()
	changed.Priority = 200
	changed.ID = "rule-from-policy" // server-owned: must be ignored
	res = apply(t, m, Mutation{Op: OpUpsertRule, Rule: changed})
	stored = rules.get("checkout", "checkout-egress-latency")
	if res[0].Outcome != OutcomeUpdated || rules.updates != 1 || stored.Priority != 200 || stored.ID != "rule-fixed" ||
		!stored.CreatedAt.Equal(fixedTime) || len(rules.rules) != 1 {
		t.Fatalf("third upsert = %+v, stored %+v, want updated in place keeping id and created_at", res[0], stored)
	}
}

func TestRuleMutator_UpsertInvalidRuleFails(t *testing.T) {
	rules := &fakeRules{}
	m := newTestMutator(rules, &fakeSpecs{})
	bad := desiredRule()
	bad.Mode = "sideways"
	res := apply(t, m, Mutation{Op: OpUpsertRule, Rule: bad})
	if res[0].Outcome != OutcomeFailed || !strings.Contains(res[0].Error, "invalid mode") || rules.creates != 0 {
		t.Fatalf("result = %+v (creates %d), want failed with the validation error and no write", res[0], rules.creates)
	}
}

func TestRuleMutator_SetEnabled(t *testing.T) {
	existing := desiredRule()
	existing.ID = "rule-1"
	rules := &fakeRules{rules: []*model.Rule{existing}}
	m := newTestMutator(rules, &fakeSpecs{})
	off, on := false, true

	res := apply(t, m, Mutation{Op: OpSetEnabled, Service: "checkout", Name: existing.Name, Enabled: &off})
	if res[0].Outcome != OutcomeUpdated || rules.get("checkout", existing.Name).Enabled || rules.updates != 1 {
		t.Fatalf("disable = %+v, want updated and persisted disabled", res[0])
	}
	res = apply(t, m, Mutation{Op: OpSetEnabled, Service: "checkout", Name: existing.Name, Enabled: &off})
	if res[0].Outcome != OutcomeUnchanged || rules.updates != 1 {
		t.Fatalf("disable again = %+v, want unchanged", res[0])
	}
	res = apply(t, m, Mutation{Op: OpSetEnabled, Service: "checkout", Name: "missing", Enabled: &on})
	if res[0].Outcome != OutcomeFailed || !strings.Contains(res[0].Error, "not found") {
		t.Fatalf("unknown rule = %+v, want failed not found", res[0])
	}
}

func TestRuleMutator_DeleteRule(t *testing.T) {
	existing := desiredRule()
	existing.ID = "rule-1"
	rules := &fakeRules{rules: []*model.Rule{existing}}
	m := newTestMutator(rules, &fakeSpecs{})

	res := apply(t, m, Mutation{Op: OpDeleteRule, Service: "checkout", Name: existing.Name})
	if res[0].Outcome != OutcomeDeleted || res[0].RuleID != "rule-1" || rules.deletes != 1 || len(rules.rules) != 0 {
		t.Fatalf("delete = %+v, want deleted", res[0])
	}
	res = apply(t, m, Mutation{Op: OpDeleteRule, Service: "checkout", Name: existing.Name})
	if res[0].Outcome != OutcomeUnchanged || rules.deletes != 1 {
		t.Fatalf("delete again = %+v, want unchanged (idempotent)", res[0])
	}
}

func TestRuleMutator_SetParams(t *testing.T) {
	existing := desiredRule()
	existing.ID = "rule-1"
	rules := &fakeRules{rules: []*model.Rule{existing}}
	specs := &fakeSpecs{specs: map[string]*model.FaultSpec{"spec-lat": latencySpec("spec-lat")}}
	m := newTestMutator(rules, specs)
	key := Mutation{Op: OpSetParams, Service: "checkout", Name: existing.Name}

	mut := key
	mut.Params = json.RawMessage(`{"delay":"500ms"}`)
	res := apply(t, m, mut)
	if res[0].Outcome != OutcomeUpdated || specs.updates != 1 || rules.updates != 1 ||
		string(specs.specs["spec-lat"].Params) != `{"delay":"500ms"}` {
		t.Fatalf("set_params = %+v (spec updates %d, rule updates %d), want spec updated and the rule re-saved", res[0], specs.updates, rules.updates)
	}
	mut.Params = json.RawMessage(`{"delay": "500ms"}`) // same value, different whitespace
	if res = apply(t, m, mut); res[0].Outcome != OutcomeUnchanged || specs.updates != 1 || rules.updates != 1 {
		t.Fatalf("same params = %+v, want unchanged and no writes", res[0])
	}

	mut.Params = json.RawMessage(`{"delay":"500ms","bogus":1}`)
	if res = apply(t, m, mut); res[0].Outcome != OutcomeFailed || res[0].Error == "" || specs.updates != 1 {
		t.Fatalf("invalid params = %+v, want failed by the fault catalog and no write", res[0])
	}

	other := desiredRule()
	other.ID, other.Name, other.Service = "rule-2", "other", "frontend"
	rules.rules = append(rules.rules, other) // shares spec-lat
	mut.Params = json.RawMessage(`{"delay":"900ms"}`)
	if res = apply(t, m, mut); res[0].Outcome != OutcomeRefused || !strings.Contains(res[0].Error, "shared") || specs.updates != 1 {
		t.Fatalf("shared spec = %+v, want refused", res[0])
	}

	cb := &model.Rule{ID: "rule-cb", Name: "cb", Service: "checkout", Mode: "inline",
		Action: model.RuleAction{Type: "cachebox", CacheBox: &model.CacheBoxRuleConfig{Mode: "replay", KeyStrategy: "exact"}}}
	rules.rules = append(rules.rules, cb)
	if res = apply(t, m, Mutation{Op: OpSetParams, Service: "checkout", Name: "cb", Params: json.RawMessage(`{}`)}); res[0].Outcome != OutcomeFailed || !strings.Contains(res[0].Error, "fault_spec") {
		t.Fatalf("set_params on a cachebox rule = %+v, want failed", res[0])
	}
}

func TestRuleMutator_AppliesInOrderAndContinuesAfterFailure(t *testing.T) {
	rules := &fakeRules{}
	m := newTestMutator(rules, &fakeSpecs{})
	off := false
	res := apply(t, m,
		Mutation{Op: OpSetEnabled, Service: "checkout", Name: "absent", Enabled: &off}, // fails
		Mutation{Op: OpUpsertRule, Rule: desiredRule()},                                 // creates
		Mutation{Op: OpSetEnabled, Service: "checkout", Name: "checkout-egress-latency", Enabled: &off}, // sees the create
	)
	if res[0].Outcome != OutcomeFailed || res[1].Outcome != OutcomeCreated || res[2].Outcome != OutcomeUpdated {
		t.Fatalf("outcomes = %s %s %s, want failed created updated", res[0].Outcome, res[1].Outcome, res[2].Outcome)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/opaharness/ -run TestRuleMutator`
Expected: `[build failed]` with `undefined: NewRuleMutator`.

- [ ] **Step 3: Implement `mutator.go`**

`Apply` loops over `ms` in order; before each mutation it reloads `rules.List(ctx)` (an error here aborts with `list rules: %w`) and finds the rule by `(service, name)` from `m.Key()`. Each op produces one `MutationResult{Op, Service, Name}`:

- `upsert_rule`: `want := *m.Rule` with `Service`/`Name` from the key; `want.Validate()` failure → `failed` with the error text, no write. Absent → `want.ID = m.newID("rule")`, `CreatedAt = UpdatedAt = m.now()`, `rules.Create` → `created` (`RuleID`). Present → `want.ID = existing.ID`, `want.CreatedAt = existing.CreatedAt`; `sameRule(existing, &want)` → `unchanged`; else `want.UpdatedAt = m.now()`, `rules.Update` → `updated`. `sameRule` compares `Enabled`, `Priority`, `Mode`, `StartPolicy`, `Match.InjectionPoint`, `maps.Equal(Match.Labels)`, `Action.Type`, `Action.FaultSpecID`, `Action.FaultCompID` and the dereferenced `Action.CacheBox` (both nil, or both set and equal).
- `set_enabled`: absent → `failed` `rule %s/%s not found`; `existing.Enabled == *m.Enabled` → `unchanged`; else copy, flip, `UpdatedAt = m.now()`, `rules.Update` → `updated`.
- `delete_rule`: absent → `unchanged`; else `rules.Delete(existing.ID)` → `deleted`.
- `set_params`: absent → `failed` not found; `existing.Action.Type != "fault_spec"` → `failed` `set_params requires a fault_spec rule`; `specs.GetSpec` error → `failed`; another rule in the list with the same `FaultSpecID` → `refused` `fault spec %s is shared by %d rules; give the rule a dedicated spec`; `jsonEqual(spec.Params, m.Params)` (compare after `json.Compact` into fresh buffers) → `unchanged`; else `spec.Params = m.Params`, `specs.UpdateSpec` (error → `failed`, it runs the catalog validation), then `touch := *existing; touch.UpdatedAt = m.now(); rules.Update(&touch)` (error → `failed`) → `updated`. The re-save is what bumps `rule_version` (and the generation, once the desired-state plan lands) so pollers recompile the spec.

Any `rules.Create/Update/Delete` error becomes `OutcomeFailed` with the error text; the loop continues.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/opaharness/ -run TestRuleMutator -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: `--- PASS` for `TestRuleMutator_UpsertCreatesThenUnchangedThenUpdates`, `TestRuleMutator_UpsertInvalidRuleFails`, `TestRuleMutator_SetEnabled`, `TestRuleMutator_DeleteRule`, `TestRuleMutator_SetParams`, `TestRuleMutator_AppliesInOrderAndContinuesAfterFailure`; `ok`.

- [ ] **Step 5: Commit**

```bash
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon add internal/opaharness/mutator.go internal/opaharness/mutator_test.go
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon commit -m "feat(opaharness): idempotent rule mutator over the rule repository

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Harness loop — inputs, evaluation, cooldown, decision log

**Files:**
- Create: `internal/opaharness/inputs.go`, `internal/opaharness/harness.go`
- Test: `internal/opaharness/inputs_test.go`, `internal/opaharness/harness_test.go`

**Interfaces:**
- Consumes: `Compile`/`Prepared`/`SourceHash`, `enforceScope`, `Mutator` (Tasks 5-6); `promql.Sample` (Task 4); `model.OPAPolicy`, `model.PolicyDecision`, `model.Rule`, `model.SDKInstance`; `id.New`.
- Produces:
  - `type InputProvider interface { Fetch(ctx context.Context, in model.PolicyInput) (any, error) }`; `type Providers map[string]InputProvider` (keyed by `PolicyInput.Source`).
  - `type VectorQuerier interface { QueryVector(ctx context.Context, query string) ([]promql.Sample, error) }` (satisfied by `*promql.Client` and `*logql.Client`); `type VectorProvider struct { Querier VectorQuerier }` — `Kind == "scalar"`: 0 samples → `nil` (JSON `null`), 1 → `float64`, n > 1 → `fmt.Errorf("scalar input %q returned %d series; aggregate the query or use kind \"vector\"", in.Name, n)`; `Kind == "vector"`: `[]promql.Sample` (empty, non-nil).
  - `type PolicySource interface { ListEnabled(ctx) ([]*model.OPAPolicy, error) }` (`*store.OPAPolicyRepo`); `type DecisionLog interface { Record(ctx, *model.PolicyDecision) error; LastChangeAt(ctx, policyID string) (time.Time, bool, error) }` (`*store.PolicyDecisionRepo`); `type RuleLister interface { List(ctx) ([]*model.Rule, error) }` (`*store.RuleRepo`); `type InstanceLister interface { ForService(ctx, service string) ([]*model.SDKInstance, error) }` (`*store.SDKRepo`).
  - `type Deps struct { Policies PolicySource; Decisions DecisionLog; Rules RuleLister; Instances InstanceLister; Providers Providers; Mutator Mutator; Logger *slog.Logger; Now func() time.Time; NewID func(prefix string) string }` (`Now`/`NewID`/`Logger` nil → `time.Now`, `id.New`, `slog.Default()`); `func New(d Deps) *Harness`.
  - `type Evaluation struct { PolicyID string "json:\"policy_id\""; EvaluatedAt time.Time "json:\"evaluated_at\""; Inputs map[string]any "json:\"inputs\""; InputDigest string "json:\"input_digest\""; Mutations []Mutation "json:\"mutations\""; Results []MutationResult "json:\"results\""; Applied bool "json:\"applied\""; Changed bool "json:\"changed\""; Error string "json:\"error,omitempty\"" }`.
  - `func (h *Harness) Run(ctx context.Context)` (1 s resolution ticker, `tick(ctx, h.now())`); `func (h *Harness) tick(ctx context.Context, now time.Time)`; `func (h *Harness) EvaluateOnce(ctx context.Context, p *model.OPAPolicy, apply bool) (*Evaluation, error)` — the error is only a decision-log write failure (`record decision: %w`); compile, input, eval and apply problems are reported in `Evaluation.Error` and recorded.
  - Rego `input` document: `{"now": RFC3339Nano UTC, "now_ns": int64, "inputs": {name: value}, "rules": [model.Rule JSON for rules whose service is in scope, priority order as List returns them], "services": scope.services, "instances": [model.SDKInstance JSON for each scope service]}`, built by marshalling to JSON and decoding with `json.Decoder.UseNumber()` so `now_ns` keeps its precision.

- [ ] **Step 1: Write the failing provider tests**

`internal/opaharness/inputs_test.go`:

```go
package opaharness

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"manteion-go/internal/model"
	"manteion-go/internal/promql"
)

type fakeQuerier struct {
	samples []promql.Sample
	err     error
}

func (f fakeQuerier) QueryVector(_ context.Context, _ string) ([]promql.Sample, error) {
	return f.samples, f.err
}

func TestVectorProvider_Fetch(t *testing.T) {
	one := []promql.Sample{{Labels: map[string]string{"service": "checkout"}, Value: 1.5}}
	two := append(append([]promql.Sample{}, one...), promql.Sample{Labels: map[string]string{"service": "cart"}, Value: 2})
	tests := []struct {
		name    string
		samples []promql.Sample
		kind    string
		want    any
		wantErr string
	}{
		{"scalar one series", one, "scalar", 1.5, ""},
		{"scalar absent series is null", []promql.Sample{}, "scalar", nil, ""},
		{"scalar rejects several series", two, "scalar", nil, "2 series"},
		{"vector returns every sample", two, "vector", two, ""},
		{"vector empty is an empty array", []promql.Sample{}, "vector", []promql.Sample{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := VectorProvider{Querier: fakeQuerier{samples: tt.samples}}.Fetch(context.Background(), model.PolicyInput{Name: "x", Query: "q", Kind: tt.kind})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Fetch = %#v, %v; want %#v", got, err, tt.want)
			}
		})
	}
}

func TestVectorProvider_FetchError(t *testing.T) {
	boom := errors.New("prometheus query failed: dial tcp")
	if _, err := (VectorProvider{Querier: fakeQuerier{err: boom}}).Fetch(context.Background(), model.PolicyInput{Name: "x", Kind: "scalar"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap the querier error", err)
	}
}
```

- [ ] **Step 2: Write the failing harness tests**

`internal/opaharness/harness_test.go`:

```go
package opaharness

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/model"
)

// --- fakes -------------------------------------------------------------------

type fakePolicies struct{ policies []*model.OPAPolicy }

func (f *fakePolicies) ListEnabled(_ context.Context) ([]*model.OPAPolicy, error) {
	var out []*model.OPAPolicy
	for _, p := range f.policies {
		if p.Enabled {
			out = append(out, p)
		}
	}
	return out, nil
}

type fakeDecisions struct {
	records    []*model.PolicyDecision
	lastChange time.Time
	hasLast    bool
	recordErr  error
}

func (f *fakeDecisions) Record(_ context.Context, d *model.PolicyDecision) error {
	if f.recordErr != nil {
		return f.recordErr
	}
	cp := *d
	f.records = append(f.records, &cp)
	return nil
}

func (f *fakeDecisions) LastChangeAt(_ context.Context, _ string) (time.Time, bool, error) {
	return f.lastChange, f.hasLast, nil
}

type fakeRuleLister struct{ rules []*model.Rule }

func (f *fakeRuleLister) List(_ context.Context) ([]*model.Rule, error) { return f.rules, nil }

type fakeInstances struct{ byService map[string][]*model.SDKInstance }

func (f *fakeInstances) ForService(_ context.Context, service string) ([]*model.SDKInstance, error) {
	return f.byService[service], nil
}

// fakeProvider answers every input by name; err fails every fetch.
type fakeProvider struct {
	values map[string]any
	err    error
}

func (f fakeProvider) Fetch(_ context.Context, in model.PolicyInput) (any, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.values[in.Name], nil
}

type fakeMutator struct {
	calls   [][]Mutation
	outcome string // reported for every mutation; "" → created
	err     error
}

func (f *fakeMutator) Apply(_ context.Context, _ string, ms []Mutation) ([]MutationResult, error) {
	f.calls = append(f.calls, ms)
	if f.err != nil {
		return nil, f.err
	}
	out := make([]MutationResult, 0, len(ms))
	for _, m := range ms {
		svc, name := m.Key()
		o := f.outcome
		if o == "" {
			o = OutcomeCreated
		}
		out = append(out, MutationResult{Op: m.Op, Service: svc, Name: name, Outcome: o})
	}
	return out, nil
}

// --- fixtures ----------------------------------------------------------------

var t0 = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

// hotRego is the documented example policy (docs/opa-harness.md).
const hotRego = `package atropos

rule_name := "checkout-egress-latency"

hot if {
	input.inputs.decisions > 50
	input.inputs.p99 > 800
}

desired_rule := {
	"name": rule_name,
	"service": "checkout",
	"enabled": true,
	"priority": 100,
	"mode": "inline",
	"match": {"injection_point": "egress", "labels": {"atropos.http.path": "/cart/checkout"}},
	"action": {"type": "fault_spec", "fault_spec_id": "spec-egress-latency-200ms"},
}

mutations contains {"op": "upsert_rule", "rule": desired_rule} if hot

mutations contains {"op": "set_enabled", "service": "checkout", "name": rule_name, "enabled": false} if {
	not hot
	some r in input.rules
	r.service == "checkout"
	r.name == rule_name
	r.enabled
}
`

const outOfScopeRego = `package atropos

mutations contains {"op": "delete_rule", "service": "frontend", "name": "x"}

mutations contains {"op": "delete_rule", "service": "checkout", "name": "y"}
`

// shapeRego proves every field of the input document is populated: it emits
// one delete per rule it sees, and only if instances, services and now are there.
const shapeRego = `package atropos

mutations contains {"op": "delete_rule", "service": "checkout", "name": r.name} if {
	some r in input.rules
	some i in input.instances
	i.status == "alive"
	input.services == ["checkout"]
	input.now_ns > 0
	input.now != ""
}
`

const emptyRego = "package atropos\n\nmutations contains m if { false; m := {} }\n"

func hotPolicy() *model.OPAPolicy {
	return &model.OPAPolicy{
		ID: "opa-hot", Name: "checkout-hot", Enabled: true, Rego: hotRego, IntervalS: 10, CooldownS: 60,
		Inputs: []model.PolicyInput{
			{Name: "decisions", Source: "loki", Query: `sum(count_over_time({service_name="checkout"} |= "atropos.decision" [1m]))`, Kind: "scalar"},
			{Name: "p99", Source: "prometheus", Query: `histogram_quantile(0.99, sum by (le) (rate(duration_milliseconds_bucket{service_name="checkout", span_name="POST /cart/checkout"}[1m])))`, Kind: "scalar"},
		},
		Scope: model.PolicyScope{Services: []string{"checkout"}}, CreatedAt: t0, UpdatedAt: t0,
	}
}

func withRego(p *model.OPAPolicy, rego string) *model.OPAPolicy { p.Rego = rego; return p }

func hotValues() map[string]any  { return map[string]any{"decisions": 120.0, "p99": 1300.0} }
func coldValues() map[string]any { return map[string]any{"decisions": 3.0, "p99": 200.0} }

type testHarness struct {
	h   *Harness
	dec *fakeDecisions
	mut *fakeMutator
	now time.Time
}

func newTestHarness(policies []*model.OPAPolicy, prov InputProvider, mut *fakeMutator, dec *fakeDecisions, rules []*model.Rule) *testHarness {
	th := &testHarness{dec: dec, mut: mut, now: t0}
	th.h = New(Deps{
		Policies:  &fakePolicies{policies: policies},
		Decisions: dec,
		Rules:     &fakeRuleLister{rules: rules},
		Instances: &fakeInstances{byService: map[string][]*model.SDKInstance{"checkout": {{ID: "checkout-1", Service: "checkout", Status: "alive"}}}},
		Providers: Providers{"prometheus": prov, "loki": prov},
		Mutator:   mut,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:       func() time.Time { return th.now },
		NewID:     func(prefix string) string { return prefix + "-fixed" },
	})
	return th
}

func (th *testHarness) tickAt(d time.Duration) { th.now = t0.Add(d); th.h.tick(context.Background(), th.now) }

// --- EvaluateOnce ------------------------------------------------------------

func TestEvaluateOnce_AppliesAndRecords(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	ev, err := th.h.EvaluateOnce(context.Background(), hotPolicy(), true)
	if err != nil {
		t.Fatalf("EvaluateOnce: %v", err)
	}
	if len(th.mut.calls) != 1 || len(th.mut.calls[0]) != 1 {
		t.Fatalf("mutator calls = %+v, want one call with one mutation", th.mut.calls)
	}
	m := th.mut.calls[0][0]
	if svc, name := m.Key(); m.Op != OpUpsertRule || svc != "checkout" || name != "checkout-egress-latency" || m.Rule == nil ||
		m.Rule.Match.Labels["atropos.http.path"] != "/cart/checkout" {
		t.Errorf("mutation = %+v, want the egress latency upsert", m)
	}
	if !ev.Applied || !ev.Changed || ev.Error != "" || len(ev.Results) != 1 || ev.Results[0].Outcome != OutcomeCreated {
		t.Errorf("evaluation = %+v, want applied+changed with one created result", ev)
	}
	if len(ev.InputDigest) != 64 || ev.Inputs["p99"] != 1300.0 || !ev.EvaluatedAt.Equal(t0) {
		t.Errorf("digest %q inputs %v at %v", ev.InputDigest, ev.Inputs, ev.EvaluatedAt)
	}
	if len(th.dec.records) != 1 {
		t.Fatalf("decisions recorded = %d, want 1", len(th.dec.records))
	}
	d := th.dec.records[0]
	var results []MutationResult
	if err := json.Unmarshal(d.Mutations, &results); err != nil || len(results) != 1 || results[0].Outcome != OutcomeCreated {
		t.Errorf("decision mutations = %s (%v), want one created result", d.Mutations, err)
	}
	if d.PolicyID != "opa-hot" || !d.Applied || d.Error != "" || !d.EvaluatedAt.Equal(t0) || d.InputDigest != ev.InputDigest || !strings.HasPrefix(d.ID, "pdec-") {
		t.Errorf("decision row = %+v", d)
	}
}

func TestEvaluateOnce_DryRunDoesNotApply(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	ev, err := th.h.EvaluateOnce(context.Background(), hotPolicy(), false)
	if err != nil {
		t.Fatalf("EvaluateOnce: %v", err)
	}
	if len(th.mut.calls) != 0 || ev.Applied || ev.Changed || len(ev.Mutations) != 1 || len(ev.Results) != 0 {
		t.Errorf("dry run: calls %d, evaluation %+v; want the proposed mutation, nothing applied", len(th.mut.calls), ev)
	}
	if len(th.dec.records) != 1 || th.dec.records[0].Applied {
		t.Errorf("dry run must still be recorded, with applied=false: %+v", th.dec.records)
	}
}

func TestEvaluateOnce_ColdDisablesExistingRule(t *testing.T) {
	existing := &model.Rule{ID: "rule-1", Name: "checkout-egress-latency", Service: "checkout", Enabled: true, Mode: "inline",
		Action: model.RuleAction{Type: "fault_spec", FaultSpecID: "spec-egress-latency-200ms"}}
	th := newTestHarness(nil, fakeProvider{values: coldValues()}, &fakeMutator{outcome: OutcomeUpdated}, &fakeDecisions{}, []*model.Rule{existing})
	ev, _ := th.h.EvaluateOnce(context.Background(), hotPolicy(), true)
	if len(ev.Mutations) != 1 || ev.Mutations[0].Op != OpSetEnabled || ev.Mutations[0].Enabled == nil || *ev.Mutations[0].Enabled {
		t.Fatalf("mutations = %+v, want set_enabled false", ev.Mutations)
	}
}

func TestEvaluateOnce_InputErrorSkipsEval(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{err: errors.New("loki returned status 502")}, &fakeMutator{}, &fakeDecisions{}, nil)
	ev, err := th.h.EvaluateOnce(context.Background(), hotPolicy(), true)
	if err != nil {
		t.Fatalf("EvaluateOnce: %v", err)
	}
	if !strings.Contains(ev.Error, "input decisions") || !strings.Contains(ev.Error, "502") || len(th.mut.calls) != 0 || ev.Applied {
		t.Errorf("evaluation = %+v, calls %d; want an input error and no apply", ev, len(th.mut.calls))
	}
	if len(th.dec.records) != 1 || th.dec.records[0].Error == "" {
		t.Errorf("errored evaluation must be recorded with its error: %+v", th.dec.records)
	}
}

func TestEvaluateOnce_RefusesOutOfScope(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	ev, _ := th.h.EvaluateOnce(context.Background(), withRego(hotPolicy(), outOfScopeRego), true)
	if len(th.mut.calls) != 1 || len(th.mut.calls[0]) != 1 || th.mut.calls[0][0].Name != "y" {
		t.Fatalf("mutator received %+v, want only the checkout mutation", th.mut.calls)
	}
	var refused, created int
	for _, r := range ev.Results {
		switch r.Outcome {
		case OutcomeRefused:
			refused++
			if r.Service != "frontend" || !strings.Contains(r.Error, "outside policy scope") {
				t.Errorf("refusal = %+v", r)
			}
		case OutcomeCreated:
			created++
		}
	}
	if refused != 1 || created != 1 || len(ev.Mutations) != 2 {
		t.Errorf("results = %+v (mutations %d)", ev.Results, len(ev.Mutations))
	}
}

func TestEvaluateOnce_InputDocumentShape(t *testing.T) {
	rules := []*model.Rule{
		{ID: "rule-1", Name: "old-rule", Service: "checkout", Enabled: false, Mode: "inline", Action: model.RuleAction{Type: "fault_spec", FaultSpecID: "s"}},
		{ID: "rule-2", Name: "fe-rule", Service: "frontend", Enabled: true, Mode: "inline", Action: model.RuleAction{Type: "fault_spec", FaultSpecID: "s"}},
	}
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{outcome: OutcomeDeleted}, &fakeDecisions{}, rules)
	ev, _ := th.h.EvaluateOnce(context.Background(), withRego(hotPolicy(), shapeRego), true)
	if ev.Error != "" || len(ev.Mutations) != 1 || ev.Mutations[0].Name != "old-rule" {
		t.Fatalf("evaluation = %+v; want exactly one delete for the in-scope rule (frontend's must not be in input.rules)", ev)
	}
}

func TestEvaluateOnce_CompileErrorIsRecorded(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	ev, err := th.h.EvaluateOnce(context.Background(), withRego(hotPolicy(), "package atropos\nmutations contains x if { x := }"), true)
	if err != nil || !strings.Contains(ev.Error, "compile") || len(th.mut.calls) != 0 || len(th.dec.records) != 1 {
		t.Fatalf("evaluation = %+v, err %v, calls %d, records %d", ev, err, len(th.mut.calls), len(th.dec.records))
	}
}

func TestEvaluateOnce_RecordFailureIsReturned(t *testing.T) {
	th := newTestHarness(nil, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{recordErr: errors.New("db down")}, nil)
	if _, err := th.h.EvaluateOnce(context.Background(), hotPolicy(), true); err == nil || !strings.Contains(err.Error(), "record decision") {
		t.Fatalf("err = %v, want the decision-log failure", err)
	}
}

// --- tick scheduling ---------------------------------------------------------

func TestTick_CooldownAfterChange(t *testing.T) {
	th := newTestHarness([]*model.OPAPolicy{hotPolicy()}, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	for _, step := range []struct {
		at        time.Duration
		wantEvals int
	}{
		{0, 1},                 // due immediately; the applied set changed → cooldown 60s
		{10 * time.Second, 1},  // interval elapsed, cooldown has not
		{59 * time.Second, 1},
		{60 * time.Second, 2},  // cooldown elapsed; changes again → cooldown to 120s
		{119 * time.Second, 2},
		{120 * time.Second, 3},
	} {
		th.tickAt(step.at)
		if got := len(th.mut.calls); got != step.wantEvals {
			t.Fatalf("at +%s: evaluations = %d, want %d", step.at, got, step.wantEvals)
		}
	}
}

func TestTick_NoCooldownWhenUnchanged(t *testing.T) {
	th := newTestHarness([]*model.OPAPolicy{hotPolicy()}, fakeProvider{values: hotValues()}, &fakeMutator{outcome: OutcomeUnchanged}, &fakeDecisions{}, nil)
	for _, step := range []struct {
		at        time.Duration
		wantEvals int
	}{{0, 1}, {9 * time.Second, 1}, {10 * time.Second, 2}, {20 * time.Second, 3}} {
		th.tickAt(step.at)
		if got := len(th.mut.calls); got != step.wantEvals {
			t.Fatalf("at +%s: evaluations = %d, want %d", step.at, got, step.wantEvals)
		}
	}
}

func TestTick_CooldownRestoredFromDecisionLog(t *testing.T) {
	dec := &fakeDecisions{hasLast: true, lastChange: t0.Add(-30 * time.Second)}
	th := newTestHarness([]*model.OPAPolicy{hotPolicy()}, fakeProvider{values: hotValues()}, &fakeMutator{}, dec, nil)
	for _, step := range []struct {
		at        time.Duration
		wantEvals int
	}{{0, 0}, {29 * time.Second, 0}, {30 * time.Second, 1}} {
		th.tickAt(step.at)
		if got := len(th.mut.calls); got != step.wantEvals {
			t.Fatalf("at +%s: evaluations = %d, want %d (cooldown from the last recorded change)", step.at, got, step.wantEvals)
		}
	}
}

func TestTick_CompileErrorIsRecordedAndOthersRun(t *testing.T) {
	broken := withRego(hotPolicy(), "package atropos\nmutations contains x if { x := }")
	broken.ID, broken.Name = "opa-broken", "broken"
	th := newTestHarness([]*model.OPAPolicy{broken, hotPolicy()}, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	th.tickAt(0)
	if len(th.mut.calls) != 1 || len(th.dec.records) != 2 {
		t.Fatalf("calls %d records %d, want the healthy policy applied and both evaluations recorded", len(th.mut.calls), len(th.dec.records))
	}
	var errored int
	for _, d := range th.dec.records {
		if d.PolicyID == "opa-broken" && strings.Contains(d.Error, "compile") {
			errored++
		}
	}
	if errored != 1 {
		t.Errorf("records = %+v, want the broken policy's error recorded", th.dec.records)
	}
}

func TestTick_RecompilesOnRegoChange(t *testing.T) {
	p := hotPolicy()
	th := newTestHarness([]*model.OPAPolicy{p}, fakeProvider{values: hotValues()}, &fakeMutator{outcome: OutcomeUnchanged}, &fakeDecisions{}, nil)
	th.tickAt(0)
	p.Rego = emptyRego
	th.tickAt(10 * time.Second)
	if len(th.mut.calls) != 2 || len(th.mut.calls[0]) != 1 || len(th.mut.calls[1]) != 0 {
		t.Fatalf("calls = %+v, want the edited policy evaluated with its new source", th.mut.calls)
	}
}

func TestTick_DisabledPolicyIsSkipped(t *testing.T) {
	p := hotPolicy()
	p.Enabled = false
	th := newTestHarness([]*model.OPAPolicy{p}, fakeProvider{values: hotValues()}, &fakeMutator{}, &fakeDecisions{}, nil)
	th.tickAt(0)
	if len(th.mut.calls) != 0 || len(th.dec.records) != 0 {
		t.Fatalf("disabled policy was evaluated: calls %d records %d", len(th.mut.calls), len(th.dec.records))
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/opaharness/`
Expected: `[build failed]` with `undefined: VectorProvider`, `undefined: New`, `undefined: Deps`.

- [ ] **Step 4: Implement `inputs.go` and `harness.go`**

`Harness` fields: `deps Deps`, `resolution time.Duration` (1 s), `mu sync.Mutex`, `prepared map[string]*Prepared` (policy ID → compiled; re-compiled when `Hash() != SourceHash(p.Rego)`), `nextAt map[string]time.Time`.

`tick(ctx, now)`:
```
policies, err := Policies.ListEnabled(ctx)   // error: log, return
seen := map[string]bool{}
for p in policies:
    seen[p.ID] = true
    next, known := nextAt[p.ID]
    if !known:
        last, found, err := Decisions.LastChangeAt(ctx, p.ID)   // error: log and treat as not found
        if found && p.CooldownS > 0 && now.Before(last.Add(cooldown)):
            nextAt[p.ID] = last.Add(cooldown); continue
    else if now.Before(next): continue
    ev, err := EvaluateOnce(ctx, p, true)      // err: log; ev may be nil → nextAt = now + interval; continue
    wait := interval
    if ev.Changed && cooldown > wait: wait = cooldown
    nextAt[p.ID] = now.Add(wait)
delete nextAt/prepared entries whose ID is not in seen
```
(`interval = time.Duration(p.IntervalS) * time.Second`, `cooldown` likewise.) `Run` logs `opa harness: starting` once, then loops on a `time.Ticker(resolution)` calling `tick(ctx, h.now())` until `ctx.Done()`.

`EvaluateOnce(ctx, p, apply)`: `now := h.now()`; `ev := &Evaluation{PolicyID: p.ID, EvaluatedAt: now, Inputs: map[string]any{}, Mutations: []Mutation{}, Results: []MutationResult{}}`; stages, each skipped once `ev.Error` is set:
1. `pq, err := h.prepare(p)` → `ev.Error = "compile: " + err.Error()`.
2. For each `in` in `p.Inputs`: `prov := Providers[in.Source]` missing → `input %s: no provider for source %q`; `v, err := prov.Fetch(ctx, in)` → `input %s: %v`; `ev.Inputs[in.Name] = v`. Then `ev.InputDigest = inputDigest(ev.Inputs)` (sha256 hex of `json.Marshal(ev.Inputs)`; computed even when a later stage fails).
3. Build the input document (`buildInput`): rules from `Rules.List` filtered by `p.Scope.Allows`, instances from `Instances.ForService` per scope service, `services`, `now`, `now_ns`, `inputs`; JSON round-trip with `UseNumber`; a store error → `ev.Error = "input document: ..."`.
4. `muts, err := pq.Eval(ctx, doc)` → `"eval: " + err`; `ev.Mutations = muts`; `allowed, refused := enforceScope(p.Scope, muts)`; `ev.Results = refused`.
5. If `apply`: `res, err := Mutator.Apply(ctx, p.ID, allowed)` → `"apply: " + err`; else `ev.Results = append(ev.Results, res...)`, `ev.Applied = true`, `ev.Changed = any r.Changed()`.
6. Record: `d := &model.PolicyDecision{ID: h.newID("pdec"), PolicyID: p.ID, EvaluatedAt: now, InputDigest: ev.InputDigest, Mutations: json.Marshal(ev.Results), Applied: ev.Applied, Error: ev.Error}`; `Decisions.Record` failure → log at Error and `return ev, fmt.Errorf("record decision: %w", err)`.
7. One slog line per evaluation: `Info("opa harness: policy evaluated", "policy_id", "policy", "input_digest", "mutations", len, "refused", n, "applied", "changed", "dry_run", !apply, "duration_ms")`, at `Warn` with `"error"` when `ev.Error != ""`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/opaharness/ -v 2>&1 | grep -E '^(--- |ok|FAIL)'` and `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test -race ./internal/opaharness/`
Expected: `--- PASS` for all `TestVectorProvider_*`, `TestEvaluateOnce_*` (8) and `TestTick_*` (6) tests; `ok` twice.

- [ ] **Step 6: Commit**

```bash
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon add internal/opaharness
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon commit -m "feat(opaharness): evaluation loop with telemetry inputs, cooldown and decision log

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: API — `/api/v1/opa/policies` CRUD, decision log, dry-run evaluate, OpenAPI

**Files:**
- Create: `internal/api/opa_handler.go`
- Test: `internal/api/opa_handler_test.go`
- Modify: `internal/api/server.go:33-58` (fields), `:60-106` (`NewServer`), `:236-242` (routes, add after the policy block)
- Modify: `Makefile:27-35` (swag `--dir` gains `internal/opaharness`)
- Regenerate: `docs/swagger.yaml`, `docs/swagger.json`

**Interfaces:**
- Consumes: `store.OPAPolicyRepo`, `store.PolicyDecisionRepo`, `store.Page` (Task 3); `opaharness.Compile`, `opaharness.Harness.EvaluateOnce`, `opaharness.Evaluation` (Tasks 5, 7); `readJSON`, `writeJSON`, `writeError`, `generateID`, `parsePagination`, `writePage`, `PageEnvelope` (`internal/api/server.go:290-311`, `pagination.go`).
- Produces, in `opa_handler.go`:
  - `type opaPolicyStore interface { Create(ctx, *model.OPAPolicy) error; Get(ctx, id string) (*model.OPAPolicy, error); Update(ctx, *model.OPAPolicy) error; Delete(ctx, id string) error; List(ctx) ([]*model.OPAPolicy, error) }`
  - `type policyDecisionLister interface { List(ctx, policyID string, p store.Page) ([]*model.PolicyDecision, int, error) }`
  - `type opaEvaluator interface { EvaluateOnce(ctx, p *model.OPAPolicy, apply bool) (*opaharness.Evaluation, error) }`
  - `Server` fields `opaPolicies opaPolicyStore`, `policyDecisions policyDecisionLister`, `opa opaEvaluator`; `NewServer` gains three trailing parameters `opaPolicies *store.OPAPolicyRepo, policyDecisions *store.PolicyDecisionRepo, harness *opaharness.Harness` (the only caller is `cmd/manteion/main.go:123-127`, updated in Task 9).
  - Routes: `POST /api/v1/opa/policies`, `GET /api/v1/opa/policies`, `GET /api/v1/opa/policies/{id}`, `PUT /api/v1/opa/policies/{id}`, `DELETE /api/v1/opa/policies/{id}`, `GET /api/v1/opa/policies/{id}/decisions`, `POST /api/v1/opa/policies/{id}/evaluate` (query `apply=true|false`, default false).

- [ ] **Step 1: Write the failing handler tests**

`internal/api/opa_handler_test.go`:

```go
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/opaharness"
	"manteion-go/internal/store"
)

// ---------------------------------------------------------------------------
// Fakes — DB-free views of the OPA repos and the harness.
// ---------------------------------------------------------------------------

type fakeOPAPolicies struct {
	byID map[string]*model.OPAPolicy
	err  error
}

func newFakeOPAPolicies(ps ...*model.OPAPolicy) *fakeOPAPolicies {
	f := &fakeOPAPolicies{byID: map[string]*model.OPAPolicy{}}
	for _, p := range ps {
		f.byID[p.ID] = p
	}
	return f
}

func (f *fakeOPAPolicies) nameTaken(name, exceptID string) bool {
	for _, p := range f.byID {
		if p.Name == name && p.ID != exceptID {
			return true
		}
	}
	return false
}

func (f *fakeOPAPolicies) Create(_ context.Context, p *model.OPAPolicy) error {
	if f.err != nil {
		return f.err
	}
	if err := p.Validate(); err != nil {
		return err
	}
	if f.nameTaken(p.Name, p.ID) {
		return store.ErrConflict
	}
	cp := *p
	f.byID[p.ID] = &cp
	return nil
}

func (f *fakeOPAPolicies) Get(_ context.Context, id string) (*model.OPAPolicy, error) {
	if f.err != nil {
		return nil, f.err
	}
	if p, ok := f.byID[id]; ok {
		cp := *p
		return &cp, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeOPAPolicies) Update(_ context.Context, p *model.OPAPolicy) error {
	if f.err != nil {
		return f.err
	}
	if err := p.Validate(); err != nil {
		return err
	}
	existing, ok := f.byID[p.ID]
	if !ok {
		return store.ErrNotFound
	}
	if f.nameTaken(p.Name, p.ID) {
		return store.ErrConflict
	}
	cp := *p
	cp.CreatedAt = existing.CreatedAt
	f.byID[p.ID] = &cp
	return nil
}

func (f *fakeOPAPolicies) Delete(_ context.Context, id string) error {
	if _, ok := f.byID[id]; !ok {
		return store.ErrNotFound
	}
	delete(f.byID, id)
	return nil
}

func (f *fakeOPAPolicies) List(_ context.Context) ([]*model.OPAPolicy, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := []*model.OPAPolicy{}
	for _, p := range f.byID {
		cp := *p
		out = append(out, &cp)
	}
	return out, nil
}

type fakeDecisionLister struct {
	byPolicy map[string][]*model.PolicyDecision
	gotPage  store.Page
	err      error
}

func (f *fakeDecisionLister) List(_ context.Context, policyID string, p store.Page) ([]*model.PolicyDecision, int, error) {
	f.gotPage = p
	if f.err != nil {
		return nil, 0, f.err
	}
	all := f.byPolicy[policyID]
	end := min(p.Offset+p.Limit, len(all))
	if p.Offset > len(all) {
		return []*model.PolicyDecision{}, len(all), nil
	}
	return all[p.Offset:end], len(all), nil
}

type fakeEvaluator struct {
	ev       *opaharness.Evaluation
	err      error
	calls    int
	gotID    string
	gotApply bool
}

func (f *fakeEvaluator) EvaluateOnce(_ context.Context, p *model.OPAPolicy, apply bool) (*opaharness.Evaluation, error) {
	f.calls++
	f.gotID, f.gotApply = p.ID, apply
	if f.err != nil {
		return nil, f.err
	}
	return f.ev, nil
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const validRego = "package atropos\n\nmutations contains m if { false; m := {} }\n"

var opaTestTime = time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC)

func opaTestPolicy() *model.OPAPolicy {
	return &model.OPAPolicy{
		ID: "opa-1", Name: "checkout-hot", Enabled: true, Rego: validRego, IntervalS: 10, CooldownS: 60,
		Inputs:    []model.PolicyInput{{Name: "p99", Source: "prometheus", Query: "up", Kind: "scalar"}},
		Scope:     model.PolicyScope{Services: []string{"checkout"}},
		CreatedAt: opaTestTime, UpdatedAt: opaTestTime,
	}
}

// opaBody is a create/update body; rego is injected so tests can vary it.
func opaBody(rego string) string {
	b, _ := json.Marshal(map[string]any{
		"name": "checkout-hot", "enabled": true, "rego": rego, "cooldown_s": 60,
		"inputs": []map[string]string{{"name": "p99", "source": "prometheus", "query": "up"}},
		"scope":  map[string]any{"services": []string{"checkout"}},
	})
	return string(b)
}

func newOPAServer(pol *fakeOPAPolicies, dec *fakeDecisionLister, ev *fakeEvaluator) *Server {
	return &Server{opaPolicies: pol, policyDecisions: dec, opa: ev, logger: discardLogger()}
}

// serveOPA drives a request through the real route table.
func serveOPA(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	s.routes(mux)
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func errorOf(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var er ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &er); err != nil {
		t.Fatalf("decode error envelope: %v (body %s)", err, w.Body.String())
	}
	return er.Error
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestHandleCreateOPAPolicy(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		existing   []*model.OPAPolicy
		storeErr   error
		wantStatus int
		wantError  string
	}{
		{"valid → 201", opaBody(validRego), nil, nil, http.StatusCreated, ""},
		{"bad json → 400", `{"name":`, nil, nil, http.StatusBadRequest, "invalid JSON"},
		{"rego parse error → 400", opaBody("package atropos\nmutations contains x if { x := }"), nil, nil, http.StatusBadRequest, "rego:"},
		{"unsafe builtin → 400", opaBody("package atropos\nmutations contains r if { r := http.send({\"method\": \"get\", \"url\": \"http://x\"}) }"), nil, nil, http.StatusBadRequest, "unsafe built-in"},
		{"missing scope → 400", strings.Replace(opaBody(validRego), `"services":["checkout"]`, `"services":[]`, 1), nil, nil, http.StatusBadRequest, "scope"},
		{"duplicate name → 409", opaBody(validRego), []*model.OPAPolicy{opaTestPolicy()}, nil, http.StatusConflict, "already exists"},
		{"store failure → 500", opaBody(validRego), nil, errors.New("db down"), http.StatusInternalServerError, "failed to create"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pol := newFakeOPAPolicies(tc.existing...)
			pol.err = tc.storeErr
			w := serveOPA(t, newOPAServer(pol, &fakeDecisionLister{}, &fakeEvaluator{}), http.MethodPost, "/api/v1/opa/policies", tc.body)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", w.Code, tc.wantStatus, w.Body.String())
			}
			if tc.wantStatus != http.StatusCreated {
				if msg := errorOf(t, w); !strings.Contains(msg, tc.wantError) {
					t.Errorf("error = %q, want it to contain %q", msg, tc.wantError)
				}
				if len(pol.byID) != len(tc.existing) {
					t.Errorf("store changed on a rejected create: %d policies", len(pol.byID))
				}
				return
			}
			var got model.OPAPolicy
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !strings.HasPrefix(got.ID, "opa-") || got.IntervalS != model.DefaultOPAIntervalS || got.CreatedAt.IsZero() || got.Inputs[0].Kind != "scalar" {
				t.Errorf("created = %+v, want minted id, defaulted interval/kind, timestamps", got)
			}
			if pol.byID[got.ID] == nil {
				t.Errorf("policy %s not stored", got.ID)
			}
		})
	}
}

func TestHandleUpdateOPAPolicy(t *testing.T) {
	changed := "package atropos\n\nmutations contains {\"op\": \"delete_rule\", \"service\": \"checkout\", \"name\": \"x\"}\n"
	tests := []struct {
		name       string
		id         string
		body       string
		wantStatus int
		wantRego   string // stored rego afterwards
	}{
		{"existing → 200, stored", "opa-1", opaBody(changed), http.StatusOK, changed},
		{"unknown → 404", "opa-nope", opaBody(changed), http.StatusNotFound, validRego},
		{"compile error → 400, untouched", "opa-1", opaBody("package atropos\nmutations contains x if { x := }"), http.StatusBadRequest, validRego},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pol := newFakeOPAPolicies(opaTestPolicy())
			w := serveOPA(t, newOPAServer(pol, &fakeDecisionLister{}, &fakeEvaluator{}), http.MethodPut, "/api/v1/opa/policies/"+tc.id, tc.body)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", w.Code, tc.wantStatus, w.Body.String())
			}
			if pol.byID["opa-1"].Rego != tc.wantRego {
				t.Errorf("stored rego = %q, want %q", pol.byID["opa-1"].Rego, tc.wantRego)
			}
			if tc.wantStatus == http.StatusOK {
				var got model.OPAPolicy
				_ = json.Unmarshal(w.Body.Bytes(), &got)
				if got.ID != "opa-1" || !got.CreatedAt.Equal(opaTestTime) || got.UpdatedAt.Equal(opaTestTime) {
					t.Errorf("response = %+v, want the stored row with path id, original created_at, new updated_at", got)
				}
			}
		})
	}
}

func TestHandleGetAndListAndDeleteOPAPolicy(t *testing.T) {
	pol := newFakeOPAPolicies(opaTestPolicy())
	s := newOPAServer(pol, &fakeDecisionLister{}, &fakeEvaluator{})

	if w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies/opa-1", ""); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"name":"checkout-hot"`) {
		t.Errorf("get: %d %s", w.Code, w.Body.String())
	}
	if w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies/nope", ""); w.Code != http.StatusNotFound || errorOf(t, w) != "policy not found" {
		t.Errorf("get unknown: %d %s", w.Code, w.Body.String())
	}
	if w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies", ""); w.Code != http.StatusOK || !strings.HasPrefix(w.Body.String(), "[{") {
		t.Errorf("list: %d %s", w.Code, w.Body.String())
	}
	if w := serveOPA(t, s, http.MethodDelete, "/api/v1/opa/policies/opa-1", ""); w.Code != http.StatusNoContent || pol.byID["opa-1"] != nil {
		t.Errorf("delete: %d, still stored %v", w.Code, pol.byID["opa-1"] != nil)
	}
	if w := serveOPA(t, s, http.MethodDelete, "/api/v1/opa/policies/opa-1", ""); w.Code != http.StatusNotFound {
		t.Errorf("delete again: %d", w.Code)
	}
	if w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies", ""); w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != "[]" {
		t.Errorf("empty list = %q, want [] never null", w.Body.String())
	}
}

func TestHandleListOPADecisions(t *testing.T) {
	decs := make([]*model.PolicyDecision, 0, 7)
	for i := 0; i < 7; i++ {
		decs = append(decs, &model.PolicyDecision{ID: "pdec-" + string(rune('a'+i)), PolicyID: "opa-1", EvaluatedAt: opaTestTime.Add(-time.Duration(i) * time.Minute), Mutations: json.RawMessage(`[]`), Applied: true})
	}
	dec := &fakeDecisionLister{byPolicy: map[string][]*model.PolicyDecision{"opa-1": decs}}
	s := newOPAServer(newFakeOPAPolicies(opaTestPolicy()), dec, &fakeEvaluator{})

	if w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies/nope/decisions", ""); w.Code != http.StatusNotFound {
		t.Errorf("unknown policy: %d", w.Code)
	}
	w := serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies/opa-1/decisions?limit=5&offset=1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	var page PageEnvelope[model.PolicyDecision]
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode page: %v", err)
	}
	if dec.gotPage != (store.Page{Limit: 5, Offset: 1}) || page.Page.Total != 7 || len(page.Data) != 5 || page.Data[0].ID != "pdec-b" || page.Page.Limit != 5 || page.Page.Offset != 1 {
		t.Errorf("page = %+v (repo got %+v)", page.Page, dec.gotPage)
	}
	w = serveOPA(t, s, http.MethodGet, "/api/v1/opa/policies/opa-1/decisions?offset=50", "")
	if !strings.Contains(w.Body.String(), `"data":[]`) {
		t.Errorf("past the end: data must be [] not null: %s", w.Body.String())
	}
}

func TestHandleEvaluateOPAPolicy(t *testing.T) {
	off := false
	ev := &opaharness.Evaluation{
		PolicyID: "opa-1", EvaluatedAt: opaTestTime, Inputs: map[string]any{"p99": 1300.0}, InputDigest: "abc",
		Mutations: []opaharness.Mutation{{Op: opaharness.OpSetEnabled, Service: "checkout", Name: "r", Enabled: &off}},
		Results:   []opaharness.MutationResult{},
	}
	tests := []struct {
		name       string
		id, query  string
		eval       *fakeEvaluator
		wantStatus int
		wantApply  bool
	}{
		{"default is a dry run", "opa-1", "", &fakeEvaluator{ev: ev}, http.StatusOK, false},
		{"apply=false", "opa-1", "?apply=false", &fakeEvaluator{ev: ev}, http.StatusOK, false},
		{"apply=true", "opa-1", "?apply=true", &fakeEvaluator{ev: ev}, http.StatusOK, true},
		{"unknown policy → 404", "nope", "", &fakeEvaluator{ev: ev}, http.StatusNotFound, false},
		{"decision log failure → 500", "opa-1", "", &fakeEvaluator{err: errors.New("record decision: db down")}, http.StatusInternalServerError, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newOPAServer(newFakeOPAPolicies(opaTestPolicy()), &fakeDecisionLister{}, tc.eval)
			w := serveOPA(t, s, http.MethodPost, "/api/v1/opa/policies/"+tc.id+"/evaluate"+tc.query, "")
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", w.Code, tc.wantStatus, w.Body.String())
			}
			if tc.wantStatus == http.StatusNotFound {
				if tc.eval.calls != 0 {
					t.Errorf("evaluator called for an unknown policy")
				}
				return
			}
			if tc.eval.calls != 1 || tc.eval.gotID != "opa-1" || tc.eval.gotApply != tc.wantApply {
				t.Errorf("evaluator calls %d id %q apply %v, want 1 opa-1 %v", tc.eval.calls, tc.eval.gotID, tc.eval.gotApply, tc.wantApply)
			}
			if tc.wantStatus != http.StatusOK {
				return
			}
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
				t.Fatalf("decode: %v", err)
			}
			for _, key := range []string{"policy_id", "evaluated_at", "inputs", "input_digest", "mutations", "results", "applied", "changed"} {
				if _, ok := raw[key]; !ok {
					t.Errorf("%s missing from %s", key, w.Body.String())
				}
			}
			if string(raw["results"]) == "null" || string(raw["mutations"]) == "null" {
				t.Errorf("mutations/results must be arrays, never null: %s", w.Body.String())
			}
		})
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/api/ -run 'OPA'`
Expected: `[build failed]` with `s.opaPolicies undefined` / `unknown field opaPolicies in struct literal`.

- [ ] **Step 3: Implement the handlers, fields, constructor params and routes**

`internal/api/server.go`: add the three fields next to `policies` (`:51`), the three trailing `NewServer` parameters and their assignments (`opaPolicies: opaPolicies, policyDecisions: policyDecisions, opa: harness`), and the route block after `:242`:

```go
	// OPA harness: Rego policies over live telemetry that mutate rules
	// (docs/opa-harness.md). evaluate defaults to a dry run (?apply=false).
	mux.HandleFunc("POST /api/v1/opa/policies", s.handleCreateOPAPolicy)
	mux.HandleFunc("GET /api/v1/opa/policies", s.handleListOPAPolicies)
	mux.HandleFunc("GET /api/v1/opa/policies/{id}", s.handleGetOPAPolicy)
	mux.HandleFunc("PUT /api/v1/opa/policies/{id}", s.handleUpdateOPAPolicy)
	mux.HandleFunc("DELETE /api/v1/opa/policies/{id}", s.handleDeleteOPAPolicy)
	mux.HandleFunc("GET /api/v1/opa/policies/{id}/decisions", s.handleListOPADecisions)
	mux.HandleFunc("POST /api/v1/opa/policies/{id}/evaluate", s.handleEvaluateOPAPolicy)
```

`internal/api/opa_handler.go` — handler behaviour (status, message):
- `handleCreateOPAPolicy`: `readJSON` → 400 `invalid JSON: ...`; mint `generateID("opa")` when `ID == ""`; `CreatedAt = UpdatedAt = time.Now()`; `p.Validate()` → 400 with the error text; `opaharness.Compile(p.Rego)` → 400 `rego: <error>`; `Create`: `store.ErrConflict` → 409 `policy name already exists`, other → 500 `failed to create policy` (logged at Error); 201 with the policy.
- `handleUpdateOPAPolicy`: path id overrides the body's; `UpdatedAt = time.Now()`; same Validate/Compile → 400; `Update`: `ErrNotFound` → 404 `policy not found`, `ErrConflict` → 409, other → 500 `failed to update policy`; then `Get` and return the stored row (so `created_at` is the original) with 200.
- `handleListOPAPolicies`: 200 `[]model.OPAPolicy`, nil → `[]`; error → 500 `failed to list policies`.
- `handleGetOPAPolicy`: 200 / 404 `policy not found` / 500 `failed to get policy`.
- `handleDeleteOPAPolicy`: 204 / 404 / 500 `failed to delete policy`.
- `handleListOPADecisions`: `Get` first (404), `limit, offset := parsePagination(r)`, `policyDecisions.List(ctx, id, store.Page{Limit: limit, Offset: offset})` → 500 `failed to list decisions`; `writePage(w, 200, items, total, limit, offset)` with `[]model.PolicyDecision` values (dereference the pointers so the envelope's generic is the value type).
- `handleEvaluateOPAPolicy`: `Get` (404); `apply := r.URL.Query().Get("apply") == "true"`; `s.opa.EvaluateOnce(ctx, p, apply)` → error → 500 `failed to record decision`; 200 with the `*opaharness.Evaluation`.

swag annotations on every handler (`docs/openapi-conventions.md:12-29`), tag `autorules`. The create block, as the template for the others:

```go
// handleCreateOPAPolicy creates a Rego policy evaluated by the OPA harness.
//
// @Summary      Create OPA policy
// @Description  Persist a Rego (v1 syntax) policy. The module must declare
// @Description  `package atropos` and define `mutations` (a set of rule mutations);
// @Description  it is compiled at save time and rejected with 400 on parse, type or
// @Description  unsafe-builtin (http.send) errors. inputs[] declare the telemetry
// @Description  (prometheus | loki instant metric queries) exposed as input.inputs.<name>;
// @Description  scope.services bounds which services' rules the policy may mutate.
// @Description  Policies are created disabled unless "enabled": true is explicit.
// @Tags         autorules
// @Accept       json
// @Produce      json
// @Param        policy  body      model.OPAPolicy  true  "Policy definition"
// @Success      201     {object}  model.OPAPolicy
// @Failure      400     {object}  api.ErrorResponse  "invalid JSON, validation error, or Rego compile error"
// @Failure      409     {object}  api.ErrorResponse  "policy name already exists"
// @Failure      500     {object}  api.ErrorResponse
// @Router       /opa/policies [post]
```

Remaining `@Router` lines: `/opa/policies [get]` (`{array} model.OPAPolicy`), `/opa/policies/{id} [get]`, `[put]` (`{object} model.OPAPolicy`; 400/404/409), `[delete]` (`204 "policy deleted"`), `/opa/policies/{id}/decisions [get]` (`@Param limit query int false`, `offset`; `@Success 200 {object} api.PageEnvelope[model.PolicyDecision]` — if swag v2.0.0-rc5 rejects the generic instantiation, annotate `{array} model.PolicyDecision` and say in the description that the body is the standard `{data, page}` envelope), `/opa/policies/{id}/evaluate [post]` (`@Param apply query bool false "apply the mutations (default false: dry run)"`, `@Success 200 {object} opaharness.Evaluation`).

- [ ] **Step 4: Run the handler tests to verify they pass**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./internal/api/ -run 'OPA' -v 2>&1 | grep -E '^(--- |ok|FAIL)'`
Expected: `--- PASS` for `TestHandleCreateOPAPolicy` (7 subtests), `TestHandleUpdateOPAPolicy` (3), `TestHandleGetAndListAndDeleteOPAPolicy`, `TestHandleListOPADecisions`, `TestHandleEvaluateOPAPolicy` (5); `ok  	manteion-go/internal/api`. `go -C ... build ./...` fails at this point only in `cmd/manteion` (`not enough arguments in call to api.NewServer`) — Task 9 fixes the caller; do not commit a red build: fold the one-line `NewServer` call update from Task 9 Step 2 into this task's commit if the implementer prefers, or pass `nil, nil, nil` temporarily and replace it in Task 9. Pick the first.

- [ ] **Step 5: Regenerate the OpenAPI spec**

Edit `Makefile:31`: `--dir cmd/manteion,internal/api,internal/model,internal/opaharness`.
Run: `make -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon openapi && grep -c "/opa/policies" /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon/docs/swagger.yaml && git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon diff --stat -- docs/`
Expected: swag prints `create docs.go`-style progress and the post-processor runs silently; the grep prints at least `3` (one per distinct path key); both `docs/swagger.yaml` and `docs/swagger.json` appear in the diff stat. Prerequisites: `swag` v2.0.0-rc5 on PATH (`go install github.com/swaggo/swag/v2/cmd/swag@v2.0.0-rc5`) and `python3 -c "import yaml"` succeeding (both present on this machine).

- [ ] **Step 6: Full verification and commit**

Run: `gofmt -l /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon/internal /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon/cmd && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon vet ./... && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./...`
Expected: gofmt prints nothing; vet clean; every package `ok` (the `cmd/manteion` call site updated as decided in Step 4).

```bash
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon add internal/api/opa_handler.go internal/api/opa_handler_test.go internal/api/server.go Makefile docs/swagger.yaml docs/swagger.json cmd/manteion/main.go
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon commit -m "feat(api): /api/v1/opa/policies CRUD, decision log and dry-run evaluate

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Wiring, gate and documentation

**Files:**
- Modify: `cmd/manteion/main.go:18-28` (imports), `:44-50` (env), `:90-111` (repos + harness), `:123-127` (`NewServer` args), `:137-144` (gate), `:179-182` (startup log)
- Create: `docs/opa-harness.md`, `docs/decisions/2026-10-opa-harness.md`
- Modify: `README.md:39` and `:67-78`; `CLAUDE.md:36-50` (packages table), `:76-85` (HTTP API table), `:114-120` (env table); `AGENTS.md:54`

**Interfaces:**
- Consumes: `store.NewOPAPolicyRepo`, `store.NewPolicyDecisionRepo` (Task 3); `logql.NewClient` (Task 4); `opaharness.New`, `Deps`, `Providers`, `VectorProvider`, `NewRuleMutator` (Tasks 6-7); `api.NewServer` signature (Task 8); `model.InputSourcePrometheus`, `model.InputSourceLoki` (Task 2).
- Produces: a running harness behind `MANTEION_POLICY_ENGINE=rego|on`; `MANTEION_LOKI_URL`.

- [ ] **Step 1: Wire the harness in `cmd/manteion/main.go`**

Imports: add `"manteion-go/internal/logql"`, `"manteion-go/internal/model"`, `"manteion-go/internal/opaharness"`. After `prometheusURL` (`:49`): `lokiURL := envOr("MANTEION_LOKI_URL", "http://loki:3100")`. After `policyEngine := ...` (`:111`):

```go
	// OPA harness (docs/opa-harness.md): Rego policies over Prometheus/Loki
	// inputs whose outputs are rule mutations applied through ruleRepo, so
	// they reach instances by the same path as PUT /api/v1/rules/{id}. Always
	// constructed (the API's dry-run evaluate uses it); only Run is gated.
	opaPolicyRepo := store.NewOPAPolicyRepo(database)
	policyDecisionRepo := store.NewPolicyDecisionRepo(database)
	harness := opaharness.New(opaharness.Deps{
		Policies:  opaPolicyRepo,
		Decisions: policyDecisionRepo,
		Rules:     ruleRepo,
		Instances: sdkRepo,
		Providers: opaharness.Providers{
			model.InputSourcePrometheus: opaharness.VectorProvider{Querier: promClient},
			model.InputSourceLoki:       opaharness.VectorProvider{Querier: logql.NewClient(lokiURL)},
		},
		Mutator: opaharness.NewRuleMutator(ruleRepo, faultRepo),
		Logger:  logger,
	})
```

`NewServer` call (`:123-127`): append `opaPolicyRepo, policyDecisionRepo, harness` after `policyRepo`. Replace the gate (`:137-144`) with:

```go
	// Policy automation. "rego" (or "on") starts the OPA harness
	// (docs/opa-harness.md); "threshold" starts the frozen PromQL engine
	// (docs/decisions/2026-06-policy-engine-freeze.md); "off" starts neither.
	switch mode := envOr("MANTEION_POLICY_ENGINE", "off"); mode {
	case "rego", "on":
		go harness.Run(ctx)
	case "threshold":
		go policyEngine.Run(ctx)
	case "off":
		logger.Info("policy automation disabled (MANTEION_POLICY_ENGINE=off; rego|on starts the OPA harness, threshold the frozen PromQL engine)")
	default:
		logger.Warn("unknown MANTEION_POLICY_ENGINE value; policy automation disabled", "value", mode)
	}
```

Add `"loki_url", lokiURL,` to the `manteion starting` log (`:179-182`).

- [ ] **Step 2: Build, vet, test; measure the binary**

Run: `go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon build ./... && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon vet ./... && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test ./... && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon build -o /tmp/manteion-after ./cmd/manteion && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon build -ldflags="-s -w" -o /tmp/manteion-after-s ./cmd/manteion && ls -l /tmp/manteion-before /tmp/manteion-after /tmp/manteion-before-s /tmp/manteion-after-s`
Expected: all packages `ok` (now including `internal/logql` and `internal/opaharness`); the after binaries are ≈ +29 MB (unstripped) / ≈ +23 MB (stripped) over Task 1 Step 1. Put the four numbers in `docs/opa-harness.md` ("Cost").

- [ ] **Step 3: Smoke test against a live database (optional but recommended)**

Run (repo root, Postgres from `docker compose up -d postgres`): `MANTEION_POLICY_ENGINE=rego MANTEION_ADDR=:9099 go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon run ./cmd/manteion` in one terminal, then:

```bash
curl -s -X POST localhost:9099/api/v1/opa/policies -H 'Content-Type: application/json' -d @- <<'JSON'
{"name":"smoke","enabled":false,"rego":"package atropos\n\nmutations contains m if { false; m := {} }\n","inputs":[{"name":"p99","source":"prometheus","query":"up"}],"scope":{"services":["checkout"]}}
JSON
curl -s -X POST "localhost:9099/api/v1/opa/policies/<id>/evaluate?apply=false"
curl -s "localhost:9099/api/v1/opa/policies/<id>/decisions"
```
Expected: the server log contains `"opa harness: starting"`; the create returns 201 with an `opa-…` id; evaluate returns 200 whose `error` says `input p99: prometheus query failed: …` (no Prometheus on this machine) with `"applied":false`; the decisions page lists that one row with the same `error`.

- [ ] **Step 4: Write `docs/opa-harness.md`**

Sections and the content each must carry:

1. **What it is** — one paragraph: ADR-1 "keep it simple"; the SDK matcher stays label-based; conditions on system state live here; outputs are rule mutations through the rule repository, so they travel to instances like any rule edit (version bump today, per-service generation once the desired-state plan lands).
2. **Policy object** — the JSON shape of `model.OPAPolicy` with field notes: `interval_s` (default 30), `cooldown_s` (0 = none), `inputs[]{name, source: prometheus|loki, query, kind: scalar|vector}`, `scope.services` (required, non-empty), `enabled` (default false).
3. **Input document** — the exact `input` shape from Task 7 Interfaces, plus the scalar semantics: absent series → `null` (comparisons against it are undefined, so `not hot` branches fire), several series → the evaluation fails; `vector` → `[{"metric": {...}, "value": v}]`.
4. **Mutations** — `data.atropos.mutations` is a set of objects; table of ops (`upsert_rule` with the full `model.Rule` JSON, server-owned `id`/timestamps ignored, keyed by `rule.service`+`rule.name`; `set_enabled{service,name,enabled}`; `set_params{service,name,params}` — replaces the referenced fault spec's params, only for `fault_spec` rules whose spec no other rule references, re-saves the rule so pollers recompile; `delete_rule{service,name}`); outcomes table (`created|updated|deleted|unchanged|refused|failed`); refusal rules (outside `scope.services`, malformed); idempotency: re-emitting the desired state every interval costs no writes.
5. **Scheduling and cooldown** — 1 s resolution; each policy every `interval_s`; after an evaluation whose applied set changed, the next evaluation waits `max(interval_s, cooldown_s)`; cooldown survives restarts because it is read back from the newest applied+changed decision.
6. **Decision log** — one row per evaluation (loop and API, dry runs included, `applied` false for them), fields, retention 1000 per policy, `GET /api/v1/opa/policies/{id}/decisions`; one slog line per evaluation (`opa harness: policy evaluated`, Warn with `error` on failure).
7. **API** — the seven routes with one-line semantics; `POST …/evaluate?apply=false` is the dry run and returns `inputs`, `input_digest`, `mutations`, `results`.
8. **Example** — the brief's scenario, verbatim:

````markdown
Policy `checkout-hot` (scope `["checkout"]`, interval 15 s, cooldown 120 s), inputs:

| name | source | kind | query |
|---|---|---|---|
| `decisions` | loki | scalar | `sum(count_over_time({service_name="checkout"} \|= "atropos.decision" [1m]))` |
| `p99` | prometheus | scalar | `histogram_quantile(0.99, sum by (le) (rate(duration_milliseconds_bucket{service_name="checkout", span_name="POST /cart/checkout"}[1m])))` |

(`atropos.decision` is the SDK's per-decision log event, ADR-3 item 2; the stream selector and the spanmetrics label names follow the bed's collector config — adjust them to yours.)

```rego
package atropos

rule_name := "checkout-egress-latency"

# N = 50 decisions in the last minute, X = 800 ms p99 for POST /cart/checkout.
hot if {
	input.inputs.decisions > 50
	input.inputs.p99 > 800
}

desired_rule := {
	"name": rule_name,
	"service": "checkout",
	"enabled": true,
	"priority": 100,
	"mode": "inline",
	"match": {"injection_point": "egress", "labels": {"atropos.http.path": "/cart/checkout"}},
	"action": {"type": "fault_spec", "fault_spec_id": "spec-egress-latency-200ms"},
}

mutations contains {"op": "upsert_rule", "rule": desired_rule} if hot

mutations contains {"op": "set_enabled", "service": "checkout", "name": rule_name, "enabled": false} if {
	not hot
	some r in input.rules
	r.service == "checkout"
	r.name == rule_name
	r.enabled
}
```

`spec-egress-latency-200ms` is a pre-created inline latency fault spec (`POST /api/v1/faults/specs`, params `{"delay":"200ms"}`).

Hot (`decisions = 120`, `p99 = 1300`), rule absent — the dry run returns:

```json
{"mutations": [{"op": "upsert_rule", "rule": {"name": "checkout-egress-latency", "service": "checkout", "enabled": true, "priority": 100, "mode": "inline", "match": {"injection_point": "egress", "labels": {"atropos.http.path": "/cart/checkout"}}, "action": {"type": "fault_spec", "fault_spec_id": "spec-egress-latency-200ms"}}}]}
```

and applying it yields `results: [{"op": "upsert_rule", "service": "checkout", "name": "checkout-egress-latency", "outcome": "created", "rule_id": "rule-…"}]`; the next hot evaluation reports `unchanged` and no cooldown starts.

Cold (`decisions = 3`, `p99 = 200`), rule present and enabled:

```json
{"mutations": [{"op": "set_enabled", "service": "checkout", "name": "checkout-egress-latency", "enabled": false}]}
```

→ `outcome: "updated"`, then `unchanged` on later cold evaluations; cold with the rule absent or already disabled → `mutations: []`. A `null` `decisions` (no log lines in the window) makes `hot` undefined, so the cold branch applies.
````

9. **Operations** — env table (`MANTEION_POLICY_ENGINE`: `off` | `rego`/`on` | `threshold`; `MANTEION_LOKI_URL`; `PROMETHEUS_URL`); the Loki endpoint used (`GET /loki/api/v1/query`, metric queries only).
10. **Cost and limits** — the measured binary sizes from Step 2; OPA pinned to v1.19.1 (Go 1.25 floor); `http.send` / `net.lookup_ip_addr` / `opa.runtime` refused; 5 s evaluation timeout; compositions and cache-box rules can be upserted like any rule but `set_params` applies to `fault_spec` rules only.

- [ ] **Step 5: Decision record and doc edits**

`docs/decisions/2026-10-opa-harness.md` (same shape as `docs/decisions/2026-06-policy-engine-freeze.md`): **Status:** accepted (2026-10-30). Context: ADR-1 of the daemon review and the user's decision; the freeze said "revisit when the orchestrator FSM port lands". Decision: metric/log-conditioned automation is the OPA harness (`internal/opaharness`), central, emitting rule mutations through `store.RuleRepo`; the SDK keeps label matching; the frozen `internal/policy` engine stays reachable only as `MANTEION_POLICY_ENGINE=threshold` and is a removal candidate once the harness has run a bed experiment (its `policy_rules` table and `/api/v1/policies` CRUD are untouched by this change). Consequences: +≈23 MB stripped binary, OPA version floor tied to the Go toolchain, a second policy language (Rego) next to the UI's rule model, decision log in Postgres with per-policy retention.

Doc rows:
- `README.md:39` → `| \`internal/opaharness/\` | OPA harness: Rego policies over Prometheus/Loki inputs that mutate rules through the rule repo (\`MANTEION_POLICY_ENGINE=rego\`); see \`docs/opa-harness.md\`. |` and keep `internal/policy/` as `| \`internal/policy/\` | Frozen PromQL threshold loop, reachable only as \`MANTEION_POLICY_ENGINE=threshold\`. |`; add `| \`internal/logql/\` | Loki instant metric-query client (harness inputs). |`.
- `README.md:77` → `| \`MANTEION_POLICY_ENGINE\` | \`off\` | \`rego\` (or \`on\`) starts the OPA harness; \`threshold\` the frozen PromQL loop |`; add `| \`MANTEION_LOKI_URL\` | \`http://loki:3100\` | Loki base URL for harness inputs |` after the `PROMETHEUS_URL` row.
- `README.md:58-63` (HTTP surface): add `OPA policies (\`/api/v1/opa/policies\` CRUD, \`/{id}/decisions\`, \`/{id}/evaluate?apply=\`)`.
- `CLAUDE.md:118` → same gate text; add the `MANTEION_LOKI_URL` row; packages table: rows for `internal/opaharness/` and `internal/logql/`; HTTP API table: `| OPA policies | \`POST/GET /api/v1/opa/policies\`, \`GET/PUT/DELETE ./{id}\`, \`GET ./{id}/decisions\` (paged), \`POST ./{id}/evaluate?apply=true\|false\` (dry run by default) |`; `internal/id/` row: add `opa- pdec-`.
- `AGENTS.md:54` → `- **Policy** — Rego policies evaluated by the OPA harness against live Prometheus/Loki inputs; outputs are rule mutations applied through the rule repository (\`internal/opaharness\`, \`docs/opa-harness.md\`). The 2026-04 PromQL threshold engine (\`internal/model/policy.go\`, \`internal/policy\`) is frozen behind \`MANTEION_POLICY_ENGINE=threshold\`.`

- [ ] **Step 6: Final verification and commit**

Run: `gofmt -l /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon/internal /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon/cmd && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon vet ./... && go -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon test -race ./... && make -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon openapi-check`
Expected: gofmt silent; vet clean; every package `ok` under `-race`; `openapi-check` prints nothing (spec up to date).

```bash
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon add cmd/manteion/main.go docs/opa-harness.md docs/decisions/2026-10-opa-harness.md README.md CLAUDE.md AGENTS.md
git -C /Users/pronei/work/faults-lab/worktrees/manteion-atropos-daemon commit -m "feat(cmd): MANTEION_POLICY_ENGINE=rego starts the OPA harness; docs

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Self-review (done while writing; findings folded in above)

- **Spec coverage.** Dependency + version check + size note → Task 1 (version floor discovered: v1.19.1, not latest). Storage (`opa_policies`, `policy_decisions`, `inputs`/`scope` JSONB shapes) → Task 3. Harness: `InputProvider`/providers by source → Task 7 (`VectorProvider` over `promql`/`logql`, Task 4); `Mutator` with the four idempotent, scope-checked ops → Tasks 5-6; `data.atropos.mutations` over `{now, inputs, rules, services, instances}` with a prepared query compiled at save (400) → Tasks 5, 7, 8; per-policy interval + cooldown after a changed apply, every evaluation logged to `policy_decisions` and slog → Task 7; gate `rego|on` / `threshold` + docs → Task 9. API CRUD + decisions + `evaluate?apply=false` + OpenAPI → Task 8. Example policy with Rego and expected mutations → Task 9 (the Rego was executed against OPA v1.19.1 while planning; hot/cold/null outputs are the ones shown). Tests: harness with fake providers/mutator and real Rego, cooldown, scope, decision-log writes with a fake repo, handler fakes, one DB-gated migration/repo test → Tasks 3, 5-8.
- **Deviations from the brief, deliberate:** `Mutator.Apply` returns `[]MutationResult` (needed for the decision log and cooldown); `set_params` targets the rule's fault spec (the only "params" in this model) and is refused for shared specs; dry runs are recorded (`applied=false`) so operators can see what they tested; `LastChangeAt` replaces the old engine's first-tick grace.
- **Type consistency.** `Mutation.Key()`, `MutationResult.Changed()`, `OutcomeRefused` etc. are defined in Task 5 and used unchanged in Tasks 6-8; `Deps` field names match `main.go` in Task 9; `store.Page{Limit, Offset}` is the existing type; `Evaluation` field tags match the handler test's key list.
- **Review Focus.** All five pinned: Task 5 (`key mismatch`, non-object mutation), Task 7 (several series, compile error isolation), Task 6 (shared spec).
- **Proportion.** Code blocks are tests (what the implementer must match exactly) plus the two algorithms the signatures do not determine (tick scheduling, `Apply` semantics) and exact copy (SQL, gate block, example policy). Implementation bodies are left to the implementer.
