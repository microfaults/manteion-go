# Atropos Client Follow-ups: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the functional gaps left by the manteion→atropos client — (1) atropos SDK consumption of the extended register response, (2) composition validation wired into the composition creation path (A13), (3) HTTP handlers for fault spec and composition CRUD (A14) — plus five preflight ergonomic cleanups that fix naming and typing confusion before more code depends on the current shape.

**Architecture:** Three-part plan across two repos.
- **Part A:** Preflight cleanups — rename misleading `Inline*` wire types to `Compiled*`, remove redundant `Position` field (slice index is authoritative), add typed Go enums for `ExecutionMode` and `Direction`, improve depth-cap error message, add composition-level `DurationMs`/`RampUpMs`/`RampDownMs` with DB migration and wire-format mirror.
- **Part B:** Atropos SDK bootstrap — add `CompiledComposition` wire types, `RegisterRequest`/`RegisterResponse`, `Register()` HTTP function, `Apply()` function that installs the response's intent state onto existing SDK objects, E2E test.
- **Part C:** Manteion CRUD handlers — `FaultStore` interface for testability, fault spec CRUD, composition CRUD with `model.ValidateComposition` wiring, route registration.

**Tech Stack:** Go 1.25 stdlib (`net/http`, `encoding/json`, `net/http/httptest`), Go testing stdlib. No new external deps.

---

## Preflight: Existing State

**Already landed — Tasks 1, 4, 5 of the original draft are DONE:**
- `atropos-go` commit `6adec43` — `compiled_rule.go` with `CompiledRule`, `InlineFault`, `DecodeCompiledRule(s)` for inline latency/error/hang; 6 tests. (Part A Task 2 renames these types. Part B Task 7 adds composition wire types.)
- `manteion-go` commit `5d68f63` — `internal/ruleconv` compiles rules with `FaultCompositionID` into inlined wire format including nested compositions; 4 new composition tests.

These foundations are kept. The rename in Part A threads through both.

---

## Context

The manteion→atropos client (commits `f2e579c` and `5d68f63`) established transport (`internal/atropos`), orchestration (`internal/atrocontrol`), rule compilation (`internal/ruleconv`), in-memory `IntentTracker`, and the extended `POST /api/v1/sdk/register` response that returns `rules`, `active_fault`, `freeze_cfg` when intent exists.

**Three gaps remain open:**

1. **Atropos SDK doesn't consume the extended register response.** Manteion writes intent and serves it on register; atropos ignores the new fields. Without this, rolling-deploy reconciliation is half-built — new pods register but start with no faults applied, polluting measurement windows.

2. **Composition validation (A13) is only half-wired.** `FaultRepo.CreateComposition` calls basic `comp.Validate()` but not the full `model.ValidateComposition` (depth cap, direction, incompatibilities). The full validation needs resolvers, which belong in a handler — not the repo.

3. **No HTTP CRUD for fault specs or compositions (A14).** All other domains have handlers; fault specs and compositions are only reachable by direct SQL. Rules can't be built until these exist.

**Preflight ergonomic cleanups (land before the gap fixes):**
- The wire types are named `InlineFault` / `InlineComposition` / `InlineCompositionMember`. The word "Inline" here means "FK resolved, embedded inline" — but collides with `FaultSpec.Category: "inline"` which is the fault **category**. Rename to `Compiled*` for clarity.
- `FaultCompositionMember.Position` duplicates slice index; nothing enforces consistency. Remove the field — DB keeps a `position` column populated from the slice index at insert time.
- `ExecutionMode` and `Direction` are stringly-typed with runtime validation. Convert to typed Go `type Foo string` enums with constants; JSON transparent, compile-time typo catch.
- Depth-cap error is uninformative. Improve wording to reference the max and the (planned) configurability.
- Composition-level duration/ramp: add `DurationMs`/`RampUpMs`/`RampDownMs` to `FaultComposition` so operators can cap the whole composition's runtime without re-setting on every leaf. Migration v4 adds columns; wire format carries them; SDK consumption deferred (no composition evaluator yet).

---

## File Structure

**atropos-go (create):**
- `register.go` — `CompiledComposition`, `CompiledCompositionMember`, `RegisterRequest`, `RegisterResponse` types; `Register`, `Apply`, `ApplyTargets`
- `register_test.go` — unit tests + E2E test using httptest.Server

**atropos-go (modify):**
- `compiled_rule.go` — rename `InlineFault` → `CompiledFault`; `DecodeCompiledRule` returns explicit error for composition rules (until SDK composition evaluator exists)
- `compiled_rule_test.go` — rename references
- `AGENTS.md` — new SDK Bootstrap section

**manteion-go (create):**
- `internal/api/fault_handler.go` — CRUD handlers for fault specs and compositions
- `internal/api/fault_handler_test.go` — handler tests against in-memory fake repo
- `internal/api/fault_store.go` — `FaultStore` interface

**manteion-go (modify):**
- `internal/model/fault.go` — `ExecutionMode` and `Direction` typed enums; remove `FaultCompositionMember.Position`; add `FaultComposition.DurationMs`/`RampUpMs`/`RampDownMs`
- `internal/model/fault_test.go` + `composition_validate_test.go` — drop `Position:` from ~46 test cases
- `internal/ruleconv/ruleconv.go` — rename `InlineFault` → `CompiledFault` et al; drop Position from wire member; add composition duration/ramp fields; better depth error
- `internal/ruleconv/ruleconv_test.go` — rename, drop Position
- `internal/store/fault_repo.go` — use slice index instead of `m.Position`; add composition duration/ramp to read/write; accept typed enums
- `internal/db/migrations.go` — add migration v4 (composition duration/ramp columns)
- `internal/api/server.go` — add `faultStore` field; register fault handler routes
- `cmd/manteion/main.go` — pass `*store.FaultRepo` to new `faultStore` param
- `AGENTS.md` — document fault endpoints

---

## Tasks

## Part A: Preflight Ergonomic Cleanups

---

### Task 1: Rename Inline* wire types to Compiled* in manteion-go

**Files:**
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/ruleconv/ruleconv.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/ruleconv/ruleconv_test.go`

- [ ] **Step 1: Rename types and all references in `ruleconv.go`**

Apply these renames with Edit (replace_all=true for each):
- `InlineFault` → `CompiledFault`
- `InlineComposition` → `CompiledComposition`
- `InlineCompositionMember` → `CompiledCompositionMember`

The resulting `CompiledRule` now references `*CompiledFault` and `*CompiledComposition` in its fields.

- [ ] **Step 2: Apply same renames in `ruleconv_test.go`**

Same three renames, replace_all=true.

- [ ] **Step 3: Verify build + tests**

Run: `cd /Users/pronei/work/faults-lab/manteion-go && go build ./... && go test ./internal/ruleconv/`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
git add internal/ruleconv/ruleconv.go internal/ruleconv/ruleconv_test.go
git commit -m "$(cat <<'EOF'
ruleconv: rename Inline* wire types to Compiled*

'Inline' conflicted with FaultSpec.Category 'inline' (the fault kind that
acts in-request vs network toxics / resource stressors). The wire types
mean 'FK resolved, embedded inline into JSON' — not 'a composition of
inline-category faults.' Renamed to Compiled* to match CompiledRule.

No behavior change; JSON tags unchanged (inner field names, not type names).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Rename InlineFault to CompiledFault in atropos-go

**Files:**
- Modify: `/Users/pronei/work/faults-lab/atropos-go/compiled_rule.go`
- Modify: `/Users/pronei/work/faults-lab/atropos-go/compiled_rule_test.go`

- [ ] **Step 1: Rename `InlineFault` → `CompiledFault` in both files**

Use Edit with replace_all=true on each file.

- [ ] **Step 2: Verify build + tests**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go build ./... && go test ./...`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/pronei/work/faults-lab/atropos-go
git add compiled_rule.go compiled_rule_test.go
git commit -m "$(cat <<'EOF'
compiled_rule: rename InlineFault to CompiledFault

Matches manteion-go's rename; 'Inline' was colliding with fault category
'inline'. No behavior change.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Remove redundant Position field from FaultCompositionMember

Slice index is authoritative. The DB keeps its `position` column (needed for `ORDER BY` in loads), but the struct no longer exposes it; the repo writes `i` from the insert loop.

**Files:**
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/model/fault.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/model/fault_test.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/model/composition_validate_test.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/store/fault_repo.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/ruleconv/ruleconv.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/ruleconv/ruleconv_test.go` (if it references Position)

- [ ] **Step 1: Remove field from `FaultCompositionMember` in `internal/model/fault.go`**

Change:
```go
type FaultCompositionMember struct {
	Position           int    `json:"position"`
	FaultSpecID        string `json:"fault_spec_id,omitempty"`
	ChildCompositionID string `json:"child_composition_id,omitempty"`
	Direction          string `json:"direction,omitempty"`
}
```

to:
```go
type FaultCompositionMember struct {
	FaultSpecID        string `json:"fault_spec_id,omitempty"`
	ChildCompositionID string `json:"child_composition_id,omitempty"`
	Direction          string `json:"direction,omitempty"`
}
```

- [ ] **Step 2: Drop `Position:` literals from model tests**

Edit `fault_test.go` and `composition_validate_test.go`. Strategy: one sed-style pass. Use Grep to find all instances, then use Edit with replace_all to transform patterns. Example transforms needed (run Edit for each pattern; each is unique by context):

Pattern `{Position: 0, ` → `{`
Pattern `{Position: 1, ` → `{`
Pattern `{Position: 0}` → `{}` (for the lone `Position: 0` case in `fault_test.go:142`)

After edits, run `grep -n "Position:" internal/model/` — expect no hits. If any remain, remove by hand.

- [ ] **Step 3: Update `internal/store/fault_repo.go`**

In `CreateComposition` — change:
```go
for _, m := range comp.Members {
	_, err := tx.ExecContext(ctx, `...`, comp.ID, m.Position, nullString(m.FaultSpecID), ...)
	if err != nil {
		return fmt.Errorf("insert composition member[%d]: %w", m.Position, err)
	}
}
```

to:
```go
for i, m := range comp.Members {
	_, err := tx.ExecContext(ctx, `...`, comp.ID, i, nullString(m.FaultSpecID), ...)
	if err != nil {
		return fmt.Errorf("insert composition member[%d]: %w", i, err)
	}
}
```

In `GetComposition` — the member-load scan currently does `rows.Scan(&m.Position, &faultSpecID, &childCompID, &direction)`. Change to discard the position into a local:
```go
var pos int
if err := rows.Scan(&pos, &faultSpecID, &childCompID, &direction); err != nil { ... }
_ = pos // slice order = insert order via ORDER BY position
```

Confirm the SELECT includes `ORDER BY position` so slice order matches insert order. (Check existing query; add if missing.)

- [ ] **Step 4: Drop Position from `internal/ruleconv/ruleconv.go`**

In `resolveComposition`, remove `Position: m.Position,` from the `CompiledCompositionMember` construction. After this change the member is assembled as:
```go
member := CompiledCompositionMember{
	Direction: m.Direction,
}
```

And drop the `Position int` field from the `CompiledCompositionMember` struct definition (along with its `json:"position"` tag).

- [ ] **Step 5: Update ruleconv tests if they reference Position**

Run: `grep -n "Position" internal/ruleconv/ruleconv_test.go`
If any hits, remove the `Position:` assignments and any assertions about them.

- [ ] **Step 6: Run full test suite**

Run: `cd /Users/pronei/work/faults-lab/manteion-go && go build ./... && go test ./...`
Expected: PASS. If any DB-integration test fails on a `position` column still being NOT NULL, that's fine — the column is still populated (by index) in the repo, we just removed the Go struct field.

- [ ] **Step 7: Commit**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
git add internal/model/fault.go internal/model/fault_test.go internal/model/composition_validate_test.go internal/store/fault_repo.go internal/ruleconv/ruleconv.go internal/ruleconv/ruleconv_test.go
git commit -m "$(cat <<'EOF'
model: remove redundant FaultCompositionMember.Position field

Slice index is authoritative for member order. The DB's position column
stays (needed for ORDER BY on load), populated from the slice index at
insert time. Removes a field that nothing enforced to match the slice
index and that clients could set inconsistently.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Add typed ExecutionMode and Direction enums in model

Go's typed-string-constant idiom: compile-time catches typos, JSON-transparent, zero runtime cost.

**Files:**
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/model/fault.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/model/fault_test.go` (if literals need updating)
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/model/composition_validate.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/store/fault_repo.go`

- [ ] **Step 1: Add enum types at the top of `internal/model/fault.go`**

Just after the package declaration and imports:

```go
// ExecutionMode controls how composition members coordinate.
type ExecutionMode string

const (
	ExecutionParallel   ExecutionMode = "parallel"
	ExecutionSequential ExecutionMode = "sequential"
)

// IsValid reports whether the ExecutionMode is a recognized constant.
func (m ExecutionMode) IsValid() bool {
	switch m {
	case ExecutionParallel, ExecutionSequential:
		return true
	}
	return false
}

// Direction is an optional per-member tag for network faults indicating
// which atropos toxic pipe the fault attaches to. Empty means not applicable.
type Direction string

const (
	DirectionUpstream   Direction = "upstream"
	DirectionDownstream Direction = "downstream"
	DirectionNone       Direction = ""
)

// IsValid reports whether the Direction is a recognized constant (or empty).
func (d Direction) IsValid() bool {
	switch d {
	case DirectionUpstream, DirectionDownstream, DirectionNone:
		return true
	}
	return false
}
```

- [ ] **Step 2: Change the struct field types in the same file**

```go
type FaultComposition struct {
	ID            string                   `json:"id"`
	Name          string                   `json:"name"`
	ExecutionMode ExecutionMode            `json:"execution_mode"` // was string
	Members       []FaultCompositionMember `json:"members"`
	CreatedAt     time.Time                `json:"created_at"`
}

type FaultCompositionMember struct {
	FaultSpecID        string    `json:"fault_spec_id,omitempty"`
	ChildCompositionID string    `json:"child_composition_id,omitempty"`
	Direction          Direction `json:"direction,omitempty"` // was string
}
```

- [ ] **Step 3: Update `Validate()` in `fault.go` to use the new enum methods**

Change:
```go
if c.ExecutionMode != "parallel" && c.ExecutionMode != "sequential" {
	return fmt.Errorf("fault composition: invalid execution_mode %q", c.ExecutionMode)
}
```

to:
```go
if !c.ExecutionMode.IsValid() {
	return fmt.Errorf("fault composition: invalid execution_mode %q", c.ExecutionMode)
}
```

And change:
```go
if m.Direction != "" && m.Direction != "upstream" && m.Direction != "downstream" {
	return fmt.Errorf("invalid direction %q", m.Direction)
}
```

to:
```go
if !m.Direction.IsValid() {
	return fmt.Errorf("invalid direction %q", m.Direction)
}
```

- [ ] **Step 4: Update `composition_validate.go` to compare against new constants**

Run: `grep -n '"parallel"\|"sequential"\|"upstream"\|"downstream"' internal/model/composition_validate.go`

For each hit, update the comparison or conversion. Example:
```go
if comp.ExecutionMode == "parallel" { ... }
```
becomes:
```go
if comp.ExecutionMode == ExecutionParallel { ... }
```

- [ ] **Step 5: Update `store/fault_repo.go`**

The repo uses `string(comp.ExecutionMode)` implicitly via the DB driver. Since `type ExecutionMode string` is a string under the hood, Postgres-side conversion is transparent. No changes needed unless there's a type error — verify with build.

If build fails, add explicit conversions: `string(comp.ExecutionMode)` in ExecContext args, `var mode string; scan into it; comp.ExecutionMode = ExecutionMode(mode)` on read.

- [ ] **Step 6: Check tests for direct string-literal comparisons that need conversion**

Run: `grep -n "ExecutionMode\|Direction:" internal/model/`
If tests have `ExecutionMode: "parallel"`, they still compile — string literals assign to a defined-string type just fine. No changes needed.

- [ ] **Step 7: Build and test**

Run: `cd /Users/pronei/work/faults-lab/manteion-go && go build ./... && go test ./...`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
git add internal/model/fault.go internal/model/composition_validate.go internal/store/fault_repo.go
git commit -m "$(cat <<'EOF'
model: typed enums for ExecutionMode and Direction

Replaces runtime string comparisons with compile-time-checked typed-string
constants. JSON serialization unchanged (defined-string types). Validate()
now uses IsValid() methods; downstream compare against the constants.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Improve depth-cap error message in ruleconv

**Files:**
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/ruleconv/ruleconv.go`

- [ ] **Step 1: Change the depth error message**

In `resolveComposition`, change:
```go
if depth >= maxCompositionDepth {
	return nil, fmt.Errorf("composition %q exceeds max depth %d", id, maxCompositionDepth)
}
```

to:
```go
if depth >= maxCompositionDepth {
	return nil, fmt.Errorf(
		"composition %q nesting exceeds max depth %d (atoms→groups→top-level). "+
			"The cap is enforced at resolution time and may be raised in a future revision.",
		id, maxCompositionDepth,
	)
}
```

- [ ] **Step 2: Update the test that checks this error**

Run: `grep -n "exceeds max depth" internal/ruleconv/ruleconv_test.go`
Update any test that string-matches the old message. Prefer `errors.Is` or `strings.Contains("nesting exceeds max depth")` to survive future wording changes.

- [ ] **Step 3: Run ruleconv tests**

Run: `cd /Users/pronei/work/faults-lab/manteion-go && go test ./internal/ruleconv/`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
git add internal/ruleconv/ruleconv.go internal/ruleconv/ruleconv_test.go
git commit -m "$(cat <<'EOF'
ruleconv: improve depth-cap error message

Now surfaces the three-level intent (atoms→groups→top-level) and notes
the cap may be raised later, so future tuning doesn't surprise readers.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Add composition-level Duration/Ramp fields + migration v4 + wire mirror

**Files:**
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/model/fault.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/db/migrations.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/store/fault_repo.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/ruleconv/ruleconv.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/ruleconv/ruleconv_test.go`

- [ ] **Step 1: Add fields to `FaultComposition` in `internal/model/fault.go`**

```go
type FaultComposition struct {
	ID            string                   `json:"id"`
	Name          string                   `json:"name"`
	ExecutionMode ExecutionMode            `json:"execution_mode"`
	DurationMs    int64                    `json:"duration_ms,omitempty"`
	RampUpMs      int64                    `json:"ramp_up_ms,omitempty"`
	RampDownMs    int64                    `json:"ramp_down_ms,omitempty"`
	Members       []FaultCompositionMember `json:"members"`
	CreatedAt     time.Time                `json:"created_at"`
}
```

- [ ] **Step 2: Add migration v4 in `internal/db/migrations.go`**

Append to the `migrations` slice:
```go
{4, "add fault_composition duration/ramp columns", `
ALTER TABLE fault_compositions
    ADD COLUMN IF NOT EXISTS duration_ms BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ramp_up_ms BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ramp_down_ms BIGINT DEFAULT 0;
`},
```

- [ ] **Step 3: Update repo INSERT and SELECT for compositions**

In `CreateComposition`, change the composition INSERT to include the new columns:
```go
_, err := tx.ExecContext(ctx, `
	INSERT INTO fault_compositions (id, name, execution_mode, duration_ms, ramp_up_ms, ramp_down_ms, created_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7)`,
	comp.ID, comp.Name, comp.ExecutionMode,
	comp.DurationMs, comp.RampUpMs, comp.RampDownMs,
	comp.CreatedAt,
)
```

In `GetComposition` and `ListCompositions` (locate via grep), update SELECT lists and Scan targets:
```go
SELECT id, name, execution_mode, duration_ms, ramp_up_ms, ramp_down_ms, created_at
FROM fault_compositions WHERE id = $1
```
and
```go
.Scan(&comp.ID, &comp.Name, &comp.ExecutionMode,
	&comp.DurationMs, &comp.RampUpMs, &comp.RampDownMs,
	&comp.CreatedAt)
```

- [ ] **Step 4: Add fields to `CompiledComposition` wire struct in `internal/ruleconv/ruleconv.go`**

```go
type CompiledComposition struct {
	Name          string                      `json:"name"`
	ExecutionMode string                      `json:"execution_mode"`
	DurationMs    int64                       `json:"duration_ms,omitempty"`
	RampUpMs      int64                       `json:"ramp_up_ms,omitempty"`
	RampDownMs    int64                       `json:"ramp_down_ms,omitempty"`
	Members       []CompiledCompositionMember `json:"members"`
}
```

Note: `ExecutionMode` stays `string` on the wire (JSON is stringly typed anyway; manteion emits, SDK decodes opaquely).

- [ ] **Step 5: Propagate in `resolveComposition`**

```go
ic := &CompiledComposition{
	Name:          comp.Name,
	ExecutionMode: string(comp.ExecutionMode),
	DurationMs:    comp.DurationMs,
	RampUpMs:      comp.RampUpMs,
	RampDownMs:    comp.RampDownMs,
	Members:       make([]CompiledCompositionMember, len(comp.Members)),
}
```

- [ ] **Step 6: Write a failing test in `ruleconv_test.go` that checks duration/ramp survive compilation**

Append:
```go
func TestCompileRule_CompositionDurationRamp(t *testing.T) {
	specs := mapSpecResolver{
		"f1": {ID: "f1", Category: "inline", FaultType: "latency", Config: json.RawMessage(`{"delay":"50ms"}`)},
		"f2": {ID: "f2", Category: "inline", FaultType: "error", Config: json.RawMessage(`{"status_code":500}`)},
	}
	comps := mapCompResolver{
		"c1": {
			ID: "c1", Name: "storm", ExecutionMode: model.ExecutionParallel,
			DurationMs: 30000, RampUpMs: 5000, RampDownMs: 5000,
			Members: []model.FaultCompositionMember{
				{FaultSpecID: "f1"},
				{FaultSpecID: "f2"},
			},
		},
	}
	r := &model.Rule{ID: "r1", Name: "r", FaultCompositionID: "c1"}

	out, err := CompileRule(r, specs, comps)
	if err != nil {
		t.Fatalf("CompileRule: %v", err)
	}
	if out.Composition == nil {
		t.Fatal("expected Composition")
	}
	if out.Composition.DurationMs != 30000 {
		t.Errorf("DurationMs = %d", out.Composition.DurationMs)
	}
	if out.Composition.RampUpMs != 5000 || out.Composition.RampDownMs != 5000 {
		t.Errorf("ramp = up:%d down:%d", out.Composition.RampUpMs, out.Composition.RampDownMs)
	}
}
```

- [ ] **Step 7: Run ruleconv + store tests**

Run: `cd /Users/pronei/work/faults-lab/manteion-go && go test ./internal/ruleconv/ ./internal/store/ ./internal/model/`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
git add internal/model/fault.go internal/db/migrations.go internal/store/fault_repo.go internal/ruleconv/ruleconv.go internal/ruleconv/ruleconv_test.go
git commit -m "$(cat <<'EOF'
model: composition-level duration/ramp + wire mirror

Adds DurationMs, RampUpMs, RampDownMs to FaultComposition so operators can
bound the whole composition's runtime without setting matching values on
every leaf. Migration v4 adds columns. Wire format (CompiledComposition)
mirrors; SDK consumption is deferred until a composition evaluator exists.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Part B: Atropos SDK Bootstrap

---

### Task 7: Add CompiledComposition wire types to atropos-go

Atropos's `compiled_rule.go` already has `CompiledRule` with `*CompiledFault` (renamed in Task 2). This adds the composition branch; `DecodeCompiledRule` returns an error for composition rules until an SDK composition evaluator exists.

**Files:**
- Modify: `/Users/pronei/work/faults-lab/atropos-go/compiled_rule.go`
- Modify: `/Users/pronei/work/faults-lab/atropos-go/compiled_rule_test.go`

- [ ] **Step 1: Add composition field to `CompiledRule` and new types to `compiled_rule.go`**

Add to the `CompiledRule` struct:
```go
type CompiledRule struct {
	Name           string               `json:"name"`
	InjectionPoint string               `json:"injection_point,omitempty"`
	Labels         map[string]string    `json:"labels,omitempty"`
	Mode           string               `json:"mode"`
	Priority       int                  `json:"priority"`
	Fault          *CompiledFault       `json:"fault,omitempty"`
	Composition    *CompiledComposition `json:"composition,omitempty"`
}
```

Add new types after `CompiledFault`:
```go
// CompiledComposition is a resolved FaultComposition tree with all specs
// inlined. Composition execution is not yet supported by the SDK evaluator;
// DecodeCompiledRule errors if a rule references one.
type CompiledComposition struct {
	Name          string                      `json:"name"`
	ExecutionMode string                      `json:"execution_mode"`
	DurationMs    int64                       `json:"duration_ms,omitempty"`
	RampUpMs      int64                       `json:"ramp_up_ms,omitempty"`
	RampDownMs    int64                       `json:"ramp_down_ms,omitempty"`
	Members       []CompiledCompositionMember `json:"members"`
}

type CompiledCompositionMember struct {
	Direction   string               `json:"direction,omitempty"`
	Fault       *CompiledFault       `json:"fault,omitempty"`
	Composition *CompiledComposition `json:"composition,omitempty"`
}
```

- [ ] **Step 2: Update `DecodeCompiledRule` to reject compositions explicitly**

At the end of the function (before the final `return sr, nil`), insert:
```go
if cr.Composition != nil {
	return StaticRule{}, fmt.Errorf(
		"rule %q references a composition; SDK composition evaluator not yet implemented",
		cr.Name,
	)
}
```

- [ ] **Step 3: Add a test for the composition-rejection error**

Append to `compiled_rule_test.go`:
```go
func TestDecodeCompiledRules_CompositionRejected(t *testing.T) {
	compiled := []CompiledRule{{
		Name: "comp-rule",
		Mode: "inline",
		Composition: &CompiledComposition{
			Name: "x", ExecutionMode: "parallel",
			Members: []CompiledCompositionMember{
				{Fault: &CompiledFault{Category: "inline", FaultType: "latency", Config: json.RawMessage(`{"delay":"10ms"}`)}},
				{Fault: &CompiledFault{Category: "inline", FaultType: "error", Config: json.RawMessage(`{"status_code":500}`)}},
			},
		},
	}}

	_, err := DecodeCompiledRules(compiled)
	if err == nil {
		t.Fatal("expected error for composition rule")
	}
}
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go test ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/pronei/work/faults-lab/atropos-go
git add compiled_rule.go compiled_rule_test.go
git commit -m "$(cat <<'EOF'
compiled_rule: add CompiledComposition wire types

Adds the composition branch to CompiledRule mirroring manteion's wire
format. DecodeCompiledRule explicitly errors on composition rules —
the SDK has no composition evaluator yet, so silently dropping the rule
would pollute measurement windows.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Add RegisterRequest / RegisterResponse types in atropos-go

**Files:**
- Create: `/Users/pronei/work/faults-lab/atropos-go/register.go`

- [ ] **Step 1: Create `register.go` with types only**

```go
package atropos

import "time"

// RegisterRequest is the POST body for /api/v1/sdk/register.
type RegisterRequest struct {
	ID      string `json:"id"`
	Service string `json:"service"`
	Version string `json:"version,omitempty"`
	Address string `json:"address"`
}

// RegisterResponse is the JSON manteion returns from /api/v1/sdk/register.
// Rules, ActiveFault, and FreezeCfg are populated only when manteion has
// intent tracked for the registering service — e.g. during a rolling deploy
// while an experiment is in progress.
type RegisterResponse struct {
	Status      string         `json:"status"`
	Rules       []CompiledRule `json:"rules,omitempty"`
	ActiveFault *FaultRequest  `json:"active_fault,omitempty"`
	FreezeCfg   *DelayRequest  `json:"freeze_cfg,omitempty"`
}

// registerTimeout is the default per-call deadline for Register.
const registerTimeout = 5 * time.Second
```

- [ ] **Step 2: Build to confirm types compile**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go build ./...`
Expected: success.

- [ ] **Step 3: Commit**

```bash
cd /Users/pronei/work/faults-lab/atropos-go
git add register.go
git commit -m "$(cat <<'EOF'
register: add RegisterRequest and RegisterResponse wire types

Types only — Register() and Apply() follow in subsequent commits.
FaultRequest and DelayRequest were already exported in 0f4bc6b.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Write failing Register test

**Files:**
- Create: `/Users/pronei/work/faults-lab/atropos-go/register_test.go`

- [ ] **Step 1: Create `register_test.go` with two subtests**

```go
package atropos_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	atropos "atropos-go"
)

func TestRegister_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/sdk/register" {
			t.Errorf("path = %s, want /api/v1/sdk/register", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		var req atropos.RegisterRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Service != "productcatalog" {
			t.Errorf("service = %q, want productcatalog", req.Service)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(atropos.RegisterResponse{
			Status: "registered",
			Rules: []atropos.CompiledRule{{
				Name:           "freeze-productcatalog",
				InjectionPoint: "egress",
				Mode:           "inline",
				Priority:       10,
				Fault: &atropos.CompiledFault{
					Category:  "inline",
					FaultType: "latency",
					Config:    json.RawMessage(`{"delay":"200ms"}`),
				},
			}},
		})
	}))
	defer server.Close()

	resp, err := atropos.Register(context.Background(), server.URL, atropos.RegisterRequest{
		ID:      "pod-abc",
		Service: "productcatalog",
		Address: "http://10.0.3.4:9090",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if resp.Status != "registered" {
		t.Errorf("status = %q, want registered", resp.Status)
	}
	if len(resp.Rules) != 1 {
		t.Fatalf("rules = %d, want 1", len(resp.Rules))
	}
	if resp.Rules[0].Fault == nil {
		t.Fatal("expected Fault to be set on compiled rule")
	}
}

func TestRegister_NonCreatedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
	}))
	defer server.Close()

	_, err := atropos.Register(context.Background(), server.URL, atropos.RegisterRequest{
		ID:      "pod-abc",
		Service: "productcatalog",
		Address: "http://10.0.3.4:9090",
	})
	if err == nil {
		t.Fatal("expected error for non-201 response")
	}
}
```

- [ ] **Step 2: Run test — expect compile failure**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go test -run TestRegister`
Expected: FAIL with `undefined: atropos.Register`.

---

### Task 10: Implement Register

**Files:**
- Modify: `/Users/pronei/work/faults-lab/atropos-go/register.go`

- [ ] **Step 1: Update imports and add Register function**

Replace the imports block at the top of `register.go`:
```go
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)
```

Append after the `registerTimeout` const:
```go
// Register POSTs a registration to manteion and returns the decoded response.
// The returned response may contain rules, active_fault, and freeze_cfg if
// manteion has intent tracked for the registering service.
//
// baseURL is manteion's base URL (e.g. "http://manteion.control.svc:8080").
// The request is subject to registerTimeout (5s) unless ctx has an earlier deadline.
func Register(ctx context.Context, baseURL string, req RegisterRequest) (RegisterResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return RegisterResponse{}, fmt.Errorf("marshal register request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, registerTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/api/v1/sdk/register", bytes.NewReader(body))
	if err != nil {
		return RegisterResponse{}, fmt.Errorf("new register request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return RegisterResponse{}, fmt.Errorf("send register request: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(httpResp.Body, 1<<20)) // 1 MiB cap
	if httpResp.StatusCode != http.StatusCreated {
		return RegisterResponse{}, fmt.Errorf("register returned status %d: %s",
			httpResp.StatusCode, string(respBody))
	}

	var resp RegisterResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return RegisterResponse{}, fmt.Errorf("decode register response: %w", err)
	}
	return resp, nil
}
```

- [ ] **Step 2: Run Register tests**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go test -run TestRegister -v`
Expected: both subtests PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/pronei/work/faults-lab/atropos-go
git add register.go register_test.go
git commit -m "$(cat <<'EOF'
register: add Register() for SDK → manteion registration

POSTs a RegisterRequest to baseURL + /api/v1/sdk/register with a 5s default
timeout, decodes the RegisterResponse (which may include intent state from
manteion's IntentTracker), and returns it. HTTP failure, non-201 status,
and body decode error all surface as typed errors.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 11: Write failing Apply tests

**Files:**
- Modify: `/Users/pronei/work/faults-lab/atropos-go/register_test.go`

- [ ] **Step 1: Append Apply tests**

```go
func TestApply_SetsRules(t *testing.T) {
	eval := atropos.NewStaticEvaluator()
	resp := atropos.RegisterResponse{
		Rules: []atropos.CompiledRule{{
			Name:           "r1",
			InjectionPoint: "egress",
			Mode:           "inline",
			Fault: &atropos.CompiledFault{
				Category:  "inline",
				FaultType: "latency",
				Config:    json.RawMessage(`{"delay":"100ms"}`),
			},
		}},
	}

	if err := atropos.Apply(resp, atropos.ApplyTargets{Evaluator: eval}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	rules := eval.Rules()
	if len(rules) != 1 {
		t.Fatalf("rules set = %d, want 1", len(rules))
	}
	if rules[0].Name != "r1" {
		t.Errorf("rule name = %q", rules[0].Name)
	}
}

func TestApply_NoRulesIsNoop(t *testing.T) {
	eval := atropos.NewStaticEvaluator(atropos.StaticRule{Name: "preexisting", Point: atropos.Ingress})
	resp := atropos.RegisterResponse{Status: "registered"}
	if err := atropos.Apply(resp, atropos.ApplyTargets{Evaluator: eval}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	rules := eval.Rules()
	if len(rules) != 1 || rules[0].Name != "preexisting" {
		t.Errorf("Apply clobbered existing rules when response had none: got %+v", rules)
	}
}

func TestApply_RulesWithoutEvaluatorErrors(t *testing.T) {
	resp := atropos.RegisterResponse{
		Rules: []atropos.CompiledRule{{Name: "r1", InjectionPoint: "egress", Mode: "inline"}},
	}
	err := atropos.Apply(resp, atropos.ApplyTargets{})
	if err == nil {
		t.Fatal("expected error: rules present but no Evaluator target")
	}
}
```

- [ ] **Step 2: Run test — expect compile failure**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go test -run TestApply`
Expected: FAIL with `undefined: atropos.Apply` or `undefined: atropos.ApplyTargets`.

---

### Task 12: Implement Apply + ApplyTargets

Before writing code, the implementer must locate the actual SDK method names — spec uses placeholder names. Run (inside atropos-go):

- `grep -n "func .* NewStaticEvaluator\|func .*SetRules\|func .*Rules()" *.go`
- `grep -n "DemoEvaluator" *.go`
- `grep -n "CacheBox\|SetDelaySource\|DistributionDelaySource" *.go cachebox_admin.go`

Use the actual names. The code below is a template; adjust method calls to match what actually exists.

**Files:**
- Modify: `/Users/pronei/work/faults-lab/atropos-go/register.go`

- [ ] **Step 1: Add `ApplyTargets` struct and `Apply` function**

```go
// ApplyTargets names the SDK objects Apply mutates. Nil targets mean "this SDK
// instance doesn't support that capability"; if the response references that
// capability, Apply returns an error.
type ApplyTargets struct {
	// Evaluator receives the decoded rule set. Required if resp.Rules is non-empty.
	Evaluator *StaticEvaluator
	// DemoEval receives the active fault. Required if resp.ActiveFault is non-nil.
	DemoEval *DemoEvaluator
	// CacheBox receives the freeze config. Required if resp.FreezeCfg is non-nil.
	CacheBox *CacheBox
}

// Apply installs the register response's intent state onto the supplied
// targets. Each category (rules, active fault, freeze config) is independent.
// Returns an error if the response carries a category but the corresponding
// target is nil, or if any component fails to apply.
//
// Rules with empty slice are treated as 'no change' — only a populated rule
// list replaces the evaluator's current rules.
func Apply(resp RegisterResponse, targets ApplyTargets) error {
	if len(resp.Rules) > 0 {
		if targets.Evaluator == nil {
			return fmt.Errorf("apply: response has %d rules but no Evaluator target", len(resp.Rules))
		}
		rules, err := DecodeCompiledRules(resp.Rules)
		if err != nil {
			return fmt.Errorf("apply rules: %w", err)
		}
		targets.Evaluator.SetRules(rules)
	}

	if resp.ActiveFault != nil {
		if targets.DemoEval == nil {
			return fmt.Errorf("apply: response has active_fault but no DemoEval target")
		}
		if err := applyActiveFault(*resp.ActiveFault, targets.DemoEval); err != nil {
			return fmt.Errorf("apply active_fault: %w", err)
		}
	}

	if resp.FreezeCfg != nil {
		if targets.CacheBox == nil {
			return fmt.Errorf("apply: response has freeze_cfg but no CacheBox target")
		}
		if err := applyFreezeCfg(*resp.FreezeCfg, targets.CacheBox); err != nil {
			return fmt.Errorf("apply freeze_cfg: %w", err)
		}
	}

	return nil
}
```

- [ ] **Step 2: Add `applyActiveFault` and `applyFreezeCfg` helpers**

These must mirror the fault factory in `admin.go` (type dispatch on `FaultRequest.Type`). Find the relevant code:
- `grep -n "case \"latency\"\|case \"error\"\|case \"hang\"" admin.go`
- Check `cachebox_admin.go` for how `DelayRequest` becomes a delay source.

Then append:

```go
// applyActiveFault builds a Fault from a FaultRequest and installs it on the
// DemoEvaluator. Mirrors the fault-type dispatch in admin.go.
func applyActiveFault(req FaultRequest, eval *DemoEvaluator) error {
	// IMPLEMENTER NOTE: the exact construction here must match what admin.go's
	// handleFaultPost does. If admin.go already exposes a helper like
	// `buildFaultFromRequest`, call it; otherwise duplicate the dispatch.
	// See admin.go for the source of truth.
	//
	// Pseudo-template (adjust to actual names):
	//   switch req.Type {
	//   case "latency": f = NewLatencyFault(...)
	//   case "error":   f = NewErrorFault(...)
	//   case "hang":    f = NewHangFault(...)
	//   }
	//   eval.Set(&Decision{Fault: f, Reason: "register", Mode: Inline}, &req)
	return fmt.Errorf("applyActiveFault not yet implemented — implementer must mirror admin.go")
}

// applyFreezeCfg installs a distribution delay source on the CacheBox from a
// DelayRequest. Mirrors cachebox_admin.go's handler.
func applyFreezeCfg(req DelayRequest, cb *CacheBox) error {
	// IMPLEMENTER NOTE: see cachebox_admin.go for the exact construction.
	// The existing admin handler parses mu/sigma/seed from the request and
	// calls cachebox.NewDistributionDelaySource(...).
	return fmt.Errorf("applyFreezeCfg not yet implemented — implementer must mirror cachebox_admin.go")
}
```

**Implementer:** replace the stubs with the real dispatch by reading `admin.go` and `cachebox_admin.go`. The two functions above are intentionally stubs so you do the grep work yourself — the plan cannot guess the exact constructor names.

- [ ] **Step 3: Run Apply tests**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go test -run TestApply -v`
Expected: `TestApply_SetsRules` and `TestApply_RulesWithoutEvaluatorErrors` PASS (neither depends on the helper stubs). `TestApply_NoRulesIsNoop` PASS.

- [ ] **Step 4: Run full atropos test suite**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go test ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/pronei/work/faults-lab/atropos-go
git add register.go register_test.go
git commit -m "$(cat <<'EOF'
register: add Apply() to install RegisterResponse intent state

Applies rules via StaticEvaluator.SetRules, active fault via DemoEvaluator,
and freeze config via CacheBox. Each category is independent; missing
targets for populated categories error out. Empty rule slices are 'no
change' to avoid clobbering preexisting local state.

applyActiveFault and applyFreezeCfg now mirror admin.go's and
cachebox_admin.go's fault factories.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 13: End-to-end Register + Apply test

**Files:**
- Modify: `/Users/pronei/work/faults-lab/atropos-go/register_test.go`

- [ ] **Step 1: Append E2E test**

```go
func TestRegisterAndApply_E2E(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(atropos.RegisterResponse{
			Status: "registered",
			Rules: []atropos.CompiledRule{{
				Name:           "freeze-productcatalog",
				InjectionPoint: "egress",
				Labels:         map[string]string{"target": "productcatalog"},
				Mode:           "inline",
				Priority:       10,
				Fault: &atropos.CompiledFault{
					Category:  "inline",
					FaultType: "latency",
					Config:    json.RawMessage(`{"delay":"50ms"}`),
				},
			}},
		})
	}))
	defer server.Close()

	eval := atropos.NewStaticEvaluator()

	resp, err := atropos.Register(context.Background(), server.URL, atropos.RegisterRequest{
		ID:      "pod-abc",
		Service: "productcatalog",
		Address: "http://10.0.3.4:9090",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := atropos.Apply(resp, atropos.ApplyTargets{Evaluator: eval}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	rules := eval.Rules()
	if len(rules) != 1 {
		t.Fatalf("rules after apply = %d, want 1", len(rules))
	}
	if rules[0].Name != "freeze-productcatalog" {
		t.Errorf("rule name = %q", rules[0].Name)
	}
	if rules[0].Decision.Fault == nil {
		t.Error("expected Decision.Fault to be set")
	}
}
```

- [ ] **Step 2: Run**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go test -run TestRegisterAndApply_E2E -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/pronei/work/faults-lab/atropos-go
git add register_test.go
git commit -m "$(cat <<'EOF'
register: E2E test for Register + Apply against fake manteion

Verifies the full bootstrap path: SDK registers, manteion returns intent,
SDK decodes and applies rules to its StaticEvaluator. The eval's rule set
after Apply matches what manteion sent.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 14: Document register in atropos AGENTS.md

**Files:**
- Modify: `/Users/pronei/work/faults-lab/atropos-go/AGENTS.md`

- [ ] **Step 1: Locate the Admin Handlers section**

Run: `grep -n "^## " AGENTS.md` — identify where to add the new section (after Admin Handlers, before any closing section).

- [ ] **Step 2: Insert a new section**

```markdown
## SDK Bootstrap

Services embedding atropos-go register with manteion on startup so manteion can serve them rules and reconcile intent on rolling deploys.

### Types

- `RegisterRequest{ID, Service, Version, Address}` — the POST body.
- `RegisterResponse{Status, Rules, ActiveFault, FreezeCfg}` — the response. Rules/ActiveFault/FreezeCfg are populated when manteion has intent tracked for the service.
- `CompiledRule`, `CompiledFault`, `CompiledComposition`, `CompiledCompositionMember` — the JSON wire format for rules, mirroring `manteion-go/internal/ruleconv`. `CompiledComposition` is carried on the wire but not yet executable on the SDK side; `DecodeCompiledRules` errors on composition rules.

### Functions

- `Register(ctx, manteionURL, req) (RegisterResponse, error)` — POSTs to `manteionURL + /api/v1/sdk/register` with a 5s default timeout.
- `Apply(resp, ApplyTargets{Evaluator, DemoEval, CacheBox}) error` — installs rules, active fault, and freeze config onto the provided SDK objects. Missing targets for populated response fields are errors.
- `DecodeCompiledRules([]CompiledRule) ([]StaticRule, error)` — lower-level helper used by Apply.

### Typical Usage

```go
eval := atropos.NewStaticEvaluator()
demo := &atropos.DemoEvaluator{}
cb := atropos.NewCacheBox(atropos.CacheBoxConfig{Store: atropos.NewCacheBoxMemStore(1024)})

atropos.Configure(atropos.WithEvaluator(eval), atropos.WithCacheBoxCoordinator(cb))

resp, err := atropos.Register(ctx, os.Getenv("ATROPOS_MANTEION_URL"), atropos.RegisterRequest{
    ID:      os.Getenv("POD_NAME"),
    Service: os.Getenv("SERVICE_NAME"),
    Address: fmt.Sprintf("http://%s:9090", os.Getenv("POD_IP")),
})
if err != nil {
    log.Fatalf("register: %v", err)
}
if err := atropos.Apply(resp, atropos.ApplyTargets{Evaluator: eval, DemoEval: demo, CacheBox: cb}); err != nil {
    log.Fatalf("apply: %v", err)
}
```

### Limitations (current)

- `DecodeCompiledRules` supports only `inline` fault category (latency, error, hang). `network` and `resource` decoding returns explicit errors.
- Composition rules are rejected on decode — the SDK has no composition evaluator yet.
```

- [ ] **Step 3: Commit**

```bash
cd /Users/pronei/work/faults-lab/atropos-go
git add AGENTS.md
git commit -m "$(cat <<'EOF'
docs: document SDK bootstrap (Register, Apply, Compiled* wire types)

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Part C: Manteion Fault CRUD Handlers

---

### Task 15: Write failing tests for fault handlers

**Files:**
- Create: `/Users/pronei/work/faults-lab/manteion-go/internal/api/fault_handler_test.go`

- [ ] **Step 1: Inspect existing API-test pattern**

Run: `ls /Users/pronei/work/faults-lab/manteion-go/internal/api/*_test.go`

If there are no test files, this is the first — use in-memory fakes (below). If there are existing tests, match their pattern.

- [ ] **Step 2: Create the test file**

```go
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"manteion-go/internal/model"
)

// fakeFaultRepo is a minimal in-memory FaultStore implementation for handler tests.
type fakeFaultRepo struct {
	specs map[string]*model.FaultSpec
	comps map[string]*model.FaultComposition
	err   error
}

func newFakeFaultRepo() *fakeFaultRepo {
	return &fakeFaultRepo{
		specs: make(map[string]*model.FaultSpec),
		comps: make(map[string]*model.FaultComposition),
	}
}

func (f *fakeFaultRepo) CreateSpec(ctx context.Context, spec *model.FaultSpec) error {
	if f.err != nil {
		return f.err
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	f.specs[spec.ID] = spec
	return nil
}

func (f *fakeFaultRepo) GetSpec(ctx context.Context, id string) (*model.FaultSpec, error) {
	if s, ok := f.specs[id]; ok {
		return s, nil
	}
	return nil, errFakeNotFound
}

func (f *fakeFaultRepo) ListSpecs(ctx context.Context) ([]*model.FaultSpec, error) {
	out := make([]*model.FaultSpec, 0, len(f.specs))
	for _, s := range f.specs {
		out = append(out, s)
	}
	return out, nil
}

func (f *fakeFaultRepo) DeleteSpec(ctx context.Context, id string) error {
	if _, ok := f.specs[id]; !ok {
		return errFakeNotFound
	}
	delete(f.specs, id)
	return nil
}

func (f *fakeFaultRepo) CreateComposition(ctx context.Context, c *model.FaultComposition) error {
	if f.err != nil {
		return f.err
	}
	f.comps[c.ID] = c
	return nil
}

func (f *fakeFaultRepo) GetComposition(ctx context.Context, id string) (*model.FaultComposition, error) {
	if c, ok := f.comps[id]; ok {
		return c, nil
	}
	return nil, errFakeNotFound
}

func (f *fakeFaultRepo) ListCompositions(ctx context.Context) ([]*model.FaultComposition, error) {
	out := make([]*model.FaultComposition, 0, len(f.comps))
	for _, c := range f.comps {
		out = append(out, c)
	}
	return out, nil
}

func (f *fakeFaultRepo) DeleteComposition(ctx context.Context, id string) error {
	if _, ok := f.comps[id]; !ok {
		return errFakeNotFound
	}
	delete(f.comps, id)
	return nil
}

func (f *fakeFaultRepo) SpecResolver(ctx context.Context) model.FaultSpecResolver {
	return func(id string) *model.FaultSpec { return f.specs[id] }
}

func (f *fakeFaultRepo) CompositionResolver(ctx context.Context) model.CompositionResolver {
	return func(id string) *model.FaultComposition { return f.comps[id] }
}

var errFakeNotFound = notFoundErr{}

type notFoundErr struct{}

func (notFoundErr) Error() string { return "not found" }

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHandleCreateFaultSpec(t *testing.T) {
	repo := newFakeFaultRepo()
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/faults/specs", s.handleCreateFaultSpec)

	body := `{
		"id":"spec-1",
		"name":"200ms latency",
		"category":"inline",
		"fault_type":"latency",
		"config":{"delay":"200ms"}
	}`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/faults/specs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	if repo.specs["spec-1"] == nil {
		t.Fatal("spec not stored")
	}
}

func TestHandleCreateFaultSpec_ValidationError(t *testing.T) {
	repo := newFakeFaultRepo()
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/faults/specs", s.handleCreateFaultSpec)

	body := `{"id":"spec-2","name":"x","category":"inline","fault_type":"nonsense","config":{}}`

	req := httptest.NewRequest(http.MethodPost, "/api/v1/faults/specs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestHandleCreateFaultComposition_ValidationCalled(t *testing.T) {
	repo := newFakeFaultRepo()
	repo.specs["spec-a"] = &model.FaultSpec{
		ID: "spec-a", Name: "latency", Category: "inline", FaultType: "latency",
		Config: json.RawMessage(`{"delay":"100ms"}`),
	}
	repo.specs["spec-b"] = &model.FaultSpec{
		ID: "spec-b", Name: "error", Category: "inline", FaultType: "error",
		Config: json.RawMessage(`{"status_code":500}`),
	}

	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/faults/compositions", s.handleCreateFaultComposition)

	body := `{
		"id":"comp-1","name":"lat+err","execution_mode":"parallel",
		"members":[
			{"fault_spec_id":"spec-a"},
			{"fault_spec_id":"spec-b"}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/faults/compositions", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", w.Code, w.Body.String())
	}
	if repo.comps["comp-1"] == nil {
		t.Fatal("composition not stored")
	}
}

func TestHandleCreateFaultComposition_DanglingSpec(t *testing.T) {
	repo := newFakeFaultRepo()
	s := &Server{faultStore: repo, logger: discardLogger()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/faults/compositions", s.handleCreateFaultComposition)

	body := `{
		"id":"comp-2","name":"bad","execution_mode":"parallel",
		"members":[
			{"fault_spec_id":"does-not-exist"},
			{"fault_spec_id":"also-missing"}
		]
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/faults/compositions", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", w.Code, w.Body.String())
	}
	if repo.comps["comp-2"] != nil {
		t.Fatal("dangling composition should not have been stored")
	}
}
```

- [ ] **Step 3: Run — expect compile failure**

Run: `cd /Users/pronei/work/faults-lab/manteion-go && go test ./internal/api/ -run TestHandleCreateFaultSpec`
Expected: FAIL with `undefined: Server.faultStore` or `undefined: handleCreateFaultSpec`.

---

### Task 16: Define FaultStore interface and wire Server field

**Files:**
- Create: `/Users/pronei/work/faults-lab/manteion-go/internal/api/fault_store.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/api/server.go`
- Modify: `/Users/pronei/work/faults-lab/manteion-go/cmd/manteion/main.go`

- [ ] **Step 1: Create `fault_store.go` with the interface**

```go
package api

import (
	"context"

	"manteion-go/internal/model"
)

// FaultStore is the subset of FaultRepo used by the fault handler.
// Defined as an interface so tests can inject a fake without a live postgres.
type FaultStore interface {
	CreateSpec(ctx context.Context, spec *model.FaultSpec) error
	GetSpec(ctx context.Context, id string) (*model.FaultSpec, error)
	ListSpecs(ctx context.Context) ([]*model.FaultSpec, error)
	DeleteSpec(ctx context.Context, id string) error

	CreateComposition(ctx context.Context, c *model.FaultComposition) error
	GetComposition(ctx context.Context, id string) (*model.FaultComposition, error)
	ListCompositions(ctx context.Context) ([]*model.FaultComposition, error)
	DeleteComposition(ctx context.Context, id string) error

	SpecResolver(ctx context.Context) model.FaultSpecResolver
	CompositionResolver(ctx context.Context) model.CompositionResolver
}
```

- [ ] **Step 2: Add `faultStore FaultStore` field to `Server` in `internal/api/server.go`**

Read the existing `Server` struct, then add the new field alongside the existing `faults *store.FaultRepo` field. Both can coexist — the handler uses the interface, existing consumers (poll handler) stay on the concrete type.

Update `NewServer(...)` to accept a `faultStore FaultStore` parameter, wiring it into the struct.

- [ ] **Step 3: Update `cmd/manteion/main.go`**

Locate the `NewServer(...)` call and pass the existing `*store.FaultRepo` value as the new `faultStore` arg. `*store.FaultRepo` already implements every `FaultStore` method, so this is a trivial change.

- [ ] **Step 4: Build**

Run: `cd /Users/pronei/work/faults-lab/manteion-go && go build ./...`
Expected: success.

- [ ] **Step 5: Commit**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
git add internal/api/fault_store.go internal/api/server.go cmd/manteion/main.go
git commit -m "$(cat <<'EOF'
api: add FaultStore interface for handler testability

Extracts the handler's dependency from *store.FaultRepo to an interface so
tests can inject a fake without spinning up postgres. Existing faults
*store.FaultRepo field stays to avoid churning the poll handler; new
faultStore field points to the same repo in production.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 17: Implement fault spec handlers

**Files:**
- Create: `/Users/pronei/work/faults-lab/manteion-go/internal/api/fault_handler.go`

- [ ] **Step 1: Create `fault_handler.go` with spec CRUD**

```go
package api

import (
	"errors"
	"net/http"
	"time"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// handleCreateFaultSpec creates a new fault spec.
func (s *Server) handleCreateFaultSpec(w http.ResponseWriter, r *http.Request) {
	var spec model.FaultSpec
	if err := readJSON(r, &spec); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if spec.ID == "" {
		spec.ID = generateID("spec")
	}
	if spec.CreatedAt.IsZero() {
		spec.CreatedAt = time.Now()
	}

	if err := s.faultStore.CreateSpec(r.Context(), &spec); err != nil {
		s.logger.Error("create fault spec failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.logger.Info("fault spec created", "id", spec.ID, "category", spec.Category, "type", spec.FaultType)
	writeJSON(w, http.StatusCreated, spec)
}

// handleListFaultSpecs returns all fault specs.
func (s *Server) handleListFaultSpecs(w http.ResponseWriter, r *http.Request) {
	specs, err := s.faultStore.ListSpecs(r.Context())
	if err != nil {
		s.logger.Error("list fault specs failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list specs")
		return
	}
	if specs == nil {
		specs = []*model.FaultSpec{}
	}
	writeJSON(w, http.StatusOK, specs)
}

// handleGetFaultSpec returns a single spec by id.
func (s *Server) handleGetFaultSpec(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	spec, err := s.faultStore.GetSpec(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "spec not found")
			return
		}
		s.logger.Error("get fault spec failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get spec")
		return
	}
	writeJSON(w, http.StatusOK, spec)
}

// handleDeleteFaultSpec removes a fault spec.
func (s *Server) handleDeleteFaultSpec(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.faultStore.DeleteSpec(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "spec not found")
			return
		}
		s.logger.Error("delete fault spec failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete spec")
		return
	}
	s.logger.Info("fault spec deleted", "id", id)
	w.WriteHeader(http.StatusNoContent)
}
```

Check that helpers `readJSON`, `writeError`, `writeJSON`, `generateID` exist in the api package. If `generateID` is absent, add it next to `writeJSON`:

```go
func generateID(prefix string) string {
	return prefix + "-" + time.Now().UTC().Format("20060102T150405.000000")
}
```

- [ ] **Step 2: Run spec handler tests**

Run: `cd /Users/pronei/work/faults-lab/manteion-go && go test ./internal/api/ -run "TestHandleCreateFaultSpec" -v`
Expected: both subtests PASS.

---

### Task 18: Implement composition handlers with ValidateComposition wiring

**Files:**
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/api/fault_handler.go` (append)

- [ ] **Step 1: Append composition handlers**

```go
// handleCreateFaultComposition creates a composition after running the full
// model.ValidateComposition (depth, direction, incompatibilities) against
// current repo contents. This closes A13 — the repo's basic Validate is NOT
// enough, it doesn't catch depth/incompat violations because those need
// resolvers.
func (s *Server) handleCreateFaultComposition(w http.ResponseWriter, r *http.Request) {
	var comp model.FaultComposition
	if err := readJSON(r, &comp); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if comp.ID == "" {
		comp.ID = generateID("comp")
	}
	if comp.CreatedAt.IsZero() {
		comp.CreatedAt = time.Now()
	}

	ctx := r.Context()
	specResolver := s.faultStore.SpecResolver(ctx)
	compResolver := s.faultStore.CompositionResolver(ctx)

	if err := model.ValidateComposition(&comp, specResolver, compResolver); err != nil {
		s.logger.Warn("composition validation failed", "id", comp.ID, "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.faultStore.CreateComposition(ctx, &comp); err != nil {
		s.logger.Error("create composition failed", "error", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.logger.Info("composition created", "id", comp.ID, "mode", comp.ExecutionMode, "members", len(comp.Members))
	writeJSON(w, http.StatusCreated, comp)
}

func (s *Server) handleListFaultCompositions(w http.ResponseWriter, r *http.Request) {
	comps, err := s.faultStore.ListCompositions(r.Context())
	if err != nil {
		s.logger.Error("list compositions failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list compositions")
		return
	}
	if comps == nil {
		comps = []*model.FaultComposition{}
	}
	writeJSON(w, http.StatusOK, comps)
}

func (s *Server) handleGetFaultComposition(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	comp, err := s.faultStore.GetComposition(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "composition not found")
			return
		}
		s.logger.Error("get composition failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to get composition")
		return
	}
	writeJSON(w, http.StatusOK, comp)
}

func (s *Server) handleDeleteFaultComposition(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.faultStore.DeleteComposition(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, "composition not found")
			return
		}
		s.logger.Error("delete composition failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete composition")
		return
	}
	s.logger.Info("composition deleted", "id", id)
	w.WriteHeader(http.StatusNoContent)
}
```

- [ ] **Step 2: Run all fault handler tests**

Run: `cd /Users/pronei/work/faults-lab/manteion-go && go test ./internal/api/ -run "TestHandleCreate|TestHandle.*Composition" -v`
Expected: all 4 tests PASS.

- [ ] **Step 3: Run full test suite**

Run: `cd /Users/pronei/work/faults-lab/manteion-go && go test ./...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
git add internal/api/fault_handler.go internal/api/fault_handler_test.go
git commit -m "$(cat <<'EOF'
api: CRUD handlers for fault specs and compositions (A13 + A14)

Spec handlers are straightforward CRUD. Composition POST runs the full
model.ValidateComposition (depth, direction, incompatibilities) before
calling the repo's CreateComposition — closing gap A13. Validation uses
the repo's SpecResolver and CompositionResolver so it reflects current
state.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 19: Register routes

**Files:**
- Modify: `/Users/pronei/work/faults-lab/manteion-go/internal/api/server.go`

- [ ] **Step 1: Add fault handler routes to the `routes` method**

After the existing rules block and before the SDK block:

```go
	// Fault specs
	mux.HandleFunc("POST /api/v1/faults/specs", s.handleCreateFaultSpec)
	mux.HandleFunc("GET /api/v1/faults/specs", s.handleListFaultSpecs)
	mux.HandleFunc("GET /api/v1/faults/specs/{id}", s.handleGetFaultSpec)
	mux.HandleFunc("DELETE /api/v1/faults/specs/{id}", s.handleDeleteFaultSpec)

	// Fault compositions
	mux.HandleFunc("POST /api/v1/faults/compositions", s.handleCreateFaultComposition)
	mux.HandleFunc("GET /api/v1/faults/compositions", s.handleListFaultCompositions)
	mux.HandleFunc("GET /api/v1/faults/compositions/{id}", s.handleGetFaultComposition)
	mux.HandleFunc("DELETE /api/v1/faults/compositions/{id}", s.handleDeleteFaultComposition)
```

- [ ] **Step 2: Build + test**

Run: `cd /Users/pronei/work/faults-lab/manteion-go && go build ./... && go test ./...`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
git add internal/api/server.go
git commit -m "$(cat <<'EOF'
api: register fault spec and composition routes

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 20: Update manteion AGENTS.md

**Files:**
- Modify: `/Users/pronei/work/faults-lab/manteion-go/AGENTS.md`

- [ ] **Step 1: Add fault endpoints to the API reference**

Locate the existing HTTP routes table/section. Append:

```markdown
### Fault Specs & Compositions

| Method | Path | Purpose |
|---|---|---|
| POST   | `/api/v1/faults/specs` | Create a fault spec |
| GET    | `/api/v1/faults/specs` | List all fault specs |
| GET    | `/api/v1/faults/specs/{id}` | Get a fault spec |
| DELETE | `/api/v1/faults/specs/{id}` | Delete a fault spec |
| POST   | `/api/v1/faults/compositions` | Create a composition (runs full ValidateComposition) |
| GET    | `/api/v1/faults/compositions` | List compositions |
| GET    | `/api/v1/faults/compositions/{id}` | Get a composition |
| DELETE | `/api/v1/faults/compositions/{id}` | Delete a composition |
```

- [ ] **Step 2: Commit**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
git add AGENTS.md
git commit -m "$(cat <<'EOF'
docs: document fault spec and composition endpoints

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] Rename `Inline*` → `Compiled*` wire types — Tasks 1, 2
- [x] Remove redundant `Position` field — Task 3
- [x] Typed `ExecutionMode` and `Direction` enums — Task 4
- [x] Better depth-cap error message — Task 5
- [x] Composition-level duration/ramp — Task 6
- [x] Atropos composition wire types — Task 7
- [x] Register request/response types, function — Tasks 8–10
- [x] Apply + ApplyTargets — Tasks 11–12
- [x] E2E register+apply — Task 13
- [x] Atropos SDK docs — Task 14
- [x] A13 composition validation wiring — Task 18
- [x] A14 HTTP handlers for fault specs and compositions — Tasks 15–19
- [x] Manteion docs — Task 20

**Explicitly deferred (out of scope):**
- Persistent audit sink — design sketch exists in `docs/plans/2026-04-16-manteion-atropos-client.md`.
- Network and resource fault decoding on the SDK side — `DecodeCompiledRules` errors on these.
- SDK-side composition evaluator — compositions reach the SDK on the wire but decoding errors.
- Per-member direction default on compositions — noted but deferred.
- Per-member timing/offsets in sequential compositions — deferred.
- Composition label/metadata fields — deferred.
- Incompatibilities as data (vs hard-coded `DefaultIncompatibilities`) — deferred.
- Composition-level timeout / cancellation semantics — deferred.
- Member↔Composition struct divergence cleanup — cosmetic, deferred.
- Periodic reconciliation sweep — register-time reconcile is MVP strategy.

**Cross-repo coordination:**
- Tasks 1, 3–6, 15–20 land on `manteion-go feat/inital-setup`.
- Tasks 2, 7–14 land on `atropos-go feat/admin-endpoints`.
- Order within a repo is as listed. Across repos: Task 1 (manteion rename) can land before or after Task 2 (atropos rename); both are independent. Register/Apply (Tasks 8–13) need Task 2 and Task 7 landed first.

**Known implementer hazards:**
- Task 3 touches ~50 test cases in `fault_test.go` + `composition_validate_test.go`. Use Grep → Edit pattern; don't try to eyeball.
- Task 6 migration v4 needs to run successfully against an existing DB. If the implementer has a local manteion running, `go build ./... && go run ./cmd/manteion` exercises it on startup.
- Task 12 — `applyActiveFault` and `applyFreezeCfg` are intentional stubs in the plan. The implementer must grep `admin.go` and `cachebox_admin.go` for actual constructor names and finish the implementations.
- Task 15 assumes no existing tests in `internal/api/`. If tests exist (added between plan writing and execution), match their pattern instead.
