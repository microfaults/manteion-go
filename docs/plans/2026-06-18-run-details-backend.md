# Run-details Backend (phase-native, epoch-2) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the manteion-ui Runs surface natively on the epoch-2 phase model — a top-level `/api/v1/phases` list/detail/faults/lifecycle API plus a `phase_fault_events` audit trail written by the orchestrator.

**Architecture:** A "run" is a phase. New append-migration table `phase_fault_events` (native `fault_event_source` enum) records each rule/cachebox apply→clear the FSM already performs (`enterPhase` opens, `finishPhase` closes). Read endpoints compose existing epoch-2 stores (`experiment_phases`, `phase_workflows`, `phase_workflow_results`) + the new audit repo; lifecycle routes delegate to the orchestrator's existing `PausePhase`/`StartPhase`/`StopPhase`. Live panels (steps/events/resources) are out of scope (v2).

**Tech Stack:** Go 1.25, database/sql + pgx stdlib, net/http (Go 1.22 routing), httptest, DB-gated integration tests (`MANTEION_TEST_DB=1`).

**Branch:** `feat/phase-fsm-port` (build on the FSM port). **Spec:** `docs/specs/2026-06-18-run-details-phase-native-design.md`.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/db/migrations.go` (modify) | append migration `{2, …}`: `fault_event_source` enum + `phase_fault_events` table |
| `internal/model/enums.go` (modify) | `FaultEventSourceValues` + register in `EnumValues` |
| `internal/model/experiment.go` (modify) | `PhaseFaultEvent` (+`Validate`), `PhaseListItem` projection |
| `internal/id/id.go` (modify) | document `fevt-` prefix |
| `internal/store/phase_fault_event_repo.go` (new) | `Create` / `EndOpenForPhase` / `ListForPhase` |
| `internal/store/experiment_repo.go` (modify) | `ListPhasesPaged` (cross-experiment list projection) |
| `internal/orchestrator/{orchestrator,runner}.go` (modify) | inject repo; open events in `enterPhase`, close in `finishPhase`; `ruleKind` helper |
| `internal/api/phase_detail_handler.go` (new) | DTOs + list/detail/faults + pause/resume/stop handlers |
| `internal/api/server.go` (modify) | `phaseFaultEvents` field, `NewServer` arg, 6 routes |
| `cmd/manteion/main.go` (modify) | construct repo, thread into orchestrator + server |
| `internal/store/phase_fault_event_repo_test.go` (new) | repo tests |
| `internal/api/phase_detail_handler_test.go` (new) | handler httptests |
| `CLAUDE.md` (modify) | API surface row |

---

## Task 1: Migration — `fault_event_source` enum + `phase_fault_events` table

**Files:**
- Modify: `internal/db/migrations.go`

- [ ] **Step 1: Add the migration SQL constant and append the migration entry**

In `internal/db/migrations.go`, add a new SQL constant near the other schema constants:

```go
// migration2 adds the phase fault-event audit trail (run-details backend).
const migration2 = `
CREATE TYPE fault_event_source AS ENUM ('rule', 'cachebox', 'fault_config');

CREATE TABLE phase_fault_events (
    id         TEXT PRIMARY KEY,
    phase_id   TEXT NOT NULL REFERENCES experiment_phases(id) ON DELETE CASCADE,
    source     fault_event_source NOT NULL,
    service    TEXT NOT NULL,
    kind       TEXT NOT NULL,
    detail     JSONB NOT NULL DEFAULT '{}',
    started_at TIMESTAMPTZ NOT NULL,
    ended_at   TIMESTAMPTZ
);
CREATE INDEX idx_phase_fault_events_phase ON phase_fault_events(phase_id);
`
```

Change the migrations slice from:

```go
var migrations = []migration{
	{1, "consolidated schema v2 (epoch 2 — prior history in git)", schemaV2},
}
```

to:

```go
var migrations = []migration{
	{1, "consolidated schema v2 (epoch 2 — prior history in git)", schemaV2},
	{2, "phase_fault_events audit trail + fault_event_source enum", migration2},
}
```

- [ ] **Step 2: Apply against the test DB and verify**

Run:
```bash
cd manteion-go
MANTEION_DATABASE_URL=postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable \
  go run ./cmd/manteion -migrate-only 2>/dev/null || true
psql postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable -c "\d phase_fault_events" -c "SELECT enumlabel FROM pg_enum e JOIN pg_type t ON t.oid=e.enumtypid WHERE t.typname='fault_event_source' ORDER BY e.enumsortorder;"
```
Expected: the table prints with columns `id, phase_id, source, service, kind, detail, started_at, ended_at`; enum labels `rule, cachebox, fault_config`. (If `-migrate-only` is not a flag, the migration runs at normal startup — `go test ./internal/store/ -run TestEnumParity` in Task 2 also drives it via `db.Open`→`Migrate`.)

- [ ] **Step 3: Build & commit**

```bash
go build ./... && go vet ./internal/db/...
git add internal/db/migrations.go
git commit -m "feat(db): phase_fault_events table + fault_event_source enum (migration 2)"
```

---

## Task 2: Model — `PhaseFaultEvent`, `PhaseListItem`, enum values, id prefix

**Files:**
- Modify: `internal/model/enums.go`, `internal/model/experiment.go`, `internal/id/id.go`
- Test: `internal/store/enum_parity_test.go` (existing — drives parity)

- [ ] **Step 1: Register the enum vocabulary**

In `internal/model/enums.go`, add to the `EnumValues` map (inside the literal):

```go
	"fault_event_source":    FaultEventSourceValues,
```

and add to the `var (...)` block of value slices:

```go
	FaultEventSourceValues    = []string{"rule", "cachebox", "fault_config"}
```

- [ ] **Step 2: Add the model types**

In `internal/model/experiment.go`, after the `PhaseRule` type, add:

```go
var validFaultEventSources = setOf(FaultEventSourceValues...)

// PhaseFaultEvent is one entry in a phase's fault apply→clear audit trail.
// EndedAt is nil while the fault is active; the orchestrator stamps it when
// the phase clears rules / thaws cache-box at finish.
type PhaseFaultEvent struct {
	ID        string          `json:"id"`
	PhaseID   string          `json:"phase_id"`
	Source    string          `json:"source"` // rule | cachebox | fault_config
	Service   string          `json:"service"`
	Kind      string          `json:"kind"` // "cachebox:replay", "inline:latency", …
	Detail    json.RawMessage `json:"detail,omitempty"`
	StartedAt time.Time       `json:"started_at"`
	EndedAt   *time.Time      `json:"ended_at,omitempty"`
}

func (e *PhaseFaultEvent) Validate() error {
	if e.ID == "" {
		return errors.New("phase fault event: id required")
	}
	if e.PhaseID == "" {
		return errors.New("phase fault event: phase_id required")
	}
	if !validFaultEventSources[e.Source] {
		return fmt.Errorf("phase fault event: invalid source %q", e.Source)
	}
	if e.Service == "" {
		return errors.New("phase fault event: service required")
	}
	if e.Kind == "" {
		return errors.New("phase fault event: kind required")
	}
	return nil
}

// PhaseListItem is the cross-experiment list projection for GET /api/v1/phases
// (a "run" row). Composed by ExperimentRepo.ListPhasesPaged.
type PhaseListItem struct {
	ID                 string     `json:"id"`
	ExperimentID       string     `json:"experiment_id"`
	ExperimentName     string     `json:"experiment_name"`
	Name               string     `json:"name"`
	Position           int        `json:"position"`
	WorkflowIDs        []string   `json:"workflow_ids"`
	FrozenServiceCount int        `json:"frozen_service_count"`
	Status             string     `json:"status"`
	StartedAt          *time.Time `json:"started_at,omitempty"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"`
}
```

(`experiment.go` already imports `encoding/json`, `errors`, `fmt`, `time`.)

- [ ] **Step 3: Document the id prefix**

In `internal/id/id.go`, extend the prefix list comment (line ~4) to include `fevt-`:

```go
// The prefix is type-discriminating (exp-, phase-, rule-, spec-, comp-,
// wf-, atk-, atkres-, fc-, policy-, anchor-, fevt-) so ids are self-describing in
```

- [ ] **Step 4: Run enum parity (drives the migration + asserts parity)**

Run:
```bash
MANTEION_TEST_DB=1 go test ./internal/store/ -run TestEnumParity -count=1 -v
```
Expected: PASS (the new `fault_event_source` enum matches `FaultEventSourceValues`).

- [ ] **Step 5: Build & commit**

```bash
go build ./... && go vet ./internal/model/...
git add internal/model/enums.go internal/model/experiment.go internal/id/id.go
git commit -m "feat(model): PhaseFaultEvent + PhaseListItem + fault_event_source values"
```

---

## Task 3: Store — `PhaseFaultEventRepo`

**Files:**
- Create: `internal/store/phase_fault_event_repo.go`
- Test: `internal/store/phase_fault_event_repo_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/store/phase_fault_event_repo_test.go`:

```go
package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"manteion-go/internal/id"
	"manteion-go/internal/model"
)

func TestPhaseFaultEventRepo(t *testing.T) {
	if testDB == nil {
		t.Skip("no test DB")
	}
	ctx := context.Background()
	repo := NewPhaseFaultEventRepo(testDB)

	// Fixtures: an experiment + phase to satisfy the FK.
	exp := &model.Experiment{ID: id.New("exp"), Name: "fe-test", Status: "planned", CreatedAt: time.Now()}
	if err := testExpRepo.Create(ctx, exp); err != nil {
		t.Fatalf("create exp: %v", err)
	}
	ph := &model.ExperimentPhase{ID: id.New("phase"), ExperimentID: exp.ID, Name: "p0", Position: 0, Status: "pending"}
	if err := testExpRepo.CreatePhase(ctx, ph); err != nil {
		t.Fatalf("create phase: %v", err)
	}

	open := &model.PhaseFaultEvent{
		ID: id.New("fevt"), PhaseID: ph.ID, Source: "cachebox", Service: "frontend",
		Kind: "cachebox:replay", Detail: json.RawMessage(`{"key_strategy":"exact"}`), StartedAt: time.Now(),
	}
	if err := repo.Create(ctx, open); err != nil {
		t.Fatalf("create event: %v", err)
	}

	got, err := repo.ListForPhase(ctx, ph.ID)
	if err != nil || len(got) != 1 {
		t.Fatalf("list: %d rows, err=%v; want 1", len(got), err)
	}
	if got[0].EndedAt != nil {
		t.Errorf("event should be open, got ended_at=%v", got[0].EndedAt)
	}

	if err := repo.EndOpenForPhase(ctx, ph.ID, time.Now()); err != nil {
		t.Fatalf("end open: %v", err)
	}
	got, _ = repo.ListForPhase(ctx, ph.ID)
	if got[0].EndedAt == nil {
		t.Error("event should be closed after EndOpenForPhase")
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `MANTEION_TEST_DB=1 go test ./internal/store/ -run TestPhaseFaultEventRepo -count=1`
Expected: compile error / FAIL — `NewPhaseFaultEventRepo` undefined.

- [ ] **Step 3: Implement the repo**

Create `internal/store/phase_fault_event_repo.go`:

```go
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"manteion-go/internal/model"
)

// PhaseFaultEventRepo persists the per-phase fault apply→clear audit trail
// (phase_fault_events). Rows are opened by the orchestrator when it pushes a
// rule or freezes a cache-box service, and closed (ended_at) when the phase
// finishes.
type PhaseFaultEventRepo struct {
	db *sql.DB
}

func NewPhaseFaultEventRepo(db *sql.DB) *PhaseFaultEventRepo {
	return &PhaseFaultEventRepo{db: db}
}

// Create inserts one fault event (ended_at left NULL = still active).
func (r *PhaseFaultEventRepo) Create(ctx context.Context, e *model.PhaseFaultEvent) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if e.StartedAt.IsZero() {
		e.StartedAt = time.Now()
	}
	detail := e.Detail
	if len(detail) == 0 {
		detail = []byte("{}")
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO phase_fault_events (id, phase_id, source, service, kind, detail, started_at, ended_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		e.ID, e.PhaseID, e.Source, e.Service, e.Kind, []byte(detail), e.StartedAt, e.EndedAt)
	if err != nil {
		return fmt.Errorf("insert phase_fault_event: %w", err)
	}
	return nil
}

// EndOpenForPhase stamps ended_at on every still-open event of the phase.
func (r *PhaseFaultEventRepo) EndOpenForPhase(ctx context.Context, phaseID string, endedAt time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE phase_fault_events SET ended_at = $2
		WHERE phase_id = $1 AND ended_at IS NULL`, phaseID, endedAt)
	if err != nil {
		return fmt.Errorf("end open phase_fault_events: %w", err)
	}
	return nil
}

// ListForPhase returns the phase's events in apply order.
func (r *PhaseFaultEventRepo) ListForPhase(ctx context.Context, phaseID string) ([]*model.PhaseFaultEvent, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, phase_id, source, service, kind, detail, started_at, ended_at
		FROM phase_fault_events WHERE phase_id = $1
		ORDER BY started_at, id`, phaseID)
	if err != nil {
		return nil, fmt.Errorf("list phase_fault_events: %w", err)
	}
	defer rows.Close()
	var out []*model.PhaseFaultEvent
	for rows.Next() {
		var (
			e       model.PhaseFaultEvent
			detail  []byte
			endedAt sql.NullTime
		)
		if err := rows.Scan(&e.ID, &e.PhaseID, &e.Source, &e.Service, &e.Kind, &detail, &e.StartedAt, &endedAt); err != nil {
			return nil, fmt.Errorf("scan phase_fault_event: %w", err)
		}
		e.Detail = detail
		e.EndedAt = nullTimeToPtr(endedAt)
		out = append(out, &e)
	}
	return out, rows.Err()
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `MANTEION_TEST_DB=1 go test ./internal/store/ -run TestPhaseFaultEventRepo -count=1 -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/phase_fault_event_repo.go internal/store/phase_fault_event_repo_test.go
git commit -m "feat(store): PhaseFaultEventRepo (create/list/end-open) + test"
```

---

## Task 4: Store — `ListPhasesPaged`

**Files:**
- Modify: `internal/store/experiment_repo.go`
- Test: `internal/store/phase_fault_event_repo_test.go` (add a test func) — or a new `experiment_repo_test.go` if the package has none for phases; the test below is self-contained.

- [ ] **Step 1: Write the failing test**

Append to `internal/store/phase_fault_event_repo_test.go`:

```go
func TestListPhasesPaged(t *testing.T) {
	if testDB == nil {
		t.Skip("no test DB")
	}
	ctx := context.Background()
	exp := &model.Experiment{ID: id.New("exp"), Name: "lp-test", Status: "planned", CreatedAt: time.Now()}
	if err := testExpRepo.Create(ctx, exp); err != nil {
		t.Fatalf("create exp: %v", err)
	}
	ph := &model.ExperimentPhase{
		ID: id.New("phase"), ExperimentID: exp.ID, Name: "iso", Position: 0, Status: "pending",
		FrozenServices: []model.CacheBoxConfig{{Service: "frontend", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}},
	}
	if err := testExpRepo.CreatePhase(ctx, ph); err != nil {
		t.Fatalf("create phase: %v", err)
	}

	items, total, err := testExpRepo.ListPhasesPaged(ctx, Page{Limit: 50, Offset: 0})
	if err != nil {
		t.Fatalf("list phases paged: %v", err)
	}
	if total < 1 {
		t.Fatalf("total=%d, want >=1", total)
	}
	var found *model.PhaseListItem
	for _, it := range items {
		if it.ID == ph.ID {
			found = it
			break
		}
	}
	if found == nil {
		t.Fatal("created phase not in page")
	}
	if found.ExperimentName != "lp-test" {
		t.Errorf("experiment_name=%q, want lp-test", found.ExperimentName)
	}
	if found.FrozenServiceCount != 1 {
		t.Errorf("frozen_service_count=%d, want 1", found.FrozenServiceCount)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `MANTEION_TEST_DB=1 go test ./internal/store/ -run TestListPhasesPaged -count=1`
Expected: FAIL — `ListPhasesPaged` undefined.

- [ ] **Step 3: Implement `ListPhasesPaged`**

Add to `internal/store/experiment_repo.go` (after `ListPhasesForExperiment`):

```go
// ListPhasesPaged returns a cross-experiment page of phase list rows (the
// "runs" list), newest-started first, each with its experiment name, workflow
// ids, and frozen-service count. workflow_ids is aggregated as JSONB to avoid
// PG-array scanning.
func (r *ExperimentRepo) ListPhasesPaged(ctx context.Context, p Page) ([]*model.PhaseListItem, int, error) {
	if p.Limit <= 0 {
		p.Limit = 20
	}
	if p.Limit > 200 {
		p.Limit = 200
	}
	if p.Offset < 0 {
		p.Offset = 0
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT p.id, p.experiment_id, e.name, p.name, p.position, p.status,
			p.started_at, p.completed_at,
			COALESCE(jsonb_array_length(p.frozen_services), 0) AS frozen_service_count,
			COALESCE(jsonb_agg(pw.workflow_id ORDER BY pw.workflow_id)
				FILTER (WHERE pw.workflow_id IS NOT NULL), '[]'::jsonb) AS workflow_ids,
			COUNT(*) OVER () AS total_count
		FROM experiment_phases p
		JOIN experiments e ON e.id = p.experiment_id
		LEFT JOIN phase_workflows pw ON pw.phase_id = p.id
		GROUP BY p.id, e.name
		ORDER BY p.started_at DESC NULLS LAST, p.position
		LIMIT $1 OFFSET $2`, p.Limit, p.Offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list phases paged: %w", err)
	}
	defer rows.Close()
	var (
		out   []*model.PhaseListItem
		total int
	)
	for rows.Next() {
		var (
			it                     model.PhaseListItem
			startedAt, completedAt sql.NullTime
			workflowIDs            []byte
		)
		if err := rows.Scan(
			&it.ID, &it.ExperimentID, &it.ExperimentName, &it.Name, &it.Position, &it.Status,
			&startedAt, &completedAt, &it.FrozenServiceCount, &workflowIDs, &total,
		); err != nil {
			return nil, 0, fmt.Errorf("scan phase list item: %w", err)
		}
		it.StartedAt = nullTimeToPtr(startedAt)
		it.CompletedAt = nullTimeToPtr(completedAt)
		if len(workflowIDs) > 0 {
			if err := json.Unmarshal(workflowIDs, &it.WorkflowIDs); err != nil {
				return nil, 0, fmt.Errorf("decode workflow_ids: %w", err)
			}
		}
		out = append(out, &it)
	}
	return out, total, rows.Err()
}
```

(`experiment_repo.go` already imports `database/sql`, `encoding/json`, `fmt`.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `MANTEION_TEST_DB=1 go test ./internal/store/ -run TestListPhasesPaged -count=1 -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/store/experiment_repo.go internal/store/phase_fault_event_repo_test.go
git commit -m "feat(store): ListPhasesPaged cross-experiment phase list projection"
```

---

## Task 5: Orchestrator — audit-trail wiring

**Files:**
- Modify: `internal/orchestrator/orchestrator.go` (constructor + field), `internal/orchestrator/runner.go` (open/close + `ruleKind`)
- Modify: `cmd/manteion/main.go` (pass repo), `internal/orchestrator/orchestrator_test.go` (test harness arg + new test)

- [ ] **Step 1: Add the field + constructor arg**

In `internal/orchestrator/orchestrator.go`, add to the struct (after `cacheStore`):

```go
	faultEvents *store.PhaseFaultEventRepo
```

Add the parameter to `New(...)` (after `cs *cachestore.Store`):

```go
	faultEvents *store.PhaseFaultEventRepo,
```

and set it in the returned struct literal:

```go
		faultEvents:     faultEvents,
```

- [ ] **Step 2: Open events in `enterPhase`, add `ruleKind`**

In `internal/orchestrator/runner.go`, add the import `"encoding/json"` and `"manteion-go/internal/id"` is already imported. Add a helper and the open-event calls.

After `o.freezeServices(ctx, p)` in `enterPhase`, insert:

```go
	o.recordFreezeEvents(ctx, p)
```

After `if err := o.pushPhaseRules(ctx, p); err != nil {` block returns success (i.e. immediately after the `pushPhaseRules` call), insert:

```go
	o.recordRuleEvents(ctx, p)
```

Add these methods to `runner.go`:

```go
// recordFreezeEvents opens a cachebox fault event per frozen service.
func (o *Orchestrator) recordFreezeEvents(ctx context.Context, p *model.ExperimentPhase) {
	if o.faultEvents == nil {
		return
	}
	for _, fs := range p.FrozenServices {
		detail, _ := json.Marshal(map[string]string{"mode": fs.Mode, "key_strategy": fs.KeyStrategy})
		ev := &model.PhaseFaultEvent{
			ID: id.New("fevt"), PhaseID: p.ID, Source: "cachebox", Service: fs.Service,
			Kind: "cachebox:" + fs.Mode, Detail: detail, StartedAt: time.Now(),
		}
		if err := o.faultEvents.Create(ctx, ev); err != nil {
			o.logger.Warn("orchestrator: record cachebox event failed",
				"phase_id", p.ID, "service", fs.Service, "error", err)
		}
	}
}

// recordRuleEvents opens a rule fault event per phase rule.
func (o *Orchestrator) recordRuleEvents(ctx context.Context, p *model.ExperimentPhase) {
	if o.faultEvents == nil {
		return
	}
	prs, err := o.experiments.ListPhaseRules(ctx, p.ID)
	if err != nil {
		o.logger.Warn("orchestrator: record rule events: list rules failed", "phase_id", p.ID, "error", err)
		return
	}
	for _, pr := range prs {
		r, err := o.rules.Get(ctx, pr.RuleID)
		if err != nil {
			continue
		}
		ev := &model.PhaseFaultEvent{
			ID: id.New("fevt"), PhaseID: p.ID, Source: "rule", Service: r.Service,
			Kind: o.ruleKind(ctx, r), StartedAt: time.Now(),
		}
		if err := o.faultEvents.Create(ctx, ev); err != nil {
			o.logger.Warn("orchestrator: record rule event failed",
				"phase_id", p.ID, "rule_id", r.ID, "error", err)
		}
	}
}

// ruleKind derives "category:fault_type" from a rule's fault spec; falls back
// to "rule" when the action is not a fault spec or the spec can't be loaded.
func (o *Orchestrator) ruleKind(ctx context.Context, r *model.Rule) string {
	if r.Action.FaultSpecID == "" {
		return "rule"
	}
	spec, err := o.faults.GetSpec(ctx, r.Action.FaultSpecID)
	if err != nil {
		return "rule"
	}
	return spec.Category + ":" + spec.FaultType
}
```

- [ ] **Step 3: Close events in `finishPhase`**

In `internal/orchestrator/runner.go` `finishPhase`, after `o.thawServices(ctx, p)`, insert:

```go
	if o.faultEvents != nil {
		if err := o.faultEvents.EndOpenForPhase(ctx, phaseID, time.Now()); err != nil {
			o.logger.Warn("orchestrator: close fault events failed", "phase_id", phaseID, "error", err)
		}
	}
```

- [ ] **Step 4: Update main.go and the test harness to compile**

In `cmd/manteion/main.go`, before the `orch :=` line add:

```go
	phaseFaultEventRepo := store.NewPhaseFaultEventRepo(database)
```

and add `cs,` → `cs, phaseFaultEventRepo,` in the `orchestrator.New(...)` call (insert after `cs`, before `logger`):

```go
	orch := orchestrator.New(experimentRepo, ruleRepo, faultRepo, workloadRepo, workflowRepo, controller, promClient, zeusClient, cs, phaseFaultEventRepo, logger)
```

In `internal/orchestrator/orchestrator_test.go`, update `newOrch` to pass a real repo (so events persist for the new test). Change the `New(...)` call to insert `store.NewPhaseFaultEventRepo(testDB)` after the cachestore arg:

```go
	o := New(testExpRepo, testRuleRepo, testFaultRepo, testWorkloadRepo, testWorkflowRepo,
		controller, nil, zc, cachestore.New(t.TempDir()), store.NewPhaseFaultEventRepo(testDB), logger)
```

- [ ] **Step 5: Write the failing test (audit trail open→close)**

Add to `internal/orchestrator/orchestrator_test.go`:

```go
func TestPhaseFaultEventsAuditTrail(t *testing.T) {
	ctx := context.Background()
	o := newOrch(t, "")        // no zeus → driver-less phases auto-complete
	feRepo := store.NewPhaseFaultEventRepo(testDB)
	exp, _ := mkExperiment(t, 1) // phase 0 is bare (no frozen services)

	// Add a second phase that freezes a service, so enterPhase records a
	// cachebox event that finishPhase must then close.
	fp := &model.ExperimentPhase{
		ID: id.New("phase"), ExperimentID: exp.ID, Name: "frozen", Position: 1, Status: "pending",
		FrozenServices: []model.CacheBoxConfig{{Service: "frontend", Mode: "replay", KeyStrategy: "exact", MutationPolicy: "deny"}},
	}
	if err := testExpRepo.CreatePhase(ctx, fp); err != nil {
		t.Fatal(err)
	}

	if err := o.StartExperiment(ctx, exp.ID); err != nil {
		t.Fatalf("start: %v", err)
	}
	waitFor(t, "experiment completed", 10*time.Second, func() bool {
		return getExp(t, exp.ID).Status == "completed"
	})

	events, err := feRepo.ListForPhase(ctx, fp.ID)
	if err != nil || len(events) == 0 {
		t.Fatalf("expected cachebox events for frozen phase, got %d err=%v", len(events), err)
	}
	for _, e := range events {
		if e.Source != "cachebox" || e.Service != "frontend" {
			t.Errorf("unexpected event: %+v", e)
		}
		if e.EndedAt == nil {
			t.Errorf("event %s not closed after phase completion", e.ID)
		}
	}
}
```

- [ ] **Step 6: Run it (fail → implement already done → pass)**

Run: `MANTEION_TEST_DB=1 go test ./internal/orchestrator/ -run TestPhaseFaultEventsAuditTrail -count=1 -v`
Expected: PASS (open events created in `enterPhase`, closed in `finishPhase` at auto-complete).

- [ ] **Step 7: Full FSM suite + build/vet**

Run:
```bash
go build ./... && go vet ./...
MANTEION_TEST_DB=1 go test -race ./internal/orchestrator/ -count=1
```
Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/orchestrator/ cmd/manteion/main.go
git commit -m "feat(orchestrator): record phase fault-event audit trail (open in enterPhase, close in finishPhase)"
```

---

## Task 6: API — DTOs + list/detail/faults handlers + routes

**Files:**
- Create: `internal/api/phase_detail_handler.go`
- Modify: `internal/api/server.go` (field, `NewServer` arg, routes), `cmd/manteion/main.go` (pass repo)
- Test: `internal/api/phase_detail_handler_test.go`

- [ ] **Step 1: Add the server field + constructor arg**

In `internal/api/server.go`, add to the `Server` struct (near `experiments`):

```go
	phaseFaultEvents *store.PhaseFaultEventRepo
```

Add a parameter to `NewServer(...)` (place it next to the other repo args, after `experiments`/`workflows` group — match the existing call site) and assign it in the struct literal:

```go
		phaseFaultEvents: phaseFaultEvents,
```

In `cmd/manteion/main.go`, pass `phaseFaultEventRepo` into the `api.NewServer(...)` call in the same positional slot you declared.

- [ ] **Step 2: Write the handlers + DTOs**

Create `internal/api/phase_detail_handler.go`:

```go
package api

import (
	"errors"
	"net/http"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// PhaseDetail is GET /api/v1/phases/{phaseId}. Per-workflow results + summable
// totals only — no averaged percentiles (percentile-aggregation caveat).
type PhaseDetail struct {
	*model.ExperimentPhase
	ExperimentName    string                       `json:"experiment_name"`
	Workflows         []model.PhaseWorkflow        `json:"workflows"`
	WorkflowResults   []*model.PhaseWorkflowResult `json:"workflow_results"`
	TotalRequestCount int64                        `json:"total_request_count"`
	TotalErrorCount   int64                        `json:"total_error_count"`
	OverallErrorRate  float64                      `json:"overall_error_rate"`
}

// PhaseFaults is GET /api/v1/phases/{phaseId}/faults.
type PhaseFaults struct {
	FrozenServices []model.CacheBoxConfig     `json:"frozen_services"`
	PhaseRules     []model.PhaseRule          `json:"phase_rules"`
	FaultEvents    []*model.PhaseFaultEvent   `json:"fault_events"`
}

func (s *Server) handleListPhases(w http.ResponseWriter, r *http.Request) {
	limit, offset := parsePagination(r)
	items, total, err := s.experiments.ListPhasesPaged(r.Context(), store.Page{Limit: limit, Offset: offset})
	if err != nil {
		s.logger.Error("list phases failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list phases")
		return
	}
	writePage(w, http.StatusOK, items, total, limit, offset)
}

func (s *Server) handleGetPhaseDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("phaseId")
	ctx := r.Context()
	phase, err := s.experiments.GetPhase(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "phase not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get phase")
		return
	}
	exp, _ := s.experiments.Get(ctx, phase.ExperimentID)
	pws, _ := s.experiments.ListPhaseWorkflows(ctx, id)
	results, _ := s.experiments.ListWorkflowResultsForPhase(ctx, id)

	var totalReq, totalErr int64
	for _, res := range results {
		totalReq += res.RequestCount
		totalErr += res.ErrorCount
	}
	rate := 0.0
	if totalReq > 0 {
		rate = float64(totalErr) / float64(totalReq)
	}
	detail := PhaseDetail{
		ExperimentPhase: phase,
		Workflows:       pws,
		WorkflowResults: nilToEmpty(results),
		TotalRequestCount: totalReq,
		TotalErrorCount:   totalErr,
		OverallErrorRate:  rate,
	}
	if exp != nil {
		detail.ExperimentName = exp.Name
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleGetPhaseFaults(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("phaseId")
	ctx := r.Context()
	phase, err := s.experiments.GetPhase(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "phase not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get phase")
		return
	}
	rules, _ := s.experiments.ListPhaseRules(ctx, id)
	var events []*model.PhaseFaultEvent
	if s.phaseFaultEvents != nil {
		events, _ = s.phaseFaultEvents.ListForPhase(ctx, id)
	}
	writeJSON(w, http.StatusOK, PhaseFaults{
		FrozenServices: phase.FrozenServices,
		PhaseRules:     nilToEmpty(rules),
		FaultEvents:    nilToEmpty(events),
	})
}
```

(`nilToEmpty` already exists in `experiment_handler.go`.)

- [ ] **Step 3: Register the read routes**

In `internal/api/server.go`, in the experiment routes section, add:

```go
	// Phase-native run-details surface (a "run" = a phase; phaseId is globally
	// unique so no experiment segment is needed). Live panels (steps/events/
	// resources) are the v2 follow-on. See docs/specs/2026-06-18-run-details-*.
	mux.HandleFunc("GET /api/v1/phases", s.handleListPhases)
	mux.HandleFunc("GET /api/v1/phases/{phaseId}", s.handleGetPhaseDetail)
	mux.HandleFunc("GET /api/v1/phases/{phaseId}/faults", s.handleGetPhaseFaults)
```

- [ ] **Step 4: Write the handler test**

Create `internal/api/phase_detail_handler_test.go` following the existing api test pattern (table-driven httptest against a `*Server`). Minimum:

```go
package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleListPhases_EmptyOK(t *testing.T) {
	s := newTestServer(t) // existing helper; if absent, construct Server with nil repos that tolerate it
	req := httptest.NewRequest(http.MethodGet, "/api/v1/phases?limit=10&offset=0", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK && rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d", rec.Code)
	}
}
```

> If the api package has no `newTestServer` helper, prefer a DB-gated test mirroring `experiment_handler` tests instead, asserting `GET /api/v1/phases/{unknown}` → 404 and a seeded phase round-trips through detail + faults. Check `internal/api/*_test.go` for the established harness and follow it; do not invent a parallel one.

- [ ] **Step 5: Run tests + build/vet**

Run:
```bash
go build ./... && go vet ./internal/api/...
MANTEION_TEST_DB=1 go test ./internal/api/ -run TestHandle.*Phase -count=1 -v
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/api/phase_detail_handler.go internal/api/phase_detail_handler_test.go internal/api/server.go cmd/manteion/main.go
git commit -m "feat(api): phase-native run-details read endpoints (list/detail/faults)"
```

---

## Task 7: API — lifecycle flat routes (pause/resume/stop)

**Files:**
- Modify: `internal/api/phase_detail_handler.go` (3 handlers), `internal/api/server.go` (3 routes)

- [ ] **Step 1: Add the lifecycle handlers**

Append to `internal/api/phase_detail_handler.go`:

```go
func (s *Server) handlePausePhaseFlat(w http.ResponseWriter, r *http.Request) {
	if err := s.orch.PausePhase(r.Context(), r.PathValue("phaseId")); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

func (s *Server) handleResumePhaseFlat(w http.ResponseWriter, r *http.Request) {
	// StartPhase resumes a paused phase (and would start a pending one).
	if err := s.orch.StartPhase(r.Context(), r.PathValue("phaseId")); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

func (s *Server) handleStopPhaseFlat(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status") // "", completed, failed, skipped
	if err := s.orch.StopPhase(r.Context(), r.PathValue("phaseId"), status); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
```

- [ ] **Step 2: Register the lifecycle routes**

In `internal/api/server.go`, right after the three phase read routes:

```go
	mux.HandleFunc("POST /api/v1/phases/{phaseId}/pause", s.handlePausePhaseFlat)
	mux.HandleFunc("POST /api/v1/phases/{phaseId}/resume", s.handleResumePhaseFlat)
	mux.HandleFunc("POST /api/v1/phases/{phaseId}/stop", s.handleStopPhaseFlat)
```

- [ ] **Step 3: Build/vet + smoke test**

Run:
```bash
go build ./... && go vet ./internal/api/...
MANTEION_TEST_DB=1 go test ./internal/api/ -count=1
```
Expected: PASS (no regressions; `orch` is non-nil in the server — these delegate to it).

- [ ] **Step 4: Commit**

```bash
git add internal/api/phase_detail_handler.go internal/api/server.go
git commit -m "feat(api): phase-native lifecycle routes (pause/resume/stop)"
```

---

## Task 8: Docs + full verification

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Document the surface**

In `CLAUDE.md`, in the HTTP API surface table, add a row:

```markdown
| Phases (run-details) | `GET /api/v1/phases`, `GET ./{phaseId}`, `GET ./{phaseId}/faults`, `POST ./{phaseId}/pause\|resume\|stop`. A "run" = a phase; live panels (steps/events/resources) are the v2 follow-on. |
```

- [ ] **Step 2: Full verification**

Run:
```bash
cd manteion-go
go build ./... && go vet ./...
go test ./...                       # ungated unit
make integration                    # DB-gated, -p 1
```
Expected: all PASS; `make integration` green (store + orchestrator + api suites, including the new `phase_fault_events` / `ListPhasesPaged` / audit-trail tests).

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: phase-native run-details API surface in CLAUDE.md"
```

---

## Self-Review

**Spec coverage:** endpoint surface → Tasks 6–7; DTOs → Task 6; `phase_fault_events` + enum → Tasks 1–2; `PhaseFaultEventRepo` → Task 3; `ListPhasesPaged` → Task 4; orchestrator audit wiring → Task 5; main wiring → Tasks 5–6; tests → every task; CLAUDE.md → Task 8. Non-goals (steps/events/resources/live-metrics, UI rewrite) correctly excluded. **No gaps.**

**Type consistency:** `model.PhaseFaultEvent` (fields ID/PhaseID/Source/Service/Kind/Detail/StartedAt/EndedAt) used identically in store (Task 3), orchestrator (Task 5), API (Task 6). `model.PhaseListItem` defined Task 2, produced Task 4, served Task 6. `PhaseFaultEventRepo` methods `Create`/`EndOpenForPhase`/`ListForPhase` consistent across Tasks 3/5/6. Orchestrator `New` arg order (… `cs`, `faultEvents`, `logger`) consistent in Task 5 main.go + test harness. `ruleKind(ctx, *model.Rule)` defined and called in Task 5.

**Open implementation note:** `fault_config`-source events (spec §"orchestrator wiring") are **deferred** out of this plan — v1 records `rule` + `cachebox` reliably; wiring fire/cancel/reaper to also write `source=fault_config` events is a small follow-on that touches the fault-config handlers and is not required for the Runs page. Documented in the spec as deferrable.
