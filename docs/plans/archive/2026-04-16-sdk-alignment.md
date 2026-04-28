# Atropos SDK Alignment: Model Cleanup & Admin Endpoints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align manteion's data model with the atropos-go SDK's actual capabilities (remove ExperimentType, fix KeyStrategy values, mark MutationPolicy as metadata-only) and add remote-control HTTP endpoints to atropos-go for cache-box introspection and runtime rule updates.

**Architecture:** Spans two repos. In manteion-go, we simplify the `Experiment` model and realign `CacheBoxConfig` with the SDK's vocabulary — no runtime behavior changes, only data model and validation. In atropos-go, we add two self-contained `http.Handler` constructors following the existing `FaultAdminHandler()` pattern: `CacheBoxAdminHandler(cb *CacheBox)` and `RulesAdminHandler(eval *StaticEvaluator)`. Consumers mount these on whatever routes they want, consistent with how the SDK's admin surface works today.

**Tech Stack:** Go 1.22+ stdlib (`net/http`, `encoding/json`, `database/sql`), pgx/v5 for postgres, Go testing stdlib.

---

## Context

Two problems emerged from recent review of the atropos-go SDK against manteion's data model:

**1. Manteion models types the SDK can't execute.** `Experiment.ExperimentType` has five values (`interference`, `isolation`, `attribution`, `scenario`, `cache_fidelity`), but the SDK has no notion of "experiment type" — it only knows about cache-box modes and faults. `scenario` is just `attribution` with `replay_with_delay`. `cache_fidelity` requires shadow-mode dispatch the SDK doesn't implement. The type field is descriptive metadata masquerading as a behavioral discriminator, and the behavior it claims to encode is already captured by the run's `FrozenServices[].Mode`.

**2. `CacheBoxConfig.KeyStrategy` uses different vocabulary than the SDK.** Manteion says `"exact" | "fuzzy" | "parametric"`. SDK says `"exact" | "exact_with_host" | "exact_with_body"`. Any config manteion stores will fail when pushed to the SDK.

**3. `MutationPolicy` and `SafeMethods` aren't implemented in the SDK at all.** The fields exist on `CacheBoxConfig` with validation but the SDK replays all methods uniformly. We keep the fields as reproducibility metadata but annotate them accordingly — no SDK changes.

**4. Atropos has no remote-control surface for cache-box or rules.** The SDK exposes `CacheBox.Stats()`, `CacheBox.SetDelaySource()`, `StaticEvaluator.SetRules()`, and `StaticEvaluator.Rules()` as in-process APIs. There are no HTTP endpoints, so manteion can't push rules or observe cache-box state. The existing `FaultAdminHandler()` pattern (a standalone function returning `http.Handler`, mounted by the caller) is the right shape for two new handlers.

## Design

### Data Model Changes (manteion-go)

**Experiment (internal/model/experiment.go)** — drop `ExperimentType`:

- Remove `ExperimentType string` field from `Experiment` struct.
- Remove `validExperimentTypes` map.
- Remove `experiment_type` validation in `Validate()`.
- DB migration v3 drops the `experiment_type` column from `experiments`.
- `ExperimentRepo` SQL (`Create`, `Get`, `List`) drops references to the column.
- Tests lose "valid interference", "valid cache_fidelity", "invalid type" cases; "valid attribution" becomes "valid".

**CacheBoxConfig (internal/model/trace.go)** — realign KeyStrategy, annotate MutationPolicy:

```go
var (
    validCacheBoxModes    = map[string]bool{"passthrough": true, "replay": true, "replay_with_delay": true}
    validKeyStrategies    = map[string]bool{"exact": true, "exact_with_host": true, "exact_with_body": true}
    validMutationPolicies = map[string]bool{"deny": true, "allow": true}
)
```

Struct comment for `MutationPolicy` / `SafeMethods` fields is updated to: `// Metadata only — not enforced by SDK in current implementation. Records operator intent for experiment reproducibility.`

No column changes needed (fields live inside `frozen_services` JSONB).

### Admin Endpoints (atropos-go)

Two new files, each a standalone handler function following `FaultAdminHandler()` conventions (self-contained, returns `http.Handler`, caller mounts it):

**cachebox_admin.go — `CacheBoxAdminHandler(cb *CacheBox) http.Handler`**

| Method | Path suffix | Body / Response |
|--------|-------------|-----------------|
| GET    | `/admin/cachebox`       | 200 → `Stats` JSON `{store: {entries, hits, misses, bytes_used, evictions}, recorder: {recorded, dropped, pending}}` |
| POST   | `/admin/cachebox/delay` | body: `{mu: float, sigma: float, seed?: uint64}` → 204. Calls `cb.SetDelaySource(NewDistributionDelaySource(mu, sigma, seed))`. |
| DELETE | `/admin/cachebox`       | 204. Calls `cb.Store().Clear()`. |

The handler routes by method + suffix within a single handler function, mirroring how `FaultAdminHandler` handles method dispatch.

**rules_admin.go — `RulesAdminHandler(eval *StaticEvaluator) http.Handler`**

| Method | Path suffix  | Body / Response |
|--------|--------------|-----------------|
| GET    | `/admin/rules` | 200 → `[]StaticRule` JSON |
| POST   | `/admin/rules` | body: `[]StaticRule` JSON → 204. Atomically replaces full rule set via `eval.SetRules(...)`. |

Rationale for why *mode changes* are in `/admin/rules` not `/admin/cachebox`: the cache-box action (`CacheBoxNone`/`Passthrough`/`Replay`/`ReplayDelay`) is a property of a `Decision`, not of the `CacheBox` coordinator itself. Switching a service from passthrough to replay means pushing a rule. The coordinator only manages the store and delay source.

### Why keep MutationPolicy / SafeMethods without SDK enforcement (Option B)

These fields capture operator intent that matters for paper reproducibility — "we only froze GET/HEAD operations" is metadata the experiment report should show. When the SDK eventually grows method filtering (post-MVP), the data model is already there. Cost today is near zero: the fields already exist and are already validated for well-formedness.

## File Structure

**manteion-go** (modify):
- `internal/model/experiment.go` — drop `ExperimentType`, `validExperimentTypes`
- `internal/model/experiment_test.go` — update test cases
- `internal/model/trace.go` — update `validKeyStrategies` map + field comment
- `internal/model/trace_test.go` — update key_strategy test cases
- `internal/db/migrations.go` — append migration v3
- `internal/store/experiment_repo.go` — remove `experiment_type` from SQL
- `AGENTS.md` — note new admin endpoints, updated KeyStrategy values

**atropos-go** (create):
- `cachebox_admin.go` — `CacheBoxAdminHandler`
- `cachebox_admin_test.go` — handler tests
- `rules_admin.go` — `RulesAdminHandler`
- `rules_admin_test.go` — handler tests

---

## Tasks

### Task 1: Update Experiment tests to reflect dropped ExperimentType

**Files:**
- Modify: `internal/model/experiment_test.go:8-41`

- [ ] **Step 1: Open `internal/model/experiment_test.go` and replace `TestExperiment_Validate` to drop the `ExperimentType` field and its validation cases**

Replace lines 8–41 with:

```go
func TestExperiment_Validate(t *testing.T) {
	base := func() Experiment {
		return Experiment{
			ID: "e1", Name: "attribution-checkout",
			PrimaryWorkloadID: "w1", Status: "planned", CreatedAt: time.Now(),
		}
	}

	tests := []struct {
		name    string
		modify  func(*Experiment)
		wantErr bool
	}{
		{"valid", nil, false},
		{"missing id", func(e *Experiment) { e.ID = "" }, true},
		{"missing name", func(e *Experiment) { e.Name = "" }, true},
		{"missing primary_workload_id", func(e *Experiment) { e.PrimaryWorkloadID = "" }, true},
		{"invalid status", func(e *Experiment) { e.Status = "bad" }, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := base()
			if tt.modify != nil {
				tt.modify(&e)
			}
			err := e.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it now fails (compile error against current struct)**

Run: `go test ./internal/model/ -run TestExperiment_Validate`
Expected: compile error — `unknown field 'ExperimentType'` is no longer referenced, but the `Experiment` struct still has `ExperimentType` so the test compiles. Then FAIL because `base()` no longer sets `ExperimentType` and existing validation rejects empty string.

### Task 2: Remove ExperimentType from Experiment struct

**Files:**
- Modify: `internal/model/experiment.go:10-59`

- [ ] **Step 1: Open `internal/model/experiment.go` and replace lines 10–59 (the Experiment struct, validation map, and Validate method)**

Replace with:

```go
// Experiment is an experiment plan for quantifying per-service contribution
// to workflow latency. The experiment's behavior is fully determined by its
// runs' FrozenServices and each cache-box's Mode — there is no separate
// "type" discriminator.
//
// PrimaryWorkloadID identifies the workflow being measured (e.g. checkout at 50 RPS).
// Background workloads (interference sources) are captured per-run via Attack
// entities with Role="background", allowing load to vary across runs.
type Experiment struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	Description       string     `json:"description,omitempty"`
	PrimaryWorkloadID string     `json:"primary_workload_id"`
	Status            string     `json:"status"`
	CreatedAt         time.Time  `json:"created_at"`
	StartedAt         *time.Time `json:"started_at,omitempty"`
	CompletedAt       *time.Time `json:"completed_at,omitempty"`
}

var validExperimentStatuses = map[string]bool{
	"planned": true, "running": true, "completed": true, "failed": true, "cancelled": true,
}

func (e *Experiment) Validate() error {
	if e.ID == "" {
		return errors.New("experiment: id required")
	}
	if e.Name == "" {
		return errors.New("experiment: name required")
	}
	if e.PrimaryWorkloadID == "" {
		return errors.New("experiment: primary_workload_id required")
	}
	if !validExperimentStatuses[e.Status] {
		return fmt.Errorf("experiment: invalid status %q", e.Status)
	}
	return nil
}
```

- [ ] **Step 2: Run the model tests to verify compile success and tests pass**

Run: `go test ./internal/model/ -run TestExperiment_Validate -v`
Expected: PASS — all 5 subtests pass.

- [ ] **Step 3: Commit**

```bash
git add internal/model/experiment.go internal/model/experiment_test.go
git commit -m "model: drop Experiment.ExperimentType field"
```

### Task 3: Add migration v3 to drop experiment_type column

**Files:**
- Modify: `internal/db/migrations.go:19-22`

- [ ] **Step 1: Append migration v3 to the migrations slice**

In `internal/db/migrations.go`, replace lines 19–22 with:

```go
var migrations = []migration{
	{1, "initial schema", initialSchema},
	{2, "add trace_anchors index", `CREATE INDEX IF NOT EXISTS idx_trace_anchors_run ON trace_anchors(experiment_run_id);`},
	{3, "drop experiments.experiment_type", `ALTER TABLE experiments DROP COLUMN IF EXISTS experiment_type;`},
}
```

- [ ] **Step 2: Build to confirm the migrations file still compiles**

Run: `go build ./...`
Expected: success.

### Task 4: Remove experiment_type from ExperimentRepo SQL

**Files:**
- Modify: `internal/store/experiment_repo.go:29-35`
- Modify: `internal/store/experiment_repo.go:46-53`
- Modify: `internal/store/experiment_repo.go:66-89`

- [ ] **Step 1: Replace the `Create` method body (lines 24–40)**

```go
func (r *ExperimentRepo) Create(ctx context.Context, exp *model.Experiment) error {
	if err := exp.Validate(); err != nil {
		return err
	}

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO experiments (id, name, description,
			primary_workload_id, status, created_at, started_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		exp.ID, exp.Name, nullString(exp.Description),
		exp.PrimaryWorkloadID, exp.Status, exp.CreatedAt, exp.StartedAt, exp.CompletedAt,
	)
	if err != nil {
		return fmt.Errorf("insert experiment: %w", err)
	}
	return nil
}
```

- [ ] **Step 2: Replace the `Get` method body (lines 42–62)**

```go
func (r *ExperimentRepo) Get(ctx context.Context, id string) (*model.Experiment, error) {
	var exp model.Experiment
	var desc sql.NullString
	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, description,
			primary_workload_id, status, created_at, started_at, completed_at
		FROM experiments WHERE id = $1`, id,
	).Scan(
		&exp.ID, &exp.Name, &desc,
		&exp.PrimaryWorkloadID, &exp.Status, &exp.CreatedAt, &exp.StartedAt, &exp.CompletedAt,
	)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get experiment: %w", err)
	}
	exp.Description = fromNullString(desc)
	return &exp, nil
}
```

- [ ] **Step 3: Replace the `List` method body (lines 64–89)**

```go
func (r *ExperimentRepo) List(ctx context.Context) ([]*model.Experiment, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, description,
			primary_workload_id, status, created_at, started_at, completed_at
		FROM experiments ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list experiments: %w", err)
	}
	defer rows.Close()

	var result []*model.Experiment
	for rows.Next() {
		var exp model.Experiment
		var desc sql.NullString
		if err := rows.Scan(
			&exp.ID, &exp.Name, &desc,
			&exp.PrimaryWorkloadID, &exp.Status, &exp.CreatedAt, &exp.StartedAt, &exp.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan experiment: %w", err)
		}
		exp.Description = fromNullString(desc)
		result = append(result, &exp)
	}
	return result, rows.Err()
}
```

- [ ] **Step 4: Update the initial schema to drop the column**

In `internal/db/migrations.go`, locate the `initialSchema` constant (contains the `CREATE TABLE experiments` statement, around line 239–251 of the file). Remove the `experiment_type` column and its CHECK constraint. The experiments table block becomes:

```sql
CREATE TABLE IF NOT EXISTS experiments (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    description         TEXT,
    primary_workload_id TEXT NOT NULL REFERENCES workloads(id),
    status              TEXT NOT NULL CHECK (status IN
        ('planned','running','completed','failed','cancelled')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ
);
```

This keeps migration v1 self-consistent for fresh database installations; migration v3 handles upgrades from existing installations that have the column.

- [ ] **Step 5: Build to verify all code compiles**

Run: `go build ./...`
Expected: success, no compile errors.

- [ ] **Step 6: Run the full model test suite**

Run: `go test ./internal/model/ -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/db/migrations.go internal/store/experiment_repo.go
git commit -m "store: drop experiment_type column; add migration v3"
```

### Task 5: Update CacheBoxConfig KeyStrategy values

**Files:**
- Modify: `internal/model/trace.go:50-66`
- Modify: `internal/model/trace_test.go:37`

- [ ] **Step 1: Update the `validKeyStrategies` map and the struct field comment**

In `internal/model/trace.go`, replace lines 50–66 with:

```go
// CacheBoxConfig describes cache-box state for a frozen service.
//
// The cache-box is the core research primitive: a service frozen at its SDK
// boundary to replay cached responses instead of doing real work. Zero CPU,
// zero queueing, but the call graph structure is preserved.
type CacheBoxConfig struct {
	Service          string                `json:"service"`
	Mode             string                `json:"mode"`             // "passthrough", "replay", "replay_with_delay"
	WorkflowScope    string                `json:"workflow_scope"`   // meta-trace-id pattern, "" = all traffic
	KeyStrategy      string                `json:"key_strategy"`     // "exact", "exact_with_host", "exact_with_body" — matches SDK cachebox.KeyStrategy
	MutationPolicy   string                `json:"mutation_policy"`  // "deny" (default), "allow". Metadata only — not enforced by SDK. Records operator intent for experiment reproducibility.
	SafeMethods      []string              `json:"safe_methods,omitempty"` // Metadata only — not enforced by SDK.
	SyntheticDelay   *SyntheticDelayConfig `json:"synthetic_delay,omitempty"`
	WarmupDurationMs int64                 `json:"warmup_duration_ms,omitempty"`
	CacheTTLMs       int64                 `json:"cache_ttl_ms,omitempty"`
}

var (
	validCacheBoxModes    = map[string]bool{"passthrough": true, "replay": true, "replay_with_delay": true}
	validKeyStrategies    = map[string]bool{"exact": true, "exact_with_host": true, "exact_with_body": true}
	validMutationPolicies = map[string]bool{"deny": true, "allow": true}
)
```

- [ ] **Step 2: Update the trace test combination run that uses "fuzzy"**

In `internal/model/experiment_test.go` (not trace_test.go — the offending case is in the experiment run test at line 77), replace the combination run test case's `KeyStrategy: "fuzzy"` with `KeyStrategy: "exact_with_host"`:

```go
{
    "valid combination",
    ExperimentRun{
        ID: "r3", ExperimentID: "e1", RunType: "combination",
        FrozenServices: []CacheBoxConfig{
            frozenProductcatalog,
            {Service: "currency", Mode: "replay", KeyStrategy: "exact_with_host", MutationPolicy: "deny"},
        },
        Status: "completed", CreatedAt: time.Now(),
    },
    false,
},
```

- [ ] **Step 3: Run model tests to confirm everything still passes**

Run: `go test ./internal/model/ -v`
Expected: PASS — all existing tests still valid (the "invalid key_strategy" test uses the literal `"bad"`, which remains invalid under the new map).

- [ ] **Step 4: Commit**

```bash
git add internal/model/trace.go internal/model/experiment_test.go
git commit -m "model: align CacheBoxConfig.KeyStrategy with SDK values; mark MutationPolicy as metadata"
```

### Task 6: Add CacheBoxAdminHandler to atropos-go — write failing test

**Files:**
- Create: `/Users/pronei/work/faults-lab/atropos-go/cachebox_admin_test.go`

- [ ] **Step 1: Create the test file with stats, delay, and clear tests**

```go
package atropos_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	atropos "github.com/microfaults/atropos-go"
)

func newTestCacheBox(t *testing.T) *atropos.CacheBox {
	t.Helper()
	cb := atropos.NewCacheBox(atropos.CacheBoxConfig{
		Store: atropos.NewCacheBoxMemStore(100),
	})
	t.Cleanup(cb.Stop)
	return cb
}

func TestCacheBoxAdminHandler_GetStats(t *testing.T) {
	cb := newTestCacheBox(t)
	h := atropos.CacheBoxAdminHandler(cb)

	req := httptest.NewRequest(http.MethodGet, "/admin/cachebox", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var got struct {
		Store    map[string]any `json:"store"`
		Recorder map[string]any `json:"recorder"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := got.Store["entries"]; !ok {
		t.Errorf("missing store.entries in response: %s", w.Body.String())
	}
	if _, ok := got.Recorder["recorded"]; !ok {
		t.Errorf("missing recorder.recorded in response: %s", w.Body.String())
	}
}

func TestCacheBoxAdminHandler_PostDelay(t *testing.T) {
	cb := newTestCacheBox(t)
	h := atropos.CacheBoxAdminHandler(cb)

	body := bytes.NewBufferString(`{"mu": 6.5, "sigma": 0.4, "seed": 42}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/cachebox/delay", body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204, body=%s", w.Code, w.Body.String())
	}
}

func TestCacheBoxAdminHandler_PostDelay_InvalidSigma(t *testing.T) {
	cb := newTestCacheBox(t)
	h := atropos.CacheBoxAdminHandler(cb)

	body := bytes.NewBufferString(`{"mu": 6.5, "sigma": -1.0}`)
	req := httptest.NewRequest(http.MethodPost, "/admin/cachebox/delay", body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestCacheBoxAdminHandler_DeleteClearsStore(t *testing.T) {
	cb := newTestCacheBox(t)
	h := atropos.CacheBoxAdminHandler(cb)

	req := httptest.NewRequest(http.MethodDelete, "/admin/cachebox", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if cb.Stats().Store.Entries != 0 {
		t.Errorf("entries = %d after DELETE, want 0", cb.Stats().Store.Entries)
	}
}

func TestCacheBoxAdminHandler_MethodNotAllowed(t *testing.T) {
	cb := newTestCacheBox(t)
	h := atropos.CacheBoxAdminHandler(cb)

	req := httptest.NewRequest(http.MethodPut, "/admin/cachebox", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", w.Code)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail (CacheBoxAdminHandler not defined)**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go test -run TestCacheBoxAdminHandler`
Expected: FAIL with `undefined: atropos.CacheBoxAdminHandler`.

### Task 7: Implement CacheBoxAdminHandler

**Files:**
- Create: `/Users/pronei/work/faults-lab/atropos-go/cachebox_admin.go`

- [ ] **Step 1: Create the handler file**

```go
package atropos

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/microfaults/atropos-go/internal/cachebox"
)

// delayRequest is the JSON body for POST /admin/cachebox/delay.
// Parameters configure a lognormal DistributionDelaySource for replay_with_delay mode.
type delayRequest struct {
	Mu    float64 `json:"mu"`
	Sigma float64 `json:"sigma"`
	Seed  uint64  `json:"seed,omitempty"`
}

// CacheBoxAdminHandler returns an http.Handler for runtime cache-box control.
//
// Supported operations:
//   - GET:    stats (store + recorder counters)
//   - POST /delay suffix: replace the delay source with a lognormal distribution
//   - DELETE: clear the cache store (keeps lifetime counters)
//
// Example:
//
//	mux.Handle("/admin/cachebox", atropos.CacheBoxAdminHandler(cb))
//	mux.Handle("/admin/cachebox/", atropos.CacheBoxAdminHandler(cb))
func CacheBoxAdminHandler(cb *CacheBox) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Dispatch: method + trailing path segment.
		suffix := ""
		if i := strings.LastIndex(r.URL.Path, "/"); i >= 0 && i < len(r.URL.Path)-1 {
			suffix = r.URL.Path[i+1:]
		}

		switch {
		case r.Method == http.MethodGet && suffix != "delay":
			stats := cb.Stats()
			_ = json.NewEncoder(w).Encode(stats)

		case r.Method == http.MethodPost && suffix == "delay":
			handleCacheBoxDelay(w, r, cb)

		case r.Method == http.MethodDelete:
			cb.Store().Clear()
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	})
}

func handleCacheBoxDelay(w http.ResponseWriter, r *http.Request, cb *CacheBox) {
	var req delayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid json: %s"}`, err), http.StatusBadRequest)
		return
	}
	if req.Sigma < 0 {
		http.Error(w, `{"error":"sigma must be non-negative"}`, http.StatusBadRequest)
		return
	}
	if req.Mu < 0 {
		http.Error(w, `{"error":"mu must be non-negative"}`, http.StatusBadRequest)
		return
	}
	ds := cachebox.NewDistributionDelaySource(req.Mu, req.Sigma, req.Seed)
	cb.SetDelaySource(ds)
	w.WriteHeader(http.StatusNoContent)
}
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go test -run TestCacheBoxAdminHandler -v`
Expected: all five subtests PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/pronei/work/faults-lab/atropos-go
git add cachebox_admin.go cachebox_admin_test.go
git commit -m "admin: add CacheBoxAdminHandler for stats, delay, clear"
```

### Task 8: Add RulesAdminHandler — write failing test

**Files:**
- Create: `/Users/pronei/work/faults-lab/atropos-go/rules_admin_test.go`

- [ ] **Step 1: Create the test file**

```go
package atropos_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	atropos "github.com/microfaults/atropos-go"
)

func TestRulesAdminHandler_GetEmpty(t *testing.T) {
	eval := atropos.NewStaticEvaluator()
	h := atropos.RulesAdminHandler(eval)

	req := httptest.NewRequest(http.MethodGet, "/admin/rules", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var rules []atropos.StaticRule
	if err := json.Unmarshal(w.Body.Bytes(), &rules); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(rules) != 0 {
		t.Errorf("rules = %d, want 0", len(rules))
	}
}

func TestRulesAdminHandler_PostReplacesRules(t *testing.T) {
	eval := atropos.NewStaticEvaluator()
	h := atropos.RulesAdminHandler(eval)

	body := bytes.NewBufferString(`[
		{"name":"freeze-productcatalog","point":1,"labels":{"service":"productcatalog"},"decision":{"cache_box":2}}
	]`)
	req := httptest.NewRequest(http.MethodPost, "/admin/rules", body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204, body=%s", w.Code, w.Body.String())
	}

	got := eval.Rules()
	if len(got) != 1 {
		t.Fatalf("rules = %d, want 1", len(got))
	}
	if got[0].Name != "freeze-productcatalog" {
		t.Errorf("name = %q, want %q", got[0].Name, "freeze-productcatalog")
	}
}

func TestRulesAdminHandler_PostInvalidJSON(t *testing.T) {
	eval := atropos.NewStaticEvaluator()
	h := atropos.RulesAdminHandler(eval)

	req := httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewBufferString(`not json`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestRulesAdminHandler_MethodNotAllowed(t *testing.T) {
	eval := atropos.NewStaticEvaluator()
	h := atropos.RulesAdminHandler(eval)

	req := httptest.NewRequest(http.MethodDelete, "/admin/rules", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", w.Code)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go test -run TestRulesAdminHandler`
Expected: FAIL with `undefined: atropos.RulesAdminHandler`.

### Task 9: Implement RulesAdminHandler

**Files:**
- Create: `/Users/pronei/work/faults-lab/atropos-go/rules_admin.go`

- [ ] **Step 1: Create the handler file**

```go
package atropos

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// RulesAdminHandler returns an http.Handler for runtime rule-set management.
//
// Supported methods:
//   - GET:  return the current rule set as JSON array
//   - POST: replace the full rule set (atomic); body is a JSON array of StaticRule
//
// Example:
//
//	eval := atropos.NewStaticEvaluator()
//	atropos.Configure(atropos.WithEvaluator(eval))
//	mux.Handle("/admin/rules", atropos.RulesAdminHandler(eval))
func RulesAdminHandler(eval *StaticEvaluator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodGet:
			rules := eval.Rules()
			if rules == nil {
				rules = []StaticRule{}
			}
			_ = json.NewEncoder(w).Encode(rules)

		case http.MethodPost:
			var rules []StaticRule
			if err := json.NewDecoder(r.Body).Decode(&rules); err != nil {
				http.Error(w, fmt.Sprintf(`{"error":"invalid json: %s"}`, err), http.StatusBadRequest)
				return
			}
			eval.SetRules(rules)
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	})
}
```

- [ ] **Step 2: Run the tests to verify they pass**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go test -run TestRulesAdminHandler -v`
Expected: all four subtests PASS.

- [ ] **Step 3: Run the full atropos test suite to confirm no regressions**

Run: `cd /Users/pronei/work/faults-lab/atropos-go && go test ./...`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
cd /Users/pronei/work/faults-lab/atropos-go
git add rules_admin.go rules_admin_test.go
git commit -m "admin: add RulesAdminHandler for runtime rule management"
```

### Task 10: Document the new endpoints in AGENTS.md (atropos-go)

**Files:**
- Modify: `/Users/pronei/work/faults-lab/atropos-go/AGENTS.md`

- [ ] **Step 1: Locate the section describing admin handlers (search for `FaultAdminHandler`) and append a companion subsection**

Add below the FaultAdminHandler documentation:

```markdown
### CacheBoxAdminHandler

`CacheBoxAdminHandler(cb *CacheBox) http.Handler` exposes runtime cache-box control.

| Method | Path            | Body                                | Response |
|--------|-----------------|-------------------------------------|----------|
| GET    | `/admin/cachebox`       | —                                    | 200 `Stats` JSON |
| POST   | `/admin/cachebox/delay` | `{"mu": float, "sigma": float, "seed"?: uint64}` | 204 |
| DELETE | `/admin/cachebox`       | —                                    | 204 (clears store) |

### RulesAdminHandler

`RulesAdminHandler(eval *StaticEvaluator) http.Handler` exposes runtime rule-set management.

| Method | Path           | Body                   | Response |
|--------|----------------|------------------------|----------|
| GET    | `/admin/rules` | —                      | 200 `[]StaticRule` JSON |
| POST   | `/admin/rules` | `[]StaticRule` JSON    | 204 (atomic replace) |
```

- [ ] **Step 2: Commit the docs**

```bash
cd /Users/pronei/work/faults-lab/atropos-go
git add AGENTS.md
git commit -m "docs: describe CacheBoxAdminHandler and RulesAdminHandler"
```

### Task 11: Update manteion AGENTS.md to reflect model changes

**Files:**
- Modify: `/Users/pronei/work/faults-lab/manteion-go/AGENTS.md`

- [ ] **Step 1: Search AGENTS.md for any references to `experiment_type`, `ExperimentType`, `fuzzy`, or `parametric` and update**

Specifically:
- Remove any mention of "5 experiment types" or enumerations including `interference`/`isolation`/`attribution`/`scenario`/`cache_fidelity`.
- Replace references to `fuzzy`/`parametric` key strategies with `exact_with_host`/`exact_with_body`.
- Add a note under the Model section: "CacheBoxConfig.MutationPolicy and CacheBoxConfig.SafeMethods are reproducibility metadata; not enforced by the SDK."

- [ ] **Step 2: Run the full manteion test suite one more time**

Run: `cd /Users/pronei/work/faults-lab/manteion-go && go test ./...`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/pronei/work/faults-lab/manteion-go
git add AGENTS.md
git commit -m "docs: reflect ExperimentType removal and SDK-aligned KeyStrategy values"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] ExperimentType removal — Tasks 1–4
- [x] KeyStrategy realignment — Task 5
- [x] MutationPolicy metadata annotation — Task 5
- [x] Atropos cache-box admin endpoints — Tasks 6–7
- [x] Atropos rules admin endpoints — Tasks 8–9
- [x] Documentation updates — Tasks 10–11

**Out of scope for this plan (deferred):**
- Sidecar proxy mode for polyglot support (future VISION.md item, not MVP)
- Wiring manteion's rule-push to actually call these new admin endpoints (separate plan — requires adding an HTTP client in manteion that tracks SDK instance URLs)
- Cache-box mode enforcement for MutationPolicy (post-MVP; when SDK grows method filtering)

**Open questions for execution:**
- atropos-go module path: `github.com/microfaults/atropos-go` (confirmed).
- The `StaticRule.Point` field uses `InjectionPoint` (an int enum). Test bodies serialize it as `1` (Egress). Confirm the JSON encoding matches during the test run — if Go encodes the underlying int, the test JSON is correct; if there's a custom `MarshalJSON` producing a string, update the POST body to use the string form.
