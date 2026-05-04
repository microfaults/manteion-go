# Cache Preloading + Zeus Workload Integration

> **Date:** 2026-04-28
> **Scope:** manteion-go — extends `2026-04-28-experiment-orchestration.md`
> **Prerequisite:** experiment orchestration plan must be fully implemented (it is)

---

## Context: Q&A Answers That Drive This Plan

**Q: What does "populate response cache on next startup" mean for the cache-box?**

The atropos cache-box is in-memory only — it is lost on pod restart. The approach is:
- During the baseline run, every atropos SDK instance streams its captured
  request/response pairs to manteion via the existing `CachePushClient` →
  `POST /api/v1/cache/ingest`.
- Manteion writes those entries to disk as JSON files (not database rows).
  File layout: `{CACHE_DIR}/{run_id}/{service}.json`
- When an isolation run starts, the orchestrator reads the baseline run's
  files and fans the entries out to every live SDK instance of each frozen
  service via a new `POST /admin/cachebox/entries` admin endpoint.
- **No pod restart required.** The cache is hot before the first request.

**Q: Does the orchestrator trigger zeus-go to start load generation, or is that manual?**

Yes — automatic. When `StartRun` is called the orchestrator POSTs to zeus-go's
`POST /api/v1/attacks` to launch the primary workload attack for the run's
duration. When `StopRun` is called it DELETEs that attack. The `AttackID`
returned by zeus is persisted on the `ExperimentRun` row so a crash-restart
can clean up.

---

## What Already Exists — Do Not Rebuild

| What | Where |
|---|---|
| `atropos.CachePushClient` — pushes entries service→manteion | `atropos-go/cache_push.go` |
| `atropos.Seed()` — pulls entries manteion→SDK on startup | `atropos-go/cache_seed.go` |
| `cachebox.WireEntry` — JSON-serializable entry wire format | `atropos-go/internal/cachebox/wire.go` |
| `atropos.CacheBoxAdminHandler` — GET stats / POST delay / DELETE clear | `atropos-go/cachebox_admin.go` |
| `atrocontrol.Controller` — fanout to SDK instances | `internal/atrocontrol/` |
| `atropos.Client` — HTTP to SDK admin endpoints | `internal/atropos/` |
| `zeus.Client.Do()` — generic HTTP to Archer | `internal/zeus/client.go` |
| `ExperimentRepo.GetRun`, `UpdateRunStatus` | `internal/store/experiment_repo.go` |
| `Orchestrator.StartRun` / `StopRun` | `internal/orchestrator/orchestrator.go` |

---

## Architecture

```
Baseline run
  atropos SDK (each service pod)
      │  CachePushClient  POST /api/v1/cache/ingest
      ▼
  manteion  ──writes──► {CACHE_DIR}/{run_id}/{service}.json

Orchestrator.StopRun("baseline") calls snapshotEntries()
  copies latest ingest files → {CACHE_DIR}/{run_id}/{service}.json
  writes cache_snapshots row (run_id, service, file_path) to postgres

Isolation run
  Orchestrator.StartRun()
      │
      ├─1─ loadBaselineEntries(experimentID)
      │       reads {CACHE_DIR}/{baseline_run_id}/{service}.json
      │
      ├─2─ controller.PreloadEntries(service, entries)
      │       POST /admin/cachebox/entries  to every SDK instance
      │
      ├─3─ enterPhase(0)          — push fault rules
      │
      └─4─ zeus.StartAttack(...)   — kick off load generation
               returns attackID → stored on ExperimentRun row

Orchestrator.StopRun()
      ├─ cancel watcher goroutine
      ├─ controller.PushRules(nil)   — clear fault rules
      └─ zeus.StopAttack(attackID)   — stop load generation
```

---

## Phase 1: New atropos-go admin endpoint — `POST /admin/cachebox/entries`

**File:** `atropos-go/cachebox_admin.go`

Add a route that accepts `[]cachebox.WireEntry` and bulk-inserts them into the
store. This is how manteion preloads a running pod's cache without a restart.

```go
// In CacheBoxAdminHandler, add new case:
case r.Method == http.MethodPost && suffix == "entries":
    handleCacheBoxPreload(w, r, cb)
```

```go
func handleCacheBoxPreload(w http.ResponseWriter, r *http.Request, cb *CacheBox) {
    var entries []cachebox.WireEntry
    if err := json.NewDecoder(r.Body).Decode(&entries); err != nil {
        http.Error(w, fmt.Sprintf(`{"error":"invalid json: %s"}`, err), http.StatusBadRequest)
        return
    }
    for i := range entries {
        e := cachebox.WireToEntry(&entries[i])
        cb.Store().Put(e.Key, e)
    }
    w.WriteHeader(http.StatusNoContent)
}
```

The `Store()` accessor must be added to `CacheBox` if not already present:

```go
func (cb *CacheBox) Store() Store { return cb.store }
```

---

## Phase 2: Manteion atropos client — `PostCacheEntries`

**File:** `internal/atropos/client_cachebox.go`

```go
func (c *Client) PostCacheEntries(ctx context.Context, addr string, entries []cachebox.WireEntry) error {
    _, err := c.doExpectStatus(ctx, http.MethodPost, addr+"/admin/cachebox/entries", entries, http.StatusNoContent)
    return err
}
```

---

## Phase 3: Manteion Controller — `PreloadEntries`

**New method** on `atrocontrol.Controller` (add to `internal/atrocontrol/freeze.go`
or a new file `internal/atrocontrol/preload.go`):

```go
// PreloadEntries fans out a set of cache entries to all live SDK instances
// of the given service. Failures are logged but do not abort the run —
// a pod that misses preload will serve cache misses rather than crashing.
func (c *Controller) PreloadEntries(
    ctx context.Context,
    service string,
    entries []cachebox.WireEntry,
    opts ...CallOption,
) (FanoutResult, error) {
    co := c.resolveCallOpts(opts)
    targets, err := c.resolveTargets(ctx, service, co.filter)
    if err != nil {
        return FanoutResult{}, err
    }
    result := fanout(ctx, targets, func(ctx context.Context, t target) error {
        return c.tx.PostCacheEntries(ctx, t.address, entries)
    }, co)
    c.logFanout("preload_entries", service, co.runID, result)
    return result, nil
}
```

---

## Phase 4: File-based cache store

**New package:** `internal/cachestore/`

**`internal/cachestore/store.go`**

```go
package cachestore

import (
    "encoding/json"
    "fmt"
    "os"
    "path/filepath"

    "atropos-go/internal/cachebox"
)

// Store persists WireEntry slices as JSON files under a root directory.
// Layout: {root}/{runID}/{service}.json
// One file per (run, service) pair; writing replaces the previous file atomically.
type Store struct {
    root string
}

func New(root string) *Store { return &Store{root: root} }

// Write serialises entries to {root}/{runID}/{service}.json.
// The parent directory is created if it does not exist.
func (s *Store) Write(runID, service string, entries []cachebox.WireEntry) error {
    dir := filepath.Join(s.root, runID)
    if err := os.MkdirAll(dir, 0o755); err != nil {
        return fmt.Errorf("cachestore: mkdir: %w", err)
    }
    path := filepath.Join(dir, service+".json")
    tmp := path + ".tmp"
    f, err := os.Create(tmp)
    if err != nil {
        return fmt.Errorf("cachestore: create temp: %w", err)
    }
    if err := json.NewEncoder(f).Encode(entries); err != nil {
        f.Close()
        os.Remove(tmp)
        return fmt.Errorf("cachestore: encode: %w", err)
    }
    f.Close()
    return os.Rename(tmp, path) // atomic replace
}

// Read returns all entries for the given (run, service) pair.
// Returns (nil, nil) if the file does not exist yet.
func (s *Store) Read(runID, service string) ([]cachebox.WireEntry, error) {
    path := filepath.Join(s.root, runID, service+".json")
    f, err := os.Open(path)
    if os.IsNotExist(err) {
        return nil, nil
    }
    if err != nil {
        return nil, fmt.Errorf("cachestore: open: %w", err)
    }
    defer f.Close()
    var entries []cachebox.WireEntry
    if err := json.NewDecoder(f).Decode(&entries); err != nil {
        return nil, fmt.Errorf("cachestore: decode: %w", err)
    }
    return entries, nil
}
```

**Environment variable:** `CACHE_DIR` (default `/var/cache/manteion`).

---

## Phase 5: Cache ingest + serve endpoints

**New file:** `internal/api/cache_handler.go`

These two endpoints are what the atropos-go SDK talks to:
`CachePushClient` → `POST /api/v1/cache/ingest`
`Seed()` → `GET /api/v1/cache/entries?service=X`

```go
package api

import (
    "net/http"
    "atropos-go/internal/cachebox"
)

// ingestEnvelope matches the body posted by atropos.CachePushClient.
type ingestEnvelope struct {
    Service  string               `json:"service"`
    Instance string               `json:"instance"`
    Entries  []cachebox.WireEntry `json:"entries"`
}

// handleCacheIngest receives a batch of WireEntries from an SDK instance and
// writes them to the file store under the currently-active baseline run for
// that service. If no run is active the batch is silently dropped.
func (s *Server) handleCacheIngest(w http.ResponseWriter, r *http.Request) {
    var env ingestEnvelope
    if err := readJSON(r, &env); err != nil {
        writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
        return
    }

    runID := s.orch.ActiveBaselineRunID(env.Service)
    if runID == "" {
        w.WriteHeader(http.StatusNoContent) // no active run — drop silently
        return
    }

    if err := s.cacheStore.Write(runID, env.Service, env.Entries); err != nil {
        s.logger.Error("cache ingest write failed",
            "service", env.Service, "run_id", runID, "error", err)
        writeError(w, http.StatusInternalServerError, "write failed")
        return
    }
    w.WriteHeader(http.StatusNoContent)
}

type seedResponse struct {
    Entries []cachebox.WireEntry `json:"entries"`
}

// handleCacheEntries serves the entries for the given service.
// Called by atropos.Seed() on SDK startup.
// Query params: service (required), run_id (optional — uses latest if omitted).
func (s *Server) handleCacheEntries(w http.ResponseWriter, r *http.Request) {
    service := r.URL.Query().Get("service")
    if service == "" {
        writeError(w, http.StatusBadRequest, "service query param required")
        return
    }
    runID := r.URL.Query().Get("run_id")
    if runID == "" {
        runID = s.orch.ActiveBaselineRunID(service)
    }
    if runID == "" {
        writeJSON(w, http.StatusOK, seedResponse{Entries: []cachebox.WireEntry{}})
        return
    }

    entries, err := s.cacheStore.Read(runID, service)
    if err != nil {
        s.logger.Error("cache entries read failed",
            "service", service, "run_id", runID, "error", err)
        writeError(w, http.StatusInternalServerError, "read failed")
        return
    }
    if entries == nil {
        entries = []cachebox.WireEntry{}
    }
    writeJSON(w, http.StatusOK, seedResponse{Entries: entries})
}
```

Add to `Server` struct and `NewServer`:
```go
cacheStore *cachestore.Store
```

Register routes in `server.go`:
```go
mux.HandleFunc("POST /api/v1/cache/ingest", s.handleCacheIngest)
mux.HandleFunc("GET  /api/v1/cache/entries", s.handleCacheEntries)
```

---

## Phase 6: Active baseline tracking in Orchestrator

The cache ingest handler needs to ask the orchestrator "which run is the active
baseline for service X?" The orchestrator already tracks active run IDs in
`o.running`. Extend it with a second map:

**File:** `internal/orchestrator/orchestrator.go`

```go
type Orchestrator struct {
    // ... existing fields ...

    mu             sync.Mutex
    running        map[string]context.CancelFunc // run ID → cancel watcher
    activeBaseline map[string]string             // service → run ID (baseline only)
}
```

Update `New()` to initialise `activeBaseline`.

In `StartRun`, when `run.RunType == "baseline"`:
```go
o.mu.Lock()
for _, fs := range run.FrozenServices {
    // For a baseline run, frozen services may be empty — register all
    // services implied by the experiment instead.
}
// Simpler: register the run itself; ingest uses experiment_id lookup if needed.
// For now: use run.ID as the active baseline for all services in the run.
o.activeBaseline[run.ID] = run.ID // keyed by service in full implementation
o.mu.Unlock()
```

In practice, key by service name: when a baseline run starts, mark every service
in `run.FrozenServices` (or, for the baseline case where FrozenServices is empty,
look up the experiment's target services from the DB) as having `activeBaseline[service] = run.ID`.

Add the exported accessor:
```go
// ActiveBaselineRunID returns the run ID of the active baseline recording
// for the given service, or "" if no baseline is in progress.
func (o *Orchestrator) ActiveBaselineRunID(service string) string {
    o.mu.Lock()
    defer o.mu.Unlock()
    return o.activeBaseline[service]
}
```

When `StopRun` is called for a baseline run, do NOT remove the active baseline
entry — the files persist and must remain readable for subsequent isolation runs.
Only overwrite `activeBaseline[service]` when a new baseline starts.

---

## Phase 7: Orchestrator — preload entries before isolation runs

**File:** `internal/orchestrator/orchestrator.go`

Add a helper that looks up the baseline run for an experiment, reads each
frozen service's file, and fans entries to live instances:

```go
// preloadCacheEntries reads the baseline snapshot for each frozen service
// and pushes entries to all live SDK instances of that service.
// Called by StartRun before enterPhase(0) for non-baseline runs.
func (o *Orchestrator) preloadCacheEntries(ctx context.Context, run *model.ExperimentRun) {
    baseline, err := o.experiments.GetBaselineRun(ctx, run.ExperimentID)
    if err != nil {
        o.logger.Warn("orchestrator: no baseline run found, skipping cache preload",
            "experiment_id", run.ExperimentID, "error", err)
        return
    }

    for _, fs := range run.FrozenServices {
        entries, err := o.cacheStore.Read(baseline.ID, fs.Service)
        if err != nil || len(entries) == 0 {
            o.logger.Warn("orchestrator: no cache entries for service, skipping preload",
                "service", fs.Service, "baseline_run_id", baseline.ID)
            continue
        }
        if _, err := o.controller.PreloadEntries(ctx, fs.Service, entries); err != nil {
            o.logger.Warn("orchestrator: preload entries failed",
                "service", fs.Service, "error", err)
        } else {
            o.logger.Info("orchestrator: cache preloaded",
                "service", fs.Service, "entries", len(entries))
        }
    }
}
```

Update `StartRun` to call `preloadCacheEntries` for non-baseline runs:
```go
func (o *Orchestrator) StartRun(ctx context.Context, runID string) error {
    run, err := o.experiments.GetRun(ctx, runID)
    // ...

    if run.RunType != "baseline" {
        o.preloadCacheEntries(ctx, run)
    }

    if err := o.enterPhase(ctx, run, 0); err != nil { ... }
    // ...
}
```

Add `cacheStore *cachestore.Store` to `Orchestrator` struct and `New()`.

---

## Phase 8: ExperimentRepo — `GetBaselineRun`

**File:** `internal/store/experiment_repo.go`

```go
// GetBaselineRun returns the most recently completed baseline run for an
// experiment, or ErrNotFound if none exists yet.
func (r *ExperimentRepo) GetBaselineRun(ctx context.Context, experimentID string) (*model.ExperimentRun, error) {
    var run model.ExperimentRun
    var frozenJSON, placementJSON, phaseRulesJSON, transCondJSON []byte

    err := r.db.QueryRowContext(ctx, `
        SELECT id, experiment_id, run_type, run_index,
            frozen_services, meta_trace_id, status, node_placement,
            started_at, completed_at, created_at,
            phase_rules, transition_cond, current_phase
        FROM experiment_runs
        WHERE experiment_id = $1
          AND run_type = 'baseline'
          AND status = 'completed'
        ORDER BY created_at DESC
        LIMIT 1`, experimentID,
    ).Scan(
        &run.ID, &run.ExperimentID, &run.RunType, &run.RunIndex,
        &frozenJSON, &run.MetaTraceID, &run.Status, &placementJSON,
        &run.StartedAt, &run.CompletedAt, &run.CreatedAt,
        &phaseRulesJSON, &transCondJSON, &run.CurrentPhase,
    )
    if err == sql.ErrNoRows {
        return nil, ErrNotFound
    }
    if err != nil {
        return nil, fmt.Errorf("get baseline run: %w", err)
    }
    // unmarshal JSON columns
    jsonbScan(frozenJSON, &run.FrozenServices)
    jsonbScan(placementJSON, &run.NodePlacement)
    jsonbScan(phaseRulesJSON, &run.PhaseRules)
    jsonbScan(transCondJSON, &run.TransitionCond)
    return &run, nil
}
```

---

## Phase 9: Zeus workload integration

### 9a. Model — `ZeusAttackID` on `ExperimentRun`

**File:** `internal/model/experiment.go`

```go
type ExperimentRun struct {
    // ... existing fields ...
    ZeusAttackID string `json:"zeus_attack_id,omitempty"` // set when orchestrator starts zeus attack
}
```

### 9b. DB migration v6

**File:** `internal/db/migrations.go`

```go
{6, "add experiment_run zeus_attack_id", `
    ALTER TABLE experiment_runs
        ADD COLUMN IF NOT EXISTS zeus_attack_id TEXT;
`},
```

### 9c. ExperimentRepo — `UpdateRunZeusAttack`

**File:** `internal/store/experiment_repo.go`

```go
func (r *ExperimentRepo) UpdateRunZeusAttack(ctx context.Context, id, attackID string) error {
    _, err := r.db.ExecContext(ctx,
        `UPDATE experiment_runs SET zeus_attack_id = $2 WHERE id = $1`, id, attackID)
    return err
}
```

Also include `zeus_attack_id` in the `GetRun` and `ListRunsByExperiment` SELECT/scan.

### 9d. Zeus client typed methods

**File:** `internal/zeus/client.go`

Add typed wrappers over the existing `Do()` method so the orchestrator does not
construct raw JSON in-line.

```go
import (
    "bytes"
    "encoding/json"
    "fmt"
)

type AttackRequest struct {
    WorkloadID  string `json:"workload_id"`
    Service     string `json:"service"`
    Role        string `json:"role"`       // "primary"
    TargetURL   string `json:"target_url"`
    TargetMethod string `json:"target_method"`
    Rate        int    `json:"rate"`
    DurationMs  int64  `json:"duration_ms"`
    MetaTraceID string `json:"meta_trace_id,omitempty"`
}

type AttackResponse struct {
    ID string `json:"id"`
}

// StartAttack POSTs to Archer's POST /api/v1/attacks and returns the attack ID.
func (c *Client) StartAttack(ctx context.Context, req AttackRequest) (string, error) {
    body, _ := json.Marshal(req)
    resp, err := c.Do(ctx, http.MethodPost, "/attacks", bytes.NewReader(body))
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
        return "", fmt.Errorf("zeus: start attack returned %d", resp.StatusCode)
    }
    var ar AttackResponse
    json.NewDecoder(resp.Body).Decode(&ar)
    return ar.ID, nil
}

// StopAttack sends DELETE /api/v1/attacks/{id} to Archer.
func (c *Client) StopAttack(ctx context.Context, attackID string) error {
    resp, err := c.Do(ctx, http.MethodDelete, "/attacks/"+attackID, nil)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
        return fmt.Errorf("zeus: stop attack returned %d", resp.StatusCode)
    }
    return nil
}
```

### 9e. Add zeus to Orchestrator

**File:** `internal/orchestrator/orchestrator.go`

```go
type Orchestrator struct {
    // ... existing fields ...
    zeus *zeus.Client // may be nil if ZEUS_URL unset
}
```

In `StartRun`, after `enterPhase(0)` and before spawning `watchPhase`:
```go
if o.zeus != nil && run.ZeusAttackID == "" {
    exp, err := o.experiments.Get(ctx, run.ExperimentID)
    if err == nil {
        attackID, err := o.zeus.StartAttack(ctx, zeus.AttackRequest{
            WorkloadID:   exp.PrimaryWorkloadID,
            Service:      "primary",
            Role:         "primary",
            TargetURL:    "",   // resolved by zeus from workload config
            TargetMethod: "GET",
            Rate:         0,    // use workload default
            DurationMs:   0,    // open-ended; stopped explicitly by StopRun
            MetaTraceID:  run.MetaTraceID,
        })
        if err != nil {
            o.logger.Warn("orchestrator: start zeus attack failed",
                "run_id", runID, "error", err)
        } else {
            run.ZeusAttackID = attackID
            o.experiments.UpdateRunZeusAttack(ctx, runID, attackID)
            o.logger.Info("orchestrator: zeus attack started",
                "run_id", runID, "attack_id", attackID)
        }
    }
}
```

In `StopRun`, after clearing fault rules:
```go
if o.zeus != nil && run.ZeusAttackID != "" {
    if err := o.zeus.StopAttack(ctx, run.ZeusAttackID); err != nil {
        o.logger.Warn("orchestrator: stop zeus attack failed",
            "run_id", runID, "attack_id", run.ZeusAttackID, "error", err)
    }
}
```

---

## Phase 10: Wire in `cmd/manteion/main.go`

```go
cacheDir := envOr("CACHE_DIR", "/var/cache/manteion")
cs := cachestore.New(cacheDir)

orch := orchestrator.New(experimentRepo, ruleRepo, faultRepo, controller, promClient, zeusClient, cs, logger)

srv := api.NewServer(...existing args..., orch, cs)
```

Update `orchestrator.New` and `api.NewServer` signatures to accept the new parameters.

**Environment variable added:**

| Variable | Default | Purpose |
|---|---|---|
| `CACHE_DIR` | `/var/cache/manteion` | Root directory for baseline response files |

---

## Implementation Order

Execute in order — each step compiles before the next:

- [ ] **Step 1** — `atropos-go/cachebox_admin.go`: add `POST /admin/cachebox/entries` route + `handleCacheBoxPreload`. Expose `Store()` accessor on `CacheBox` if missing. `go build ./... ` in `atropos-go/`
- [ ] **Step 2** — `internal/atropos/client_cachebox.go`: add `PostCacheEntries`. `go build ./internal/atropos/...`
- [ ] **Step 3** — `internal/atrocontrol/` (new `preload.go`): add `PreloadEntries`. `go build ./internal/atrocontrol/...`
- [ ] **Step 4** — `internal/cachestore/store.go`: implement `Store`, `Write`, `Read`. `go build ./internal/cachestore/...`
- [ ] **Step 5** — `internal/store/experiment_repo.go`: add `GetBaselineRun`, `UpdateRunZeusAttack`. Add `zeus_attack_id` to SELECT/scan in `GetRun`/`ListRunsByExperiment`. `go build ./internal/store/...`
- [ ] **Step 6** — `internal/model/experiment.go`: add `ZeusAttackID` field. `go build ./internal/model/...`
- [ ] **Step 7** — `internal/db/migrations.go`: append migration v6. `go build ./internal/db/...`
- [ ] **Step 8** — `internal/zeus/client.go`: add `StartAttack`, `StopAttack`. `go build ./internal/zeus/...`
- [ ] **Step 9** — `internal/orchestrator/orchestrator.go`: add `activeBaseline` map, `ActiveBaselineRunID()`, `preloadCacheEntries()`, zeus calls in `StartRun`/`StopRun`. Add `zeus` and `cacheStore` fields. `go build ./internal/orchestrator/...`
- [ ] **Step 10** — `internal/api/cache_handler.go`: implement `handleCacheIngest`, `handleCacheEntries`. Register routes. Add `cacheStore` to `Server`. `go build ./internal/api/...`
- [ ] **Step 11** — `cmd/manteion/main.go`: wire `cachestore.New`, update `orchestrator.New` and `api.NewServer`. `go build ./cmd/manteion`
- [ ] **Step 12** — `go build ./...` + `go vet ./...` + `go test ./...`

---

## Key Constraints

- **Files, not DB rows** — `WireEntry` bytes live on disk. The DB only tracks
  metadata (which run produced which service's file). This is intentional: a
  full checkout response corpus can be tens of MB and would bloat Postgres.
- **Baseline run, `FrozenServices` is empty** — baseline runs have no frozen
  services by model constraint. `preloadCacheEntries` is skipped for baseline
  runs (`run.RunType != "baseline"` guard). `ActiveBaselineRunID` is keyed by
  service names derived from the experiment's isolation runs — or simply from
  the experiment's service list if that field is added to `Experiment`.
- **Ingest drops silently when no baseline is active** — this prevents spurious
  file writes from stale SDK instances. The `204 No Content` response avoids
  CachePushClient retries.
- **Zeus is optional** — if `ZEUS_URL` is unset or zeus is unreachable, the
  orchestrator logs a warning but does not fail the run. Load generation must
  be started manually in that case.
- **Zeus attack duration** — send `duration_ms: 0` (or a large value) to
  indicate open-ended; the orchestrator stops it explicitly via `StopAttack`.
  Confirm with the zeus-go API whether `0` means open-ended or requires a
  specific sentinel.
- **`POST /admin/cachebox/entries` is append or replace?** — `Store().Put()`
  overwrites per key. Calling this endpoint twice with the same entries is
  idempotent; duplicate keys are silently overwritten.
