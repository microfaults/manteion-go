# Manually-Triggered Persistent Faults — Task Breakdown

## Branches

| Repo | Branch | Off |
|---|---|---|
| `atropos-go` | `feat/long-running-faults` | `develop` |
| `manteion-go` | `feat/atrocontrol/long-running-faults` | `develop` |

---

## Phase 1: Multi-Slot DemoEvaluator + Completion Callback

**Repo**: `atropos-go` → `feat/fault-configs`

### Tasks

- [x] **1.1 Refactor DemoEvaluator to multi-slot** — `admin.go`
  - Replace single `decision`/`req` with `slots map[string]*faultSlot`
  - `faultSlot` struct: `{ decision *Decision, req *FaultRequest, callbackURL string }`
  - Slot key = `category:type` (e.g. `"inline:latency"`)
  - `Set(decision, req)` → upserts slot
  - `Clear()` → clears ALL slots, fires callbacks
  - `ClearSlot(category, faultType)` → clears one slot, fires callback
  - `Evaluate()` → iterates slots, returns first non-nil decision
  - `Active() *FaultRequest` → first active (backward compat)
  - `ActiveSlots() []*FaultRequest` → all active
  - `ActiveSlot(category, faultType) *FaultRequest` → one or nil

- [x] **1.2 Add CallbackURL field to FaultRequest** — `admin.go`
  - `CallbackURL string json:"callback_url,omitempty"`
  - Stored in `faultSlot.callbackURL` on `Set()`

- [x] **1.3 Add slot-based DELETE route** — `admin.go`
  - `DELETE /admin/fault/{category}/{type}` → `eval.ClearSlot()`
  - `DELETE /admin/fault` still clears all (backward compat)
  - `GET /admin/fault` returns all active faults

- [x] **1.4 Create callback.go**
  - `notifyCompletion(url string)` — fire-and-forget POST, 5s timeout

- [x] **1.5 Tests** — `admin_test.go`
  - Set latency + CPU → both active
  - Set latency again → replaces old latency, CPU untouched
  - ClearSlot("inline", "latency") → CPU still active
  - Clear() → all gone
  - Callback fires on clear
  - Backward compat with single-fault usage

### Verify
```bash
go build ./... && go vet ./... && go test -v -run TestDemoEvaluator ./...
```

---

## Phase 2: Fault Config Domain Model + Migration

**Repo**: `manteion-go` → `feat/fault-configs`

### Tasks

- [x] **2.1 Create internal/model/fault_config.go**
  - `FaultConfigStatus`: `ready`, `active`, `completed`, `manually_cancelled`, `failed`
  - `FaultConfig` struct: ID, Name, Description, Service, Category, FaultType, FaultReq (`json.RawMessage`), Status, CreatedAt, UpdatedAt, FiredAt, CompletedAt
  - `Validate()` — required fields + category/fault_type validation (reuse `validFaultTypes`)
  - `CanFire()` — true when status ≠ active
  - `CanEdit()` / `CanDelete()` — true when status ≠ active
  - `ConflictKey()` — returns `"category:fault_type"`

- [x] **2.2 Create internal/model/fault_config_test.go**
  - Table-driven tests for Validate, CanFire, CanEdit, CanDelete

- [x] **2.3 Add migration 6** — `internal/db/migrations.go`
  ```sql
  CREATE TABLE fault_configs (
      id, name, description, service, category, fault_type,
      fault_request JSONB, status, created_at, updated_at, fired_at, completed_at
  );
  -- Indexes: service, status, partial index for conflict check
  ```

### Verify
```bash
go build ./... && go test -v -run TestFaultConfig ./internal/model/...
```

---

## Phase 3: Fault Config Store Repository

**Repo**: `manteion-go` → `feat/fault-configs` (continues)

### Tasks

- [x] **3.1 Create internal/store/fault_config_repo.go**
  - `FaultConfigRepo` struct + `NewFaultConfigRepo(db)`
  - CRUD: `Create`, `Get`, `List`, `ListByService`, `Update`, `Delete`
  - State transitions: `MarkFired`, `MarkCompleted`, `MarkManuallyCancelled`, `MarkFailed`
  - Conflict check: `HasActiveConflict(ctx, service, category, faultType, excludeID) (bool, error)`
  - Update/Delete enforce `status != 'active'` in WHERE clause → `ErrNotFound` if blocked

### Verify
```bash
go build ./...
```

---

## Phase 4: CRUD API Endpoints

**Repo**: `manteion-go` → `feat/fault-configs` (continues)

### Tasks

- [x] **4.1 Create internal/api/fault_config_store.go** — interface for test injection
  - Subset of `FaultConfigRepo` methods

- [x] **4.2 Create internal/api/fault_config_handler.go** — CRUD handlers
  - `handleCreateFaultConfig` — POST `/api/v1/fault-configs` → 201
  - `handleListFaultConfigs` — GET `/api/v1/fault-configs` → 200 (filters: `?service=`, `?status=`)
  - `handleGetFaultConfig` — GET `/api/v1/fault-configs/{id}` → 200/404
  - `handleUpdateFaultConfig` — PUT `/api/v1/fault-configs/{id}` → 200/404/409 (409 if active)
  - `handleDeleteFaultConfig` — DELETE `/api/v1/fault-configs/{id}` → 204/404/409 (409 if active)

- [x] **4.3 Wire into server.go**
  - Add `faultConfigs` field to `Server`, update `NewServer()`, register 5 CRUD routes

### Verify
```bash
go build ./... && go vet ./...
```

---

## Phase 5: Fire / Cancel / Completed Lifecycle

**Repo**: `manteion-go` → `feat/fault-configs` (continues)

### Tasks

- [x] **5.1 Add Controller + selfURL to Server** — `server.go`
  - `ctrl *atrocontrol.Controller`, `selfURL string`

- [x] **5.2 Add DeleteFaultSlot to atropos client** — `internal/atropos/client_fault.go`
  - `DeleteFaultSlot(ctx, addr, category, faultType) error`
  - DELETE `addr + "/admin/fault/" + category + "/" + faultType`

- [x] **5.3 handleFireFaultConfig** — `fault_config_handler.go`
  - Load config → `CanFire()` check → 409
  - `HasActiveConflict()` ��� 409 with descriptive message
  - Unmarshal FaultReq, inject `callback_url`
  - `ctrl.InjectFault(ctx, service, faultReq, WithRunID(id))`
  - `MarkFired(ctx, id)`
  - SSE broadcast `fault_config_state_changed`
  - Return 200 with fanout result

- [x] **5.4 handleCancelFaultConfig** — `fault_config_handler.go`
  - Check status == active → 409 if not
  - Clear specific slot via new `DeleteFaultSlot` client method
  - `MarkManuallyCancelled(ctx, id)`
  - SSE broadcast
  - Return 200

- [x] **5.5 handleFaultConfigCompleted** — `fault_config_handler.go`
  - SDK callback → `MarkCompleted(ctx, id)`
  - SSE broadcast
  - Return 204

- [x] **5.6 SSE helper** — `broadcastFaultConfigChanged(service, id, status)`

- [x] **5.7 Register lifecycle routes** — `server.go`
  - POST `/api/v1/fault-configs/{id}/fire`
  - POST `/api/v1/fault-configs/{id}/cancel`
  - POST `/api/v1/fault-configs/{id}/completed`

- [x] **5.8 Tests** — `fault_config_handler_test.go`
  - Fire happy path, conflict rejection, re-fire from completed
  - Cancel happy path, cancel non-active → 409
  - Completed callback
  - SSE events

### Verify
```bash
go build ./... && go vet ./... && go test -v -run TestFaultConfig ./internal/api/...
```

---

## Phase 6: Integration Wiring + Verification

**Repo**: `manteion-go` → `feat/fault-configs` (continues)

### Tasks

- [x] **6.1 Wire in cmd/manteion/main.go**
  - Create `FaultConfigRepo`, pass to `NewServer()`
  - `selfURL` from env `MANTEION_SELF_URL` or `MANTEION_URL`

- [x] **6.2 Update SDK registration** — `api/sdk_handler.go`
  - On register, query active fault configs for the service
  - Include in intent response so new instances get active faults

- [x] **6.3 End-to-end verification**
  ```bash
  docker-compose up -d
  # Create → Fire → Check → Cancel → Re-fire cycle
  go build ./... && go vet ./... && go test ./...
  ```

---

## Progress

| Phase | Description | Status |
|---|---|---|
| 1 | Multi-slot DemoEvaluator (atropos-go) | `[x]` |
| 2 | Model + Migration (manteion-go) | `[x]` |
| 3 | Store Repository (manteion-go) | `[x]` |
| 4 | CRUD API (manteion-go) | `[x]` |
| 5 | Fire/Cancel/Completed (manteion-go) | `[x]` |
| 6 | Integration Wiring (manteion-go) | `[x]` |
