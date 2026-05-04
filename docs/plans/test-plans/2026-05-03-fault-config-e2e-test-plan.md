# Fault Config E2E Test Plan

Feature: Manually-Triggered Persistent Faults (phases 2–6)
Repos: `manteion-go`, `atropos-go`, `service-beds`

---

## Prerequisites

### 1. Cluster running

```bash
kubectl get nodes
# All nodes Ready
```

### 2. Service mesh deployed

```bash
cd service-beds/microservices-demo-go

# Vendor dependencies (pick your OS)
.\run_vendor.bat      # Windows
./run_vendor.sh       # Linux / macOS

skaffold run
# Wait until all pods are Running
kubectl get pods
```

### 3. Port-forward services you'll test against

Run each in a separate terminal tab and leave them open.

```bash
kubectl port-forward svc/frontend-external  8081:80    &
kubectl port-forward svc/checkoutservice    5050:5050  &
kubectl port-forward svc/cartservice        7070:7070  &
kubectl port-forward svc/productcatalogservice 3550:3550 &
kubectl port-forward svc/grafana            3000:3000  &
```

### 4. PostgreSQL for manteion

```bash
# Quickstart with Docker (skip if you already have a DB)
docker run -d --name manteion-pg \
  -e POSTGRES_USER=manteion \
  -e POSTGRES_PASSWORD=manteion \
  -e POSTGRES_DB=manteion \
  -p 5432:5432 \
  postgres:16-alpine
```

### 5. Run manteion locally

```bash
cd manteion-go

MANTEION_DATABASE_URL="postgres://manteion:manteion@localhost:5432/manteion?sslmode=disable" \
MANTEION_SELF_URL="http://host.docker.internal:8080" \
go run ./cmd/manteion
# Manteion base URL = http://localhost:8080
```

> **`MANTEION_SELF_URL`** must be the URL the Atropos SDK instances inside the cluster
> can reach manteion at.  Use `http://host.docker.internal:8080` for Docker Desktop K8s,
> or set a LoadBalancer / NodePort and use that IP.

### 6. Point SDK instances at manteion

Each service must register with manteion on startup.  Set the env var before
deploying (or patch the running deployment):

```bash
kubectl set env deployment/checkoutservice \
  ATROPOS_SERVER_URL=http://host.docker.internal:8080

kubectl set env deployment/cartservice \
  ATROPOS_SERVER_URL=http://host.docker.internal:8080
# repeat for any service you intend to fault
```

Wait for rollouts:

```bash
kubectl rollout status deployment/checkoutservice
kubectl rollout status deployment/cartservice
```

Confirm registration in manteion logs:

```
{"level":"INFO","msg":"sdk registered","id":"...","service":"checkoutservice"}
```

---

## Quick-Reference: Service Ports

| Service | containerPort |
|---|---|
| frontend | 8080 (external: 80) |
| checkoutservice | 5050 |
| cartservice | 7070 |
| productcatalogservice | 3550 |
| shippingservice | 50051 |
| paymentservice | 50051 |
| emailservice | 5000 |
| adservice | 9555 |
| recommendationservice | 8080 |
| currencyservice | 7000 |
| Grafana | 3000 |
| Prometheus | 9090 |

---

## T1 — CRUD Lifecycle (no fault fired)

**Goal**: verify create / get / list / update / delete work correctly before any fault is active.

```bash
# Create
FC_ID=$(curl -s -X POST http://localhost:8080/api/v1/fault-configs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "checkout-latency-300ms",
    "service": "checkoutservice",
    "category": "inline",
    "fault_type": "latency",
    "fault_request": {"type":"latency","delay":"300ms"}
  }' | jq -r .id)
echo "FC_ID=$FC_ID"
```

| Step | Command | Expected |
|---|---|---|
| Get | `curl -s http://localhost:8080/api/v1/fault-configs/$FC_ID \| jq .status` | `"ready"` |
| List all | `curl -s http://localhost:8080/api/v1/fault-configs \| jq length` | ≥ 1 |
| Filter by service | `curl -s "http://localhost:8080/api/v1/fault-configs?service=checkoutservice" \| jq length` | ≥ 1 |
| Update (allowed) | PUT with new name → `jq .name` | `"checkout-latency-updated"` |
| Delete (allowed) | `curl -s -o /dev/null -w "%{http_code}" -X DELETE .../fault-configs/$FC_ID` | `204` |
| Get after delete | `curl -s -o /dev/null -w "%{http_code}" .../fault-configs/$FC_ID` | `404` |

```bash
# Update
curl -s -X PUT http://localhost:8080/api/v1/fault-configs/$FC_ID \
  -H "Content-Type: application/json" \
  -d '{
    "name": "checkout-latency-updated",
    "service": "checkoutservice",
    "category": "inline",
    "fault_type": "latency",
    "fault_request": {"type":"latency","delay":"500ms"}
  }' | jq .name
# ✓ "checkout-latency-updated"

# Delete
curl -s -o /dev/null -w "%{http_code}" \
  -X DELETE http://localhost:8080/api/v1/fault-configs/$FC_ID
# ✓ 204
```

---

## T2 — Input Validation (400s)

**Goal**: confirm the API rejects invalid inputs with descriptive 400 errors.

```bash
BASE='http://localhost:8080/api/v1/fault-configs'
HDR='-H "Content-Type: application/json"'

# Missing service
curl -s -X POST $BASE -H "Content-Type: application/json" \
  -d '{"name":"x","category":"inline","fault_type":"latency","fault_request":{"type":"latency"}}' \
  | jq .error
# ✓ contains "service required"

# Invalid category
curl -s -X POST $BASE -H "Content-Type: application/json" \
  -d '{"name":"x","service":"svc","category":"bogus","fault_type":"latency","fault_request":{"type":"latency"}}' \
  | jq .error
# ✓ contains "invalid category"

# Valid category, wrong fault_type for it
curl -s -X POST $BASE -H "Content-Type: application/json" \
  -d '{"name":"x","service":"svc","category":"inline","fault_type":"cpu","fault_request":{"type":"cpu"}}' \
  | jq .error
# ✓ contains "invalid fault_type"

# Empty fault_request
curl -s -X POST $BASE -H "Content-Type: application/json" \
  -d '{"name":"x","service":"svc","category":"inline","fault_type":"latency"}' \
  | jq .error
# ✓ contains "fault_request required"

# GET non-existent
curl -s -o /dev/null -w "%{http_code}" $BASE/does-not-exist
# ✓ 404
```

---

## T3 — Fire → Observe Latency → Auto-Complete Callback

**Goal**: fire a timed fault, see it in the SDK, watch latency spike in Grafana,
then confirm the SDK fires the callback and manteion marks it `completed`.

```bash
# Create a 10-second latency fault
FC_ID=$(curl -s -X POST http://localhost:8080/api/v1/fault-configs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "checkout-10s-latency",
    "service": "checkoutservice",
    "category": "inline",
    "fault_type": "latency",
    "fault_request": {"type":"latency","delay":"10s"}
  }' | jq -r .id)

# Fire
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_ID/fire | jq .
# ✓ 200, result has targeted/ok counts

# Confirm active
curl -s http://localhost:8080/api/v1/fault-configs/$FC_ID | jq .status
# ✓ "active"

# Confirm SDK slot is live
curl -s http://localhost:5050/admin/fault | jq '{active,fault}'
# ✓ active: true, fault.type: "latency"

# Observe latency (checkout flow takes ~10s now)
time curl -s -o /dev/null http://localhost:8081/
# ✓ real time ≥ 10s

# Wait for auto-complete, then verify callback was received
sleep 12
curl -s http://localhost:8080/api/v1/fault-configs/$FC_ID | jq '{status,completed_at}'
# ✓ status: "completed", completed_at: non-null

# SDK slot cleared
curl -s http://localhost:5050/admin/fault | jq .active
# ✓ false
```

**Grafana check** (open http://localhost:3000):
- Dashboard: *Atropos Service Overview* → service: `checkoutservice`
- Ingress p99 latency panel shows ≥ 10 s spike during the active window

---

## T4 — Fire → Manual Cancel

**Goal**: fire a persistent (no duration) fault, confirm it's active, cancel it,
confirm the SDK slot is cleared and status is `manually_cancelled`.

```bash
# Create a persistent error fault (no duration → stays until cancelled)
FC_ID=$(curl -s -X POST http://localhost:8080/api/v1/fault-configs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "checkout-503-error",
    "service": "checkoutservice",
    "category": "inline",
    "fault_type": "error",
    "fault_request": {"type":"error","status_code":503,"message":"chaos engineering"}
  }' | jq -r .id)

# Fire
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_ID/fire | jq .
# ✓ 200

# Update blocked (409)
curl -s -o /dev/null -w "%{http_code}" \
  -X PUT http://localhost:8080/api/v1/fault-configs/$FC_ID \
  -H "Content-Type: application/json" \
  -d '{"name":"new","service":"checkoutservice","category":"inline","fault_type":"error","fault_request":{"type":"error","status_code":500}}'
# ✓ 409

# Delete blocked (409)
curl -s -o /dev/null -w "%{http_code}" \
  -X DELETE http://localhost:8080/api/v1/fault-configs/$FC_ID
# ✓ 409

# Checkout returns 503
curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/cart/checkout 2>/dev/null || \
  curl -s -o /dev/null -w "%{http_code}" http://localhost:5050/PlaceOrder
# ✓ 503

# Cancel
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_ID/cancel | jq .
# ✓ 200

# Status = manually_cancelled
curl -s http://localhost:8080/api/v1/fault-configs/$FC_ID | jq .status
# ✓ "manually_cancelled"

# SDK slot cleared
curl -s http://localhost:5050/admin/fault | jq .active
# ✓ false
```

---

## T5 — Re-fire from Terminal States

**Goal**: confirm `CanFire()` returns true for `completed`, `manually_cancelled`,
and `failed` — a config can be re-triggered without recreating it.

```bash
# Using FC_ID from T4 (manually_cancelled)
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_ID/fire | jq .
# ✓ 200

curl -s http://localhost:8080/api/v1/fault-configs/$FC_ID | jq .status
# ✓ "active"

# Cancel again to clean up
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_ID/cancel
# ✓ 200

# Using FC_ID from T3 (completed)
curl -s -X POST http://localhost:8080/api/v1/fault-configs/<T3_FC_ID>/fire | jq .
# ✓ 200 — re-fire from completed works
curl -s -X POST http://localhost:8080/api/v1/fault-configs/<T3_FC_ID>/cancel
```

---

## T6 — Same-Type Conflict (409) + Different-Type Coexistence

**Goal**: two faults of the same `category:fault_type` on the same service are
blocked; two faults of different types on the same service are allowed.

```bash
# Fire inline:latency on checkoutservice
FC_A=$(curl -s -X POST http://localhost:8080/api/v1/fault-configs \
  -H "Content-Type: application/json" \
  -d '{"name":"lat-A","service":"checkoutservice","category":"inline","fault_type":"latency","fault_request":{"type":"latency","delay":"60s"}}' \
  | jq -r .id)
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_A/fire
# ✓ 200

# Second inline:latency on same service → 409
FC_B=$(curl -s -X POST http://localhost:8080/api/v1/fault-configs \
  -H "Content-Type: application/json" \
  -d '{"name":"lat-B","service":"checkoutservice","category":"inline","fault_type":"latency","fault_request":{"type":"latency","delay":"10s"}}' \
  | jq -r .id)
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_B/fire | jq .
# ✓ 409, error contains "inline:latency fault is already active"

# Different type (inline:error) on same service → 200
FC_C=$(curl -s -X POST http://localhost:8080/api/v1/fault-configs \
  -H "Content-Type: application/json" \
  -d '{"name":"err-C","service":"checkoutservice","category":"inline","fault_type":"error","fault_request":{"type":"error","status_code":503}}' \
  | jq -r .id)
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_C/fire | jq .
# ✓ 200 — different slot, coexists

# SDK shows two active slots
curl -s http://localhost:5050/admin/fault | jq '.faults | length'
# ✓ 2

# Cancel A — B slot for latency is now free, C (error) still active
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_A/cancel
curl -s http://localhost:5050/admin/fault | jq '.faults | length'
# ✓ 1 (only error remains)

curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_B/fire | jq .
# ✓ 200 — latency slot is now free

# Clean up
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_B/cancel
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_C/cancel
```

---

## T7 — Cancel Non-Active (409)

**Goal**: cancelling a config that is not `active` returns 409.

```bash
# Create but do NOT fire
FC_ID=$(curl -s -X POST http://localhost:8080/api/v1/fault-configs \
  -H "Content-Type: application/json" \
  -d '{"name":"unfired","service":"checkoutservice","category":"inline","fault_type":"latency","fault_request":{"type":"latency","delay":"1s"}}' \
  | jq -r .id)

curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_ID/cancel | jq .
# ✓ 409, error contains "fault config is not active"

# Clean up
curl -s -X DELETE http://localhost:8080/api/v1/fault-configs/$FC_ID
```

---

## T8 — Late-Joining Instance Receives Active Fault

**Goal**: an SDK instance that registers *after* a fault is already fired gets
`active_fault_configs` in the register response and immediately exhibits the fault.

```bash
# Fire a long-lived latency on checkoutservice
FC_ID=$(curl -s -X POST http://localhost:8080/api/v1/fault-configs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "persist-latency",
    "service": "checkoutservice",
    "category": "inline",
    "fault_type": "latency",
    "fault_request": {"type":"latency","delay":"120s"}
  }' | jq -r .id)
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_ID/fire

# Simulate a new instance joining (rolling restart)
kubectl rollout restart deployment/checkoutservice
kubectl rollout status deployment/checkoutservice --timeout=60s

# Manteion logs should show new registration AND active_fault_configs in response:
# {"level":"INFO","msg":"sdk registered","id":"<new-id>","service":"checkoutservice"}

# New pod immediately exhibits the fault
time curl -s -o /dev/null http://localhost:8081/
# ✓ real time ≥ 120s  (or observe in Grafana latency panel)

# Clean up
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_ID/cancel
```

---

## T9 — SSE State Change Events

**Goal**: fire and cancel events are broadcast over the SSE stream.

Open two terminals.

**Terminal A** (subscribe):
```bash
curl -N "http://localhost:8080/api/v1/sdk/events?service=checkoutservice"
```

**Terminal B** (drive):
```bash
FC_ID=$(curl -s -X POST http://localhost:8080/api/v1/fault-configs \
  -H "Content-Type: application/json" \
  -d '{"name":"sse-test","service":"checkoutservice","category":"inline","fault_type":"latency","fault_request":{"type":"latency","delay":"30s"}}' \
  | jq -r .id)

curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_ID/fire
```

**Terminal A** should receive:
```
event: fault_config_state_changed
data: {"id":"<FC_ID>","status":"active"}
```

```bash
# Back in Terminal B
curl -s -X POST http://localhost:8080/api/v1/fault-configs/$FC_ID/cancel
```

**Terminal A** should receive:
```
event: fault_config_state_changed
data: {"id":"<FC_ID>","status":"manually_cancelled"}
```

---

## T10 — List Status Filter

**Goal**: `?status=` query param returns only configs in that state.

```bash
BASE="http://localhost:8080/api/v1/fault-configs"

# After running T3–T7 you should have configs in multiple states
curl -s "$BASE?status=ready"              | jq length
curl -s "$BASE?status=active"             | jq length
curl -s "$BASE?status=completed"          | jq length
curl -s "$BASE?status=manually_cancelled" | jq length
curl -s "$BASE?status=failed"             | jq length

# Combined service+status filter
curl -s "$BASE?service=checkoutservice&status=active" | jq '.[].name'
```

Each result set should contain only configs with the requested status.

---

## T11 — Grafana Observability Check

**Goal**: confirm faults are visible in the dashboard without curl.

1. Open **http://localhost:3000**
2. Go to **Dashboards → Atropos Service Overview**
3. Select `checkoutservice` from the `$service` dropdown
4. While a latency fault is active you should see:
   - **Ingress p99 latency** panel: spike to the injected delay value
   - **Ingress error rate** panel: spike when an error fault is active
5. After cancel: panels return to baseline within one scrape interval (~15 s)

---

## Pass Criteria Summary

| Test | Pass condition |
|---|---|
| T1 | Full CRUD round-trip without errors, 201/200/204 codes |
| T2 | All invalid inputs return 400 with descriptive `error` field |
| T3 | Fire returns 200; SDK shows active; latency observable; callback transitions to `completed` |
| T4 | Fire → cancel: SDK slot cleared, status `manually_cancelled`, service recovers |
| T5 | Re-fire from `completed` and `manually_cancelled` returns 200 |
| T6 | Same-type second fire → 409; different-type second fire → 200; SDK shows 2 slots |
| T7 | Cancel non-active → 409 |
| T8 | Rolling restart pod picks up active fault from register response |
| T9 | SSE emits `fault_config_state_changed` on fire and cancel |
| T10 | `?status=` filter returns only matching configs |
| T11 | Grafana latency/error panels reflect injected faults in real time |
