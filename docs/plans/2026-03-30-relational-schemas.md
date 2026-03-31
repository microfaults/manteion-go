# Manteion-go: Relational Data Model Schemas

## Context

Building on the scaffolding plan (`docs/plans/2026-03-30-scaffolding.md`), this branch adds the underlying relational schemas for manteion's data model. The schemas capture four domains:

1. **Workload-attack relationships** -- k6 workloads with sub-attacks (vegeta precision loads)
2. **Composable fault rules** -- hierarchical fault composition with incompatibility constraints
3. **Trace metadata anchors** -- pointers into Jaeger/Prometheus for cache-box experiments
4. **Experiment results** -- outcomes of workload experiments across the combinatorial space

Branch: `feat/relational-schemas`

## Files to Create

```
internal/
├── model/
│   ├── fault.go              # FaultSpec, FaultComposition, FaultCompositionMember, FaultIncompatibility
│   ├── workload.go           # Flow, Persona, Workload, Attack (+Service, +Role), AttackResult (+Service)
│   ├── experiment.go         # Experiment (+PrimaryWorkloadID), ExperimentRun, WorkflowRunResult, ServiceRunResult, ContributionResult (+CacheBoxMode)
│   ├── trace.go              # TraceAnchor, CacheBoxConfig, SyntheticDelayConfig, HistogramBucket
│   └── policy.go             # PolicyRule, PolicyCondition, PolicyAction, AttackTargetSpec
```

All types defined as Go structs with JSON tags. In-memory stores wrap these with `sync.RWMutex` maps (same pattern as zeus-go `workload.Registry`). The relational model uses string IDs for foreign keys -- no ORM, just struct fields with clear reference semantics.

---

## Schema 1: Workload-Attack Relationships

A k6 **workload** (high-level flow like "browse" or "checkout") drives broad traffic. Within or alongside it, multiple vegeta **attacks** (precision loads) target specific endpoints. Attacks belong to a workload via `WorkloadID`.

```
Workload 1──* Attack (role: primary|background, service) 1──1 AttackResult (service)
   │
   ├── references Flow (by name, e.g. "online-boutique-browse")
   └── references Persona (by name, e.g. "aggressive")
```

### `internal/model/workload.go`

```go
// Flow is a k6 flow definition. Steps/thresholds stored as opaque JSON
// since k6 consumes these directly.
type Flow struct {
    ID                string          `json:"id"`           // e.g. "online-boutique-browse"
    Name              string          `json:"name"`
    Description       string          `json:"description,omitempty"`
    Targets           []string        `json:"targets"`      // service names in the flow DAG
    EstimatedRPSPerVU float64         `json:"estimated_rps_per_vu"`
    Steps             json.RawMessage `json:"steps"`        // step DAG (k6 runner consumes this)
    Thresholds        json.RawMessage `json:"thresholds,omitempty"`
    CreatedAt         time.Time       `json:"created_at"`
}

// Persona defines a k6 behavioral profile.
type Persona struct {
    ID            string  `json:"id"`           // e.g. "aggressive"
    Name          string  `json:"name"`
    Description   string  `json:"description,omitempty"`
    ExploreProb   float64 `json:"explore_prob"`
    EngageProb    float64 `json:"engage_prob"`
    CommitProb    float64 `json:"commit_prob"`
    RepeatProb    float64 `json:"repeat_prob,omitempty"`
    ThinkTimeMin  int     `json:"think_time_min_ms"`
    ThinkTimeMax  int     `json:"think_time_max_ms"`
}

// Workload represents a k6 load generation session.
type Workload struct {
    ID          string    `json:"id"`
    Name        string    `json:"name"`
    FlowID      string    `json:"flow_id"`      // FK -> Flow.ID
    PersonaID   string    `json:"persona_id"`   // FK -> Persona.ID
    VUs         int       `json:"vus"`
    Rate        float64   `json:"rate"`
    MetaTraceID string    `json:"meta_trace_id"`
    Status      string    `json:"status"`       // pending, running, completed, stopped, failed
    StartedAt   *time.Time `json:"started_at,omitempty"`
    CompletedAt *time.Time `json:"completed_at,omitempty"`
    CreatedAt   time.Time `json:"created_at"`
}

// Attack represents a vegeta precision load targeting a specific endpoint.
type Attack struct {
    ID              string            `json:"id"`
    WorkloadID      string            `json:"workload_id,omitempty"`      // FK -> Workload.ID (nullable for standalone)
    ExperimentRunID string            `json:"experiment_run_id,omitempty"` // FK -> ExperimentRun.ID
    PolicyRuleID    string            `json:"policy_rule_id,omitempty"`   // FK -> PolicyRule.ID (if policy-triggered)
    Service         string            `json:"service"`                    // target service name (e.g., "productcatalog")
    Role            string            `json:"role"`                       // "primary" (measured workflow) or "background" (interference source)
    TargetURL       string            `json:"target_url"`
    TargetMethod    string            `json:"target_method"`
    TargetHeaders   map[string]string `json:"target_headers,omitempty"`
    Rate            int               `json:"rate"`          // req/sec
    DurationMs      int64             `json:"duration_ms"`
    DedupBypass     string            `json:"dedup_bypass,omitempty"`
    MetaTraceID     string            `json:"meta_trace_id,omitempty"`
    Status          string            `json:"status"`        // pending, running, completed, stopped
    StartedAt       *time.Time        `json:"started_at,omitempty"`
    CompletedAt     *time.Time        `json:"completed_at,omitempty"`
    CreatedAt       time.Time         `json:"created_at"`
}

// AttackResult stores the outcome metrics of a completed attack.
type AttackResult struct {
    AttackID      string         `json:"attack_id"`   // FK -> Attack.ID (1:1)
    Service       string         `json:"service"`     // target service name (denormalized from Attack for query convenience)
    TotalRequests uint64         `json:"total_requests"`
    DurationMs    int64          `json:"duration_ms"`
    RateActual    float64        `json:"rate_actual"`
    SuccessRate   float64        `json:"success_rate"`
    StatusCodes   map[string]int `json:"status_codes"` // {"200": 450, "500": 50}
    LatencyP50Us  int64          `json:"latency_p50_us"`
    LatencyP90Us  int64          `json:"latency_p90_us"`
    LatencyP95Us  int64          `json:"latency_p95_us"`
    LatencyP99Us  int64          `json:"latency_p99_us"`
    LatencyMinUs  int64          `json:"latency_min_us"`
    LatencyMaxUs  int64          `json:"latency_max_us"`
    BytesInTotal  int64          `json:"bytes_in_total"`
    BytesOutTotal int64          `json:"bytes_out_total"`
    Errors        []string       `json:"errors,omitempty"`
    CompletedAt   time.Time      `json:"completed_at"`
}
```

---

## Schema 2: Composable Fault Rules

### Atomic Faults

Each `FaultSpec` maps to exactly one atropos-go fault type. Three categories (inline, network, resource) with type-discriminated config.

```go
// FaultSpec is an atomic fault definition.
type FaultSpec struct {
    ID         string          `json:"id"`
    Name       string          `json:"name"`
    Category   string          `json:"category"`    // "inline", "network", "resource"
    FaultType  string          `json:"fault_type"`  // see valid types per category below
    Config     json.RawMessage `json:"config"`      // type-specific parameters
    DurationMs int64           `json:"duration_ms,omitempty"`
    RampUpMs   int64           `json:"ramp_up_ms,omitempty"`
    RampDownMs int64           `json:"ramp_down_ms,omitempty"`
    CreatedAt  time.Time       `json:"created_at"`
}
```

**Valid `fault_type` values by category** (from `atropos-go/internal/fault/`):

| Category | FaultType | Config fields | Key constraint |
|----------|-----------|---------------|----------------|
| inline | `error` | `status_code` (100-599), `message` | No duration needed |
| inline | `hang` | (none beyond FaultConfig) | Duration required |
| inline | `latency` | `delay_ms`, `jitter_ms` | At least one > 0 |
| network | `blackhole` | (none) | Pre-dial hijack, no data flows |
| network | `drip` | `chunk_size` (bytes), `interval_ms` | Controls byte-by-byte timing |
| network | `latency` | `delay_ms`, `jitter_ms` | Per-chunk delay |
| network | `loss` | `rate` (0.0-1.0), `retransmit_delay_ms`, `reset_threshold` | Can trigger RST |
| network | `rst` | `after_bytes`, `after_duration_ms` | TCP connection reset |
| network | `throttle` | `bytes_per_sec` | Token-bucket rate limit |
| resource | `cpu` | `target_load` (0.0-1.0) | Duty-cycle spinning |
| resource | `memory` | `target_load`, `chunk_size`, `thrashing`, `thrash_workers` | RSS allocation |
| resource | `io` | `read_rate`, `file_size`, `file_count`, `workers`, `mode` | Token-bucket I/O |

### Fault Composition

A `FaultComposition` groups atomic faults (or other compositions) to run **parallel** or **sequential**. Max depth = 3. The tree structure uses a member list where each member is either an atomic `FaultSpec` or a child `FaultComposition`.

```go
// FaultComposition groups faults for parallel or sequential execution.
type FaultComposition struct {
    ID            string                   `json:"id"`
    Name          string                   `json:"name"`
    ExecutionMode string                   `json:"execution_mode"` // "parallel" or "sequential"
    Members       []FaultCompositionMember `json:"members"`
    CreatedAt     time.Time                `json:"created_at"`
}

// FaultCompositionMember is one slot in a composition.
// Exactly one of FaultSpecID or ChildCompositionID must be set.
type FaultCompositionMember struct {
    Position           int    `json:"position"`            // execution order (sequential) / grouping (parallel)
    FaultSpecID        string `json:"fault_spec_id,omitempty"`        // FK -> FaultSpec.ID
    ChildCompositionID string `json:"child_composition_id,omitempty"` // FK -> FaultComposition.ID
    Direction          string `json:"direction,omitempty"`             // "upstream", "downstream", "" (non-network faults)
}
```

### Fault Incompatibility Rules

Derived from analysis of atropos-go proxy/fault code. These are **data, not code** -- stored so the API can validate composition requests at creation time.

```go
// FaultIncompatibility defines a pair of fault types that cannot be composed.
type FaultIncompatibility struct {
    FaultTypeA     string `json:"fault_type_a"`     // e.g. "network:blackhole"
    FaultTypeB     string `json:"fault_type_b"`     // e.g. "network:rst"
    Scope          string `json:"scope"`            // "parallel", "sequential", "any"
    ConstraintType string `json:"constraint_type"`  // "hard" or "soft"
    Reason         string `json:"reason"`
}
```

**Hard incompatibilities** (physically undefined behavior):

| A | B | Scope | Reason |
|---|---|-------|--------|
| `network:blackhole` | `network:*` (any other network toxic) | parallel, same direction | Blackhole hijacks pre-dial; second toxic's Pipe() never runs. Only ONE toxic per direction in current proxy (`conn.go` takes LAST). |
| `network:drip` | `network:throttle` | parallel, same direction | Both control stream timing. Drip writes chunk-by-chunk with pauses; throttle rate-limits. If composed in same direction, behavior is undefined (which controls pacing?). |
| `network:drip` | `network:latency` | parallel, same direction | Both add per-chunk delays. Drip's chunk_size (typically 1 byte) + latency per-chunk delay = double timing control, confusing. |

**Soft incompatibilities** (redundant/confusing but technically executable):

| A | B | Scope | Reason |
|---|---|-------|--------|
| `inline:hang` | `network:blackhole` | parallel | Both block the request -- redundant. Hang blocks app thread; blackhole blocks TCP. |
| `network:loss` (with reset_threshold>0) | `network:rst` | parallel | Both can reset the connection. Whichever threshold hits first wins -- intent is ambiguous. |
| `inline:error` | `inline:latency` | sequential (error first) | Error completes immediately, latency after it is meaningless if error already returned. |
| `inline:error` | `inline:hang` | parallel | Error completes immediately, hang blocks -- conflicting intent. |
| `resource:cpu` | `resource:memory` | parallel | Memory allocation triggers GC which skews CPU duty-cycle measurements. Results may be misleading. |

**Valid compositions** (explicitly fine):

| A | B | Scope | Why |
|---|---|-------|-----|
| `network:latency` (upstream) | `network:throttle` (downstream) | parallel | Different directions -- independent stream workers. |
| `inline:latency` | `resource:cpu` | parallel | Different mechanisms, additive effects. |
| `network:loss` | `resource:io` | parallel | Network + resource, independent. |
| `inline:latency` | `inline:error` | sequential (latency then error) | Adds delay before returning error -- valid chaos scenario. |
| `resource:cpu` | `resource:io` | parallel | Independent mechanisms (spinning vs. disk). |
| `network:latency` | `network:rst` | sequential | Progressive degradation: slow then reset -- valid failure cascade. |
| `network:throttle` | `network:blackhole` | sequential | Bandwidth degrades then drops entirely. |

### Rules (updated from scaffolding plan)

Rules now reference either an atomic `FaultSpec` or a `FaultComposition`:

```go
// Rule binds a fault (atomic or composed) to a service + match criteria.
type Rule struct {
    ID                 string        `json:"id"`
    Name               string        `json:"name"`
    Service            string        `json:"service"`
    Enabled            bool          `json:"enabled"`
    Priority           int           `json:"priority"`
    Match              MatchCriteria `json:"match"`
    FaultSpecID        string        `json:"fault_spec_id,omitempty"`        // FK -> FaultSpec.ID (one of)
    FaultCompositionID string        `json:"fault_composition_id,omitempty"` // FK -> FaultComposition.ID (one of)
    Mode               string        `json:"mode"`  // "inline" or "background"
    CreatedAt          time.Time     `json:"created_at"`
    UpdatedAt          time.Time     `json:"updated_at"`
}

type MatchCriteria struct {
    InjectionPoint string            `json:"injection_point,omitempty"` // ingress/egress/transient/custom, "" = any
    Labels         map[string]string `json:"labels,omitempty"`          // AND semantics
}
```

---

## Schema 3: Trace Metadata Anchors

Manteion is the source of truth for *when and where* to look up trace data, not the trace data itself. `TraceAnchor` stores the pointer into the backing trace store.

```go
// TraceAnchor is a pointer into an external trace/metrics backend.
type TraceAnchor struct {
    ID              string          `json:"id"`
    ExperimentRunID string          `json:"experiment_run_id"` // FK -> ExperimentRun.ID
    MetaTraceID     string          `json:"meta_trace_id"`     // W3C baggage value
    Service         string          `json:"service"`
    Backend         string          `json:"backend"`           // "jaeger", "prometheus", "tempo"
    QueryHint       json.RawMessage `json:"query_hint"`        // backend-specific (time range, filters)
    CollectedAt     time.Time       `json:"collected_at"`
}

// CacheBoxConfig describes cache-box state for a frozen service.
//
// The cache-box is the core research primitive: a service frozen at its SDK
// boundary to replay cached responses instead of doing real work. Zero CPU,
// zero queueing, but the call graph structure is preserved.
type CacheBoxConfig struct {
    Service       string `json:"service"`
    Mode          string `json:"mode"`           // "passthrough", "replay", "replay_with_delay"
    WorkflowScope string `json:"workflow_scope"` // meta-trace-id pattern to match, "" = all traffic

    // Cache key derivation -- content-addressable by request signature.
    // Matching cascades: exact -> fuzzy -> parametric (first hit wins).
    KeyStrategy   string `json:"key_strategy"`   // "exact", "fuzzy", "parametric"
    // exact:      method + path + normalized body hash (default, needed for gRPC/GraphQL)
    // fuzzy:      method + path, ignore body (sufficient for stateless GET endpoints)
    // parametric: regex on path + template extraction (e.g., /products/{id})

    // Mutation safety -- POST/PUT/DELETE never replayed without explicit opt-in.
    MutationPolicy    string   `json:"mutation_policy"`     // "deny" (default), "allow"
    SafeMethods       []string `json:"safe_methods,omitempty"` // whitelist when policy=allow, e.g. ["POST /search"]

    // Synthetic delay for replay_with_delay mode.
    SyntheticDelayConfig *SyntheticDelayConfig `json:"synthetic_delay,omitempty"`

    // Cache warming -- how long to record in passthrough before switching to replay.
    WarmupDurationMs int64 `json:"warmup_duration_ms,omitempty"` // 0 = assume pre-warmed

    // Cache staleness -- per-entry TTL. After expiry, entry evicted, passthrough fallback.
    CacheTTLMs int64 `json:"cache_ttl_ms,omitempty"` // 0 = no expiry
}

// SyntheticDelayConfig parameterizes the delay injected in replay_with_delay mode.
// Stores both the full observed histogram and a parametric (lognormal) fit.
// At replay time, sample from the fitted distribution.
type SyntheticDelayConfig struct {
    // Summary percentiles (always populated).
    P50Us  int64 `json:"p50_us"`
    P95Us  int64 `json:"p95_us"`
    P99Us  int64 `json:"p99_us"`

    // Full observed histogram for sampling.
    HistogramBuckets []HistogramBucket `json:"histogram_buckets,omitempty"`

    // Parametric fit (lognormal) derived from histogram.
    FitMu    *float64 `json:"fit_mu,omitempty"`    // lognormal mu
    FitSigma *float64 `json:"fit_sigma,omitempty"` // lognormal sigma
}

type HistogramBucket struct {
    UpperBoundUs int64 `json:"upper_bound_us"`
    Count        int64 `json:"count"`
}
```

---

## Schema 4: Experiments and Results

An **experiment** is a plan (baseline + N isolation runs + combination runs). Each **experiment run** is one execution with specific services frozen. Runs produce two kinds of results: workflow-level latency (from load generator) and per-service metrics (from traces + Prometheus).

```
Experiment 1──* ExperimentRun 1──* WorkflowRunResult (end-to-end workflow latency)
                    │          1──* ServiceRunResult  (per-service metrics + cache-box fidelity)
                    │
                    ├── * Attack (role=primary|background, via ExperimentRunID FK)
                    ├── * TraceAnchor (via ExperimentRunID FK)
                    └── * CacheBoxConfig (embedded in run)
```

```go
// Experiment is an experiment plan. Maps to one of 5 experiment types
// from the research protocol:
//   - interference:     cross-workflow interference quantification (all live, varying load)
//   - isolation:        freeze shared services, show interference eliminated
//   - attribution:      freeze all except one, measure single-service contribution
//   - scenario:         inject synthetic latency into frozen service ("what-if")
//   - cache_fidelity:   measure response divergence between live and cached
type Experiment struct {
    ID                 string    `json:"id"`
    Name               string    `json:"name"`
    Description        string    `json:"description,omitempty"`
    ExperimentType     string    `json:"experiment_type"` // interference, isolation, attribution, scenario, cache_fidelity
    PrimaryWorkloadID  string    `json:"primary_workload_id"`  // FK -> Workload.ID (the workflow being measured, e.g. checkout)
    Status             string    `json:"status"`               // planned, running, completed, failed, cancelled
    CreatedAt          time.Time `json:"created_at"`
    StartedAt          *time.Time `json:"started_at,omitempty"`
    CompletedAt        *time.Time `json:"completed_at,omitempty"`
}
// Background workloads (interference sources) are captured per-run via Attack entities
// with Role="background" linked to each ExperimentRun. This allows varying background
// load across runs (e.g., browse at 100 → 2000 RPS) while keeping the primary workload
// constant (e.g., checkout at 50 RPS). See Attack.Role.

// ExperimentRun is one execution phase within an experiment.
// An attribution experiment has 1 baseline + N isolation + C(N,2) combination runs.
// Isolation runs CAN overlap (they freeze different services). If any run fails,
// the experiment aborts. Partial results from completed runs are preserved.
type ExperimentRun struct {
    ID             string           `json:"id"`
    ExperimentID   string           `json:"experiment_id"`  // FK -> Experiment.ID
    RunType        string           `json:"run_type"`       // "baseline", "isolation", "combination"
    RunIndex       int              `json:"run_index"`      // ordering within experiment
    FrozenServices []CacheBoxConfig `json:"frozen_services,omitempty"`
    MetaTraceID    string           `json:"meta_trace_id"`
    Status         string           `json:"status"`         // pending, running, completed, failed

    // Pod placement snapshot -- for randomization across runs to address
    // infrastructure confounds (CFS throttling, LLC evictions, veth queueing).
    // Effects consistent across placements = service-graph attributable.
    NodePlacement  map[string]string `json:"node_placement,omitempty"` // service -> k8s node

    StartedAt      *time.Time       `json:"started_at,omitempty"`
    CompletedAt    *time.Time       `json:"completed_at,omitempty"`
    CreatedAt      time.Time        `json:"created_at"`
}

// WorkflowRunResult stores end-to-end workflow latency for one run.
// One row per (run, workflow) pair. This is the measurement the delta formula
// operates on — it captures what the load generator (vegeta/k6) observes at
// the workflow entry point, not individual service latency.
//
// Source: aggregated from AttackResults of primary-role attacks, or from
// k6 summary output for broad-traffic workloads.
type WorkflowRunResult struct {
    ID              string  `json:"id"`
    ExperimentRunID string  `json:"experiment_run_id"` // FK -> ExperimentRun.ID
    Workflow        string  `json:"workflow"`           // browse, checkout, etc.

    // End-to-end latency as measured by the load generator.
    LatencyP50Us    int64   `json:"latency_p50_us"`
    LatencyP95Us    int64   `json:"latency_p95_us"`
    LatencyP99Us    int64   `json:"latency_p99_us"`
    LatencyP999Us   int64   `json:"latency_p999_us"`

    RequestCount    int64   `json:"request_count"`
    ErrorRate       float64 `json:"error_rate"`
    ThroughputRPS   float64 `json:"throughput_rps"`

    // Escape hatch for full distribution data (histogram buckets, raw arrays).
    RawMetrics      json.RawMessage `json:"raw_metrics,omitempty"`
}

// ServiceRunResult stores per-service metrics for one service in one run.
// One row per (run, service, workflow) triple. Source: trace backends
// (Jaeger/Tempo for per-service latency), Prometheus (resource utilization),
// and cache-box internals (fidelity metrics for frozen services).
type ServiceRunResult struct {
    ID              string  `json:"id"`
    ExperimentRunID string  `json:"experiment_run_id"` // FK -> ExperimentRun.ID
    Service         string  `json:"service"`
    Workflow        string  `json:"workflow,omitempty"` // per-workflow breakdown (optional)

    // Per-service latency from traces (ingress span duration at this service).
    LatencyP50Us    *int64  `json:"latency_p50_us,omitempty"`
    LatencyP95Us    *int64  `json:"latency_p95_us,omitempty"`
    LatencyP99Us    *int64  `json:"latency_p99_us,omitempty"`

    // Resource utilization per service pod.
    CPUMillicores   *int64  `json:"cpu_millicores,omitempty"`
    MemoryMB        *int64  `json:"memory_mb,omitempty"`

    // Cache-box specific (nil if service not frozen in this run).
    CacheHitRate    *float64 `json:"cache_hit_rate,omitempty"`
    CacheExactMatch *float64 `json:"cache_exact_match,omitempty"` // response fidelity: exact match %
    CacheStaleness  *float64 `json:"cache_staleness_ms,omitempty"` // avg age of served cached response

    // Escape hatch for full distribution data.
    RawMetrics      json.RawMessage `json:"raw_metrics,omitempty"`
}

// ContributionResult is derived by comparing workflow-level latency between
// a baseline run and an isolation run.
// Computed: delta_service = baseline_workflow_latency - isolated_workflow_latency.
// Stored per (experiment, frozen_service, workflow) after all runs complete.
//
// The CacheBoxMode field distinguishes two types of isolation:
//   - "replay":            removes ALL contribution (contention + intrinsic) → delta = total contribution
//   - "replay_with_delay": preserves intrinsic timing → delta = contention-only contribution
// Intrinsic cost = total contribution - contention contribution (compare two ContributionResults
// for the same service with different CacheBoxMode values).
type ContributionResult struct {
    ID              string  `json:"id"`
    ExperimentID    string  `json:"experiment_id"`    // FK -> Experiment.ID
    Service         string  `json:"service"`          // the frozen service
    Workflow        string  `json:"workflow"`
    CacheBoxMode    string  `json:"cachebox_mode"`    // "replay" or "replay_with_delay"
    BaselineRunID   string  `json:"baseline_run_id"`  // FK -> ExperimentRun.ID
    IsolationRunID  string  `json:"isolation_run_id"` // FK -> ExperimentRun.ID

    // Marginal contribution: how much workflow latency this service adds.
    // Computed from WorkflowRunResult rows for baseline vs isolation runs.
    DeltaP50Us      int64   `json:"delta_p50_us"`     // baseline_p50 - isolated_p50
    DeltaP95Us      int64   `json:"delta_p95_us"`
    DeltaP99Us      int64   `json:"delta_p99_us"`

    // Interaction detection (only for combination runs):
    // If delta_both != delta_A + delta_B, the services interact nonlinearly.
    // Positive InteractionEffect = superadditive interference (worse than sum of parts).
    InteractionEffect *float64 `json:"interaction_effect,omitempty"` // delta_combined - sum(delta_individual)
    CombinationRunID  string   `json:"combination_run_id,omitempty"` // FK -> ExperimentRun.ID
}
```

---

## Schema 5: Policy Engine (migrated from zeus-go)

Policy ownership moves from zeus-go's Archer to manteion. Archer becomes a pure attack execution engine (launch/stop/list). Manteion evaluates metric thresholds and triggers attacks or cache-box mode changes.

```go
// PolicyRule defines a metric-triggered action. Evaluates conditions on a tick
// interval and launches attacks (or cache-box mode changes) when conditions are met.
type PolicyRule struct {
    ID        string          `json:"id"`
    Name      string          `json:"name"`
    Enabled   bool            `json:"enabled"`
    Condition PolicyCondition `json:"condition"`
    Action    PolicyAction    `json:"action"`
    Cooldown  time.Duration   `json:"cooldown"` // minimum time between triggers
    CreatedAt time.Time       `json:"created_at"`
}

// PolicyCondition is a threshold check against a named metric.
type PolicyCondition struct {
    Metric    string  `json:"metric"`    // e.g. "active_workloads", "checkout_p99_us"
    Operator  string  `json:"operator"`  // gt, gte, lt, lte, eq
    Threshold float64 `json:"threshold"`
}

// PolicyAction describes what happens when a condition fires.
// Can trigger an attack (via zeus proxy) or a cache-box mode change (via SDK push).
type PolicyAction struct {
    ActionType     string           `json:"action_type"` // "attack" or "cachebox_mode_change"
    AttackTarget   *AttackTargetSpec `json:"attack_target,omitempty"`
    CacheBoxChange *CacheBoxConfig   `json:"cachebox_change,omitempty"`
}

type AttackTargetSpec struct {
    URL         string `json:"url"`
    Method      string `json:"method"`
    Rate        int    `json:"rate"`
    DurationMs  int64  `json:"duration_ms"`
    DedupBypass string `json:"dedup_bypass,omitempty"`
}
```

---

## Entity-Relationship Summary

```
Flow ──< Workload >── Persona
              │
              ├──< Attack (service, role=primary|background) >── AttackResult (service)
              │       │
              │       ├── ExperimentRun (FK)
              │       └── PolicyRule (FK, if policy-triggered)
              │
              └── Experiment (PrimaryWorkloadID; background workloads via Attack.Role per run)
                     │  (typed: interference/isolation/attribution/scenario/cache_fidelity)
                     │
                     ├──< ExperimentRun
                     │         │
                     │         ├──< WorkflowRunResult (end-to-end workflow latency from load gen)
                     │         ├──< ServiceRunResult  (per-service metrics, cache-box fidelity, resource util)
                     │         ├──< Attack (role=primary: measured workflow, role=background: interference)
                     │         ├──< TraceAnchor
                     │         └──  CacheBoxConfig[] (embedded, with key strategy + mutation + TTL)
                     │                 └── SyntheticDelayConfig (histogram + lognormal fit)
                     │
                     └──< ContributionResult (delta = baseline_workflow_latency - isolated_workflow_latency)
                              ├── CacheBoxMode: "replay" (total) vs "replay_with_delay" (contention-only)
                              ├── intrinsic cost = total_delta - contention_delta
                              └── references BaselineRun, IsolationRun, CombinationRun

FaultSpec ──< FaultCompositionMember(+direction) >── FaultComposition (max depth 3)
                                                           │
Rule ── (FaultSpec | FaultComposition)                     └──< FaultCompositionMember (recursive)

PolicyRule ── PolicyCondition + PolicyAction (attack OR cachebox_mode_change)
              (migrated from zeus-go Archer -- manteion owns policy evaluation)

FaultIncompatibility (reference data, validated at composition creation)
```

---

## Design Decisions

1. **Composition depth cap = 3** -- atoms -> groups -> top-level composition. Direction field on `FaultCompositionMember` disambiguates network toxic placement (upstream/downstream).

2. **Sequential network toxics are valid** -- model progressive degradation (latency -> RST, throttle -> blackhole). Proxy swaps active toxic at phase boundaries.

3. **Flow storage = hybrid** -- snapshot flow config into manteion at workload creation. Long-term: manteion becomes source of truth with admin UI. `Flow.Steps` as `json.RawMessage` supports both.

4. **Synthetic delay = parametric fit + histogram** -- store full observed histogram AND lognormal fit parameters. Sample from fitted distribution at replay time.

5. **Cache warmup = duration-based, staleness = TTL per entry** -- `WarmupDurationMs` on config, `CacheTTLMs` per entry. Stateful endpoints get short TTL; stateless get long TTL.

6. **Isolation runs can overlap, abort on failure** -- runs freeze different services (no conflict). Single run failure aborts experiment. Partial results preserved.

7. **Policy engine moves to manteion** -- zeus-go Archer becomes pure execution engine. Manteion evaluates conditions and triggers attacks or cache-box changes.

8. **Split results into workflow-level and service-level** -- `WorkflowRunResult` stores end-to-end workflow latency from the load generator (what the delta formula operates on). `ServiceRunResult` stores per-service metrics from traces/Prometheus (resource utilization, cache-box fidelity). The delta formula in VISION.md always compares workflow-level latency (e.g., "checkout p99 when productcatalog is frozen vs live"), not individual service latency.

9. **Attack.Role distinguishes primary from background workloads** -- Interference experiments (VISION.md lines 132-133) run multiple concurrent workloads: checkout at constant 50 RPS (primary, being measured) + browse at varying RPS (background, interference source). `Experiment.PrimaryWorkloadID` identifies the measured workflow. Background workloads are captured per-run via Attack entities with `Role="background"`, allowing load to vary across runs.

10. **ContributionResult.CacheBoxMode enables contention vs intrinsic decomposition** -- VISION.md distinguishes replay (removes all contribution) from replay-with-delay (preserves intrinsic timing). Two ContributionResult rows per service (one per mode) yield: total_delta (replay), contention_delta (replay-with-delay), intrinsic_cost = total - contention. No schema redesign needed -- just a mode indicator per row.

11. **Attack and AttackResult carry a Service field** -- Enables composition from individual vegeta results upward to ServiceRunResult and WorkflowRunResult without URL parsing. AttackResult.Service is denormalized from Attack for query convenience.

---

## Implementation Order

1. `go.mod` (no new deps needed)
2. `internal/model/fault.go` -- FaultSpec, FaultComposition, FaultCompositionMember, FaultIncompatibility
3. `internal/model/workload.go` -- Flow, Persona, Workload, Attack (with Service + Role), AttackResult (with Service)
4. `internal/model/trace.go` -- TraceAnchor, CacheBoxConfig, SyntheticDelayConfig, HistogramBucket
5. `internal/model/policy.go` -- PolicyRule, PolicyCondition, PolicyAction, AttackTargetSpec
6. `internal/model/experiment.go` -- Experiment (PrimaryWorkloadID), ExperimentRun, WorkflowRunResult, ServiceRunResult, ContributionResult (with CacheBoxMode)
7. Validation methods on each type (`Validate() error`)
8. Incompatibility seed data (the table of hard/soft constraints)
9. Composition depth validation (max 3) and per-direction network toxic enforcement

## Verification

1. `go build ./...` compiles
2. `go vet ./...` passes
3. Unit tests for `Validate()` methods on key types
4. Unit test: incompatibility rules catch known-bad compositions (blackhole+throttle, drip+drip same direction)
5. Unit test: valid compositions pass (latency upstream + throttle downstream)
6. Unit test: composition depth > 3 rejected
7. Unit test: two network toxics same direction in parallel rejected; different directions passes
8. Unit test: `CacheBoxConfig.Validate()` -- mutation policy deny blocks POST, allow with whitelist passes
9. Unit test: `ContributionResult` delta computation from two WorkflowRunResult rows (baseline vs isolation)
10. Unit test: `ContributionResult` with CacheBoxMode="replay" vs "replay_with_delay" yields total vs contention delta
11. Unit test: `PolicyRule.Validate()` -- condition operators, action types
12. Unit test: `Attack.Validate()` -- Role must be "primary" or "background"; Service required
13. Unit test: experiment abort propagation when isolation run fails
