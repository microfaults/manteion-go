# OPA harness

## What it is

ADR-1 of the daemon design review was resolved as "keep it simple": the
per-request matcher in the atropos SDK stays label-based, and conditions on
system state (latency percentiles, error rates, log volumes) are evaluated
centrally, here. The OPA harness (`internal/opaharness`) evaluates Rego
policies against live Prometheus and Loki query results. A policy's output
is a set of rule mutations, and the harness applies them through the rule
repository (`store.RuleRepo`). Each write bumps the service's desired-state
generation in the same transaction, so a mutation reaches instances exactly
like `PUT /api/v1/rules/{id}`: on their next poll. The harness never contacts
an instance.

## Policy object

`model.OPAPolicy`, as stored and served by `/api/v1/opa/policies`:

```json
{
  "id": "opa-…",
  "name": "checkout-hot",
  "enabled": false,
  "rego": "package atropos\n\nmutations contains m if { … }\n",
  "interval_s": 30,
  "cooldown_s": 120,
  "inputs": [
    {"name": "p99", "source": "prometheus", "query": "histogram_quantile(…)", "kind": "scalar"}
  ],
  "scope": {"services": ["checkout"]},
  "created_at": "…",
  "updated_at": "…"
}
```

- `id`: minted as `opa-<uuidv7>` on create when the body has none.
- `name`: required and unique (409 on a collision).
- `enabled`: defaults to `false`; only enabled policies run in the loop. The
  API's evaluate works on any policy, enabled or not.
- `rego`: a Rego v1 module that declares `package atropos` and defines
  `mutations`. It is compiled when it is saved, and a parse, type or
  unsafe-builtin error is a 422 (`rego: …`).
- `interval_s`: seconds between evaluations. `0` becomes the default of 30;
  anything else must be at least 1.
- `cooldown_s`: the minimum gap after an evaluation that changed something
  (see [Scheduling and cooldown](#scheduling-and-cooldown)). `0` means no
  cooldown; negative values are rejected.
- `inputs[]`: `name` is bound to `input.inputs.<name>` and must match
  `^[A-Za-z_][A-Za-z0-9_]*$` (names are unique within a policy). `source` is
  `prometheus` or `loki`. `query` is an instant metric query in PromQL or
  LogQL. `kind` is `scalar` (the default) or `vector`. A policy may have no
  inputs.
- `scope.services`: required and non-empty, with no blank entries. The policy
  may only mutate rules of these services, and `input.rules` and
  `input.instances` cover only these services.

## Input document

The document a policy sees as `input`:

```json
{
  "now": "2026-11-06T20:15:09.123456789Z",
  "now_ns": 1793996109123456789,
  "inputs": {"p99": 1300, "decisions": null, "per_route": [{"metric": {"route": "/cart"}, "value": 4}]},
  "rules": [ /* model.Rule JSON of every rule in scope, in priority order */ ],
  "services": ["checkout"],
  "instances": [ /* model.SDKInstance JSON of every instance of the scope services */ ]
}
```

- `now` is the evaluation time in RFC 3339 with nanoseconds (UTC), and
  `now_ns` is the same instant in unix nanoseconds. Numbers reach the
  evaluator as exact JSON numbers, so `now_ns` keeps its precision.
- `inputs.<name>`, kind `scalar`: no series gives `null`. A comparison
  against `null` is undefined, so a rule body that tests it does not hold and
  `not hot` branches fire. One series gives its value as a number. Several
  series fail the evaluation (`scalar input "x" returned N series; aggregate
  the query or use kind "vector"`): aggregate the query, or declare it as a
  vector.
- `inputs.<name>`, kind `vector`: `[{"metric": {...labels}, "value": v}, ...]`,
  and `[]` when there are no series.
- A sample whose value is NaN or ±Inf (Prometheus answers an error ratio
  with no traffic as NaN) reaches the policy as `null`: a vector sample's
  `value` is `null`, and a scalar whose single series is non-finite is
  `null`. Nothing errors, and the input digest and the stored decision see
  the same `null`. Guard vector samples with `s.value != null`, for example
  `some s in input.inputs.per_route; s.value != null; s.value > 0.05`.
- `rules`: every rule whose `service` is in scope, in the order the rule
  repository lists them (priority).
- `instances`: every registered instance of the scope services, whatever its
  status, stale and dead ones included. Filter on `status` (`"alive"`) in Rego
  where it matters.

Inputs are fetched in declaration order, and the first failing one stops the
evaluation (`input <name>: query: prometheus query failed: …`).

## Mutations

A policy emits `data.atropos.mutations`, a set of objects. An undefined set
means nothing to do. A member that does not decode as a mutation (a string,
say) fails the evaluation (`eval: decode mutations: …`); it is never silently
skipped.

| `op` | Fields | Effect |
|---|---|---|
| `upsert_rule` | `rule`: full `model.Rule` JSON | Creates the rule keyed by `rule.service` + `rule.name`, or updates it in place when it differs. Server-owned `id`, `created_at` and `updated_at` are ignored: an update keeps the stored id and creation time. Top-level `service`/`name`, when given, must equal the rule's. The rule must pass the same `Validate` as rule CRUD. |
| `set_enabled` | `service`, `name`, `enabled` | Sets the rule's `enabled` flag. An absent rule is `failed`. |
| `set_params` | `service`, `name`, `params` (a JSON object) | Replaces the params of the fault spec the rule references, validated by the fault catalog, then re-saves the rule so its generation bumps and pollers recompile. Only for `fault_spec` rules (anything else, or an absent rule, is `failed`), and only when no other rule references that spec (a shared spec is `refused`). |
| `delete_rule` | `service`, `name` | Deletes the rule. An absent rule is `unchanged`. |

Each mutation gets one result, `{op, service, name, outcome, error?, rule_id?}`:

| `outcome` | Meaning |
|---|---|
| `created` | `upsert_rule` created the rule |
| `updated` | the rule (or, for `set_params`, its spec) was written |
| `deleted` | `delete_rule` removed the rule |
| `unchanged` | the store already holds the desired state; nothing was written |
| `refused` | never reached the store: malformed (unknown op, missing fields, `params` not an object, an `upsert_rule` whose top-level key disagrees with its rule), a service outside `scope.services`, or a `set_params` on a shared spec |
| `failed` | the store or validation rejected it (absent rule, invalid rule, catalog-rejected params, a write error); the next mutation still runs |
| `dry_run` | decision log only: an in-scope mutation of a dry run, evaluated but not applied |

Mutations apply in order, and the rule list is reloaded before each one, so
a mutation sees the effect of the ones before it. Every op is idempotent: a
policy that re-emits its desired state on every interval costs no writes and
no generation bumps once the state holds.

## Scheduling and cooldown

The loop ticks once a second and never overlaps ticks. On each tick it
evaluates, with apply, every enabled policy that is due:

- A policy is due `interval_s` after its last evaluation.
- After an evaluation whose applied results changed something (`created`,
  `updated` or `deleted`), the next one waits `max(interval_s, cooldown_s)`.
  `unchanged` results start no cooldown. An applying evaluation through the
  API (`?apply=true`) moves the schedule the same way.
- The first time the loop sees a policy (after a start, or after the policy
  is enabled), it reads the newest applied decision that changed something
  from the decision log (`LastChangeAt`). If that decision plus `cooldown_s`
  is still in the future, the policy waits until then. The cooldown therefore
  survives a restart.

A policy's failure is recorded and logged, and the tick moves on to the next
policy. Shutdown (the server's signal context) stops the loop, and a tick
cancelled midway leaves the remaining policies unevaluated.

## Decision log

Every evaluation is one row in `policy_decisions`: loop and API, dry runs
included.

| Field | Content |
|---|---|
| `id` | `pdec-<uuidv7>` |
| `policy_id` | the policy |
| `evaluated_at` | the evaluation time |
| `input_digest` | lowercase hex sha256 of the JSON of `input.inputs` (map keys sorted), over the inputs fetched; empty when the policy failed to compile |
| `mutations` | a `[]MutationResult` in every row: the scope refusals followed by the mutator's results for an applying evaluation, or the refusals followed by one `dry_run` result per in-scope would-be mutation for a dry run |
| `applied` | true when the mutator ran and either finished or applied something; `false` on every dry run and when the evaluation failed before applying |
| `error` | the first compile, input, input-document, eval or apply failure; the later stages are skipped |

Retention is the newest 1000 decisions per policy, pruned in the same
transaction as each insert; deleting a policy deletes its log.
`GET /api/v1/opa/policies/{id}/decisions` pages it newest first.

Each evaluation also logs one slog line, `opa harness: policy evaluated`, with
`policy_id`, `policy`, `input_digest`, `mutations`, `refused`, `applied`,
`changed`, `dry_run` and `duration_ms`. It is Info, or Warn with `error` when
the evaluation failed.

## API

| Route | Semantics |
|---|---|
| `POST /api/v1/opa/policies` | Create. 201 with the stored policy; 400 bad JSON, 409 name taken, 422 validation or Rego compile error. |
| `GET /api/v1/opa/policies` | Every policy, oldest created first; `[]` when there are none. |
| `GET /api/v1/opa/policies/{id}` | One policy; 404 when unknown. |
| `PUT /api/v1/opa/policies/{id}` | Replace (the body's `id` is ignored), validated and compiled as on create; `created_at` kept. |
| `DELETE /api/v1/opa/policies/{id}` | 204; the decision log goes with it. |
| `GET /api/v1/opa/policies/{id}/decisions` | The decision log, newest first, in the `{data, page}` envelope (`limit` default 20, max 200; `offset`). |
| `POST /api/v1/opa/policies/{id}/evaluate?apply=false\|true` | Evaluate once against live telemetry, enabled or not. |

`evaluate` is a dry run unless `apply=true`. It answers 200 with the
evaluation, failures included in `error`:

```json
{"policy_id": "opa-…", "evaluated_at": "…", "inputs": {"p99": 1300}, "input_digest": "…",
 "mutations": [ /* what the policy emitted */ ], "results": [ /* see below */ ],
 "applied": false, "changed": false, "error": "…"}
```

On a dry run `results` holds only the scope refusals, since nothing was
applied; the decision it records lists the would-be mutations as `dry_run`.
With `apply=true`, `results` holds the refusals and the mutator's results. A
500 means the decision could not be recorded. `cmd/manteion` always
constructs the harness, so evaluate works with `apply=true` too whatever
`MANTEION_POLICY_ENGINE` says; that variable gates only the loop. (The
handler answers 503 only when built without a harness.)

## Example

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

Hot (`decisions = 120`, `p99 = 1300`), rule absent: the dry run's `mutations` is

```json
[{"op": "upsert_rule", "rule": {"id": "", "name": "checkout-egress-latency", "service": "checkout", "enabled": true, "priority": 100, "match": {"injection_point": "egress", "labels": {"atropos.http.path": "/cart/checkout"}}, "action": {"type": "fault_spec", "fault_spec_id": "spec-egress-latency-200ms"}, "mode": "inline", "created_at": "0001-01-01T00:00:00Z", "updated_at": "0001-01-01T00:00:00Z"}}]
```

(the rule is echoed as `model.Rule` JSON, so the server-owned fields the
policy left out show their zero values; the mutator ignores them). Applying
it yields `results: [{"op": "upsert_rule", "service": "checkout", "name":
"checkout-egress-latency", "outcome": "created", "rule_id": "rule-…"}]` and
starts the 120 s cooldown. The next hot evaluation reports `unchanged` and
starts no cooldown.

Cold (`decisions = 3`, `p99 = 200`), rule present and enabled:

```json
[{"op": "set_enabled", "service": "checkout", "name": "checkout-egress-latency", "enabled": false}]
```

→ `outcome: "updated"`, then `unchanged` on later cold evaluations. Cold with
the rule absent or already disabled gives `mutations: []`. A `null`
`decisions` (no log lines in the window) makes `hot` undefined, so the cold
branch applies.

## Operations

| Variable | Default | Purpose |
|---|---|---|
| `MANTEION_POLICY_ENGINE` | `off` | `rego` (or `on`) starts the OPA harness loop; `threshold` starts the frozen PromQL engine (`docs/decisions/2026-06-policy-engine-freeze.md`); `off` starts neither; any other value logs a Warn and starts neither. |
| `MANTEION_LOKI_URL` | `http://loki:3100` | Loki base URL for `loki` inputs. |
| `PROMETHEUS_URL` | `http://prometheus:9090` | Prometheus base URL for `prometheus` inputs (shared with the orchestrator). |

On start the loop logs `opa harness: starting`, and `manteion starting`
carries `loki_url`. Prometheus inputs use `GET /api/v1/query`; Loki inputs use
`GET /loki/api/v1/query` (instant queries). Both accept metric queries only: a
Loki log (streams) query fails with `metric queries only`. Each query has a
5 s HTTP timeout.

**Loki reachability.** A `loki` input needs a Loki that manteion can reach.
In the lab topology Loki runs on the Mac bound to `127.0.0.1:3100`, while
Prometheus runs in the cluster. A manteion inside k3s therefore cannot reach
the default `MANTEION_LOKI_URL`. Run manteion on the Mac with
`MANTEION_LOKI_URL=http://127.0.0.1:3100`, or expose Loki's 3100 on the LAN
on purpose. A failing Loki query is recorded on the decision's `error`
(`input decisions: query: loki query failed: …`) and never mutates
anything, so a wrong URL degrades the policy instead of crashing manteion.

## Cost and limits

- Binary size (darwin/arm64, go1.27.3, workspace mode), measured with
  `go build` and `go build -ldflags="-s -w"` of `./cmd/manteion`:

  | | before (Task 1, 48ee114) | after (this change) | delta |
  |---|---|---|---|
  | unstripped | 29182226 | 50717666 | +21535440 (≈ +21.5 MB) |
  | stripped | 19271026 | 35356802 | +16085776 (≈ +16.1 MB) |

  The "before" build predates the OPA dependency and the atropos pin advance
  (`303c6d1`), so the delta includes both; OPA is most of it.
- OPA is pinned to v1.19.1, whose `go.mod` says `go 1.25.0` and so fits this
  module's `go 1.25.5`. The OPA version is tied to the Go toolchain: moving
  OPA forward can require raising the module's `go` directive.
- The evaluator is sandboxed: `http.send`, `net.lookup_ip_addr` and
  `opa.runtime` are refused at compile time, builtin errors are strict, and
  each evaluation (and each compile) runs under a 5 s timeout.
- Trust model: policies are operator-authored, and the sandbox is not a
  boundary against a hostile author. It blocks the network and process
  inspection (the denied builtins above are every builtin in OPA v1.19.1
  that performs I/O; `TestSandbox_PinsNondeterministicBuiltins` fails an OPA
  upgrade that adds a nondeterministic builtin until it is classified), and
  it bounds evaluation time (the timeout), not memory: `net.cidr_expand` of a
  wide prefix or a large comprehension can still allocate heavily within the
  5 s.
- Compositions and cache-box rules can be upserted, toggled and deleted like
  any rule, but `set_params` applies to `fault_spec` rules only.
