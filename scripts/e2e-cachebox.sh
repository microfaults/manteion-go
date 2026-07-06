#!/usr/bin/env bash
# e2e-cachebox.sh — drive a baseline→isolation experiment through manteion and
# verify cache-box recording fidelity END TO END, including the drain
# completeness of the baseline and the fidelity VERDICT of the isolation phase.
#
# What changed vs the v1 script:
#   - Workflows are real DSL v2 documents (version:"2" + root node), so zeus's
#     k6 launcher actually executes them — the phase is WORKFLOW-driven, not a
#     flat vegeta attack. (Flat attacks remain available additively via
#     target_url, but the baseline here is pure workflow load.)
#   - Assertions are first-class: baseline drain == clean, isolation verdict ==
#     VALID, on top of the recorded-count and replay-hit-rate checks.
#   - Rates are explicit (estimated_rps_per_vu in the DSL) rather than riding
#     the VUs≈rate fallback, for reproducibility.
#
# Flow:
#   1. baseline  (persist_cache=true, NO frozen services): SDKs record cache
#      from the workflow-driven traffic and push it into the phase. The drain
#      barrier must report CLEAN (every expected instance flushed completely).
#   2. isolation (freeze FREEZE_SVC in replay; same workflows): manteion
#      preloads the baseline recording, freezes the service, and replays. The
#      per-phase verdict must be VALID (no replay miss, preload committed,
#      baseline not degraded, telemetry present).
#   3. verify: baseline recorded_entry_count>0 + drain clean; isolation verdict
#      VALID + hit_rate>=HIT_THRESHOLD with request_count>0.
#
# Prereqs: manteion (epoch-2, migrations>=8), zeus (with the k6 launcher +
# k6 binary in the image), and an atropos-instrumented mesh whose FROZEN
# service records+pushes cache. Needs curl + jq.
#
# Config via env (defaults target a local online-boutique slice):
set -euo pipefail

MANTEION="${MANTEION_URL:-http://localhost:9090}"
FREEZE_SVC="${FREEZE_SVC:-frontend}"
# Absolute base URL the k6 engine hits (must be reachable from the zeus host).
BASE_URL="${BASE_URL:-http://frontend:8080}"
# Two request paths exercised by the workflow's DAG.
PATH1="${PATH1:-/}"
PATH2="${PATH2:-/product/OLJCESPC7Z}"
VUS="${VUS:-20}"
RPS_PER_VU="${RPS_PER_VU:-1}"
DURATION_SEC="${DURATION_SEC:-30}"
HIT_THRESHOLD="${HIT_THRESHOLD:-0.8}"
POLL_TIMEOUT_SEC="${POLL_TIMEOUT_SEC:-240}"

api() { curl -fsS -H 'content-type: application/json' "$@"; }
say() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
fail() { printf '\033[31mFAIL: %s\033[0m\n' "$*"; exit 1; }

# --- 0. preflight ------------------------------------------------------------
command -v jq >/dev/null || fail "jq is required"
api "$MANTEION/readyz" >/dev/null || fail "manteion not ready at $MANTEION"

# --- 1. author two DSL v2 workflows -----------------------------------------
# A minimal v2 doc: a sequence of two request nodes with a small inter-step
# delay. estimated_rps_per_vu pins the open-loop rate. base_url is overridden
# per-run by manteion, but we set it so the doc validates standalone.
say "creating DSL v2 workflows"
# Request nodes carry a RELATIVE `path`; the k6 engine prepends base_url
# (which manteion overrides per-run via BASE_URL). estimated_rps_per_vu pins
# the open-loop rate.
mk_wf() {
  local name="$1" p1="$2" p2="$3"
  api -X POST "$MANTEION/api/v1/workflows" -d "$(jq -n \
    --arg n "$name" --arg base "$BASE_URL" --arg p1 "$p1" --arg p2 "$p2" \
    --argjson rps "$RPS_PER_VU" '
    {
      name: $n,
      dsl: {
        name: $n,
        version: "2",
        base_url: $base,
        estimated_rps_per_vu: $rps,
        targets: ["frontend"],
        default_delay: { min_ms: 50, max_ms: 200 },
        root: {
          type: "sequence",
          id: "root",
          children: [
            { type: "request", id: "s1", method: "GET", path: $p1 },
            { type: "delay", id: "think", min_ms: 100, max_ms: 300 },
            { type: "request", id: "s2", method: "GET", path: $p2 }
          ]
        }
      }
    }')" | jq -r '.id'
}
WF1=$(mk_wf "e2e-cb-browse-$RANDOM" "$PATH1" "$PATH2")
WF2=$(mk_wf "e2e-cb-product-$RANDOM" "$PATH2" "$PATH1")
echo "  wf1=$WF1  wf2=$WF2"

# --- 2. create the experiment (baseline + isolation) -------------------------
# Both phases run the SAME two workflows. No target_url ⇒ pure workflow-run
# load (no additive flat attack); add target_url to a workflow row to layer a
# vegeta attack on top.
say "creating experiment"
wf_row() { jq -n --arg id "$1" --argjson vus "$VUS" --argjson dur "$DURATION_SEC" \
  '{workflow_id:$id, vus:$vus, duration_sec:$dur}'; }
EXP_BODY=$(jq -n \
  --argjson w1 "$(wf_row "$WF1")" --argjson w2 "$(wf_row "$WF2")" \
  --arg svc "$FREEZE_SVC" '
{
  name: "e2e cachebox fidelity (v2)",
  phases: [
    { name: "baseline", position: 0, frozen_services: [], persist_cache: true,
      workflows: [ $w1, $w2 ], rule_ids: [] },
    { name: ("isolation-"+$svc), position: 1, persist_cache: false,
      frozen_services: [ {service:$svc, mode:"replay", key_strategy:"canonical_v2", mutation_policy:"deny"} ],
      workflows: [ $w1, $w2 ], rule_ids: [] }
  ]
}')
EXP=$(api -X POST "$MANTEION/api/v1/experiments" -d "$EXP_BODY")
EXP_ID=$(jq -r '.id' <<<"$EXP")
BASE_PHASE=$(jq -r '.phases[] | select(.name=="baseline") | .id' <<<"$EXP")
ISO_PHASE=$(jq -r '.phases[] | select(.name|startswith("isolation")) | .id' <<<"$EXP")
echo "  experiment=$EXP_ID  baseline=$BASE_PHASE  isolation=$ISO_PHASE"

# --- 3. start + wait ---------------------------------------------------------
say "starting experiment"
api -X POST "$MANTEION/api/v1/experiments/$EXP_ID/start" >/dev/null
deadline=$(( $(date +%s) + POLL_TIMEOUT_SEC ))
while :; do
  st=$(api "$MANTEION/api/v1/experiments/$EXP_ID" | jq -r '.status')
  echo "  experiment status: $st"
  case "$st" in
    completed) break ;;
    failed|cancelled) fail "experiment $st — inspect phase statuses and manteion logs" ;;
  esac
  [ "$(date +%s)" -ge "$deadline" ] && fail "timeout waiting for completion after ${POLL_TIMEOUT_SEC}s"
  sleep 5
done

# --- 4. fetch results --------------------------------------------------------
base_results() { api "$MANTEION/api/v1/experiments/$EXP_ID/phases/$BASE_PHASE/results"; }
iso_results()  { api "$MANTEION/api/v1/experiments/$EXP_ID/phases/$ISO_PHASE/results"; }

say "baseline: recording completeness"
BR=$(base_results)
recorded=$(jq -r --arg s "$FREEZE_SVC" '[.service_cache[]|select(.service==$s)|.recorded_entry_count]|first // 0' <<<"$BR")
drain_status=$(jq -r '.drain.status // "missing"' <<<"$BR")
echo "  recorded_entry_count=$recorded  drain=$drain_status"
awk "BEGIN{exit !($recorded>0)}" || fail "baseline recorded_entry_count=$recorded (want >0) — SDK not recording/pushing?"
[ "$drain_status" = "clean" ] || fail "baseline drain=$drain_status (want clean) — an instance did not flush completely; check missing_instances in $BR"

say "isolation: replay fidelity + verdict"
IR=$(iso_results)
verdict=$(jq -r '.verdict.verdict // "missing"' <<<"$IR")
reasons=$(jq -rc '.verdict.reasons // []' <<<"$IR")
hit=$(jq -r --arg s "$FREEZE_SVC" '[.service_cache[]|select(.service==$s)|.cache_hit_rate]|first // 0' <<<"$IR")
reqs=$(jq -r --arg s "$FREEZE_SVC" '[.service_cache[]|select(.service==$s)|.request_count]|first // 0' <<<"$IR")
echo "  verdict=$verdict reasons=$reasons  hit_rate=$hit  request_count=$reqs"

[ "$verdict" = "VALID" ] || fail "isolation verdict=$verdict reasons=$reasons (want VALID)"
awk "BEGIN{exit !($reqs>0)}"       || fail "isolation request_count=$reqs (want >0) — no replay traffic reached the frozen service"
awk "BEGIN{exit !($hit>=$HIT_THRESHOLD)}" || fail "isolation hit_rate=$hit (want >=$HIT_THRESHOLD)"

say "PASS — baseline recorded=$recorded (drain clean); isolation VALID, hit_rate=$hit (req=$reqs) >= $HIT_THRESHOLD"
