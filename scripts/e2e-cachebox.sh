#!/usr/bin/env bash
# e2e-cachebox.sh — drive a baseline→isolation experiment through manteion and
# verify cachebox recording FIDELITY (hit-rate + coverage).
#
# Flow:
#   1. baseline phase  (persist_cache=true, NO frozen services, >=2 workflows):
#      SDKs record cache from the attack-driven traffic and ingest into the
#      phase. Harvest writes recorded_entry_count per service.
#   2. isolation phase (freeze FREEZE_SVC in replay; same workflows): manteion
#      preloads the baseline's entries and freezes the service; the replayed
#      traffic should hit the cache. Harvest writes cache_hit_rate +
#      request_count + recorded_entry_count.
#   3. verify: baseline recorded_entry_count>0; isolation hit_rate>=HIT_THRESHOLD
#      with request_count>0 ⇒ faithful recording.
#
# Prereqs: manteion (epoch-2, migrations>=3), zeus, and an atropos-instrumented
# mesh recording+pushing cache to manteion (see service-beds). Needs curl + jq.
#
# Config via env (defaults target a local online-boutique slice):
set -euo pipefail

MANTEION="${MANTEION_URL:-http://localhost:9090}"
FREEZE_SVC="${FREEZE_SVC:-frontend}"
# Two workflows = two attack targets driven in parallel during each phase.
WF1_URL="${WF1_URL:-http://localhost:8080/}"
WF2_URL="${WF2_URL:-http://localhost:8080/product/OLJCESPC7Z}"
VUS="${VUS:-20}"
DURATION_SEC="${DURATION_SEC:-30}"
HIT_THRESHOLD="${HIT_THRESHOLD:-0.8}"
POLL_TIMEOUT_SEC="${POLL_TIMEOUT_SEC:-180}"

api() { curl -fsS -H 'content-type: application/json' "$@"; }
say() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

# --- 1. create two workflows (FK targets + zeus materialization) -------------
say "creating workflows"
mk_wf() {
  local name="$1" url="$2"
  api -X POST "$MANTEION/api/v1/workflows" -d "$(jq -n --arg n "$name" --arg u "$url" \
    '{name:$n, dsl:{type:"request", method:"GET", url:$u}}')" | jq -r '.id'
}
WF1=$(mk_wf "e2e-cb-wf1-$RANDOM" "$WF1_URL")
WF2=$(mk_wf "e2e-cb-wf2-$RANDOM" "$WF2_URL")
echo "  wf1=$WF1  wf2=$WF2"

# --- 2. create the experiment (baseline + isolation) -------------------------
say "creating experiment"
EXP_BODY=$(jq -n \
  --arg wf1 "$WF1" --arg wf2 "$WF2" --arg u1 "$WF1_URL" --arg u2 "$WF2_URL" \
  --arg svc "$FREEZE_SVC" --argjson vus "$VUS" --argjson dur "$DURATION_SEC" '
{
  name: "e2e cachebox fidelity",
  phases: [
    { name: "baseline", position: 0, frozen_services: [], persist_cache: true,
      workflows: [
        {workflow_id:$wf1, vus:$vus, duration_sec:$dur, target_url:$u1, target_method:"GET"},
        {workflow_id:$wf2, vus:$vus, duration_sec:$dur, target_url:$u2, target_method:"GET"}
      ], rule_ids: [] },
    { name: ("isolation-"+$svc), position: 1, persist_cache: false,
      frozen_services: [ {service:$svc, mode:"replay", key_strategy:"exact", mutation_policy:"deny"} ],
      workflows: [
        {workflow_id:$wf1, vus:$vus, duration_sec:$dur, target_url:$u1, target_method:"GET"},
        {workflow_id:$wf2, vus:$vus, duration_sec:$dur, target_url:$u2, target_method:"GET"}
      ], rule_ids: [] }
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
    failed|cancelled) echo "experiment $st — aborting"; exit 1 ;;
  esac
  [ "$(date +%s)" -ge "$deadline" ] && { echo "timeout waiting for completion"; exit 1; }
  sleep 5
done

# --- 4. fetch fidelity from phase results ------------------------------------
phase_cache() { # phaseId service -> the service_cache row
  api "$MANTEION/api/v1/experiments/$EXP_ID/phases/$1/results" \
    | jq -c --arg s "$2" '.service_cache[] | select(.service==$s)'
}
say "verifying fidelity for service=$FREEZE_SVC"
BASE_ROW=$(phase_cache "$BASE_PHASE" "$FREEZE_SVC" || true)
ISO_ROW=$(phase_cache "$ISO_PHASE" "$FREEZE_SVC" || true)
echo "  baseline:  ${BASE_ROW:-<none>}"
echo "  isolation: ${ISO_ROW:-<none>}"

recorded=$(jq -r '.recorded_entry_count // 0' <<<"${BASE_ROW:-{}}")
hit=$(jq -r '.cache_hit_rate // 0' <<<"${ISO_ROW:-{}}")
reqs=$(jq -r '.request_count // 0' <<<"${ISO_ROW:-{}}")

fail=0
awk "BEGIN{exit !($recorded>0)}"   || { echo "FAIL: baseline recorded_entry_count=$recorded (want >0)"; fail=1; }
awk "BEGIN{exit !($reqs>0)}"       || { echo "FAIL: isolation request_count=$reqs (want >0)"; fail=1; }
awk "BEGIN{exit !($hit>=$HIT_THRESHOLD)}" || { echo "FAIL: isolation hit_rate=$hit (want >=$HIT_THRESHOLD)"; fail=1; }

if [ "$fail" -eq 0 ]; then
  say "PASS — recorded=$recorded entries, replay hit_rate=$hit (req=$reqs) >= $HIT_THRESHOLD"
else
  say "FIDELITY CHECK FAILED — inspect cache ingest (recording_phase_id wiring) + preload"
  exit 1
fi
