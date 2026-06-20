# E2E: baseline experiment + cachebox recording fidelity

Verifies the cache-box decomposition end-to-end: a **baseline** phase records
cache from real traffic, an **isolation** phase replays it against a frozen
service, and we check that the replay faithfully serves the recorded entries.

Fidelity here = **hit-rate + coverage** (the SDK exposes hit/miss counters
only; true exact-match/staleness instrumentation is a documented follow-on):

- **coverage** — `recorded_entry_count`: how many distinct entries the SDK
  captured for a service during the baseline (and, on the isolation phase, how
  many were available to replay).
- **hit-rate** — `cache_hit_rate` on the isolation phase: fraction of replayed
  requests served from cache. `request_count` (= hits+misses) disambiguates
  "0 requests served" from "all misses".

A faithful recording ⇒ baseline `recorded_entry_count > 0` **and** isolation
`cache_hit_rate ≥ threshold` with `request_count > 0`.

## How the pieces wire (manteion side)

1. The orchestrator starts a **baseline** phase (`persist_cache=true`, no frozen
   services). Starting/finishing it bumps `rule_version` so SDKs re-poll.
2. The SDK poll/register response carries `recording_phase_id` =
   `ExperimentRepo.RunningPersistCachePhaseID` (the running baseline). The
   atropos SDK hands it to `ApplyTargets.PhaseIDSink` →
   `CachePushClient.SetPhaseID`, so the service's cache pushes
   (`POST /api/v1/cache/ingest`) are tagged with the baseline phase id.
3. manteion's `cachestore` persists entries under `{phase_id}/{service}.jsonl`.
   On baseline completion, `harvestCacheStats` writes `recorded_entry_count`
   per recording service (`cachestore.Services` + `Read`).
4. The **isolation** phase preloads the baseline entries to the frozen service
   (`PreloadEntries`) and freezes it in replay. On completion, harvest reads
   live `Store.Hits/Misses` → `cache_hit_rate` + `request_count`, and carries
   the baseline coverage as `recorded_entry_count`.

The `service-beds` services must wire `ApplyTargets.PhaseIDSink =
cbPush.SetPhaseID` (their task) for step 2 to take effect.

## Prereqs

- manteion (epoch-2, migrations ≥ 3) reachable at `$MANTEION_URL`.
- zeus reachable by manteion (drives the per-workflow attacks).
- An atropos-instrumented mesh slice that records cache and pushes to manteion
  (`MANTEION_URL` set in the services; `PhaseIDSink` wired). See the service-beds
  local-mesh task.
- `curl` + `jq`.

## Run

```bash
MANTEION_URL=http://localhost:9090 \
FREEZE_SVC=frontend \
WF1_URL=http://localhost:8080/ \
WF2_URL=http://localhost:8080/product/OLJCESPC7Z \
VUS=20 DURATION_SEC=30 HIT_THRESHOLD=0.8 \
  ./scripts/e2e-cachebox.sh
```

The script creates two workflows, an experiment with a baseline + isolation
phase (each driving both workflows), starts it, waits for completion, then reads
`service_cache` from each phase's results
(`GET /api/v1/experiments/{id}/phases/{phaseId}/results`) and asserts the
fidelity thresholds. Exit 0 = PASS.

## Troubleshooting

- **baseline `recorded_entry_count` = 0** → the SDK isn't ingesting. Check the
  `recording_phase_id` reaches the service (poll response) and that
  `PhaseIDSink → SetPhaseID` is wired; check `POST /api/v1/cache/ingest` is
  reachable and `persist_cache=true` on the baseline.
- **isolation `request_count` = 0** → no traffic hit the frozen service during
  replay; check the workflow `target_url`s actually route through `FREEZE_SVC`.
- **low `cache_hit_rate`** → recording didn't cover the replayed request set
  (key-strategy mismatch, non-deterministic params, or thin coverage). Compare
  baseline vs isolation `recorded_entry_count` and the key strategy.
