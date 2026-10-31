# Long-Running Manual Faults

A long-running fault (`fault_configs`) is a manually-fired, optionally-timed
fault — a **side channel** to the primary rule-attached fault path, not a
replacement for it. An operator creates one, fires it, and it stays active until
its `duration_ms` elapses (0 = until cancelled).

## Lifecycle

```
ready ──fire──▶ active ──┬── duration elapses ─▶ completed   (reaper)
                         └── cancel ───────────▶ cancelled
```

- `POST /api/v1/faults/configs` — create (status `ready`).
- `POST /api/v1/faults/configs/{id}/fire` — activate.
- `POST /api/v1/faults/configs/{id}/cancel` — deactivate.
- `DELETE /api/v1/faults/configs/{id}` — remove (deleting an active config also stops it).

## Delivery: poll reconciliation, not push

manteion does **not** push faults to instances. Instead, the SDK poll response
(`GET /api/v1/sdk/rules`) carries an `active_faults` list, and the atropos-go SDK
reconciles its applied faults against that list on every poll — applying new
ones (keyed by a stable fault ID) and dropping any no longer present. Duration
expiry and stale-fault reaping are handled SDK-side (server-side infinite-
duration whitelist + fault watchdog); manteion only owns the desired state.

The poll is keyed on the document's ETag (`"<generation>-<content hash>"`),
which the SDK sends back in `If-None-Match`: a poll naming the current document
gets a `304`, and the first poll after a change gets a `200` carrying the
updated `active_faults` for the SDK to reconcile. Fire, cancel and delete bump
the service's generation, which also wakes polls held with `wait`, so they
answer at once instead of when their wait runs out.

An expired fault leaves the document as soon as its duration elapses (the
content hash moves). The fault config reaper (30s) then flips
`active → completed` in the DB and bumps the generation, waking the polls held
meanwhile.

## NOTE — behavioral change to the SDK poll contract

Before this feature, `GET /api/v1/sdk/rules` returned only `{version, rules}`.
It now **always** returns `active_faults` on a `200` (and `freeze_cfg` when one
is desired), and the SDK reconciles against them. Two consequences worth knowing:

- The poll and the register response serve the same materialized document:
  `active_faults` are the service's active fault configs, filtered by the
  instance's capabilities. The register response emits `active_faults`
  (plural) instead of the legacy singular `active_fault`, which the old SDK
  ignored. Experiment faults held in the in-memory intent tracker are no longer
  served.
- `freeze_cfg` is still read from the **in-memory** intent tracker until it is
  derived from the frozen phase. If manteion restarts while a service is
  frozen, the next `200` carries no `freeze_cfg`.
