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

Fire/cancel/expiry bump the rule version, so the change rides the existing
version-based poll fast-path: an unchanged poll still gets a `304`, and the next
`200` after a bump carries the updated `active_faults` for the SDK to reconcile.

The fault config reaper (30s) only flips `active → completed` in the DB once a
finite duration elapses; the SDK drops the fault on its next poll.

## NOTE — behavioral change to the SDK poll contract

Before this feature, `GET /api/v1/sdk/rules` returned only `{version, rules}`.
It now **always** returns `active_faults` and `freeze_cfg` on a `200`, and the
SDK reconciles against them. Two consequences worth knowing:

- This is also the delivery path for **experiment-driven** faults: the poll set
  is the union of manual fault configs and the in-memory experiment intent. The
  register response likewise now emits `active_faults` (plural) instead of the
  legacy singular `active_fault`, which the current SDK ignored.
- The experiment intent tracker is **in-memory**. If manteion restarts while an
  experiment is mid-flight, the next `200` will not include that experiment's
  in-flight faults, so the SDK will reconcile them away (the SDK watchdog would
  reap them within its grace period regardless). Durable experiment-fault
  delivery across restarts would require persisting intent — tracked separately.
