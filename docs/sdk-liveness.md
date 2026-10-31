# SDK Liveness & Instance Management

Manteion tracks the connectivity health of registered atropos-go SDK instances
using a passive heartbeat — the SDK's existing rule-polling cycle, not active
probing.

## Heartbeat

- Every `GET /api/v1/sdk/rules?service=&instance_id=&version=&wait=` poll
  updates `last_poll_at` for the instance, together with `observed_generation`
  (the generation the instance holds, from `version`). Registration sets
  `last_poll_at` too.
- The touch runs when the poll **arrives**: before the `If-None-Match` / `304
  Not Modified` decision and before any hold, so liveness stays accurate even
  when nothing changes.
- The poll is a long-poll: when the instance's ETag is current, manteion holds
  the request until the service's generation moves or `wait` elapses (at most
  60 s; the SDK asks for 30 s). The answer does not touch `last_poll_at` again,
  so an idle instance's touches are one hold apart (plus its turnaround).
- The SDK reports its `poll_interval_ms` at registration; manteion stores it and
  derives status from it (no fixed global threshold). A long-polling SDK
  advertises `max(poll interval, hold)`, so the thresholds below are counted
  in holds: with a 30 s hold an idle instance is touched every ~30 s and stays
  `alive` (within 2 × 30 s), reads `stale` after a missed poll or two, and
  `dead` only after 5 × 30 s = 150 s of silence. An SDK that advertised its
  poll interval alone, with the default 10 s interval, would read `stale`
  while parked on a 30 s hold (beyond 2 × 10 s) and `dead` on a 60 s hold
  (beyond 5 × 10 s).

## Status (computed at read time)

`GET /api/v1/sdk/instances` returns a computed `status` per instance:

| Status | Condition (since `last_poll_at`) |
|--------|----------------------------------|
| alive  | within 2× poll interval          |
| stale  | between 2× and 5× poll interval  |
| dead   | beyond 5× poll interval          |

Status is never persisted — it is a SQL expression over `last_poll_at` and
`poll_interval_ms`.

## Reaper (opt-in)

A background reaper deletes instances that have been dead for more than 10
minutes (`(5 × poll_interval) + 10m`), so replaced pods don't clutter the
dashboard. Disabled by default.

| Variable | Default | Effect |
|----------|---------|--------|
| `MANTEION_SDK_PURGE_ENABLED` | `false` | `true` enables the 1-minute purge loop |
