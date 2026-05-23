# SDK Liveness & Instance Management

Manteion tracks the connectivity health of registered atropos-go SDK instances
using a passive heartbeat — the SDK's existing rule-polling cycle, not active
probing.

## Heartbeat

- Every `GET /api/v1/sdk/rules` poll updates `last_poll_at` for the instance.
- The touch runs **before** the `304 Not Modified` check, so liveness stays
  accurate even when there are no rule changes.
- The SDK reports its `poll_interval_ms` at registration; manteion stores it and
  derives status from it (no fixed global threshold).

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
