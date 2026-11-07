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
- A poll naming an `instance_id` manteion does not know (deregistered, purged,
  or a database reset) is answered `404`, and the SDK registers again: the
  registry, not the poll, is what the orchestrator's convergence barrier reads,
  so an unregistered pod must not keep applying documents unseen.
- The poll is a long-poll: when the instance holds the current generation
  (`version`) and its ETag (the content hash alone) is current, manteion holds
  the request until the service's generation moves or `wait` elapses (at most
  60 s; the SDK asks for 30 s). A current ETag with an older `version` is
  answered `304` at once with the generation to adopt. The answer does not
  touch `last_poll_at` again, so an idle instance's touches are one hold apart
  (plus its turnaround).
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

## Observed state (desired-state v1)

Beside liveness, every instance row carries what the instance last told
manteion it holds, exposed on `GET /api/v1/sdk/instances` and
`GET /api/v1/sdk/instances/{id}`:

| Field | Written by |
|-------|------------|
| `observed_generation` | every poll, from its `version` (the generation the instance holds); a status report never lowers it |
| `observed_etag`, `observed_unsupported`, `observed_errors`, `replay_set_id`, `replay_set_state`, `observed_at` | `POST /api/v1/sdk/status`, a full snapshot after every apply and on each replay-set state change (`fetching`, `committed`, `failed`) |

A (re-)registration resets all of them (`observed_generation` back to 0), so
the convergence barrier waits for a freshly registered instance's first poll.

The orchestrator's convergence barrier reads them. A phase entry or resume
bumps the generation of each service the phase touches and waits, up to
`MANTEION_CONVERGE_TIMEOUT` (default 30 s, at most 1 h), until every `alive`
instance of those services has `observed_generation` at or above its bump and,
on a frozen service, reports the assigned replay set `committed`. The timeout
fails the entry (a resume goes back to `paused`), naming each lagging instance
with its last reported errors; a phase start and a resume (of a phase or an
experiment) answer 504 `not_converged` with the `lagging` and `suspect`
lists. Only `alive` instances
are waited on: a `stale` one is named as suspect and not waited on, so a pod
that stopped polling (an old pod of a Deployment roll that never deregistered)
cannot hold an entry up. A frozen service with no `alive` instance, a replay
set an instance reports `failed`, and an instance of a frozen service whose
host advertises no cache-box fail the entry at once. Exit and drain wait the
same bound and only log.

The computed `status` is unchanged by any of this: liveness stays the
poll-arrival heartbeat above.

## Reaper (opt-in)

A background reaper deletes instances that have been dead for more than 10
minutes (`(5 × poll_interval) + 10m`), so replaced pods don't clutter the
dashboard. Disabled by default.

| Variable | Default | Effect |
|----------|---------|--------|
| `MANTEION_SDK_PURGE_ENABLED` | `false` | `true` enables the 1-minute purge loop |
