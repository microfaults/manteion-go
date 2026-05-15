# SDK Liveness & Instance Management

Manteion tracks the connectivity health of all registered Atropos-go SDK instances using a passive heartbeat mechanism.

## Liveness Detection
Unlike active probing (pinging), Manteion uses the SDK's existing rule-polling cycle as a heartbeat.

### Heartbeat Mechanism
- Every time an SDK instance calls `GET /api/v1/sdk/rules`, Manteion updates the `last_poll_at` timestamp for that `instance_id`.
- This update happens even if the poll returns `304 Not Modified`, ensuring liveness is tracked correctly during periods of no rule changes.

### Status Computation
Liveness status is computed on-the-fly when reading from the database (e.g., via `GET /api/v1/sdk/instances`). It uses the `poll_interval_ms` reported by the SDK during registration:

| Status  | Condition | Visual |
|---------|-----------|--------|
| **Alive** | `last_poll_at` within 2x interval | Green |
| **Stale** | `last_poll_at` between 2x and 5x interval | Yellow |
| **Dead**  | `last_poll_at` beyond 5x interval | Red |

## Instance Purging (Reaper)
To prevent the dashboard from being cluttered with "dead" instances (e.g., from pods that have been deleted and replaced), Manteion includes a background reaper.

### Behavior
- The reaper runs once per minute.
- It deletes any SDK instance that has been in a **Dead** state for more than 10 minutes.
- **Feature Flag**: This behavior is disabled by default and must be explicitly enabled via environment variable.

### Configuration
| Variable | Description | Default |
|----------|-------------|---------|
| `MANTEION_SDK_PURGE_ENABLED` | Set to `true` to enable automatic deletion of dead instances. | `false` |
