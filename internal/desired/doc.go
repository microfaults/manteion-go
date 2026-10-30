// Package desired is manteion's desired-state materializer (protocol v1,
// docs/plans/2026-10-30-desired-state-protocol.md): per-service generations
// bumped with every input change, the change broker that wakes long-polls and
// SSE subscribers, and the pure function DB → RuleSync document per
// (service, instance) with its content ETag.
package desired
