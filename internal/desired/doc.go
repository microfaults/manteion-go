// Package desired is manteion's desired-state materializer (protocol v1,
// docs/plans/2026-10-30-desired-state-protocol.md): per-service generations
// bumped with every input change, the change broker that wakes long-polls and
// SSE subscribers, and the function DB → RuleSync document per
// (service, instance) with its content ETag. Until plan 2, Materializer.Freeze
// is the in-memory intent tracker, which freeze/thaw change without a bump; a
// fault-spec edit changes the document without one too, and a fault config's
// expiry changes it before the reaper's bump. There the content hash moves
// while the generation does not (yet).
package desired
