// Package desired is manteion's desired-state materializer (protocol v1,
// docs/plans/2026-10-30-desired-state-protocol.md): per-service generations
// bumped with every input change, whose bumps wake held long-polls and, through
// the SSE broker, SSE subscribers, and the function DB → RuleSync document per
// (service, instance) with its content ETag. The freeze config and replay set
// in it come from the running phase that freezes the service and the artifact
// assigned to it (PhaseSources), so they move with the phase's status and the
// orchestrator's bumps. A fault-spec edit, attaching or detaching a phase's
// rules and deleting a phase or an experiment change the document without a
// bump, and a fault config's expiry changes it before the reaper's bump. There
// the content hash moves while the generation does not (yet).
package desired
