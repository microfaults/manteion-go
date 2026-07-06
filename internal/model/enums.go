package model

// This file is the single Go source of truth for the stable vocabularies
// that exist as native Postgres ENUM types in the epoch-2 schema
// (internal/db/migrations.go). Each XxxValues slice mirrors its pg_enum
// label set IN ORDER; the integration test internal/store/enum_parity_test.go
// asserts the two never drift. The valid* maps used by Validate() methods
// are derived from these slices.
//
// fault_type is deliberately absent: it stays TEXT in the DB and is
// validated by the fault catalog (internal/faultcatalog, backed by
// atropos-go/faultparams), so new atropos fault types need no migration.

// EnumValues maps each Postgres enum type name to its ordered Go label set —
// the parity test ranges over this.
var EnumValues = map[string][]string{
	"experiment_status":     ExperimentStatusValues,
	"phase_status":          PhaseStatusValues,
	"fault_category":        FaultCategoryValues,
	"fault_host":            FaultHostValues,
	"network_direction":     NetworkDirectionValues,
	"injection_point":       InjectionPointValues,
	"rule_mode":             RuleModeValues,
	"rule_action_type":      RuleActionTypeValues,
	"start_policy":          StartPolicyValues,
	"cachebox_mode":         CacheBoxModeValues,
	"cachebox_key_strategy": CacheBoxKeyStrategyValues,
	"execution_mode":        ExecutionModeValues,
	"trace_backend":         TraceBackendValues,
	"fault_config_status":   FaultConfigStatusValues,
	"fault_event_source":    FaultEventSourceValues,
}

var (
	ExperimentStatusValues    = []string{"planned", "running", "completed", "failed", "cancelled"}
	PhaseStatusValues         = []string{"pending", "running", "paused", "completed", "failed", "skipped", "draining"}
	FaultCategoryValues       = []string{"inline", "network", "resource"}
	FaultHostValues           = []string{"proxy", "inline", "process"}
	NetworkDirectionValues    = []string{"upstream", "downstream"}
	InjectionPointValues      = []string{"ingress", "egress", "transient", "custom"}
	RuleModeValues            = []string{"inline", "background"}
	RuleActionTypeValues      = []string{"fault_spec", "fault_composition", "cachebox"}
	StartPolicyValues         = []string{"deduplicate_by_rule", "always_start"}
	CacheBoxModeValues        = []string{"passthrough", "replay", "replay_with_delay"}
	CacheBoxKeyStrategyValues = []string{"exact", "exact_with_host", "exact_with_body", "canonical_v2"}
	ExecutionModeValues       = []string{"parallel", "sequential"}
	TraceBackendValues        = []string{"jaeger", "prometheus", "tempo"}
	FaultConfigStatusValues   = []string{"ready", "active", "completed", "cancelled"}
	FaultEventSourceValues    = []string{"rule", "cachebox", "fault_config"}
)

// setOf builds the membership map a Validate() method checks against.
func setOf(values ...string) map[string]bool {
	m := make(map[string]bool, len(values))
	for _, v := range values {
		m[v] = true
	}
	return m
}
