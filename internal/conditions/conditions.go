// Package conditions provides shared metric condition evaluation used by both
// the policy engine and the orchestrator phase FSM.
package conditions

import "math"

// Met evaluates `value <op> threshold`. NaN or +/-Inf values are treated as
// "indeterminate" and not met (conservative: don't fire on missing/garbage data).
// Unknown operators also return false so a typo'd rule never silently fires.
func Met(value float64, operator string, threshold float64) bool {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return false
	}
	switch operator {
	case "gt":
		return value > threshold
	case "gte":
		return value >= threshold
	case "lt":
		return value < threshold
	case "lte":
		return value <= threshold
	case "eq":
		return value == threshold
	}
	return false
}
