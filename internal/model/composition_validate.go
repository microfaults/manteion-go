package model

import (
	"errors"
	"fmt"
	"strings"
)

// FaultSpecResolver looks up a FaultSpec by ID. Returns nil if not found.
type FaultSpecResolver func(id string) *FaultSpec

// CompositionResolver looks up a FaultComposition by ID. Returns nil if not found.
type CompositionResolver func(id string) *FaultComposition

// ValidateCompositionDepth walks the composition tree and rejects if depth exceeds maxDepth.
// Depth 1 = all members are atomic fault specs. Depth 2 = members include child
// compositions whose members are all atomic. Max allowed = 3.
func ValidateCompositionDepth(comp *FaultComposition, resolve CompositionResolver, maxDepth int) error {
	return walkDepth(comp, resolve, 1, maxDepth)
}

func walkDepth(comp *FaultComposition, resolve CompositionResolver, current, max int) error {
	if current > max {
		return fmt.Errorf("composition %q exceeds max depth %d", comp.ID, max)
	}
	for _, m := range comp.Members {
		if m.ChildCompositionID != "" {
			child := resolve(m.ChildCompositionID)
			if child == nil {
				return fmt.Errorf("composition %q: child %q not found", comp.ID, m.ChildCompositionID)
			}
			if err := walkDepth(child, resolve, current+1, max); err != nil {
				return err
			}
		}
	}
	return nil
}

// networkMemberInfo is a resolved member with its fault spec category and direction.
type networkMemberInfo struct {
	FaultType string
	Direction Direction
}

// ValidateNetworkDirections enforces that a parallel composition has at most one
// network toxic per direction. Different directions are fine (e.g., latency
// upstream + throttle downstream). Sequential compositions skip this check
// since toxics are swapped at phase boundaries.
func ValidateNetworkDirections(comp *FaultComposition, resolveFault FaultSpecResolver, resolveComp CompositionResolver) error {
	if comp.ExecutionMode != ExecutionParallel {
		return nil
	}

	networkMembers, err := collectNetworkMembers(comp, resolveFault, resolveComp)
	if err != nil {
		return err
	}

	// Group by direction — at most one network toxic per direction.
	byDirection := map[string][]string{} // direction -> list of fault types
	for _, nm := range networkMembers {
		dir := string(nm.Direction)
		if dir == "" {
			dir = "unspecified"
		}
		byDirection[dir] = append(byDirection[dir], nm.FaultType)
	}

	for dir, types := range byDirection {
		if len(types) > 1 {
			return fmt.Errorf("parallel composition %q: multiple network toxics in direction %q: %v",
				comp.ID, dir, types)
		}
	}
	return nil
}

// collectNetworkMembers recursively collects all network fault specs from a composition.
func collectNetworkMembers(comp *FaultComposition, resolveFault FaultSpecResolver, resolveComp CompositionResolver) ([]networkMemberInfo, error) {
	var result []networkMemberInfo
	for _, m := range comp.Members {
		if m.FaultSpecID != "" {
			spec := resolveFault(m.FaultSpecID)
			if spec == nil {
				return nil, fmt.Errorf("fault spec %q not found", m.FaultSpecID)
			}
			if spec.Category == "network" {
				result = append(result, networkMemberInfo{
					FaultType: spec.Category + ":" + spec.FaultType,
					Direction: m.Direction,
				})
			}
		} else if m.ChildCompositionID != "" {
			child := resolveComp(m.ChildCompositionID)
			if child == nil {
				return nil, fmt.Errorf("child composition %q not found", m.ChildCompositionID)
			}
			childMembers, err := collectNetworkMembers(child, resolveFault, resolveComp)
			if err != nil {
				return nil, err
			}
			// Child members inherit parent member's direction if they don't have one.
			for _, cm := range childMembers {
				if cm.Direction == "" && m.Direction != "" {
					cm.Direction = m.Direction
				}
				result = append(result, cm)
			}
		}
	}
	return result, nil
}

// ValidateCompositionIncompatibilities checks all pairs of leaf faults in a
// composition against the incompatibility rules. Returns the first hard
// incompatibility found, or a list of soft warnings.
func ValidateCompositionIncompatibilities(
	comp *FaultComposition,
	resolveFault FaultSpecResolver,
	resolveComp CompositionResolver,
	rules []FaultIncompatibility,
) (hardErr error, softWarnings []string) {
	leaves, err := collectLeaves(comp, resolveFault, resolveComp)
	if err != nil {
		return err, nil
	}

	for i := 0; i < len(leaves); i++ {
		for j := i + 1; j < len(leaves); j++ {
			a := leaves[i]
			b := leaves[j]
			for _, rule := range rules {
				if !matchesPair(a, b, rule, comp.ExecutionMode) {
					continue
				}
				msg := fmt.Sprintf("%s + %s: %s", a, b, rule.Reason)
				if rule.ConstraintType == "hard" {
					return fmt.Errorf("hard incompatibility in composition %q: %s", comp.ID, msg), nil
				}
				softWarnings = append(softWarnings, msg)
			}
		}
	}
	return nil, softWarnings
}

// collectLeaves returns all leaf fault type strings (e.g. "network:blackhole")
// from a composition tree.
func collectLeaves(comp *FaultComposition, resolveFault FaultSpecResolver, resolveComp CompositionResolver) ([]string, error) {
	var result []string
	for _, m := range comp.Members {
		if m.FaultSpecID != "" {
			spec := resolveFault(m.FaultSpecID)
			if spec == nil {
				return nil, fmt.Errorf("fault spec %q not found", m.FaultSpecID)
			}
			result = append(result, spec.Category+":"+spec.FaultType)
		} else if m.ChildCompositionID != "" {
			child := resolveComp(m.ChildCompositionID)
			if child == nil {
				return nil, fmt.Errorf("child composition %q not found", m.ChildCompositionID)
			}
			childLeaves, err := collectLeaves(child, resolveFault, resolveComp)
			if err != nil {
				return nil, err
			}
			result = append(result, childLeaves...)
		}
	}
	return result, nil
}

// matchesPair checks if a pair of fault types matches an incompatibility rule.
// For sequential scope, order matters: FaultTypeA must match the earlier member (a)
// and FaultTypeB the later member (b). For parallel/any scope, order is symmetric.
func matchesPair(a, b string, rule FaultIncompatibility, mode ExecutionMode) bool {
	if rule.Scope != "any" && rule.Scope != string(mode) {
		return false
	}
	if rule.Scope == "sequential" {
		// Order-sensitive: A must come before B.
		return matchesType(a, rule.FaultTypeA) && matchesType(b, rule.FaultTypeB)
	}
	return (matchesType(a, rule.FaultTypeA) && matchesType(b, rule.FaultTypeB)) ||
		(matchesType(a, rule.FaultTypeB) && matchesType(b, rule.FaultTypeA))
}

// matchesType checks if a concrete fault type matches a rule type pattern.
// Supports wildcards: "network:*" matches any network type.
func matchesType(concrete, pattern string) bool {
	if pattern == concrete {
		return true
	}
	if strings.HasSuffix(pattern, ":*") {
		prefix := strings.TrimSuffix(pattern, ":*")
		return strings.HasPrefix(concrete, prefix+":")
	}
	return false
}

// ValidateComposition runs all composition-level validations: depth, direction,
// and incompatibilities. Returns the first hard error encountered.
func ValidateComposition(
	comp *FaultComposition,
	resolveFault FaultSpecResolver,
	resolveComp CompositionResolver,
) error {
	if err := comp.Validate(); err != nil {
		return err
	}
	if err := ValidateCompositionDepth(comp, resolveComp, 3); err != nil {
		return err
	}
	if err := ValidateNetworkDirections(comp, resolveFault, resolveComp); err != nil {
		return err
	}
	hardErr, _ := ValidateCompositionIncompatibilities(comp, resolveFault, resolveComp, DefaultIncompatibilities())
	if hardErr != nil {
		if errors.Is(hardErr, errNotFound) {
			return hardErr
		}
		return hardErr
	}
	return nil
}

var errNotFound = errors.New("not found")
