package demo

import "slices"

// FaultKind is the kind of a fault phase's fault. The list is closed:
// FaultMemory and FaultCPU are the only kinds.
type FaultKind string

const (
	// FaultMemory holds a share of the target's memory limit (atropos
	// resource/memory).
	FaultMemory FaultKind = "memory"
	// FaultCPU burns a share of the target's CPU limit (atropos resource/cpu).
	FaultCPU FaultKind = "cpu"
)

// FaultStep is one fault phase of a scenario: the fault, the service it runs
// in, the intensity label a visitor sees, and the atropos target_load behind
// that label (the share of the service's limit the fault adds on top of what
// the service already uses).
type FaultStep struct {
	Kind       FaultKind
	Target     string
	Intensity  string
	TargetLoad float64
}

// Workload is the load every phase of a scenario's run sends: the manteion
// workflow, the zeus dataset it reads (looked up by name), and a one-line
// description for the page.
type Workload struct {
	WorkflowID, DatasetName, Description string
}

// Scenario is one entry of the catalog: a run of three phases (no fault, the
// memory fault, the CPU fault) under the same workload.
type Scenario struct {
	ID, Title, Description string
	Default                bool
	Workload               Workload
	Memory, CPU            FaultStep
}

// faultSteps are sc's steps by phase position (PhaseNames): none for the
// baseline, then the memory step, then the CPU step.
func faultSteps(sc *Scenario) [3]*FaultStep {
	return [3]*FaultStep{nil, &sc.Memory, &sc.CPU}
}

// The catalog's targets: each runs with a 200m CPU and 128Mi memory limit on
// the lab.
const (
	targetCatalog = "productcatalogservice"
	targetCart    = "cartservice"
)

// Intensity labels and the target_load each stands for, the same on both
// targets. The box trial (2026-11-23) ran these with no restart and no OOM
// kill; memory stops at 0.2 because heavier loads were still held when the
// next queued run's memory phase would begin.
const (
	intensityModerate = "moderate"
	intensityHeavy    = "heavy"

	memoryModerate = 0.1
	memoryHeavy    = 0.2
	cpuModerate    = 0.5
	cpuHeavy       = 1.0
)

// browse is the one workload of the first release: the manteion workflow
// demo-browse on the zeus dataset demo-products (looked up by name at build
// time, because its id changes whenever zeus restarts).
var browse = Workload{
	WorkflowID:  "demo-browse",
	DatasetName: "demo-products",
	Description: "Each simulated visitor opens the shop's home page, may then open a product page, and may add that product to the cart; products are picked from a list of the shop's own.",
}

// catalog is the fixed, ordered list of scenarios. Nothing adds to it at run
// time.
var catalog = [...]Scenario{
	{
		ID:          "catalog-moderate",
		Title:       "Product catalog, moderate",
		Description: "The service that lists the shop's products takes about a tenth of its memory limit more than usual, then spends about half of its CPU limit on busy work.",
		Default:     true,
		Workload:    browse,
		Memory:      FaultStep{Kind: FaultMemory, Target: targetCatalog, Intensity: intensityModerate, TargetLoad: memoryModerate},
		CPU:         FaultStep{Kind: FaultCPU, Target: targetCatalog, Intensity: intensityModerate, TargetLoad: cpuModerate},
	},
	{
		ID:          "catalog-heavy",
		Title:       "Product catalog, heavy",
		Description: "The service that lists the shop's products takes about a fifth of its memory limit more than usual, then tries to spend all of its CPU limit on busy work.",
		Workload:    browse,
		Memory:      FaultStep{Kind: FaultMemory, Target: targetCatalog, Intensity: intensityHeavy, TargetLoad: memoryHeavy},
		CPU:         FaultStep{Kind: FaultCPU, Target: targetCatalog, Intensity: intensityHeavy, TargetLoad: cpuHeavy},
	},
	{
		ID:          "cart-moderate",
		Title:       "Cart, moderate",
		Description: "The service that keeps shopping carts takes about a tenth of its memory limit more than usual, then spends about half of its CPU limit on busy work.",
		Workload:    browse,
		Memory:      FaultStep{Kind: FaultMemory, Target: targetCart, Intensity: intensityModerate, TargetLoad: memoryModerate},
		CPU:         FaultStep{Kind: FaultCPU, Target: targetCart, Intensity: intensityModerate, TargetLoad: cpuModerate},
	},
	{
		ID:          "cart-heavy",
		Title:       "Cart, heavy",
		Description: "The service that keeps shopping carts takes about a fifth of its memory limit more than usual, then tries to spend all of its CPU limit on busy work.",
		Workload:    browse,
		Memory:      FaultStep{Kind: FaultMemory, Target: targetCart, Intensity: intensityHeavy, TargetLoad: memoryHeavy},
		CPU:         FaultStep{Kind: FaultCPU, Target: targetCart, Intensity: intensityHeavy, TargetLoad: cpuHeavy},
	},
}

// Catalog returns a copy of the catalog, in order. Exactly one entry is the
// default.
func Catalog() []Scenario {
	return slices.Clone(catalog[:])
}

// ScenarioByID returns the scenario whose id is exactly id: no case folding,
// no trimming.
func ScenarioByID(id string) (Scenario, bool) {
	for _, sc := range catalog {
		if sc.ID == id {
			return sc, true
		}
	}
	return Scenario{}, false
}

// DefaultScenario returns the catalog's default, the scenario a run without a
// choice uses.
func DefaultScenario() Scenario {
	for _, sc := range catalog {
		if sc.Default {
			return sc
		}
	}
	panic("demo: the catalog has no default scenario")
}

// CatalogTargets returns the distinct services the catalog's faults run in,
// sorted.
func CatalogTargets() []string {
	var out []string
	for _, sc := range catalog {
		out = append(out, sc.Memory.Target, sc.CPU.Target)
	}
	slices.Sort(out)
	return slices.Compact(out)
}
