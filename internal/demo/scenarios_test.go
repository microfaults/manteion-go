package demo

import (
	"maps"
	"regexp"
	"slices"
	"testing"
)

// trialHeavyMemory is, per target service, the heaviest memory target_load
// the box trial (2026-11-23) proved safe: no restart, no OOM kill, memory
// given back before the next run's memory phase.
var trialHeavyMemory = map[string]float64{
	"productcatalogservice": 0.2,
	"cartservice":           0.2,
}

var scenarioIDPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// TestCatalog_Shape: enough scenarios over enough targets and intensities,
// exactly one default, ids safe to put in a URL, and every fault inside the
// range the box trial proved.
func TestCatalog_Shape(t *testing.T) {
	cat := Catalog()
	if len(cat) < 4 {
		t.Fatalf("catalog has %d scenarios; want at least 4", len(cat))
	}
	ids := map[string]bool{}
	targets := map[string]bool{}
	intensities := map[string]bool{}
	defaults := 0
	for _, sc := range cat {
		if !scenarioIDPattern.MatchString(sc.ID) {
			t.Errorf("id %q does not match %s", sc.ID, scenarioIDPattern)
		}
		if ids[sc.ID] {
			t.Errorf("id %q appears twice", sc.ID)
		}
		ids[sc.ID] = true
		if sc.Default {
			defaults++
		}
		if sc.Title == "" || sc.Description == "" {
			t.Errorf("%s: title %q, description %q; want both", sc.ID, sc.Title, sc.Description)
		}
		if sc.Workload.WorkflowID == "" || sc.Workload.DatasetName == "" || sc.Workload.Description == "" {
			t.Errorf("%s: workload %+v; want a workflow, a dataset and a description", sc.ID, sc.Workload)
		}
		if sc.Memory.Kind != "memory" || sc.CPU.Kind != "cpu" {
			t.Errorf("%s: kinds %q, %q; want memory, cpu", sc.ID, sc.Memory.Kind, sc.CPU.Kind)
		}
		for _, st := range []FaultStep{sc.Memory, sc.CPU} {
			if st.Target == "" || st.Intensity == "" {
				t.Errorf("%s: %s step %+v; want a target and an intensity", sc.ID, st.Kind, st)
			}
			targets[st.Target] = true
			intensities[st.Intensity] = true
		}
		if l := sc.Memory.TargetLoad; !(l > 0 && l <= 0.2) {
			t.Errorf("%s: memory target_load %v; want in (0, 0.2]", sc.ID, l)
		}
		if heavy, ok := trialHeavyMemory[sc.Memory.Target]; !ok || sc.Memory.TargetLoad > heavy {
			t.Errorf("%s: memory target_load %v on %s; the trial proved at most %v (tried: %v)",
				sc.ID, sc.Memory.TargetLoad, sc.Memory.Target, heavy, ok)
		}
		if l := sc.CPU.TargetLoad; !(l > 0 && l <= 1) {
			t.Errorf("%s: cpu target_load %v; want in (0, 1]", sc.ID, l)
		}
		if _, ok := trialHeavyMemory[sc.CPU.Target]; !ok {
			t.Errorf("%s: cpu target %s was not tried on the box", sc.ID, sc.CPU.Target)
		}
	}
	if len(targets) < 2 {
		t.Errorf("targets %v; want at least 2", slices.Sorted(maps.Keys(targets)))
	}
	if len(intensities) < 2 {
		t.Errorf("intensities %v; want at least 2", slices.Sorted(maps.Keys(intensities)))
	}
	if defaults != 1 {
		t.Errorf("%d default scenarios; want exactly 1", defaults)
	}
	if def := DefaultScenario(); !def.Default || !ids[def.ID] {
		t.Errorf("DefaultScenario() = %+v; want the catalog's default", def)
	}
	if got, want := CatalogTargets(), slices.Sorted(maps.Keys(targets)); !slices.Equal(got, want) {
		t.Errorf("CatalogTargets() = %v; want %v", got, want)
	}
	if scenarioPhaseNames != [3]string{"baseline", "memory", "cpu"} {
		t.Errorf("scenario phases = %v; want baseline, memory, cpu", scenarioPhaseNames)
	}
}

// TestCatalog_Entries pins the catalog to the box trial's values, in order.
func TestCatalog_Entries(t *testing.T) {
	type entry struct {
		id          string
		def         bool
		target      string
		intensity   string
		memory, cpu float64
	}
	want := []entry{
		{"catalog-moderate", true, "productcatalogservice", "moderate", 0.1, 0.5},
		{"catalog-heavy", false, "productcatalogservice", "heavy", 0.2, 1.0},
		{"cart-moderate", false, "cartservice", "moderate", 0.1, 0.5},
		{"cart-heavy", false, "cartservice", "heavy", 0.2, 1.0},
	}
	cat := Catalog()
	if len(cat) != len(want) {
		t.Fatalf("catalog has %d scenarios; want %d", len(cat), len(want))
	}
	for i, w := range want {
		sc := cat[i]
		got := entry{sc.ID, sc.Default, sc.Memory.Target, sc.Memory.Intensity, sc.Memory.TargetLoad, sc.CPU.TargetLoad}
		if got != w {
			t.Errorf("scenario %d = %+v; want %+v", i, got, w)
		}
		if sc.CPU.Target != w.target || sc.CPU.Intensity != w.intensity {
			t.Errorf("%s: cpu step on %s at %s; want %s at %s", sc.ID, sc.CPU.Target, sc.CPU.Intensity, w.target, w.intensity)
		}
		if sc.Workload.WorkflowID != "demo-browse" || sc.Workload.DatasetName != "demo-products" {
			t.Errorf("%s: workload %s on %s; want demo-browse on demo-products", sc.ID, sc.Workload.WorkflowID, sc.Workload.DatasetName)
		}
	}
	if got := CatalogTargets(); !slices.Equal(got, []string{"cartservice", "productcatalogservice"}) {
		t.Errorf("CatalogTargets() = %v", got)
	}
	if DefaultScenario().ID != "catalog-moderate" {
		t.Errorf("default = %s; want catalog-moderate", DefaultScenario().ID)
	}
}

// TestScenarioByID: only an exact id selects a scenario.
func TestScenarioByID(t *testing.T) {
	for _, sc := range Catalog() {
		got, ok := ScenarioByID(sc.ID)
		if !ok || got != sc {
			t.Errorf("ScenarioByID(%q) = %+v, %v; want the catalog's entry", sc.ID, got, ok)
		}
	}
	for _, id := range []string{
		"CATALOG-MODERATE", "Catalog-Moderate", " catalog-moderate", "catalog-moderate ", "\tcatalog-moderate",
		"catalog-moderate\n", "", "unknown", "catalog", "catalog-moderate-", "catalog_moderate",
	} {
		if got, ok := ScenarioByID(id); ok || got != (Scenario{}) {
			t.Errorf("ScenarioByID(%q) = %+v, %v; want not found", id, got, ok)
		}
	}
}

// TestCatalog_ReturnsCopy: what a caller does to a returned catalog or
// scenario never reaches the next call.
func TestCatalog_ReturnsCopy(t *testing.T) {
	before := Catalog()
	got := Catalog()
	got[0].ID = "changed"
	got[0].Default = !got[0].Default
	got[0].Memory.TargetLoad = 0.9
	got[0].Workload.WorkflowID = "changed"
	got[len(got)-1] = Scenario{}
	_ = append(got[:1], Scenario{ID: "appended"})
	targets := CatalogTargets()
	targets[0] = "changed"

	sc, _ := ScenarioByID(before[0].ID)
	sc.CPU.TargetLoad = 0.01
	def := DefaultScenario()
	def.Memory.Target = "changed"

	if after := Catalog(); !slices.Equal(after, before) {
		t.Errorf("catalog after mutating a returned copy = %+v; want %+v", after, before)
	}
	if got := CatalogTargets(); got[0] == "changed" {
		t.Errorf("CatalogTargets() after mutating a returned copy = %v", got)
	}
	if again, _ := ScenarioByID(before[0].ID); again != before[0] {
		t.Errorf("ScenarioByID after mutating a returned value = %+v", again)
	}
	if DefaultScenario().Memory.Target == "changed" {
		t.Errorf("DefaultScenario after mutating a returned value = %+v", DefaultScenario())
	}
}
