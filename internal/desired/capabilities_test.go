package desired

import (
	"testing"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"

	"manteion-go/internal/model"
)

func fault(cat string) *atroposdk.CompiledFault {
	return &atroposdk.CompiledFault{Category: cat, FaultType: "x"}
}

func TestCapabilitiesOf(t *testing.T) {
	if CapabilitiesOf(nil) != nil {
		t.Error("nil instance must allow everything")
	}
	if CapabilitiesOf(&model.SDKInstance{Capabilities: []string{}}) != nil {
		t.Error("empty list means absent = all")
	}
	c := CapabilitiesOf(&model.SDKInstance{Capabilities: []string{"inline", "cachebox"}})
	if !c.Allows("inline") || !c.Allows("cachebox") || c.Allows("network") || c.Allows("resource") {
		t.Errorf("Allows wrong for %v", c)
	}
	if !Capabilities(nil).Allows("resource") {
		t.Error("nil capabilities must allow every category")
	}
	// Unknown strings are ignored: the sidecar may advertise "netem"; it must
	// neither unlock a known category nor be rejected.
	c = CapabilitiesOf(&model.SDKInstance{Capabilities: []string{"netem", "inline"}})
	if !c.Allows("inline") || c.Allows("network") || c.Allows("netem") {
		t.Errorf("unknown capability string changed the filter: %v", c)
	}
}

func TestRuleCategories(t *testing.T) {
	cases := []struct {
		name string
		rule atroposdk.CompiledRule
		want []string
	}{
		{"empty category is inline", atroposdk.CompiledRule{Fault: fault("")}, []string{"inline"}},
		{"network fault", atroposdk.CompiledRule{Fault: fault("network")}, []string{"network"}},
		{"cachebox", atroposdk.CompiledRule{CacheBox: &atroposdk.CompiledCacheBox{Mode: "replay"}}, []string{"cachebox"}},
		{"nested composition lists every leaf", atroposdk.CompiledRule{Composition: &atroposdk.CompiledComposition{Members: []atroposdk.CompiledCompositionMember{
			{Fault: fault("inline")},
			{Composition: &atroposdk.CompiledComposition{Members: []atroposdk.CompiledCompositionMember{{Fault: fault("resource")}}}},
		}}}, []string{"inline", "resource"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RuleCategories(tc.rule)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestFilterRulesAndFaults(t *testing.T) {
	rules := []atroposdk.CompiledRule{
		{Name: "inl", Fault: fault("inline")},
		{Name: "net", Fault: fault("network")},
		{Name: "comp-mixed", Composition: &atroposdk.CompiledComposition{Members: []atroposdk.CompiledCompositionMember{{Fault: fault("inline")}, {Fault: fault("network")}}}},
		{Name: "cb", CacheBox: &atroposdk.CompiledCacheBox{Mode: "passthrough"}},
	}
	names := func(rs []atroposdk.CompiledRule) []string {
		out := []string{}
		for _, r := range rs {
			out = append(out, r.Name)
		}
		return out
	}
	if got := names(FilterRules(rules, nil)); len(got) != 4 {
		t.Errorf("nil caps dropped rules: %v", got)
	}
	got := names(FilterRules(rules, Capabilities{"inline": true, "cachebox": true}))
	if len(got) != 2 || got[0] != "inl" || got[1] != "cb" {
		t.Errorf("inline+cachebox → %v, want [inl cb] (composition with a network member must go)", got)
	}
	if got := FilterRules(nil, Capabilities{"inline": true}); got == nil {
		t.Error("FilterRules must return an empty, non-nil slice")
	}
	faults := []atroposdk.FaultRequest{{ID: "a", Category: ""}, {ID: "b", Category: "resource"}}
	if got := FilterFaults(faults, Capabilities{"inline": true}); len(got) != 1 || got[0].ID != "a" {
		t.Errorf("FilterFaults = %v, want [a]", got)
	}
}

// A host that advertised only strings this control plane does not know has not
// advertised "nothing": the filter applies and nothing passes it. Collapsing
// that to nil (absent = all) would hand every category to a host that claimed
// none it could run.
func TestCapabilitiesOf_OnlyUnknownStringsAllowsNothing(t *testing.T) {
	c := CapabilitiesOf(&model.SDKInstance{Capabilities: []string{"netem"}})
	if c == nil {
		t.Fatal("an unknown-only list collapsed to nil, which allows everything")
	}
	for _, category := range []string{"inline", "network", "resource", "cachebox", "netem"} {
		if c.Allows(category) {
			t.Errorf("Allows(%q) = true for an unknown-only list", category)
		}
	}
}

func TestFilterFaults_EmptyIsNonNil(t *testing.T) {
	if got := FilterFaults(nil, Capabilities{"inline": true}); got == nil {
		t.Error("nil input: FilterFaults must return an empty, non-nil slice")
	}
	all := []atroposdk.FaultRequest{{ID: "a", Category: "network"}}
	if got := FilterFaults(all, Capabilities{"inline": true}); got == nil || len(got) != 0 {
		t.Errorf("everything filtered out: got %#v, want an empty, non-nil slice", got)
	}
}
