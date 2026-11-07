package model

import "testing"

func validOPAPolicy() OPAPolicy {
	return OPAPolicy{
		ID: "opa-1", Name: "checkout-hot", Enabled: true,
		Rego:      "package atropos\n\nmutations contains m if { false; m := {} }\n",
		IntervalS: 30, CooldownS: 120,
		Inputs: []PolicyInput{{Name: "p99", Source: "prometheus", Query: "up", Kind: "scalar"}},
		Scope:  PolicyScope{Services: []string{"checkout"}},
	}
}

func TestOPAPolicy_Validate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*OPAPolicy)
		wantErr bool
	}{
		{"valid", nil, false},
		{"missing id", func(p *OPAPolicy) { p.ID = "" }, true},
		{"missing name", func(p *OPAPolicy) { p.Name = "" }, true},
		{"blank rego", func(p *OPAPolicy) { p.Rego = "  \n" }, true},
		{"negative interval", func(p *OPAPolicy) { p.IntervalS = -1 }, true},
		{"negative cooldown", func(p *OPAPolicy) { p.CooldownS = -1 }, true},
		{"input unknown source", func(p *OPAPolicy) { p.Inputs[0].Source = "tempo" }, true},
		{"input unknown kind", func(p *OPAPolicy) { p.Inputs[0].Kind = "matrix" }, true},
		{"input name not an identifier", func(p *OPAPolicy) { p.Inputs[0].Name = "p-99" }, true},
		{"input blank query", func(p *OPAPolicy) { p.Inputs[0].Query = " " }, true},
		{"duplicate input name", func(p *OPAPolicy) { p.Inputs = append(p.Inputs, p.Inputs[0]) }, true},
		{"no inputs is allowed", func(p *OPAPolicy) { p.Inputs = nil }, false},
		{"empty scope", func(p *OPAPolicy) { p.Scope.Services = nil }, true},
		{"blank scope service", func(p *OPAPolicy) { p.Scope.Services = []string{""} }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := validOPAPolicy()
			if tt.modify != nil {
				tt.modify(&p)
			}
			err := p.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestOPAPolicy_ValidateDefaults(t *testing.T) {
	p := validOPAPolicy()
	p.IntervalS = 0
	p.Inputs[0].Kind = ""
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if p.IntervalS != DefaultOPAIntervalS {
		t.Errorf("IntervalS = %d, want %d", p.IntervalS, DefaultOPAIntervalS)
	}
	if p.Inputs[0].Kind != InputKindScalar {
		t.Errorf("Kind = %q, want %q", p.Inputs[0].Kind, InputKindScalar)
	}
}

func TestPolicyScope_Allows(t *testing.T) {
	s := PolicyScope{Services: []string{"checkout", "cart"}}
	for svc, want := range map[string]bool{"checkout": true, "cart": true, "frontend": false, "": false} {
		if got := s.Allows(svc); got != want {
			t.Errorf("Allows(%q) = %v, want %v", svc, got, want)
		}
	}
}
