package conditions

import (
	"math"
	"testing"
)

func TestMet(t *testing.T) {
	tests := []struct {
		name      string
		value     float64
		operator  string
		threshold float64
		want      bool
	}{
		{"gt true", 10, "gt", 5, true},
		{"gt false", 5, "gt", 10, false},
		{"gt equal", 5, "gt", 5, false},
		{"gte true", 10, "gte", 5, true},
		{"gte equal", 5, "gte", 5, true},
		{"gte false", 4, "gte", 5, false},
		{"lt true", 3, "lt", 5, true},
		{"lt false", 10, "lt", 5, false},
		{"lte true", 5, "lte", 5, true},
		{"lte false", 6, "lte", 5, false},
		{"eq true", 5, "eq", 5, true},
		{"eq false", 4, "eq", 5, false},
		{"unknown operator", 5, "neq", 5, false},
		{"NaN value", math.NaN(), "gt", 0, false},
		{"positive Inf", math.Inf(1), "gt", 0, false},
		{"negative Inf", math.Inf(-1), "lt", 0, false},
		{"NaN with eq", math.NaN(), "eq", math.NaN(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Met(tt.value, tt.operator, tt.threshold)
			if got != tt.want {
				t.Errorf("Met(%v, %q, %v) = %v, want %v",
					tt.value, tt.operator, tt.threshold, got, tt.want)
			}
		})
	}
}
