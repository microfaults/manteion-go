package opaharness

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"manteion-go/internal/model"
	"manteion-go/internal/promql"
)

type fakeQuerier struct {
	samples []promql.Sample
	err     error
}

func (f fakeQuerier) QueryVector(_ context.Context, _ string) ([]promql.Sample, error) {
	return f.samples, f.err
}

func TestVectorProvider_Fetch(t *testing.T) {
	one := []promql.Sample{{Labels: map[string]string{"service": "checkout"}, Value: 1.5}}
	two := append(append([]promql.Sample{}, one...), promql.Sample{Labels: map[string]string{"service": "cart"}, Value: 2})
	tests := []struct {
		name    string
		samples []promql.Sample
		kind    string
		want    any
		wantErr string
	}{
		{"scalar one series", one, "scalar", 1.5, ""},
		{"scalar absent series is null", []promql.Sample{}, "scalar", nil, ""},
		{"scalar rejects several series", two, "scalar", nil, "2 series"},
		{"vector returns every sample", two, "vector", []InputSample{{Labels: one[0].Labels, Value: ptr(1.5)}, {Labels: two[1].Labels, Value: ptr(2.0)}}, ""},
		{"vector empty is an empty array", []promql.Sample{}, "vector", []InputSample{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := VectorProvider{Querier: fakeQuerier{samples: tt.samples}}.Fetch(context.Background(), model.PolicyInput{Name: "x", Query: "q", Kind: tt.kind})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Fetch = %#v, %v; want %#v", got, err, tt.want)
			}
		})
	}
}

func ptr(v float64) *float64 { return &v }

func TestVectorProvider_FetchError(t *testing.T) {
	boom := errors.New("prometheus query failed: dial tcp")
	if _, err := (VectorProvider{Querier: fakeQuerier{err: boom}}).Fetch(context.Background(), model.PolicyInput{Name: "x", Kind: "scalar"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap the querier error", err)
	}
}

// nonFiniteSamples is a vector with one sample of each non-finite kind and one
// finite sample, as Prometheus answers an error ratio with no traffic.
func nonFiniteSamples() []promql.Sample {
	return []promql.Sample{
		{Labels: map[string]string{"route": "nan"}, Value: math.NaN()},
		{Labels: map[string]string{"route": "pinf"}, Value: math.Inf(1)},
		{Labels: map[string]string{"route": "ninf"}, Value: math.Inf(-1)},
		{Labels: map[string]string{"route": "ok"}, Value: 0.25},
	}
}

func TestVectorProvider_NonFiniteBecomesNull(t *testing.T) {
	got, err := VectorProvider{Querier: fakeQuerier{samples: nonFiniteSamples()}}.Fetch(context.Background(), model.PolicyInput{Name: "x", Query: "q", Kind: "vector"})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal %#v: %v (a non-finite sample must not break the JSON encoding)", got, err)
	}
	want := `[{"metric":{"route":"nan"},"value":null},{"metric":{"route":"pinf"},"value":null},` +
		`{"metric":{"route":"ninf"},"value":null},{"metric":{"route":"ok"},"value":0.25}]`
	if string(raw) != want {
		t.Errorf("vector JSON = %s, want %s", raw, want)
	}

	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		one := []promql.Sample{{Labels: map[string]string{}, Value: v}}
		got, err := VectorProvider{Querier: fakeQuerier{samples: one}}.Fetch(context.Background(), model.PolicyInput{Name: "x", Query: "q", Kind: "scalar"})
		if err != nil || got != nil {
			t.Errorf("scalar %v: Fetch = %#v, %v; want nil (JSON null), no error", v, got, err)
		}
	}
}

func TestVectorProvider_UnknownKindHasOnePrefix(t *testing.T) {
	_, err := VectorProvider{Querier: fakeQuerier{}}.Fetch(context.Background(), model.PolicyInput{Name: "x", Query: "q", Kind: "matrix"})
	if err == nil || err.Error() != `unknown kind "matrix"` {
		t.Fatalf("err = %v, want exactly %q (the harness adds the input prefix)", err, `unknown kind "matrix"`)
	}
}
