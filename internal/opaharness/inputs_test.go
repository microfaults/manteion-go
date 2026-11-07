package opaharness

import (
	"context"
	"errors"
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
		{"vector returns every sample", two, "vector", two, ""},
		{"vector empty is an empty array", []promql.Sample{}, "vector", []promql.Sample{}, ""},
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

func TestVectorProvider_FetchError(t *testing.T) {
	boom := errors.New("prometheus query failed: dial tcp")
	if _, err := (VectorProvider{Querier: fakeQuerier{err: boom}}).Fetch(context.Background(), model.PolicyInput{Name: "x", Kind: "scalar"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap the querier error", err)
	}
}
