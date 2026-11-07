package opaharness

import (
	"context"
	"fmt"
	"math"

	"manteion-go/internal/model"
	"manteion-go/internal/promql"
)

// InputProvider fetches the value of one policy input. The value is bound to
// input.inputs.<in.Name> in the Rego input document and must marshal to JSON.
type InputProvider interface {
	Fetch(ctx context.Context, in model.PolicyInput) (any, error)
}

// Providers maps a PolicyInput.Source (model.InputSourcePrometheus,
// model.InputSourceLoki) to the provider that answers it.
type Providers map[string]InputProvider

// VectorQuerier runs an instant metric query and returns its vector. It is
// satisfied by *promql.Client and *logql.Client.
type VectorQuerier interface {
	QueryVector(ctx context.Context, query string) ([]promql.Sample, error)
}

// VectorProvider answers a PolicyInput by running its Query through Querier.
//
// Kind "scalar" (the default an empty Kind gets from model validation): no
// series yields nil (JSON null, so a policy comparing it is undefined), one
// series yields its value as a float64, and several series are an error: the
// query must aggregate to one series or declare kind "vector".
//
// Kind "vector" yields every sample as []InputSample (JSON
// [{"metric": {...}, "value": v}, ...]); no series yields an empty, non-nil
// slice (JSON []).
//
// A non-finite value (NaN, +Inf or -Inf: Prometheus answers an error ratio
// with no traffic as NaN) is JSON null: a vector sample's "value" is null, a
// scalar whose single sample is non-finite is null. JSON has no encoding for
// them, so the input document, the input digest and the stored decision all
// see this same representation, and nothing errors; a policy guards with
// s.value != null.
//
// A querier error is returned wrapped, so errors.Is sees it.
type VectorProvider struct {
	Querier VectorQuerier
}

var _ InputProvider = VectorProvider{}

// Fetch implements InputProvider.
func (p VectorProvider) Fetch(ctx context.Context, in model.PolicyInput) (any, error) {
	samples, err := p.Querier.QueryVector(ctx, in.Query)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	switch in.Kind {
	case model.InputKindScalar, "":
		switch n := len(samples); n {
		case 0:
			return nil, nil
		case 1:
			if v := samples[0].Value; finite(v) {
				return v, nil
			}
			return nil, nil
		default:
			return nil, fmt.Errorf("scalar input %q returned %d series; aggregate the query or use kind \"vector\"", in.Name, n)
		}
	case model.InputKindVector:
		out := make([]InputSample, 0, len(samples))
		for _, s := range samples {
			is := InputSample{Labels: s.Labels}
			if finite(s.Value) {
				v := s.Value
				is.Value = &v
			}
			out = append(out, is)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown kind %q", in.Kind)
	}
}

// InputSample is one sample of a vector input as the policy sees it: Value
// is nil (JSON null) when the sample is NaN or infinite.
type InputSample struct {
	Labels map[string]string `json:"metric"`
	Value  *float64          `json:"value"`
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
