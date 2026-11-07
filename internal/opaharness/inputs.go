package opaharness

import (
	"context"
	"fmt"

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
// Kind "vector" yields every sample as []promql.Sample (JSON
// [{"metric": {...}, "value": v}, ...]); no series yields an empty, non-nil
// slice (JSON []).
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
			return samples[0].Value, nil
		default:
			return nil, fmt.Errorf("scalar input %q returned %d series; aggregate the query or use kind \"vector\"", in.Name, n)
		}
	case model.InputKindVector:
		if samples == nil {
			samples = []promql.Sample{}
		}
		return samples, nil
	default:
		return nil, fmt.Errorf("input %q: unknown kind %q", in.Name, in.Kind)
	}
}
