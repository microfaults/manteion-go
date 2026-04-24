package api

import (
	"context"

	"manteion-go/internal/model"
)

// FaultStore is the subset of FaultRepo used by the fault handler.
// Defined as an interface so tests can inject a fake without a live postgres.
type FaultStore interface {
	CreateSpec(ctx context.Context, spec *model.FaultSpec) error
	GetSpec(ctx context.Context, id string) (*model.FaultSpec, error)
	ListSpecs(ctx context.Context) ([]*model.FaultSpec, error)
	DeleteSpec(ctx context.Context, id string) error

	CreateComposition(ctx context.Context, c *model.FaultComposition) error
	GetComposition(ctx context.Context, id string) (*model.FaultComposition, error)
	ListCompositions(ctx context.Context) ([]*model.FaultComposition, error)
	DeleteComposition(ctx context.Context, id string) error

	SpecResolver(ctx context.Context) model.FaultSpecResolver
	CompositionResolver(ctx context.Context) model.CompositionResolver
}
