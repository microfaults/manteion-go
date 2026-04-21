package atrocontrol

import (
	"context"

	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

type InstanceResolver interface {
	ForService(ctx context.Context, service string) ([]*model.SDKInstance, error)
	ForInstance(ctx context.Context, instanceID string) (*model.SDKInstance, error)
}

type InstanceFilter func(*model.SDKInstance) bool

func FilterAliveOrSuspect(i *model.SDKInstance) bool {
	return i.Status == "" || i.Status == "alive" || i.Status == "suspect"
}

func FilterAll(_ *model.SDKInstance) bool { return true }

// RepoResolver adapts store.SDKRepo to the InstanceResolver interface.
type RepoResolver struct{ Repo *store.SDKRepo }

func (r *RepoResolver) ForService(ctx context.Context, service string) ([]*model.SDKInstance, error) {
	return r.Repo.ForService(ctx, service)
}

func (r *RepoResolver) ForInstance(ctx context.Context, instanceID string) (*model.SDKInstance, error) {
	return r.Repo.Get(ctx, instanceID)
}
