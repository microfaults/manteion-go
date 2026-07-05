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

// FilterLive keeps every instance that may still be serving traffic. The
// computed liveness vocabulary is alive|stale|dead (store/sdk_repo.go):
// "stale" is a slow-polling but LIVE pod -- it must be frozen, receive
// rule pushes, and it still owes drain records; excluding it leaks live
// calls during isolation and shrinks the drain gate's expected set
// (false-clean). Only "dead" (beyond 5x poll interval) is excluded.
// The previous filter matched "suspect", a status nothing ever computes,
// so stale instances silently dropped out of every fanout.
func FilterLive(i *model.SDKInstance) bool {
	return i.Status == "" || i.Status == "alive" || i.Status == "stale"
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
