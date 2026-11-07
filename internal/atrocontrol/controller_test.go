package atrocontrol

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"manteion-go/internal/atropos"
	"manteion-go/internal/atropostest"
	"manteion-go/internal/model"
)

// fakeResolver returns canned instance lists keyed by service name.
type fakeResolver struct {
	services map[string][]*model.SDKInstance
}

func (f *fakeResolver) ForService(_ context.Context, service string) ([]*model.SDKInstance, error) {
	insts, ok := f.services[service]
	if !ok {
		return nil, nil
	}
	return insts, nil
}

func (f *fakeResolver) ForInstance(_ context.Context, id string) (*model.SDKInstance, error) {
	for _, insts := range f.services {
		for _, inst := range insts {
			if inst.ID == id {
				return inst, nil
			}
		}
	}
	return nil, fmt.Errorf("instance %q not found", id)
}

// setupAtroposServers starts N independent atropos admin servers (real SDK
// control surface via atropos.Serve, offline), returning their URLs.
// Cleanup is registered on t.
func setupAtroposServers(t *testing.T, n int) []string {
	t.Helper()
	var urls []string
	for range n {
		urls = append(urls, atropostest.NewServer(t).URL)
	}
	return urls
}

func newTestController(t *testing.T, urls []string, opts ...ControllerOption) *Controller {
	t.Helper()
	instances := make([]*model.SDKInstance, len(urls))
	for i, u := range urls {
		instances[i] = &model.SDKInstance{
			ID:      fmt.Sprintf("pod-%d", i),
			Service: "productcatalog",
			Address: u,
		}
	}

	resolver := &fakeResolver{
		services: map[string][]*model.SDKInstance{
			"productcatalog": instances,
		},
	}

	tx := atropos.NewClient()
	return New(tx, resolver, append([]ControllerOption{
		WithDefaultTimeout(5 * time.Second),
		WithDefaultConcurrency(4),
	}, opts...)...)
}

func TestNoInstancesError(t *testing.T) {
	resolver := &fakeResolver{services: map[string][]*model.SDKInstance{}}
	tx := atropos.NewClient()
	ctrl := New(tx, resolver)
	ctx := context.Background()

	if _, err := ctrl.StatusByService(ctx, "nonexistent"); err == nil || !strings.Contains(err.Error(), `no instances found for service "nonexistent"`) {
		t.Fatalf("StatusByService(nonexistent) = %v, want the no-instances error", err)
	}
}

func TestStatusByService(t *testing.T) {
	urls := setupAtroposServers(t, 2)
	ctrl := newTestController(t, urls)
	ctx := context.Background()

	status, err := ctrl.StatusByService(ctx, "productcatalog")
	if err != nil {
		t.Fatalf("StatusByService: %v", err)
	}
	if len(status.Instances) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(status.Instances))
	}
	for _, inst := range status.Instances {
		if inst.Err != nil {
			t.Fatalf("instance %s had error: %v", inst.InstanceID, inst.Err)
		}
		if inst.Fault == nil {
			t.Fatalf("instance %s: expected fault status", inst.InstanceID)
		}
	}
}
