package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"manteion-go/internal/desired"
	"manteion-go/internal/model"
	"manteion-go/internal/store"
)

// fakeFaultConfigs is an in-memory fault_configs table behind the delete
// handler's seam. getErr and deleteErr inject failures; deleted records the ids
// whose delete succeeded.
type fakeFaultConfigs struct {
	byID      map[string]*model.FaultConfig
	getErr    error
	deleteErr error
	deleted   []string
}

func (f *fakeFaultConfigs) Get(_ context.Context, id string) (*model.FaultConfig, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	if cfg, ok := f.byID[id]; ok {
		cp := *cfg
		return &cp, nil
	}
	return nil, store.ErrNotFound
}

func (f *fakeFaultConfigs) Delete(_ context.Context, id string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	if _, ok := f.byID[id]; !ok {
		return store.ErrNotFound
	}
	delete(f.byID, id)
	f.deleted = append(f.deleted, id)
	return nil
}

// TestHandleDeleteFaultConfig_BumpsTheConfigsServiceGeneration pins that a
// delete that lands is an input change to its service's desired state (an
// active config drops out of the document): it bumps that service's generation
// exactly once and announces it, whatever the config's status. A delete that
// does not happen bumps nothing, and a failed bump does not fail the delete.
func TestHandleDeleteFaultConfig_BumpsTheConfigsServiceGeneration(t *testing.T) {
	has := func(status model.FaultConfigStatus) map[string]*model.FaultConfig {
		return map[string]*model.FaultConfig{"fc-1": {ID: "fc-1", Name: "latency", Service: "cart", Status: status}}
	}
	tests := []struct {
		name       string
		fake       *fakeFaultConfigs
		failBump   bool // the generation store is down
		wantStatus int
		wantBumped bool
		wantGone   bool
	}{
		{name: "active config", fake: &fakeFaultConfigs{byID: has(model.FaultConfigActive)},
			wantStatus: http.StatusNoContent, wantBumped: true, wantGone: true},
		{name: "ready config", fake: &fakeFaultConfigs{byID: has(model.FaultConfigReady)},
			wantStatus: http.StatusNoContent, wantBumped: true, wantGone: true},
		{name: "completed config", fake: &fakeFaultConfigs{byID: has(model.FaultConfigCompleted)},
			wantStatus: http.StatusNoContent, wantBumped: true, wantGone: true},
		{name: "unknown config", fake: &fakeFaultConfigs{},
			wantStatus: http.StatusNotFound},
		{name: "read fails", fake: &fakeFaultConfigs{byID: has(model.FaultConfigActive), getErr: errors.New("db down")},
			wantStatus: http.StatusInternalServerError},
		{name: "delete fails", fake: &fakeFaultConfigs{byID: has(model.FaultConfigActive), deleteErr: errors.New("db down")},
			wantStatus: http.StatusInternalServerError},
		{name: "deleted by someone else after the read", fake: &fakeFaultConfigs{byID: has(model.FaultConfigActive), deleteErr: store.ErrNotFound},
			wantStatus: http.StatusNotFound},
		{name: "bump fails", fake: &fakeFaultConfigs{byID: has(model.FaultConfigActive)}, failBump: true,
			wantStatus: http.StatusNoContent, wantGone: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			broker := desired.NewBroker()
			var genStore desired.Store = desired.NewMemoryStore()
			if tc.failBump {
				genStore = failingGenerationStore{}
			}
			gens := desired.NewGenerations(genStore, broker)
			cart, checkout := broker.Subscribe("cart"), broker.Subscribe("checkout")
			defer broker.Unsubscribe("cart", cart)
			defer broker.Unsubscribe("checkout", checkout)

			s := &Server{configDeleter: tc.fake, generations: gens, logger: discardLogger()}
			mux := http.NewServeMux()
			s.routes(mux)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest(http.MethodDelete, "/api/v1/faults/configs/fc-1", nil))

			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if gone := slices.Equal(tc.fake.deleted, []string{"fc-1"}); gone != tc.wantGone {
				t.Errorf("deleted = %q, want fc-1 deleted: %v", tc.fake.deleted, tc.wantGone)
			}
			var wantEvents []string
			if tc.wantBumped {
				wantEvents = []string{`rules_changed {"generation":2}`} // absent = 1, one bump = 2
			}
			if got := drainEvents(cart); !slices.Equal(got, wantEvents) {
				t.Errorf("cart events = %q, want %q", got, wantEvents)
			}
			if got := drainEvents(checkout); len(got) != 0 {
				t.Errorf("checkout was announced: %q", got)
			}
			if tc.failBump {
				return // the store that would report the generation is the one that is down
			}
			wantGen := uint64(1)
			if tc.wantBumped {
				wantGen = 2
			}
			if got, _ := gens.Get(context.Background(), "cart"); got != wantGen {
				t.Errorf("generation(cart) = %d, want %d", got, wantGen)
			}
			if got, _ := gens.Get(context.Background(), "checkout"); got != 1 {
				t.Errorf("generation(checkout) = %d, want 1", got)
			}
		})
	}
}
