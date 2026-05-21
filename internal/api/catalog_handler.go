package api

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"manteion-go/internal/model"
)

// =========================================================================
// Workflow-builder catalog
//
// GET /api/v1/catalog/endpoints aggregates HTTP routes published by live
// atropos-go SDK instances and returns them as flat endpoint entries the
// UI uses for the workflow-builder picker.
//
// Liveness: SDKs are filtered to those that polled within the last 2
// minutes (handler-side, via SDKRepo.ListLive). Stale rows aren't
// surfaced; an SDK that crashes drops out of the catalog after one miss.
//
// Caching: handler-side 30s cache keeps the picker snappy when the user
// opens the workflow dialog repeatedly.
//
// No curated fallback: the pre-merge flows-catalog-personas-apis branch
// carried a static slice of demo endpoints as a fallback when no SDKs
// published routes. That coupled the catalog to a demo-services list;
// the new design returns `{"data": [], ...}` with a `hint` so the UI can
// show an "SDKs not publishing routes" empty state.
// =========================================================================

// catalogFreshness is how stale an SDK can be before its routes are
// hidden from the catalog response.
const catalogFreshness = 2 * time.Minute

// catalogCacheTTL is how long the handler caches a built response.
const catalogCacheTTL = 30 * time.Second

// CatalogEndpoint is the flat picker entry. ID is "{service}__{slug}",
// where slug is a path-safe transform of "METHOD /path" — stable across
// pod restarts and unique within (service, method, path).
type CatalogEndpoint struct {
	ID          string   `json:"id"`
	Service     string   `json:"service"`
	Method      string   `json:"method"`
	Path        string   `json:"path"`
	Description string   `json:"description,omitempty"`
	DependsOn   []string `json:"depends_on,omitempty"`
}

type catalogResponse struct {
	Data []CatalogEndpoint `json:"data"`
	Hint string            `json:"hint,omitempty"`
}

// catalogCache holds the most recently built response. Refreshed lazily
// when an incoming request finds the cached payload stale.
type catalogCache struct {
	mu        sync.Mutex
	expiresAt time.Time
	payload   catalogResponse
}

var serverCatalogCache catalogCache

func (s *Server) handleListCatalogEndpoints(w http.ResponseWriter, r *http.Request) {
	resp, err := s.catalogResponseCached(r)
	if err != nil {
		s.logger.Error("catalog handler failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load catalog")
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) catalogResponseCached(r *http.Request) (catalogResponse, error) {
	serverCatalogCache.mu.Lock()
	defer serverCatalogCache.mu.Unlock()

	if time.Now().Before(serverCatalogCache.expiresAt) {
		return serverCatalogCache.payload, nil
	}

	instances, err := s.sdk.ListLive(r.Context(), catalogFreshness)
	if err != nil {
		return catalogResponse{}, fmt.Errorf("list live sdk instances: %w", err)
	}

	endpoints := endpointsFromInstances(instances)
	resp := catalogResponse{Data: endpoints}
	if len(endpoints) == 0 {
		if len(instances) == 0 {
			resp.Hint = "No SDK instances have polled in the last 2 minutes — start an atropos-go SDK to populate this list."
		} else {
			resp.Hint = "SDK instances are alive but none publish routes — call atropos sdk.RegisterRoutes() at startup."
		}
	}

	serverCatalogCache.payload = resp
	serverCatalogCache.expiresAt = time.Now().Add(catalogCacheTTL)
	return resp, nil
}

// endpointsFromInstances merges per-instance route lists into a single
// deduplicated catalog. Each (service, method, path) appears once even
// when multiple pods of the same service report it.
//
// Cross-service dependencies in `depends_on` are passed through;
// intra-service references are normalized to "METHOD /path" form.
// Unresolvable references (target route absent from the assembled
// catalog) are dropped at the consumer; the catalog itself does not
// validate.
func endpointsFromInstances(instances []*model.SDKInstance) []CatalogEndpoint {
	type key struct {
		service, method, path string
	}
	merged := make(map[key]CatalogEndpoint)

	for _, inst := range instances {
		for _, route := range inst.Routes {
			k := key{
				service: inst.Service,
				method:  strings.ToUpper(strings.TrimSpace(route.Method)),
				path:    strings.TrimSpace(route.Path),
			}
			if k.method == "" || k.path == "" {
				continue
			}
			ep, exists := merged[k]
			if !exists {
				ep = CatalogEndpoint{
					ID:          endpointID(k.service, k.method, k.path),
					Service:     k.service,
					Method:      k.method,
					Path:        k.path,
					Description: route.Description,
				}
			}
			// Merge depends_on across instances; deduplicate at end.
			for _, dep := range route.DependsOn {
				ep.DependsOn = appendUnique(ep.DependsOn, resolveDepID(dep, k.service))
			}
			merged[k] = ep
		}
	}

	out := make([]CatalogEndpoint, 0, len(merged))
	for _, ep := range merged {
		out = append(out, ep)
	}
	sort.Slice(out, func(i, j int) bool { return lessEndpoint(out[i], out[j]) })
	return out
}

// resolveDepID accepts the two `depends_on` syntaxes published by SDKs:
//
//	"METHOD /path"          → same service (currentService used as prefix)
//	"service METHOD /path"  → fully qualified
//
// Returns the canonical catalog id ("{service}__{slug}"). Caller passes
// the current service so same-service deps are resolved correctly.
func resolveDepID(raw, currentService string) string {
	raw = strings.TrimSpace(raw)
	parts := strings.SplitN(raw, " ", 3)
	switch len(parts) {
	case 2:
		// "METHOD /path"
		return endpointID(currentService, strings.ToUpper(parts[0]), parts[1])
	case 3:
		// "service METHOD /path"
		return endpointID(parts[0], strings.ToUpper(parts[1]), parts[2])
	default:
		return raw
	}
}

func endpointID(service, method, path string) string {
	return service + "__" + pathSlug(method, path)
}

// pathSlug renders a METHOD + path pair into a deterministic slug
// (lowercase letters/numbers, "_" for unsafe chars). Stable across
// restarts so the UI can store the slug in URL state.
func pathSlug(method, path string) string {
	combined := strings.ToLower(method + path)
	var b strings.Builder
	b.Grow(len(combined))
	for _, r := range combined {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

func appendUnique(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}

func lessEndpoint(a, b CatalogEndpoint) bool {
	if a.Service != b.Service {
		return a.Service < b.Service
	}
	if a.Path != b.Path {
		return a.Path < b.Path
	}
	return a.Method < b.Method
}
