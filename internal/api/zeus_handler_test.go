package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"manteion-go/internal/zeus"
)

// Pins the proxy contract the UI's filtered zeus reads depend on: the query
// string manteion receives must reach zeus verbatim. zeus filters its list
// endpoints by it (GET /runs?experiment_id=&status=), so a proxy that
// forwards only r.URL.Path silently turns every filtered read into "all
// runs". The stub records what zeus actually saw; the table also checks that
// status, headers, and body still pass straight through.
func TestZeusProxy_ForwardsQueryString(t *testing.T) {
	type seen struct {
		method, path, rawQuery string
	}
	var got seen
	stubStatus := http.StatusOK

	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = seen{method: r.Method, path: r.URL.Path, rawQuery: r.URL.RawQuery}
		w.Header().Set("X-Zeus-Stub", "1")
		w.WriteHeader(stubStatus)
		io.WriteString(w, `{"runs":[]}`)
	}))
	defer stub.Close()

	s := &Server{logger: discardLogger(), zeus: zeus.NewClient(stub.URL)}

	cases := []struct {
		name       string
		method     string
		target     string // as received by manteion
		stubStatus int    // what zeus answers; must pass through unchanged
		wantPath   string // path zeus sees (client prepends /api/v1)
		wantQuery  string // RawQuery zeus sees, byte-for-byte
	}{
		{
			name:       "runs filtered by experiment and status",
			method:     http.MethodGet,
			target:     "/api/v1/zeus/runs?experiment_id=exp-123&status=running",
			stubStatus: http.StatusOK,
			wantPath:   "/api/v1/runs",
			wantQuery:  "experiment_id=exp-123&status=running",
		},
		{
			name:       "no query string adds no stray '?'",
			method:     http.MethodGet,
			target:     "/api/v1/zeus/runs",
			stubStatus: http.StatusOK,
			wantPath:   "/api/v1/runs",
			wantQuery:  "",
		},
		{
			name:       "percent-encoding is forwarded, not decoded or re-encoded",
			method:     http.MethodGet,
			target:     "/api/v1/zeus/workflows?name=checkout%20p99&tag=a%2Bb",
			stubStatus: http.StatusOK,
			wantPath:   "/api/v1/workflows",
			wantQuery:  "name=checkout%20p99&tag=a%2Bb",
		},
		{
			name:       "nested resource keeps its query and zeus status",
			method:     http.MethodGet,
			target:     "/api/v1/zeus/datasets/ds-1/sample?n=10",
			stubStatus: http.StatusNotFound,
			wantPath:   "/api/v1/datasets/ds-1/sample",
			wantQuery:  "n=10",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got = seen{}
			stubStatus = tc.stubStatus

			req := httptest.NewRequest(tc.method, tc.target, nil)
			w := httptest.NewRecorder()
			s.zeusProxy(w, req)

			if got.method != tc.method {
				t.Errorf("zeus saw method %q, want %q", got.method, tc.method)
			}
			if got.path != tc.wantPath {
				t.Errorf("zeus saw path %q, want %q", got.path, tc.wantPath)
			}
			if got.rawQuery != tc.wantQuery {
				t.Errorf("zeus saw query %q, want %q", got.rawQuery, tc.wantQuery)
			}

			// Everything else must still be a verbatim passthrough.
			if w.Code != tc.stubStatus {
				t.Errorf("proxied status = %d, want %d (body %q)", w.Code, tc.stubStatus, w.Body.String())
			}
			if h := w.Header().Get("X-Zeus-Stub"); h != "1" {
				t.Errorf("X-Zeus-Stub header = %q, want %q (headers not copied)", h, "1")
			}
			if body := w.Body.String(); body != `{"runs":[]}` {
				t.Errorf("proxied body = %q, want %q", body, `{"runs":[]}`)
			}
		})
	}
}
