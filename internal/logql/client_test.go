package logql

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func fakeLoki(t *testing.T, status int, body string) (*Client, *http.Request) {
	t.Helper()
	var got http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = *r
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return NewClient(srv.URL + "/"), &got
}

func TestQueryVector(t *testing.T) {
	c, got := fakeLoki(t, http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"42"]}]}}`)
	q := `sum(count_over_time({service_name="checkout"} |= "atropos.decision" [1m]))`
	samples, err := c.QueryVector(context.Background(), q)
	if err != nil {
		t.Fatalf("QueryVector: %v", err)
	}
	if got.URL.Path != "/loki/api/v1/query" || got.URL.Query().Get("query") != q {
		t.Errorf("request = %s %s, want GET /loki/api/v1/query with the query param", got.URL.Path, got.URL.RawQuery)
	}
	if len(samples) != 1 || samples[0].Value != 42 {
		t.Errorf("samples = %+v, want one sample 42", samples)
	}
}

func TestQueryVector_StreamsRejected(t *testing.T) {
	c, _ := fakeLoki(t, http.StatusOK, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
	if _, err := c.QueryVector(context.Background(), `{service_name="checkout"}`); err == nil || !strings.Contains(err.Error(), `"streams"`) {
		t.Fatalf("err = %v, want the unsupported resultType error", err)
	}
}

func TestQueryVector_HTTPError(t *testing.T) {
	c, _ := fakeLoki(t, http.StatusBadRequest, "parse error at line 1")
	if _, err := c.QueryVector(context.Background(), "nope"); err == nil || !strings.Contains(err.Error(), "status 400") {
		t.Fatalf("err = %v, want 'loki returned status 400'", err)
	}
}
