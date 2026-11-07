package promql

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestDecodeVector(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		want    []Sample
		wantErr string
	}{
		{"vector", `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"service":"checkout"},"value":[1700000000,"1.5"]},{"metric":{"service":"cart"},"value":[1700000000.5,"2"]}]}}`,
			[]Sample{{Labels: map[string]string{"service": "checkout"}, Value: 1.5}, {Labels: map[string]string{"service": "cart"}, Value: 2}}, ""},
		{"empty vector", `{"status":"success","data":{"resultType":"vector","result":[]}}`, []Sample{}, ""},
		{"scalar", `{"status":"success","data":{"resultType":"scalar","result":[1700000000,"3"]}}`, []Sample{{Labels: map[string]string{}, Value: 3}}, ""},
		{"streams", `{"status":"success","data":{"resultType":"streams","result":[]}}`, nil, `resultType "streams"`},
		{"error status", `{"status":"error","errorType":"bad_data","error":"parse error at 1"}`, nil, "bad_data"},
		{"not json", `<html>`, nil, "decode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeVector(strings.NewReader(tt.body))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeVector: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("samples = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func fakeProm(t *testing.T, body string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" || r.URL.Query().Get("query") == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return NewClient(srv.URL)
}

func TestQueryVector(t *testing.T) {
	c := fakeProm(t, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"le":"+Inf"},"value":[1700000000,"42"]}]}}`)
	got, err := c.QueryVector(context.Background(), "up")
	if err != nil || len(got) != 1 || got[0].Value != 42 || got[0].Labels["le"] != "+Inf" {
		t.Fatalf("QueryVector = %+v, %v; want one sample 42 with le=+Inf", got, err)
	}
}

func TestQueryInstant_KeepsNoDataError(t *testing.T) {
	c := fakeProm(t, `{"status":"success","data":{"resultType":"vector","result":[]}}`)
	if _, err := c.QueryInstant(context.Background(), "up"); err == nil || !strings.Contains(err.Error(), "returned no data") {
		t.Fatalf("err = %v, want the historical 'returned no data' error", err)
	}
	c = fakeProm(t, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"600"]}]}}`)
	if v, err := c.QueryInstant(context.Background(), "up"); err != nil || v != 600 {
		t.Fatalf("QueryInstant = %v, %v; want 600", v, err)
	}
}
