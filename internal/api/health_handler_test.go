package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeDB struct{ err error }

func (f *fakeDB) PingContext(_ context.Context) error { return f.err }

type fakeRuleVersioner struct{ err error }

func (f *fakeRuleVersioner) Version(_ context.Context) (uint64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return 1, nil
}

func TestHandleInit_AllGatesPass(t *testing.T) {
	s := &Server{
		logger:  slog.New(slog.DiscardHandler),
		dbPing:  &fakeDB{},
		rulever: &fakeRuleVersioner{},
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sdk/init", nil)
	w := httptest.NewRecorder()
	s.handleInit(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %s", w.Code, w.Body.String())
	}
}

func TestHandleInit_DBFails(t *testing.T) {
	s := &Server{
		logger:  slog.New(slog.DiscardHandler),
		dbPing:  &fakeDB{err: errors.New("connection refused")},
		rulever: &fakeRuleVersioner{},
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sdk/init", nil)
	w := httptest.NewRecorder()
	s.handleInit(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body = %s", w.Code, w.Body.String())
	}
}

func TestHandleInit_RuleStoreUninit(t *testing.T) {
	s := &Server{
		logger:  slog.New(slog.DiscardHandler),
		dbPing:  &fakeDB{},
		rulever: &fakeRuleVersioner{err: errors.New("no rows")},
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sdk/init", nil)
	w := httptest.NewRecorder()
	s.handleInit(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body = %s", w.Code, w.Body.String())
	}
}
