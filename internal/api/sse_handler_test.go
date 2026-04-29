package api

import (
	"bufio"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newSSEServer() *Server {
	return &Server{
		logger: slog.New(slog.DiscardHandler),
		broker: NewEventBroker(),
	}
}

func TestSSEHandler_MissingServiceParam(t *testing.T) {
	s := newSSEServer()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sdk/events", nil)
	w := httptest.NewRecorder()
	s.handleSSEEvents(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestSSEHandler_StreamsEvents(t *testing.T) {
	s := newSSEServer()

	// Use a real HTTP server so we get a proper streaming response.
	srv := httptest.NewServer(http.HandlerFunc(s.handleSSEEvents))
	defer srv.Close()

	ctx := t.Context()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"?service=checkout", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	// Give the handler time to subscribe before broadcasting.
	time.Sleep(20 * time.Millisecond)
	s.broker.Broadcast("checkout", Event{Type: "rules_changed", Data: `{"version":5}`})

	lines := make(chan string, 8)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()

	deadline := time.After(2 * time.Second)
	var got []string
	for {
		select {
		case line := <-lines:
			got = append(got, line)
			if len(got) >= 2 {
				goto check
			}
		case <-deadline:
			t.Fatalf("timed out waiting for SSE lines, got: %v", got)
		}
	}
check:
	if !strings.Contains(got[0], "event: rules_changed") {
		t.Errorf("line 0 = %q, want event: rules_changed", got[0])
	}
	if !strings.Contains(got[1], `"version":5`) {
		t.Errorf("line 1 = %q, want version 5", got[1])
	}
}

func TestSSEHandler_ClientDisconnect(t *testing.T) {
	s := newSSEServer()

	srv := httptest.NewServer(http.HandlerFunc(s.handleSSEEvents))
	defer srv.Close()

	ctx, cancel := context.WithCancel(t.Context())

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"?service=checkout", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()

	// Cancel after the body is closed to simulate client disconnect.
	cancel()

	// After disconnect the broker should clean up within a short window.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if s.broker.ClientCount() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("broker still has %d clients after client disconnect", s.broker.ClientCount())
}
