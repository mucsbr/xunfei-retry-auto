package main

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryOnBusy503(t *testing.T) {
	var attempts atomic.Int32
	var seenBodies []string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upstream request body: %v", err)
		}
		seenBodies = append(seenBodies, string(body))

		if r.URL.Path != "/anthropic/v1/messages" {
			t.Fatalf("unexpected upstream path: %s", r.URL.Path)
		}
		if r.URL.RawQuery != "model=test" {
			t.Fatalf("unexpected query: %s", r.URL.RawQuery)
		}

		if attempt == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"code":10310,"message":"The system is busy, please try again later.","type":"api_error"},"type":"error"}`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	proxy := newTestProxy(t, upstream.URL+"/anthropic", 1)
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/messages?model=test", bytes.NewBufferString(`{"prompt":"hello"}`))
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if len(seenBodies) != 2 || seenBodies[0] != seenBodies[1] {
		t.Fatalf("request body was not replayed on retry: %#v", seenBodies)
	}
	if rec.Body.String() != `{"ok":true}` {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestRetryOnEngineTimeout503(t *testing.T) {
	var attempts atomic.Int32

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`status_code=503, engine timeout`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	proxy := newTestProxy(t, upstream.URL+"/anthropic", 1)
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/messages", bytes.NewBufferString(`{"prompt":"hello"}`))
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestRetryOnAuthorizationFailed429(t *testing.T) {
	var attempts atomic.Int32

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`status_code=429, authorization failed`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	proxy := newTestProxy(t, upstream.URL+"/anthropic", 1)
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/messages", bytes.NewBufferString(`{"prompt":"hello"}`))
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestRetryOnWrappedAuthorizationFailed429(t *testing.T) {
	var attempts atomic.Int32

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`status_code=429, authorization failed`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	proxy := newTestProxy(t, upstream.URL+"/anthropic", 1)
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/messages", bytes.NewBufferString(`{"prompt":"hello"}`))
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestRetryOnInvalidArgument400(t *testing.T) {
	var attempts atomic.Int32

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`status_code=400, error, status code: 400, status: 400 Bad Request, message: Invalid Argument`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	proxy := newTestProxy(t, upstream.URL+"/anthropic", 1)
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/messages", bytes.NewBufferString(`{"prompt":"hello"}`))
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestRetryOnWrappedInvalidArgument400(t *testing.T) {
	var attempts atomic.Int32

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`status_code=400, error, status code: 400, status: 400 Bad Request, message: Invalid Argument`))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	proxy := newTestProxy(t, upstream.URL+"/anthropic", 1)
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/messages", bytes.NewBufferString(`{"prompt":"hello"}`))
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestDoesNotRetryNonBusy503(t *testing.T) {
	var attempts atomic.Int32

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"code":999,"message":"maintenance"}}`))
	}))
	defer upstream.Close()

	proxy := newTestProxy(t, upstream.URL+"/anthropic", 3)
	req := httptest.NewRequest(http.MethodPost, "http://proxy/anthropic", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestStopsAfterMaxRetries(t *testing.T) {
	var attempts atomic.Int32

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"message":"busy"}}`))
	}))
	defer upstream.Close()

	proxy := newTestProxy(t, upstream.URL+"/anthropic", 1)
	req := httptest.NewRequest(http.MethodPost, "http://proxy/", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if rec.Body.String() != `{"error":{"message":"busy"}}` {
		t.Fatalf("unexpected final body: %s", rec.Body.String())
	}
}

func newTestProxy(t *testing.T, upstreamRaw string, maxRetries int) *proxyServer {
	t.Helper()

	upstream, err := url.Parse(upstreamRaw)
	if err != nil {
		t.Fatalf("parse upstream url: %v", err)
	}

	return newProxyServer(config{
		ListenAddr:     ":0",
		UpstreamURL:    upstream,
		MaxRetries:     maxRetries,
		RetryBackoff:   time.Millisecond,
		RequestTimeout: 0,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}
