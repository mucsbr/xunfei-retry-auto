package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
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

		if r.URL.Path != "/anthropic/v1/chat/completions" {
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
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions?model=test", bytes.NewBufferString(`{"prompt":"hello"}`))
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
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{"prompt":"hello"}`))
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
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{"prompt":"hello"}`))
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
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{"prompt":"hello"}`))
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
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{"prompt":"hello"}`))
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
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{"prompt":"hello"}`))
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestRetryGroupFanoutCancelsLosersOnFirst200(t *testing.T) {
	var retryStarted atomic.Int32
	var canceled atomic.Int32
	var roundTrips atomic.Int32
	allRetryRequestsStarted := make(chan struct{})
	var closeAllStarted sync.Once

	proxy := newTestProxyWithRetryGroupBase(t, "http://upstream/anthropic", 1, 5)
	proxy.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		roundTrip := roundTrips.Add(1)
		if roundTrip == 1 {
			return newTestHTTPResponse(http.StatusServiceUnavailable, `{"error":{"message":"busy"}}`), nil
		}

		started := retryStarted.Add(1)
		if started == 5 {
			closeAllStarted.Do(func() {
				close(allRetryRequestsStarted)
			})
		}

		if roundTrip == 2 {
			select {
			case <-allRetryRequestsStarted:
			case <-time.After(2 * time.Second):
				return newTestHTTPResponse(http.StatusInternalServerError, "retry fanout did not start all requests"), nil
			}
			return newTestHTTPResponse(http.StatusOK, `{"winner":true}`), nil
		}

		select {
		case <-req.Context().Done():
			canceled.Add(1)
			return nil, req.Context().Err()
		case <-time.After(2 * time.Second):
			return newTestHTTPResponse(http.StatusInternalServerError, "loser request was not canceled"), nil
		}
	})}

	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{"prompt":"hello"}`))
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := roundTrips.Load(); got != 6 {
		t.Fatalf("round trips = %d, want initial 1 + retry group 5", got)
	}
	if rec.Body.String() != `{"winner":true}` {
		t.Fatalf("body = %s", rec.Body.String())
	}
	waitForAtomicInt32(t, &canceled, 4)
}

func TestRetryGroupReturnsTerminalError(t *testing.T) {
	var retryStarted atomic.Int32
	var canceled atomic.Int32
	var roundTrips atomic.Int32
	allRetryRequestsStarted := make(chan struct{})
	var closeAllStarted sync.Once

	proxy := newTestProxyWithRetryGroupBase(t, "http://upstream/anthropic", 1, 5)
	proxy.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		roundTrip := roundTrips.Add(1)
		if roundTrip == 1 {
			return newTestHTTPResponse(http.StatusServiceUnavailable, `{"error":{"message":"busy"}}`), nil
		}

		started := retryStarted.Add(1)
		if started == 5 {
			closeAllStarted.Do(func() {
				close(allRetryRequestsStarted)
			})
		}

		if roundTrip == 2 {
			select {
			case <-allRetryRequestsStarted:
			case <-time.After(2 * time.Second):
				return newTestHTTPResponse(http.StatusInternalServerError, "retry fanout did not start all requests"), nil
			}
			return newTestHTTPResponse(http.StatusUnauthorized, `{"error":"bad key"}`), nil
		}

		select {
		case <-req.Context().Done():
			canceled.Add(1)
			return nil, req.Context().Err()
		case <-time.After(2 * time.Second):
			return newTestHTTPResponse(http.StatusInternalServerError, "loser request was not canceled"), nil
		}
	})}

	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{"prompt":"hello"}`))
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", rec.Code, rec.Body.String())
	}
	if got := roundTrips.Load(); got != 6 {
		t.Fatalf("round trips = %d, want initial 1 + retry group 5", got)
	}
	if rec.Body.String() != `{"error":"bad key"}` {
		t.Fatalf("body = %s", rec.Body.String())
	}
	waitForAtomicInt32(t, &canceled, 4)
}

func TestRetryGroupsIncreaseSizeByRound(t *testing.T) {
	var roundTrips atomic.Int32
	var secondRoundStarted atomic.Int32
	var canceled atomic.Int32
	secondRoundAllStarted := make(chan struct{})
	var closeSecondRoundStarted sync.Once

	proxy := newTestProxyWithRetryGroupBase(t, "http://upstream/anthropic", 2, 2)
	proxy.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		roundTrip := roundTrips.Add(1)
		switch roundTrip {
		case 1, 2, 3:
			return newTestHTTPResponse(http.StatusServiceUnavailable, `{"error":{"message":"busy"}}`), nil
		}

		started := secondRoundStarted.Add(1)
		if started == 3 {
			closeSecondRoundStarted.Do(func() {
				close(secondRoundAllStarted)
			})
		}

		if roundTrip == 4 {
			select {
			case <-secondRoundAllStarted:
			case <-time.After(2 * time.Second):
				return newTestHTTPResponse(http.StatusInternalServerError, "second retry group did not start all requests"), nil
			}
			return newTestHTTPResponse(http.StatusOK, `{"round":2}`), nil
		}

		select {
		case <-req.Context().Done():
			canceled.Add(1)
			return nil, req.Context().Err()
		case <-time.After(2 * time.Second):
			return newTestHTTPResponse(http.StatusInternalServerError, "loser request was not canceled"), nil
		}
	})}

	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{"prompt":"hello"}`))
	rec := httptest.NewRecorder()

	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := roundTrips.Load(); got != 6 {
		t.Fatalf("round trips = %d, want initial 1 + first group 2 + second group 3", got)
	}
	if rec.Body.String() != `{"round":2}` {
		t.Fatalf("body = %s", rec.Body.String())
	}
	waitForAtomicInt32(t, &canceled, 2)
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
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{}`))
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
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{}`))
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

func TestAdminMetricsRecordsSuccessfulRetry(t *testing.T) {
	var attempts atomic.Int32

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"message":"busy"}}`))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	proxy := newTestProxy(t, upstream.URL+"/anthropic", 1)
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	snapshot := getAdminSnapshot(t, proxy)
	if snapshot.Summary.TotalRequests != 1 {
		t.Fatalf("total requests = %d, want 1", snapshot.Summary.TotalRequests)
	}
	if snapshot.Summary.SuccessRequests != 1 {
		t.Fatalf("success requests = %d, want 1", snapshot.Summary.SuccessRequests)
	}
	if snapshot.Summary.RetriedRequests != 1 {
		t.Fatalf("retried requests = %d, want 1", snapshot.Summary.RetriedRequests)
	}
	if snapshot.Summary.RetryAttemptsCompleted != 1 || snapshot.Summary.RetryAttemptsSucceeded != 1 {
		t.Fatalf("retry attempts = %d/%d, want 1/1", snapshot.Summary.RetryAttemptsSucceeded, snapshot.Summary.RetryAttemptsCompleted)
	}
	if len(snapshot.Records) != 1 || snapshot.Records[0].StatusCode != http.StatusOK {
		t.Fatalf("records = %#v, want one 200 record", snapshot.Records)
	}
}

func TestAdminMetricsStoresRawErrorDetail(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream-Error", "raw")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"bad key"}`))
	}))
	defer upstream.Close()

	proxy := newTestProxy(t, upstream.URL+"/anthropic", 1)
	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", rec.Code, rec.Body.String())
	}

	snapshot := getAdminSnapshot(t, proxy)
	if len(snapshot.Records) != 1 {
		t.Fatalf("records len = %d, want 1", len(snapshot.Records))
	}
	errorID := snapshot.Records[0].ErrorID
	if errorID == "" {
		t.Fatalf("missing error id in record: %#v", snapshot.Records[0])
	}

	detail := getAdminErrorDetail(t, proxy, errorID)
	if detail.StatusCode != http.StatusUnauthorized {
		t.Fatalf("error detail status = %d, want 401", detail.StatusCode)
	}
	if got := detail.Headers["X-Upstream-Error"]; len(got) != 1 || got[0] != "raw" {
		t.Fatalf("error detail header = %#v, want raw header", got)
	}
	if detail.Body != `{"error":"bad key"}` {
		t.Fatalf("error detail body = %q", detail.Body)
	}
}

func TestAdminMetricsKeeps200WhenClientCancelHappensAfterBodyBytes(t *testing.T) {
	proxy := newTestProxyWithRetryGroupBase(t, "http://upstream/anthropic", 1, 5)
	proxy.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     http.StatusText(http.StatusOK),
			Header:     make(http.Header),
			Body:       &cancelAfterDataBody{data: []byte(`{"ok":true}`)},
		}, nil
	})}

	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != `{"ok":true}` {
		t.Fatalf("body = %q, want successful payload", rec.Body.String())
	}

	snapshot := getAdminSnapshot(t, proxy)
	if snapshot.Summary.SuccessRequests != 1 {
		t.Fatalf("success requests = %d, want 1", snapshot.Summary.SuccessRequests)
	}
	if len(snapshot.Records) != 1 {
		t.Fatalf("records len = %d, want 1", len(snapshot.Records))
	}
	record := snapshot.Records[0]
	if record.StatusCode != http.StatusOK {
		t.Fatalf("record status = %d, want 200", record.StatusCode)
	}
	if record.ErrorID != "" {
		t.Fatalf("record error id = %q, want empty", record.ErrorID)
	}
}

func TestAdminRoutesNeverProxyToUpstream(t *testing.T) {
	var roundTrips atomic.Int32

	proxy := newTestProxyWithRetryGroupBase(t, "http://upstream/anthropic", 1, 5)
	proxy.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		roundTrips.Add(1)
		return newTestHTTPResponse(http.StatusOK, `{"unexpected":true}`), nil
	})}

	paths := []string{
		"/admin/api/metrics",
		"/admin/api/metrics/",
		"/admin/not-found",
	}
	for _, path := range paths {
		req := httptest.NewRequest(http.MethodGet, "http://proxy"+path, nil)
		rec := httptest.NewRecorder()
		proxy.ServeHTTP(rec, req)

		if path == "/admin/not-found" && rec.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404", path, rec.Code)
		}
		if path != "/admin/not-found" && rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200; body=%s", path, rec.Code, rec.Body.String())
		}
	}

	if got := roundTrips.Load(); got != 0 {
		t.Fatalf("admin routes proxied upstream %d times, want 0", got)
	}
}

func TestOnlyChatCompletionsPathsProxyToUpstream(t *testing.T) {
	var roundTrips atomic.Int32

	proxy := newTestProxyWithRetryGroupBase(t, "http://upstream/anthropic", 1, 5)
	proxy.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		roundTrips.Add(1)
		return newTestHTTPResponse(http.StatusOK, `{"ok":true}`), nil
	})}

	blockedPaths := []string{"/favicon.ico", "/v1/messages", "/"}
	for _, path := range blockedPaths {
		req := httptest.NewRequest(http.MethodPost, "http://proxy"+path, bytes.NewBufferString(`{}`))
		rec := httptest.NewRecorder()
		proxy.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404", path, rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodPost, "http://proxy/v1/chat/completions", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("allowed path status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := roundTrips.Load(); got != 1 {
		t.Fatalf("round trips = %d, want only allowed path to proxy once", got)
	}
}

func newTestProxy(t *testing.T, upstreamRaw string, maxRetries int) *proxyServer {
	return newTestProxyWithRetryGroupBase(t, upstreamRaw, maxRetries, 1)
}

func newTestProxyWithRetryGroupBase(t *testing.T, upstreamRaw string, maxRetries int, retryGroupBase int) *proxyServer {
	t.Helper()

	upstream, err := url.Parse(upstreamRaw)
	if err != nil {
		t.Fatalf("parse upstream url: %v", err)
	}

	return newProxyServer(config{
		ListenAddr:     ":0",
		UpstreamURL:    upstream,
		MaxRetries:     maxRetries,
		RetryGroupBase: retryGroupBase,
		RetryBackoff:   time.Millisecond,
		RequestTimeout: 0,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func waitForAtomicInt32(t *testing.T, value *atomic.Int32, want int32) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if value.Load() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("value = %d, want %d", value.Load(), want)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newTestHTTPResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Status:     http.StatusText(statusCode),
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}

type cancelAfterDataBody struct {
	data []byte
	read bool
}

func (b *cancelAfterDataBody) Read(p []byte) (int, error) {
	if !b.read {
		b.read = true
		return copy(p, b.data), nil
	}
	return 0, context.Canceled
}

func (b *cancelAfterDataBody) Close() error {
	return nil
}

func getAdminSnapshot(t *testing.T, proxy *proxyServer) adminSnapshot {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "http://proxy/admin/api/metrics", nil)
	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin metrics status = %d, body=%s", rec.Code, rec.Body.String())
	}

	var snapshot adminSnapshot
	if err := json.NewDecoder(rec.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode admin metrics: %v", err)
	}
	return snapshot
}

func getAdminErrorDetail(t *testing.T, proxy *proxyServer, errorID string) errorDetail {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "http://proxy/admin/api/errors/"+errorID, nil)
	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin error status = %d, body=%s", rec.Code, rec.Body.String())
	}

	var detail errorDetail
	if err := json.NewDecoder(rec.Body).Decode(&detail); err != nil {
		t.Fatalf("decode admin error: %v", err)
	}
	return detail
}
