package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	defaultListenAddr  = ":8080"
	defaultUpstreamURL = "https://maas-coding-api.cn-huabei-1.xf-yun.com/anthropic"
	defaultMaxRetries  = 1
	defaultBackoff     = 500 * time.Millisecond
)

var hopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

type config struct {
	ListenAddr     string
	UpstreamURL    *url.URL
	MaxRetries     int
	RetryBackoff   time.Duration
	RequestTimeout time.Duration
}

type proxyServer struct {
	cfg     config
	client  *http.Client
	logger  *slog.Logger
	counter atomic.Uint64
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := loadConfig()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	proxy := newProxyServer(cfg, logger)
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           proxy,
		ReadHeaderTimeout: 30 * time.Second,
	}

	logger.Info("xunfei retry proxy starting",
		"listen_addr", cfg.ListenAddr,
		"upstream_url", cfg.UpstreamURL.String(),
		"max_retries", cfg.MaxRetries,
		"retry_backoff", cfg.RetryBackoff.String(),
		"request_timeout", durationForLog(cfg.RequestTimeout),
	)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func loadConfig() (config, error) {
	upstreamRaw := getenv("UPSTREAM_URL", defaultUpstreamURL)
	upstream, err := url.Parse(upstreamRaw)
	if err != nil {
		return config{}, fmt.Errorf("parse UPSTREAM_URL: %w", err)
	}
	if upstream.Scheme == "" || upstream.Host == "" {
		return config{}, fmt.Errorf("UPSTREAM_URL must include scheme and host")
	}

	maxRetries, err := parseNonNegativeInt("MAX_RETRIES", defaultMaxRetries)
	if err != nil {
		return config{}, err
	}

	backoff, err := parseDurationEnv("RETRY_BACKOFF", defaultBackoff)
	if err != nil {
		return config{}, err
	}

	timeout, err := parseDurationEnv("REQUEST_TIMEOUT", 0)
	if err != nil {
		return config{}, err
	}

	return config{
		ListenAddr:     getenv("LISTEN_ADDR", defaultListenAddr),
		UpstreamURL:    upstream,
		MaxRetries:     maxRetries,
		RetryBackoff:   backoff,
		RequestTimeout: timeout,
	}, nil
}

func newProxyServer(cfg config, logger *slog.Logger) *proxyServer {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	return &proxyServer{
		cfg: cfg,
		client: &http.Client{
			Transport: transport,
			Timeout:   cfg.RequestTimeout,
		},
		logger: logger,
	}
}

func (p *proxyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}

	start := time.Now()
	requestID := p.requestID(r)
	targetURL := buildTargetURL(p.cfg.UpstreamURL, r)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		p.logger.Warn("failed to read request body", "request_id", requestID, "error", err)
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	p.logger.Info("proxy request started",
		"request_id", requestID,
		"method", r.Method,
		"path", r.URL.RequestURI(),
		"target", targetURL.Redacted(),
		"remote_addr", clientIP(r),
	)

	var finalResp *http.Response
	var lastRetryableBody []byte
	retryableRetries := 0

	for attempt := 0; attempt <= p.cfg.MaxRetries; attempt++ {
		resp, err := p.doAttempt(r, targetURL, body, requestID, attempt)
		if err != nil {
			p.logger.Warn("upstream request failed",
				"request_id", requestID,
				"attempt", attempt+1,
				"error", err,
			)
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}

		if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusBadRequest {
			respBody, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr != nil {
				p.logger.Warn("failed to read upstream retryable response candidate body",
					"request_id", requestID,
					"attempt", attempt+1,
					"error", readErr,
				)
				http.Error(w, "bad gateway", http.StatusBadGateway)
				return
			}

			if retryable, retryReason := retryableResponseReason(resp.StatusCode, respBody); retryable {
				lastRetryableBody = respBody
				if attempt < p.cfg.MaxRetries {
					retryableRetries++
					p.logger.Warn("upstream retryable response, retrying",
						"request_id", requestID,
						"attempt", attempt+1,
						"next_attempt", attempt+2,
						"max_retries", p.cfg.MaxRetries,
						"status", resp.StatusCode,
						"retry_reason", retryReason,
					)

					if !sleepWithContext(r.Context(), p.cfg.RetryBackoff) {
						p.logger.Warn("request canceled before retry", "request_id", requestID)
						return
					}
					continue
				}

				writeBufferedResponse(w, resp, respBody)
				p.logger.Info("proxy request completed",
					"request_id", requestID,
					"status", resp.StatusCode,
					"attempts", attempt+1,
					"retryable_retries", retryableRetries,
					"retry_reason", retryReason,
					"duration_ms", time.Since(start).Milliseconds(),
				)
				return
			}

			writeBufferedResponse(w, resp, respBody)
			p.logger.Info("proxy request completed",
				"request_id", requestID,
				"status", resp.StatusCode,
				"attempts", attempt+1,
				"retryable_retries", retryableRetries,
				"retryable_match", false,
				"duration_ms", time.Since(start).Milliseconds(),
			)
			return
		}

		finalResp = resp
		break
	}

	if finalResp == nil {
		p.logger.Warn("retryable response loop ended without response",
			"request_id", requestID,
			"retryable_retries", retryableRetries,
		)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write(lastRetryableBody)
		return
	}
	defer finalResp.Body.Close()

	copyHeaders(w.Header(), finalResp.Header)
	removeHopHeaders(w.Header())
	w.WriteHeader(finalResp.StatusCode)
	bytesWritten, copyErr := copyResponseBody(w, finalResp.Body)
	if copyErr != nil {
		if isCanceledError(r.Context(), copyErr) {
			p.logger.Info("client canceled response stream",
				"request_id", requestID,
				"status", finalResp.StatusCode,
				"bytes_written", bytesWritten,
				"error", copyErr,
			)
			return
		}

		p.logger.Warn("failed to copy upstream response body",
			"request_id", requestID,
			"status", finalResp.StatusCode,
			"bytes_written", bytesWritten,
			"error", copyErr,
		)
		return
	}

	p.logger.Info("proxy request completed",
		"request_id", requestID,
		"status", finalResp.StatusCode,
		"attempts", retryableRetries+1,
		"retryable_retries", retryableRetries,
		"bytes_written", bytesWritten,
		"duration_ms", time.Since(start).Milliseconds(),
	)
}

func (p *proxyServer) doAttempt(original *http.Request, target *url.URL, body []byte, requestID string, attempt int) (*http.Response, error) {
	req, err := http.NewRequestWithContext(original.Context(), original.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header = original.Header.Clone()
	removeHopHeaders(req.Header)
	req.Header.Set("X-Request-Id", requestID)
	appendForwardedHeaders(req, original)
	req.Host = target.Host
	req.ContentLength = int64(len(body))

	p.logger.Info("upstream attempt",
		"request_id", requestID,
		"attempt", attempt+1,
		"method", req.Method,
		"target", target.Redacted(),
	)

	return p.client.Do(req)
}

func (p *proxyServer) requestID(r *http.Request) string {
	if requestID := strings.TrimSpace(r.Header.Get("X-Request-Id")); requestID != "" {
		return requestID
	}
	if requestID := strings.TrimSpace(r.Header.Get("X-Request-ID")); requestID != "" {
		return requestID
	}
	return fmt.Sprintf("xfrp-%d-%d", time.Now().UnixNano(), p.counter.Add(1))
}

func buildTargetURL(base *url.URL, incoming *http.Request) *url.URL {
	target := *base
	target.Path = joinRequestPath(base.Path, incoming.URL.Path)
	target.RawQuery = joinRawQuery(base.RawQuery, incoming.URL.RawQuery)
	target.Fragment = ""
	return &target
}

func joinRequestPath(basePath string, requestPath string) string {
	if basePath == "" {
		basePath = "/"
	}
	if requestPath == "" || requestPath == "/" {
		return basePath
	}

	baseClean := strings.TrimRight(basePath, "/")
	if baseClean == "" {
		return requestPath
	}
	if requestPath == baseClean || strings.HasPrefix(requestPath, baseClean+"/") {
		return requestPath
	}

	if strings.HasSuffix(baseClean, "/") || strings.HasPrefix(requestPath, "/") {
		return baseClean + requestPath
	}
	return baseClean + "/" + requestPath
}

func joinRawQuery(baseQuery string, requestQuery string) string {
	switch {
	case baseQuery == "":
		return requestQuery
	case requestQuery == "":
		return baseQuery
	default:
		return baseQuery + "&" + requestQuery
	}
}

func retryableResponseReason(statusCode int, body []byte) (bool, string) {
	var payload struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err == nil {
		message := strings.ToLower(payload.Error.Message)
		if statusCode == http.StatusServiceUnavailable && payload.Error.Code == 10310 {
			return true, "busy_code_10310"
		}
		if statusCode == http.StatusServiceUnavailable && strings.Contains(message, "engine timeout") {
			return true, "engine_timeout"
		}
		if statusCode == http.StatusServiceUnavailable && strings.Contains(message, "busy") {
			return true, "busy_message"
		}
		if statusCode == http.StatusServiceUnavailable && strings.Contains(message, "try again later") {
			return true, "try_again_later"
		}
		if statusCode == http.StatusTooManyRequests && strings.Contains(message, "authorization failed") {
			return true, "authorization_failed_429"
		}
		if statusCode == http.StatusServiceUnavailable && strings.Contains(message, "authorization failed") && strings.Contains(message, "429") {
			return true, "authorization_failed_429"
		}
		if statusCode == http.StatusBadRequest && strings.Contains(message, "invalid argument") {
			return true, "invalid_argument_400"
		}
		if statusCode == http.StatusServiceUnavailable && strings.Contains(message, "invalid argument") && strings.Contains(message, "400") {
			return true, "invalid_argument_400"
		}
	}

	text := strings.ToLower(string(body))
	switch {
	case statusCode == http.StatusServiceUnavailable && strings.Contains(text, `"code":10310`):
		return true, "busy_code_10310"
	case statusCode == http.StatusServiceUnavailable && strings.Contains(text, "engine timeout"):
		return true, "engine_timeout"
	case statusCode == http.StatusServiceUnavailable && strings.Contains(text, "system is busy"):
		return true, "busy_message"
	case statusCode == http.StatusServiceUnavailable && strings.Contains(text, "busy") && strings.Contains(text, "try again later"):
		return true, "busy_message"
	case statusCode == http.StatusTooManyRequests && strings.Contains(text, "authorization failed"):
		return true, "authorization_failed_429"
	case statusCode == http.StatusServiceUnavailable && strings.Contains(text, "authorization failed") && strings.Contains(text, "429"):
		return true, "authorization_failed_429"
	case statusCode == http.StatusBadRequest && strings.Contains(text, "invalid argument"):
		return true, "invalid_argument_400"
	case statusCode == http.StatusServiceUnavailable && strings.Contains(text, "invalid argument") && strings.Contains(text, "400"):
		return true, "invalid_argument_400"
	default:
		return false, ""
	}
}

func appendForwardedHeaders(req *http.Request, original *http.Request) {
	host := original.Host
	if host != "" {
		req.Header.Set("X-Forwarded-Host", host)
	}

	proto := "http"
	if original.TLS != nil {
		proto = "https"
	}
	req.Header.Set("X-Forwarded-Proto", proto)

	if ip := clientIP(original); ip != "" {
		prior := req.Header.Get("X-Forwarded-For")
		if prior == "" {
			req.Header.Set("X-Forwarded-For", ip)
			return
		}
		req.Header.Set("X-Forwarded-For", prior+", "+ip)
	}
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func copyHeaders(dst http.Header, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func removeHopHeaders(header http.Header) {
	if connection := header.Get("Connection"); connection != "" {
		for _, item := range strings.Split(connection, ",") {
			if name := strings.TrimSpace(item); name != "" {
				header.Del(name)
			}
		}
	}
	for _, headerName := range hopHeaders {
		header.Del(headerName)
	}
}

func writeBufferedResponse(w http.ResponseWriter, resp *http.Response, body []byte) {
	copyHeaders(w.Header(), resp.Header)
	removeHopHeaders(w.Header())
	w.Header().Del("Content-Length")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}

func copyResponseBody(w http.ResponseWriter, body io.Reader) (int64, error) {
	if flusher, ok := w.(http.Flusher); ok {
		return io.Copy(flushWriter{writer: w, flusher: flusher}, body)
	}
	return io.Copy(w, body)
}

func isCanceledError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	return ctx != nil && errors.Is(ctx.Err(), context.Canceled)
}

type flushWriter struct {
	writer  io.Writer
	flusher http.Flusher
}

func (f flushWriter) Write(p []byte) (int, error) {
	n, err := f.writer.Write(p)
	if n > 0 {
		f.flusher.Flush()
	}
	return n, err
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func getenv(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func parseNonNegativeInt(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("%s must be >= 0", key)
	}
	return value, nil
}

func parseDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	if raw == "0" {
		return 0, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a Go duration like 500ms, 2s, or 1m: %w", key, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("%s must be >= 0", key)
	}
	return value, nil
}

func durationForLog(d time.Duration) string {
	if d == 0 {
		return "disabled"
	}
	return d.String()
}
