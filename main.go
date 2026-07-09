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
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultListenAddr  = ":8080"
	defaultUpstreamURL = "https://maas-coding-api.cn-huabei-1.xf-yun.com/anthropic"
	defaultMaxRetries  = 1
	defaultRetryGroup  = 5
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
	RetryGroupBase int
	RetryBackoff   time.Duration
	RequestTimeout time.Duration
	MetricsDBPath  string
}

type attemptKind int

const (
	attemptKindSuccess attemptKind = iota
	attemptKindRetryable
	attemptKindTerminal
	attemptKindFailure
)

type attemptMeta struct {
	Attempt    int
	RetryRound int
	GroupIndex int
	GroupSize  int
}

type attemptResult struct {
	kind                   attemptKind
	resp                   *http.Response
	body                   []byte
	buffered               bool
	err                    error
	retryReason            string
	cancel                 context.CancelFunc
	meta                   attemptMeta
	retryAttemptsCompleted int
	retryAttemptsSucceeded int
}

type proxyServer struct {
	cfg     config
	client  *http.Client
	logger  *slog.Logger
	metrics *metricsStore
	counter atomic.Uint64
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := loadConfig()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	proxy, err := newProxyServer(cfg, logger)
	if err != nil {
		logger.Error("failed to initialize proxy", "error", err)
		os.Exit(1)
	}
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           proxy,
		ReadHeaderTimeout: 30 * time.Second,
	}

	logger.Info("xunfei retry proxy starting",
		"listen_addr", cfg.ListenAddr,
		"upstream_url", cfg.UpstreamURL.String(),
		"max_retries", cfg.MaxRetries,
		"retry_group_base", cfg.RetryGroupBase,
		"retry_backoff", cfg.RetryBackoff.String(),
		"request_timeout", durationForLog(cfg.RequestTimeout),
		"metrics_db_path", cfg.MetricsDBPath,
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

	retryGroupBase, err := parsePositiveInt("RETRY_GROUP_BASE", defaultRetryGroup)
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
		RetryGroupBase: retryGroupBase,
		RetryBackoff:   backoff,
		RequestTimeout: timeout,
		MetricsDBPath:  getenv("METRICS_DB_PATH", defaultMetricsDBPath),
	}, nil
}

func newProxyServer(cfg config, logger *slog.Logger) (*proxyServer, error) {
	if cfg.RetryGroupBase <= 0 {
		cfg.RetryGroupBase = defaultRetryGroup
	}

	metrics, err := newMetricsStore(cfg.MetricsDBPath, defaultMaxErrorBody, logger)
	if err != nil {
		return nil, err
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	return &proxyServer{
		cfg: cfg,
		client: &http.Client{
			Transport: transport,
			Timeout:   cfg.RequestTimeout,
		},
		logger:  logger,
		metrics: metrics,
	}, nil
}

func (p *proxyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p.serveAdminHTTP(w, r) {
		return
	}

	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
		return
	}

	if !isAllowedProxyPath(r.URL.Path) {
		http.NotFound(w, r)
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

	upstreamRequestsIssued := 1
	retryRoundsStarted := 0
	retryAttemptsCompleted := 0
	retryAttemptsSucceeded := 0
	result := p.executeStandaloneAttempt(r.Context(), r, targetURL, body, requestID, attemptMeta{Attempt: 1})

	for {
		switch result.kind {
		case attemptKindSuccess, attemptKindTerminal:
			p.writeAttemptResponse(w, r, result, requestID, start, upstreamRequestsIssued, retryRoundsStarted, retryAttemptsCompleted, retryAttemptsSucceeded)
			return
		case attemptKindFailure:
			p.writeAttemptFailure(w, r, result, requestID, start, upstreamRequestsIssued, retryRoundsStarted, retryAttemptsCompleted, retryAttemptsSucceeded)
			return
		case attemptKindRetryable:
			if retryRoundsStarted >= p.cfg.MaxRetries {
				p.writeAttemptResponse(w, r, result, requestID, start, upstreamRequestsIssued, retryRoundsStarted, retryAttemptsCompleted, retryAttemptsSucceeded)
				return
			}

			if retryRoundsStarted > 0 && !sleepWithContext(r.Context(), p.cfg.RetryBackoff) {
				p.logger.Warn("request canceled before retry group", "request_id", requestID)
				return
			}

			retryRoundsStarted++
			groupSize := p.cfg.RetryGroupBase + retryRoundsStarted - 1
			firstAttempt := upstreamRequestsIssued + 1
			upstreamRequestsIssued += groupSize

			p.logger.Warn("starting upstream retry group",
				"request_id", requestID,
				"retry_round", retryRoundsStarted,
				"group_size", groupSize,
				"max_retries", p.cfg.MaxRetries,
				"trigger_status", result.statusCode(),
				"trigger_retry_reason", result.retryReason,
			)

			result = p.runRetryGroup(r.Context(), r, targetURL, body, requestID, retryRoundsStarted, groupSize, firstAttempt)
			retryAttemptsCompleted += result.retryAttemptsCompleted
			retryAttemptsSucceeded += result.retryAttemptsSucceeded
		}
	}
}

func (p *proxyServer) executeStandaloneAttempt(parent context.Context, original *http.Request, target *url.URL, body []byte, requestID string, meta attemptMeta) attemptResult {
	ctx, cancel := context.WithCancel(parent)
	return p.executeAttempt(ctx, cancel, original, target, body, requestID, meta)
}

func (p *proxyServer) runRetryGroup(parent context.Context, original *http.Request, target *url.URL, body []byte, requestID string, retryRound int, groupSize int, firstAttempt int) attemptResult {
	results := make(chan attemptResult)
	settled := make(chan struct{})
	var settleOnce sync.Once
	settle := func() {
		settleOnce.Do(func() {
			close(settled)
		})
	}

	cancels := make([]context.CancelFunc, groupSize)
	for i := 0; i < groupSize; i++ {
		ctx, cancel := context.WithCancel(parent)
		cancels[i] = cancel
		meta := attemptMeta{
			Attempt:    firstAttempt + i,
			RetryRound: retryRound,
			GroupIndex: i + 1,
			GroupSize:  groupSize,
		}

		go func(attemptCtx context.Context, attemptCancel context.CancelFunc, attemptMeta attemptMeta) {
			result := p.executeAttempt(attemptCtx, attemptCancel, original, target, body, requestID, attemptMeta)
			select {
			case results <- result:
			case <-settled:
				closeAttemptResult(result)
			}
		}(ctx, cancel, meta)
	}

	var lastRetryable attemptResult
	var lastFailure attemptResult
	haveRetryable := false
	haveFailure := false
	retryAttemptsCompleted := 0
	retryAttemptsSucceeded := 0

	for completed := 0; completed < groupSize; completed++ {
		result := <-results
		if !isCanceledError(parent, result.err) {
			retryAttemptsCompleted++
			if result.kind == attemptKindSuccess {
				retryAttemptsSucceeded++
			}
		}

		switch result.kind {
		case attemptKindSuccess:
			settle()
			cancelAttemptContexts(cancels, result.meta.GroupIndex-1)
			p.logger.Info("retry group won by upstream 200",
				"request_id", requestID,
				"retry_round", retryRound,
				"group_size", groupSize,
				"group_index", result.meta.GroupIndex,
				"attempt", result.meta.Attempt,
			)
			result.retryAttemptsCompleted = retryAttemptsCompleted
			result.retryAttemptsSucceeded = retryAttemptsSucceeded
			return result
		case attemptKindTerminal:
			settle()
			cancelAttemptContexts(cancels, result.meta.GroupIndex-1)
			p.logger.Info("retry group stopped by terminal upstream response",
				"request_id", requestID,
				"retry_round", retryRound,
				"group_size", groupSize,
				"group_index", result.meta.GroupIndex,
				"attempt", result.meta.Attempt,
				"status", result.statusCode(),
			)
			result.retryAttemptsCompleted = retryAttemptsCompleted
			result.retryAttemptsSucceeded = retryAttemptsSucceeded
			return result
		case attemptKindFailure:
			if isCanceledError(parent, result.err) {
				settle()
				cancelAttemptContexts(cancels, -1)
				result.retryAttemptsCompleted = retryAttemptsCompleted
				result.retryAttemptsSucceeded = retryAttemptsSucceeded
				return result
			}
			lastFailure = result
			haveFailure = true
		case attemptKindRetryable:
			lastRetryable = result
			haveRetryable = true
		}
	}

	settle()
	if haveRetryable {
		p.logger.Warn("retry group completed with only retryable upstream responses",
			"request_id", requestID,
			"retry_round", retryRound,
			"group_size", groupSize,
			"retry_reason", lastRetryable.retryReason,
			"status", lastRetryable.statusCode(),
		)
		lastRetryable.retryAttemptsCompleted = retryAttemptsCompleted
		lastRetryable.retryAttemptsSucceeded = retryAttemptsSucceeded
		return lastRetryable
	}
	if haveFailure {
		p.logger.Warn("retry group completed with only upstream request failures",
			"request_id", requestID,
			"retry_round", retryRound,
			"group_size", groupSize,
			"error", lastFailure.err,
		)
		lastFailure.retryAttemptsCompleted = retryAttemptsCompleted
		lastFailure.retryAttemptsSucceeded = retryAttemptsSucceeded
		return lastFailure
	}

	return attemptResult{
		kind:                   attemptKindFailure,
		err:                    errors.New("retry group finished without results"),
		retryAttemptsCompleted: retryAttemptsCompleted,
		retryAttemptsSucceeded: retryAttemptsSucceeded,
	}
}

func (p *proxyServer) executeAttempt(ctx context.Context, cancel context.CancelFunc, original *http.Request, target *url.URL, body []byte, requestID string, meta attemptMeta) attemptResult {
	result := attemptResult{
		kind:   attemptKindFailure,
		cancel: cancel,
		meta:   meta,
	}

	resp, err := p.doAttempt(ctx, original, target, body, requestID, meta)
	if err != nil {
		cancel()
		result.err = err
		return result
	}
	result.resp = resp

	if resp.StatusCode == http.StatusOK {
		result.kind = attemptKindSuccess
		return result
	}

	if resp.StatusCode != http.StatusOK {
		respBody, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		cancel()
		result.body = respBody
		result.buffered = true
		if readErr != nil {
			result.err = readErr
			return result
		}

		if retryable, retryReason := retryableResponseReason(resp.StatusCode, respBody); retryable {
			result.kind = attemptKindRetryable
			result.retryReason = retryReason
			return result
		}

		result.kind = attemptKindTerminal
		return result
	}

	result.kind = attemptKindTerminal
	return result
}

func (p *proxyServer) doAttempt(ctx context.Context, original *http.Request, target *url.URL, body []byte, requestID string, meta attemptMeta) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, original.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header = original.Header.Clone()
	removeHopHeaders(req.Header)
	req.Header.Set("X-Request-Id", requestID)
	appendForwardedHeaders(req, original)
	req.Host = target.Host
	req.ContentLength = int64(len(body))

	logAttrs := []any{
		"request_id", requestID,
		"attempt", meta.Attempt,
		"method", req.Method,
		"target", target.Redacted(),
	}
	if meta.RetryRound > 0 {
		logAttrs = append(logAttrs,
			"retry_round", meta.RetryRound,
			"group_index", meta.GroupIndex,
			"group_size", meta.GroupSize,
		)
	}
	p.logger.Info("upstream attempt", logAttrs...)

	return p.client.Do(req)
}

func (p *proxyServer) writeAttemptResponse(w http.ResponseWriter, r *http.Request, result attemptResult, requestID string, start time.Time, upstreamRequestsIssued int, retryRoundsStarted int, retryAttemptsCompleted int, retryAttemptsSucceeded int) {
	defer closeAttemptResult(result)

	status := result.statusCode()
	if result.buffered {
		firstByteMS := time.Since(start).Milliseconds()
		writeBufferedResponse(w, result.resp, result.body)
		errorID := ""
		if status != http.StatusOK {
			errorID = p.metrics.storeError(requestID, start, status, result.resp.Header, result.body)
		}
		p.recordRequestMetrics(requestID, start, firstByteMS, status, upstreamRequestsIssued, retryRoundsStarted, retryAttemptsCompleted, retryAttemptsSucceeded, errorID)
		p.logRequestCompleted(requestID, status, upstreamRequestsIssued, retryRoundsStarted, result, int64(len(result.body)), time.Since(start))
		return
	}

	firstByteMS := int64(-1)
	onFirstByte := func() {
		if firstByteMS < 0 {
			firstByteMS = time.Since(start).Milliseconds()
		}
	}

	copyHeaders(w.Header(), result.resp.Header)
	removeHopHeaders(w.Header())
	w.WriteHeader(status)
	bytesWritten, copyErr := copyResponseBody(w, result.resp.Body, onFirstByte)
	if firstByteMS < 0 && bytesWritten == 0 {
		firstByteMS = time.Since(start).Milliseconds()
	}
	if copyErr != nil {
		if isCanceledError(r.Context(), copyErr) {
			errorID := ""
			if status != http.StatusOK {
				errorID = p.metrics.storeError(requestID, start, status, result.resp.Header, []byte("client canceled response stream: "+copyErr.Error()))
			}
			p.recordRequestMetrics(requestID, start, firstByteMS, status, upstreamRequestsIssued, retryRoundsStarted, retryAttemptsCompleted, retryAttemptsSucceeded, errorID)
			p.logger.Info("client canceled response stream",
				"request_id", requestID,
				"status", status,
				"bytes_written", bytesWritten,
				"error", copyErr,
			)
			return
		}

		errorID := p.metrics.storeError(requestID, start, status, result.resp.Header, []byte(copyErr.Error()))
		p.recordRequestMetrics(requestID, start, firstByteMS, status, upstreamRequestsIssued, retryRoundsStarted, retryAttemptsCompleted, retryAttemptsSucceeded, errorID)
		p.logger.Warn("failed to copy upstream response body",
			"request_id", requestID,
			"status", status,
			"bytes_written", bytesWritten,
			"error", copyErr,
		)
		return
	}

	p.recordRequestMetrics(requestID, start, firstByteMS, status, upstreamRequestsIssued, retryRoundsStarted, retryAttemptsCompleted, retryAttemptsSucceeded, "")
	p.logRequestCompleted(requestID, status, upstreamRequestsIssued, retryRoundsStarted, result, bytesWritten, time.Since(start))
}

func (p *proxyServer) writeAttemptFailure(w http.ResponseWriter, r *http.Request, result attemptResult, requestID string, start time.Time, upstreamRequestsIssued int, retryRoundsStarted int, retryAttemptsCompleted int, retryAttemptsSucceeded int) {
	defer closeAttemptResult(result)

	if isCanceledError(r.Context(), result.err) {
		errorID := p.metrics.storeError(requestID, start, 499, http.Header{}, []byte("request canceled during upstream request: "+result.err.Error()))
		p.recordRequestMetrics(requestID, start, time.Since(start).Milliseconds(), 499, upstreamRequestsIssued, retryRoundsStarted, retryAttemptsCompleted, retryAttemptsSucceeded, errorID)
		p.logger.Info("request canceled during upstream request",
			"request_id", requestID,
			"upstream_requests_issued", upstreamRequestsIssued,
			"retry_rounds_started", retryRoundsStarted,
			"error", result.err,
		)
		return
	}

	errorID := p.metrics.storeError(requestID, start, http.StatusBadGateway, http.Header{}, []byte(result.err.Error()))
	p.recordRequestMetrics(requestID, start, time.Since(start).Milliseconds(), http.StatusBadGateway, upstreamRequestsIssued, retryRoundsStarted, retryAttemptsCompleted, retryAttemptsSucceeded, errorID)
	p.logger.Warn("upstream request failed",
		"request_id", requestID,
		"attempt", result.meta.Attempt,
		"retry_round", result.meta.RetryRound,
		"upstream_requests_issued", upstreamRequestsIssued,
		"retry_rounds_started", retryRoundsStarted,
		"error", result.err,
	)
	http.Error(w, "bad gateway", http.StatusBadGateway)
}

func (p *proxyServer) recordRequestMetrics(requestID string, start time.Time, firstByteMS int64, status int, upstreamRequestsIssued int, retryRoundsStarted int, retryAttemptsCompleted int, retryAttemptsSucceeded int, errorID string) {
	p.metrics.record(requestRecord{
		RequestID:              requestID,
		StartedAt:              start,
		FirstByteMS:            firstByteMS,
		StatusCode:             status,
		Success:                status == http.StatusOK,
		Retried:                retryRoundsStarted > 0,
		RetryRounds:            retryRoundsStarted,
		UpstreamRequestsIssued: upstreamRequestsIssued,
		RetryAttemptsCompleted: retryAttemptsCompleted,
		RetryAttemptsSucceeded: retryAttemptsSucceeded,
		ErrorID:                errorID,
	})
}

func (p *proxyServer) logRequestCompleted(requestID string, status int, upstreamRequestsIssued int, retryRoundsStarted int, result attemptResult, bytesWritten int64, duration time.Duration) {
	attrs := []any{
		"request_id", requestID,
		"status", status,
		"upstream_requests_issued", upstreamRequestsIssued,
		"retry_rounds_started", retryRoundsStarted,
		"bytes_written", bytesWritten,
		"duration_ms", duration.Milliseconds(),
	}
	if result.retryReason != "" {
		attrs = append(attrs, "retry_reason", result.retryReason)
	}
	if result.meta.RetryRound > 0 {
		attrs = append(attrs,
			"winning_retry_round", result.meta.RetryRound,
			"winning_group_index", result.meta.GroupIndex,
			"winning_group_size", result.meta.GroupSize,
		)
	}
	p.logger.Info("proxy request completed", attrs...)
}

func (r attemptResult) statusCode() int {
	if r.resp == nil {
		return 0
	}
	return r.resp.StatusCode
}

func cancelAttemptContexts(cancels []context.CancelFunc, keep int) {
	for i, cancel := range cancels {
		if cancel == nil || i == keep {
			continue
		}
		cancel()
	}
}

func closeAttemptResult(result attemptResult) {
	if result.resp != nil && result.resp.Body != nil {
		_ = result.resp.Body.Close()
	}
	if result.cancel != nil {
		result.cancel()
	}
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

func isAllowedProxyPath(path string) bool {
	return strings.Contains(path, "/chat/completions")
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

func copyResponseBody(w http.ResponseWriter, body io.Reader, onFirstByte func()) (int64, error) {
	writer := &firstByteWriter{writer: w, onFirstByte: onFirstByte}
	if flusher, ok := w.(http.Flusher); ok {
		return io.Copy(flushWriter{writer: writer, flusher: flusher}, body)
	}
	return io.Copy(writer, body)
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

type firstByteWriter struct {
	writer      io.Writer
	onFirstByte func()
	wrote       bool
}

func (f *firstByteWriter) Write(p []byte) (int, error) {
	n, err := f.writer.Write(p)
	if n > 0 && !f.wrote {
		f.wrote = true
		if f.onFirstByte != nil {
			f.onFirstByte()
		}
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

func parsePositiveInt(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be > 0", key)
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
