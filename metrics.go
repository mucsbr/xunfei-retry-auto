package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

const (
	defaultMetricsDBPath = "data/xunfei-retry-proxy.db"
	defaultMaxErrorBody  = 256 * 1024
)

type metricsStore struct {
	db           *sql.DB
	logger       *slog.Logger
	nextErrorID  atomic.Uint64
	maxErrorBody int
}

type requestRecord struct {
	RequestID              string    `json:"request_id"`
	StartedAt              time.Time `json:"started_at"`
	FirstByteMS            int64     `json:"first_byte_ms"`
	StatusCode             int       `json:"status_code"`
	Success                bool      `json:"success"`
	Retried                bool      `json:"retried"`
	RetryRounds            int       `json:"retry_rounds"`
	UpstreamRequestsIssued int       `json:"upstream_requests_issued"`
	RetryAttemptsCompleted int       `json:"retry_attempts_completed"`
	RetryAttemptsSucceeded int       `json:"retry_attempts_succeeded"`
	ErrorID                string    `json:"error_id,omitempty"`
}

type errorDetail struct {
	ID         string              `json:"id"`
	RequestID  string              `json:"request_id"`
	At         time.Time           `json:"at"`
	StatusCode int                 `json:"status_code"`
	Headers    map[string][]string `json:"headers"`
	Body       string              `json:"body"`
	Truncated  bool                `json:"truncated"`
}

type adminSnapshot struct {
	Summary adminSummary    `json:"summary"`
	Records []requestRecord `json:"records"`
}

type adminSummary struct {
	UpdatedAt               time.Time     `json:"updated_at"`
	TotalRequests           int           `json:"total_requests"`
	SuccessRequests         int           `json:"success_requests"`
	RetriedRequests         int           `json:"retried_requests"`
	SuccessRate             float64       `json:"success_rate"`
	RetryRate               float64       `json:"retry_rate"`
	AverageFirstByteMS      float64       `json:"average_first_byte_ms"`
	RetryAttemptsCompleted  int           `json:"retry_attempts_completed"`
	RetryAttemptsSucceeded  int           `json:"retry_attempts_succeeded"`
	RetryAttemptSuccessRate float64       `json:"retry_attempt_success_rate"`
	FiveHourSuccessRequests int           `json:"five_hour_success_requests"`
	WeekSuccessRequests     int           `json:"week_success_requests"`
	MonthSuccessRequests    int           `json:"month_success_requests"`
	HourBuckets             []countBucket `json:"hour_buckets"`
	DayBuckets              []countBucket `json:"day_buckets"`
}

type countBucket struct {
	Label string `json:"label"`
	Count int    `json:"count"`
}

func newMetricsStore(dbPath string, maxErrorBody int, logger *slog.Logger) (*metricsStore, error) {
	if dbPath == "" {
		dbPath = ":memory:"
	}
	if maxErrorBody <= 0 {
		maxErrorBody = defaultMaxErrorBody
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	if dbPath != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			return nil, fmt.Errorf("create metrics db directory: %w", err)
		}
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open metrics sqlite db: %w", err)
	}
	db.SetMaxOpenConns(1)

	store := &metricsStore{
		db:           db,
		logger:       logger,
		maxErrorBody: maxErrorBody,
	}
	if err := store.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (m *metricsStore) migrate() error {
	statements := []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS request_records (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			request_id TEXT NOT NULL,
			started_at_ms INTEGER NOT NULL,
			first_byte_ms INTEGER NOT NULL,
			status_code INTEGER NOT NULL,
			success INTEGER NOT NULL,
			retried INTEGER NOT NULL,
			retry_rounds INTEGER NOT NULL,
			upstream_requests_issued INTEGER NOT NULL,
			retry_attempts_completed INTEGER NOT NULL,
			retry_attempts_succeeded INTEGER NOT NULL,
			error_id TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_request_records_started_at_ms ON request_records(started_at_ms)`,
		`CREATE INDEX IF NOT EXISTS idx_request_records_success_started_at_ms ON request_records(success, started_at_ms)`,
		`CREATE TABLE IF NOT EXISTS error_details (
			id TEXT PRIMARY KEY,
			request_id TEXT NOT NULL,
			at_ms INTEGER NOT NULL,
			status_code INTEGER NOT NULL,
			headers_json TEXT NOT NULL,
			body TEXT NOT NULL,
			truncated INTEGER NOT NULL
		)`,
	}

	for _, statement := range statements {
		if _, err := m.db.Exec(statement); err != nil {
			return fmt.Errorf("migrate metrics sqlite db: %w", err)
		}
	}
	return nil
}

func (m *metricsStore) record(record requestRecord) {
	_, err := m.db.Exec(
		`INSERT INTO request_records (
			request_id,
			started_at_ms,
			first_byte_ms,
			status_code,
			success,
			retried,
			retry_rounds,
			upstream_requests_issued,
			retry_attempts_completed,
			retry_attempts_succeeded,
			error_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.RequestID,
		timeToMillis(record.StartedAt),
		record.FirstByteMS,
		record.StatusCode,
		boolToInt(record.Success),
		boolToInt(record.Retried),
		record.RetryRounds,
		record.UpstreamRequestsIssued,
		record.RetryAttemptsCompleted,
		record.RetryAttemptsSucceeded,
		nullableString(record.ErrorID),
	)
	if err != nil {
		m.logger.Warn("failed to persist request metrics", "error", err, "request_id", record.RequestID)
	}
}

func (m *metricsStore) storeError(requestID string, at time.Time, statusCode int, headers http.Header, body []byte) string {
	id := fmt.Sprintf("err-%d-%d", time.Now().UnixNano(), m.nextErrorID.Add(1))

	storedBody := body
	truncated := false
	if len(storedBody) > m.maxErrorBody {
		storedBody = storedBody[:m.maxErrorBody]
		truncated = true
	}

	headersJSON, err := json.Marshal(cloneHeaders(headers))
	if err != nil {
		m.logger.Warn("failed to encode upstream error headers", "error", err, "request_id", requestID)
		headersJSON = []byte(`{}`)
	}

	_, err = m.db.Exec(
		`INSERT INTO error_details (
			id,
			request_id,
			at_ms,
			status_code,
			headers_json,
			body,
			truncated
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id,
		requestID,
		timeToMillis(at),
		statusCode,
		string(headersJSON),
		string(storedBody),
		boolToInt(truncated),
	)
	if err != nil {
		m.logger.Warn("failed to persist upstream error detail", "error", err, "request_id", requestID)
		return ""
	}

	return id
}

func (m *metricsStore) getError(id string) (errorDetail, bool) {
	var detail errorDetail
	var atMS int64
	var headersJSON string
	var truncated int

	err := m.db.QueryRow(
		`SELECT id, request_id, at_ms, status_code, headers_json, body, truncated
		FROM error_details
		WHERE id = ?`,
		id,
	).Scan(&detail.ID, &detail.RequestID, &atMS, &detail.StatusCode, &headersJSON, &detail.Body, &truncated)
	if err == sql.ErrNoRows {
		return errorDetail{}, false
	}
	if err != nil {
		m.logger.Warn("failed to read upstream error detail", "error", err, "error_id", id)
		return errorDetail{}, false
	}

	detail.At = millisToTime(atMS)
	detail.Truncated = truncated != 0
	if err := json.Unmarshal([]byte(headersJSON), &detail.Headers); err != nil {
		m.logger.Warn("failed to decode upstream error headers", "error", err, "error_id", id)
		detail.Headers = map[string][]string{}
	}

	return detail, true
}

func (m *metricsStore) snapshot(now time.Time) adminSnapshot {
	summary := m.summary(now)
	records := m.recentRecords(200)
	return adminSnapshot{
		Summary: summary,
		Records: records,
	}
}

func (m *metricsStore) summary(now time.Time) adminSummary {
	fiveHourStart := now.Add(-5 * time.Hour)
	weekStart := now.AddDate(0, 0, -7)
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())

	var summary adminSummary
	summary.UpdatedAt = now
	err := m.db.QueryRow(
		`SELECT
			COUNT(*),
			COALESCE(SUM(success), 0),
			COALESCE(SUM(retried), 0),
			COALESCE(AVG(CASE WHEN first_byte_ms >= 0 THEN first_byte_ms END), 0),
			COALESCE(SUM(retry_attempts_completed), 0),
			COALESCE(SUM(retry_attempts_succeeded), 0),
			COALESCE(SUM(CASE WHEN success = 1 AND started_at_ms >= ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN success = 1 AND started_at_ms >= ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN success = 1 AND started_at_ms >= ? THEN 1 ELSE 0 END), 0)
		FROM request_records`,
		timeToMillis(fiveHourStart),
		timeToMillis(weekStart),
		timeToMillis(monthStart),
	).Scan(
		&summary.TotalRequests,
		&summary.SuccessRequests,
		&summary.RetriedRequests,
		&summary.AverageFirstByteMS,
		&summary.RetryAttemptsCompleted,
		&summary.RetryAttemptsSucceeded,
		&summary.FiveHourSuccessRequests,
		&summary.WeekSuccessRequests,
		&summary.MonthSuccessRequests,
	)
	if err != nil {
		m.logger.Warn("failed to read metrics summary", "error", err)
	}

	if summary.TotalRequests > 0 {
		summary.SuccessRate = float64(summary.SuccessRequests) / float64(summary.TotalRequests) * 100
		summary.RetryRate = float64(summary.RetriedRequests) / float64(summary.TotalRequests) * 100
	}
	if summary.RetryAttemptsCompleted > 0 {
		summary.RetryAttemptSuccessRate = float64(summary.RetryAttemptsSucceeded) / float64(summary.RetryAttemptsCompleted) * 100
	}
	summary.HourBuckets = m.hourBuckets(now, 5)
	summary.DayBuckets = m.dayBuckets(now, 7)

	return summary
}

func (m *metricsStore) recentRecords(limit int) []requestRecord {
	rows, err := m.db.Query(
		`SELECT
			request_id,
			started_at_ms,
			first_byte_ms,
			status_code,
			success,
			retried,
			retry_rounds,
			upstream_requests_issued,
			retry_attempts_completed,
			retry_attempts_succeeded,
			COALESCE(error_id, '')
		FROM request_records
		ORDER BY started_at_ms DESC, id DESC
		LIMIT ?`,
		limit,
	)
	if err != nil {
		m.logger.Warn("failed to read recent request records", "error", err)
		return []requestRecord{}
	}
	defer rows.Close()

	records := make([]requestRecord, 0, limit)
	for rows.Next() {
		var record requestRecord
		var startedAtMS int64
		var success int
		var retried int
		if err := rows.Scan(
			&record.RequestID,
			&startedAtMS,
			&record.FirstByteMS,
			&record.StatusCode,
			&success,
			&retried,
			&record.RetryRounds,
			&record.UpstreamRequestsIssued,
			&record.RetryAttemptsCompleted,
			&record.RetryAttemptsSucceeded,
			&record.ErrorID,
		); err != nil {
			m.logger.Warn("failed to scan request record", "error", err)
			continue
		}
		record.StartedAt = millisToTime(startedAtMS)
		record.Success = success != 0
		record.Retried = retried != 0
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		m.logger.Warn("failed to iterate request records", "error", err)
	}

	return records
}

func (m *metricsStore) hourBuckets(now time.Time, count int) []countBucket {
	buckets := make([]countBucket, 0, count)
	currentHour := now.Truncate(time.Hour)
	for i := count - 1; i >= 0; i-- {
		start := currentHour.Add(-time.Duration(i) * time.Hour)
		end := start.Add(time.Hour)
		buckets = append(buckets, countBucket{
			Label: start.Format("15:04"),
			Count: m.countSuccessfulBetween(start, end),
		})
	}
	return buckets
}

func (m *metricsStore) dayBuckets(now time.Time, count int) []countBucket {
	buckets := make([]countBucket, 0, count)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	for i := count - 1; i >= 0; i-- {
		start := today.AddDate(0, 0, -i)
		end := start.AddDate(0, 0, 1)
		buckets = append(buckets, countBucket{
			Label: start.Format("01-02"),
			Count: m.countSuccessfulBetween(start, end),
		})
	}
	return buckets
}

func (m *metricsStore) countSuccessfulBetween(start time.Time, end time.Time) int {
	var count int
	err := m.db.QueryRow(
		`SELECT COUNT(*)
		FROM request_records
		WHERE success = 1 AND started_at_ms >= ? AND started_at_ms < ?`,
		timeToMillis(start),
		timeToMillis(end),
	).Scan(&count)
	if err != nil {
		m.logger.Warn("failed to count successful requests", "error", err)
	}
	return count
}

func cloneHeaders(headers http.Header) map[string][]string {
	cloned := make(map[string][]string, len(headers))
	for key, values := range headers {
		clonedValues := make([]string, len(values))
		copy(clonedValues, values)
		cloned[key] = clonedValues
	}
	return cloned
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func timeToMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func millisToTime(ms int64) time.Time {
	return time.Unix(0, ms*int64(time.Millisecond))
}
