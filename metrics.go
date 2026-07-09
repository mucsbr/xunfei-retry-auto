package main

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

const (
	defaultMaxMetricRecords = 2000
	defaultMaxErrorBody     = 256 * 1024
)

type metricsStore struct {
	mu           sync.RWMutex
	nextErrorID  uint64
	maxRecords   int
	maxErrorBody int
	records      []requestRecord
	errors       map[string]errorDetail
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

func newMetricsStore(maxRecords int, maxErrorBody int) *metricsStore {
	if maxRecords <= 0 {
		maxRecords = defaultMaxMetricRecords
	}
	if maxErrorBody <= 0 {
		maxErrorBody = defaultMaxErrorBody
	}

	return &metricsStore{
		maxRecords:   maxRecords,
		maxErrorBody: maxErrorBody,
		errors:       make(map[string]errorDetail),
	}
}

func (m *metricsStore) record(record requestRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.records = append(m.records, record)
	if len(m.records) <= m.maxRecords {
		return
	}

	drop := len(m.records) - m.maxRecords
	for _, oldRecord := range m.records[:drop] {
		if oldRecord.ErrorID != "" {
			delete(m.errors, oldRecord.ErrorID)
		}
	}
	m.records = append([]requestRecord(nil), m.records[drop:]...)
}

func (m *metricsStore) storeError(requestID string, at time.Time, statusCode int, headers http.Header, body []byte) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextErrorID++
	id := fmt.Sprintf("err-%d", m.nextErrorID)

	storedBody := body
	truncated := false
	if len(storedBody) > m.maxErrorBody {
		storedBody = storedBody[:m.maxErrorBody]
		truncated = true
	}

	m.errors[id] = errorDetail{
		ID:         id,
		RequestID:  requestID,
		At:         at,
		StatusCode: statusCode,
		Headers:    cloneHeaders(headers),
		Body:       string(storedBody),
		Truncated:  truncated,
	}
	return id
}

func (m *metricsStore) getError(id string) (errorDetail, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	detail, ok := m.errors[id]
	return detail, ok
}

func (m *metricsStore) snapshot(now time.Time) adminSnapshot {
	m.mu.RLock()
	records := make([]requestRecord, len(m.records))
	copy(records, m.records)
	m.mu.RUnlock()

	summary := buildAdminSummary(records, now)
	sort.Slice(records, func(i, j int) bool {
		return records[i].StartedAt.After(records[j].StartedAt)
	})
	if len(records) > 200 {
		records = records[:200]
	}

	return adminSnapshot{
		Summary: summary,
		Records: records,
	}
}

func buildAdminSummary(records []requestRecord, now time.Time) adminSummary {
	var successRequests int
	var retriedRequests int
	var firstByteSum int64
	var firstByteCount int64
	var retryAttemptsCompleted int
	var retryAttemptsSucceeded int

	fiveHourStart := now.Add(-5 * time.Hour)
	weekStart := now.AddDate(0, 0, -7)
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	var fiveHourSuccess int
	var weekSuccess int
	var monthSuccess int

	for _, record := range records {
		if record.Success {
			successRequests++
			if !record.StartedAt.Before(fiveHourStart) {
				fiveHourSuccess++
			}
			if !record.StartedAt.Before(weekStart) {
				weekSuccess++
			}
			if !record.StartedAt.Before(monthStart) {
				monthSuccess++
			}
		}
		if record.Retried {
			retriedRequests++
		}
		if record.FirstByteMS >= 0 {
			firstByteSum += record.FirstByteMS
			firstByteCount++
		}
		retryAttemptsCompleted += record.RetryAttemptsCompleted
		retryAttemptsSucceeded += record.RetryAttemptsSucceeded
	}

	totalRequests := len(records)
	summary := adminSummary{
		UpdatedAt:               now,
		TotalRequests:           totalRequests,
		SuccessRequests:         successRequests,
		RetriedRequests:         retriedRequests,
		RetryAttemptsCompleted:  retryAttemptsCompleted,
		RetryAttemptsSucceeded:  retryAttemptsSucceeded,
		FiveHourSuccessRequests: fiveHourSuccess,
		WeekSuccessRequests:     weekSuccess,
		MonthSuccessRequests:    monthSuccess,
		HourBuckets:             buildHourBuckets(records, now, 5),
		DayBuckets:              buildDayBuckets(records, now, 7),
	}

	if totalRequests > 0 {
		summary.SuccessRate = float64(successRequests) / float64(totalRequests) * 100
		summary.RetryRate = float64(retriedRequests) / float64(totalRequests) * 100
	}
	if firstByteCount > 0 {
		summary.AverageFirstByteMS = float64(firstByteSum) / float64(firstByteCount)
	}
	if retryAttemptsCompleted > 0 {
		summary.RetryAttemptSuccessRate = float64(retryAttemptsSucceeded) / float64(retryAttemptsCompleted) * 100
	}

	return summary
}

func buildHourBuckets(records []requestRecord, now time.Time, count int) []countBucket {
	buckets := make([]countBucket, 0, count)
	currentHour := now.Truncate(time.Hour)
	for i := count - 1; i >= 0; i-- {
		start := currentHour.Add(-time.Duration(i) * time.Hour)
		end := start.Add(time.Hour)
		buckets = append(buckets, countBucket{
			Label: start.Format("15:04"),
			Count: countSuccessfulRecords(records, start, end),
		})
	}
	return buckets
}

func buildDayBuckets(records []requestRecord, now time.Time, count int) []countBucket {
	buckets := make([]countBucket, 0, count)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	for i := count - 1; i >= 0; i-- {
		start := today.AddDate(0, 0, -i)
		end := start.AddDate(0, 0, 1)
		buckets = append(buckets, countBucket{
			Label: start.Format("01-02"),
			Count: countSuccessfulRecords(records, start, end),
		})
	}
	return buckets
}

func countSuccessfulRecords(records []requestRecord, start time.Time, end time.Time) int {
	count := 0
	for _, record := range records {
		if !record.Success {
			continue
		}
		if !record.StartedAt.Before(start) && record.StartedAt.Before(end) {
			count++
		}
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
