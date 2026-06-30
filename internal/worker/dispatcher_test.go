package worker

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sh-rest/conveyor/internal/models"
	"github.com/sh-rest/conveyor/internal/signing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- hand-crafted mocks ---

type mockQuerier struct {
	details      models.GetDeliveryWithDetailsRow
	detailsErr   error
	inFlightErr  error
	successCalled bool
	successArg    models.UpdateDeliverySuccessParams
	failedCalled  bool
	failedArg     models.UpdateDeliveryFailedParams
}

func (m *mockQuerier) GetDeliveryWithDetails(_ context.Context, _ pgtype.UUID) (models.GetDeliveryWithDetailsRow, error) {
	return m.details, m.detailsErr
}
func (m *mockQuerier) UpdateDeliveryInFlight(_ context.Context, _ pgtype.UUID) (models.Delivery, error) {
	return models.Delivery{}, m.inFlightErr
}
func (m *mockQuerier) UpdateDeliverySuccess(_ context.Context, arg models.UpdateDeliverySuccessParams) (models.Delivery, error) {
	m.successCalled = true
	m.successArg = arg
	return models.Delivery{}, nil
}
func (m *mockQuerier) UpdateDeliveryFailed(_ context.Context, arg models.UpdateDeliveryFailedParams) (models.Delivery, error) {
	m.failedCalled = true
	m.failedArg = arg
	return models.Delivery{}, nil
}

type mockScheduler struct {
	called      bool
	scheduledID string
	scheduledAt time.Time
	err         error
}

func (m *mockScheduler) Schedule(_ context.Context, id string, at time.Time) error {
	m.called = true
	m.scheduledID = id
	m.scheduledAt = at
	return m.err
}

type mockRateLimiter struct {
	allowed bool
	err     error
}

func (m *mockRateLimiter) Allow(_ context.Context, _, _ string, _ int32) (bool, error) {
	return m.allowed, m.err
}

// --- test helpers ---

const testDeliveryID = "d24d912d-4e1e-48d3-9b65-71778111f80c"

func makeDetails(srv *httptest.Server) models.GetDeliveryWithDetailsRow {
	var id, epID pgtype.UUID
	_ = id.Scan(testDeliveryID)
	_ = epID.Scan("a0000000-0000-0000-0000-000000000001")
	return models.GetDeliveryWithDetailsRow{
		ID:            id,
		AttemptNumber: 0,
		EventType:     "order.created",
		Payload:       []byte(`{"order":1}`),
		EndpointID:    epID,
		Url:           srv.URL,
		Secret:        "test-secret",
		TimeoutMs:     5000,
		IsActive:      true,
		RateLimitRps:  10,
	}
}

func newDispatcher(q *mockQuerier, s *mockScheduler, rl *mockRateLimiter) *Dispatcher {
	return NewDispatcher(q, s, rl)
}

// --- TestNextDelay ---

func TestNextDelay_Bounds(t *testing.T) {
	const iterations = 500
	const capMs = 3_600_000.0

	for attempt := 0; attempt <= 8; attempt++ {
		ceiling := math.Min(1000*math.Pow(2, float64(attempt)), capMs)
		for i := 0; i < iterations; i++ {
			d := nextDelay(attempt)
			assert.GreaterOrEqual(t, d.Milliseconds(), int64(0), "attempt %d: negative delay", attempt)
			assert.LessOrEqual(t, float64(d.Milliseconds()), ceiling, "attempt %d: exceeds ceiling %.0f ms", attempt, ceiling)
		}
	}
}

func TestNextDelay_Cap(t *testing.T) {
	const capMs = 3_600_000.0
	for i := 0; i < 200; i++ {
		d := nextDelay(14) // 2^14 * 1000 >> 1h, must be capped
		assert.LessOrEqual(t, float64(d.Milliseconds()), capMs)
	}
}

// --- Dispatcher unit tests ---

func TestDispatch_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q := &mockQuerier{details: makeDetails(srv)}
	s := &mockScheduler{}
	rl := &mockRateLimiter{allowed: true}
	d := newDispatcher(q, s, rl)
	d.Dispatch(context.Background(), testDeliveryID)

	assert.True(t, q.successCalled)
	assert.False(t, q.failedCalled)
	assert.False(t, s.called)
}

func TestDispatch_SetsSignatureHeader(t *testing.T) {
	var capturedSig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get("X-Conveyor-Signature")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	det := makeDetails(srv)
	q := &mockQuerier{details: det}
	d := newDispatcher(q, &mockScheduler{}, &mockRateLimiter{allowed: true})
	d.Dispatch(context.Background(), testDeliveryID)

	expected := signing.Sign(det.Secret, det.Payload)
	assert.Equal(t, expected, capturedSig)
}

func TestDispatch_SetsEventTypeHeader(t *testing.T) {
	var capturedType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedType = r.Header.Get("X-Conveyor-Event-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	det := makeDetails(srv)
	q := &mockQuerier{details: det}
	d := newDispatcher(q, &mockScheduler{}, &mockRateLimiter{allowed: true})
	d.Dispatch(context.Background(), testDeliveryID)

	assert.Equal(t, det.EventType, capturedType)
}

func TestDispatch_InactiveEndpoint(t *testing.T) {
	var requestCount int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&requestCount, 1)
	}))
	defer srv.Close()

	det := makeDetails(srv)
	det.IsActive = false
	q := &mockQuerier{details: det}
	s := &mockScheduler{}
	d := newDispatcher(q, s, &mockRateLimiter{allowed: true})
	d.Dispatch(context.Background(), testDeliveryID)

	assert.Equal(t, int64(0), atomic.LoadInt64(&requestCount), "no HTTP request expected")
	assert.False(t, q.successCalled)
	assert.False(t, q.failedCalled)
	assert.False(t, s.called)
}

func TestDispatch_RateLimited(t *testing.T) {
	var requestCount int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&requestCount, 1)
	}))
	defer srv.Close()

	q := &mockQuerier{details: makeDetails(srv)}
	s := &mockScheduler{}
	rl := &mockRateLimiter{allowed: false}
	d := newDispatcher(q, s, rl)

	before := time.Now()
	d.Dispatch(context.Background(), testDeliveryID)

	require.True(t, s.called, "scheduler should be called for throttled delivery")
	assert.Equal(t, testDeliveryID, s.scheduledID)
	assert.WithinDuration(t, before.Add(time.Second), s.scheduledAt, 200*time.Millisecond)
	assert.Equal(t, int64(0), atomic.LoadInt64(&requestCount), "no HTTP request expected when throttled")
	assert.False(t, q.successCalled)
	assert.False(t, q.failedCalled)
}

func TestDispatch_RateLimiterError_FailsOpen(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q := &mockQuerier{details: makeDetails(srv)}
	rl := &mockRateLimiter{allowed: false, err: errors.New("redis unavailable")}
	d := newDispatcher(q, &mockScheduler{}, rl)
	d.Dispatch(context.Background(), testDeliveryID)

	// fail open: dispatch continues despite rate limiter error
	assert.True(t, q.successCalled, "should succeed when rate limiter errors (fail open)")
}

func TestDispatch_4xx_DeadLetter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	q := &mockQuerier{details: makeDetails(srv)}
	s := &mockScheduler{}
	d := newDispatcher(q, s, &mockRateLimiter{allowed: true})
	d.Dispatch(context.Background(), testDeliveryID)

	require.True(t, q.failedCalled)
	assert.Equal(t, models.DeliveryStatusDeadLettered, q.failedArg.Status)
	assert.False(t, s.called, "no retry scheduled for permanent 4xx")
}

func TestDispatch_408_NotPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusRequestTimeout)
	}))
	defer srv.Close()

	q := &mockQuerier{details: makeDetails(srv)}
	s := &mockScheduler{}
	d := newDispatcher(q, s, &mockRateLimiter{allowed: true})
	d.Dispatch(context.Background(), testDeliveryID)

	require.True(t, q.failedCalled)
	assert.Equal(t, models.DeliveryStatusFailed, q.failedArg.Status, "408 should retry, not dead-letter")
	assert.True(t, s.called)
}

func TestDispatch_429_NotPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	q := &mockQuerier{details: makeDetails(srv)}
	s := &mockScheduler{}
	d := newDispatcher(q, s, &mockRateLimiter{allowed: true})
	d.Dispatch(context.Background(), testDeliveryID)

	require.True(t, q.failedCalled)
	assert.Equal(t, models.DeliveryStatusFailed, q.failedArg.Status, "429 should retry, not dead-letter")
	assert.True(t, s.called)
}

func TestDispatch_5xx_Retry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	q := &mockQuerier{details: makeDetails(srv)}
	s := &mockScheduler{}
	d := newDispatcher(q, s, &mockRateLimiter{allowed: true})
	d.Dispatch(context.Background(), testDeliveryID)

	require.True(t, q.failedCalled)
	assert.Equal(t, models.DeliveryStatusFailed, q.failedArg.Status)
	require.True(t, s.called)
	assert.Equal(t, testDeliveryID, s.scheduledID)
	assert.True(t, s.scheduledAt.After(time.Now()), "retry should be in the future")
}

func TestDispatch_Exhausted_Attempt6(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	det := makeDetails(srv)
	det.AttemptNumber = 6 // = maxAttempts - 1: triggers dead-letter
	q := &mockQuerier{details: det}
	s := &mockScheduler{}
	d := newDispatcher(q, s, &mockRateLimiter{allowed: true})
	d.Dispatch(context.Background(), testDeliveryID)

	require.True(t, q.failedCalled)
	assert.Equal(t, models.DeliveryStatusDeadLettered, q.failedArg.Status)
	assert.False(t, s.called, "exhausted delivery must not be retried")
}

func TestDispatch_NotExhausted_Attempt5(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	det := makeDetails(srv)
	det.AttemptNumber = 5 // one below exhaustion threshold
	q := &mockQuerier{details: det}
	s := &mockScheduler{}
	d := newDispatcher(q, s, &mockRateLimiter{allowed: true})
	d.Dispatch(context.Background(), testDeliveryID)

	require.True(t, q.failedCalled)
	assert.Equal(t, models.DeliveryStatusFailed, q.failedArg.Status, "attempt 5 should still retry")
	assert.True(t, s.called)
}

func TestDispatch_InvalidDeliveryID(t *testing.T) {
	q := &mockQuerier{}
	d := newDispatcher(q, &mockScheduler{}, &mockRateLimiter{allowed: true})

	// Should not panic — invalid UUID is logged and returned early
	assert.NotPanics(t, func() {
		d.Dispatch(context.Background(), "not-a-uuid")
	})
	assert.False(t, q.successCalled)
	assert.False(t, q.failedCalled)
}

func TestDispatch_GetDetailsError(t *testing.T) {
	q := &mockQuerier{detailsErr: errors.New("db unavailable")}
	d := newDispatcher(q, &mockScheduler{}, &mockRateLimiter{allowed: true})

	assert.NotPanics(t, func() {
		d.Dispatch(context.Background(), testDeliveryID)
	})
	assert.False(t, q.successCalled)
	assert.False(t, q.failedCalled)
}

func TestDispatch_UpdateInFlightError(t *testing.T) {
	var requestCount int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q := &mockQuerier{details: makeDetails(srv), inFlightErr: errors.New("db write failed")}
	d := newDispatcher(q, &mockScheduler{}, &mockRateLimiter{allowed: true})
	d.Dispatch(context.Background(), testDeliveryID)

	assert.Equal(t, int64(0), atomic.LoadInt64(&requestCount), "no HTTP request if UpdateDeliveryInFlight fails")
	assert.False(t, q.successCalled)
	assert.False(t, q.failedCalled)
}
