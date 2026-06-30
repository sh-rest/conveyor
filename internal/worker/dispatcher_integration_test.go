//go:build integration

package worker_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
	"github.com/sh-rest/conveyor/internal/models"
	"github.com/sh-rest/conveyor/internal/queue"
	"github.com/sh-rest/conveyor/internal/ratelimit"
	"github.com/sh-rest/conveyor/internal/testutil"
	"github.com/sh-rest/conveyor/internal/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var seedCounter int64

// seedDelivery inserts project → endpoint → event → delivery inside a test transaction.
// Returns the delivery UUID as a hyphenated string.
func seedDelivery(t *testing.T, ctx context.Context, q *models.Queries, endpointURL string, rateLimitRPS int32) string {
	t.Helper()
	ids := seedNDeliveries(t, ctx, q, endpointURL, rateLimitRPS, 1)
	return ids[0]
}

// seedNDeliveries inserts 1 project + 1 endpoint + N (event + delivery) pairs.
// All deliveries share the same endpoint — critical for rate limit tests where
// the limiter key is per-endpoint-ID.
func seedNDeliveries(t *testing.T, ctx context.Context, q *models.Queries, endpointURL string, rateLimitRPS int32, n int) []string {
	t.Helper()
	ctr := atomic.AddInt64(&seedCounter, 1)

	proj, err := q.CreateProject(ctx, models.CreateProjectParams{
		Name:   fmt.Sprintf("test-project-%d", ctr),
		ApiKey: fmt.Sprintf("whk_test_%d", ctr),
	})
	require.NoError(t, err)

	ep, err := q.CreateEndpoint(ctx, models.CreateEndpointParams{
		ProjectID:    proj.ID,
		Url:          endpointURL,
		Secret:       "test-secret",
		RateLimitRps: rateLimitRPS,
		TimeoutMs:    5000,
	})
	require.NoError(t, err)

	ids := make([]string, n)
	for i := range n {
		ev, err := q.CreateEvent(ctx, models.CreateEventParams{
			ProjectID: proj.ID,
			EventType: "test.event",
			Payload:   []byte(fmt.Sprintf(`{"n":%d}`, i)),
		})
		require.NoError(t, err)

		dlv, err := q.CreateDelivery(ctx, models.CreateDeliveryParams{
			EventID:    ev.ID,
			EndpointID: ep.ID,
		})
		require.NoError(t, err)
		ids[i] = uuidToStr(dlv.ID)
	}
	return ids
}

func uuidToStr(u pgtype.UUID) string {
	b := u.Bytes
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func uuidFromString(t *testing.T, s string) pgtype.UUID {
	t.Helper()
	var id pgtype.UUID
	require.NoError(t, id.Scan(s))
	return id
}

func newIntegrationComponents(t *testing.T) (*redis.Client, *queue.Scheduler, *ratelimit.Limiter) {
	t.Helper()
	rdb := testutil.NewRedis(t)
	testutil.FlushRedis(t, rdb)
	rdb.XGroupCreateMkStream(context.Background(), queue.DeliveryStream, queue.ConsumerGroup, "0")
	return rdb, queue.NewScheduler(rdb), ratelimit.NewLimiter(rdb)
}

// --- Integration Tests ---

func TestDispatchIntegration_Success(t *testing.T) {
	pool := testutil.NewPool(t)
	_, q := testutil.NewTestTx(t, pool)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	deliveryID := seedDelivery(t, ctx, q, srv.URL, 10)
	rdb, sched, limiter := newIntegrationComponents(t)
	_ = rdb
	d := worker.NewDispatcher(q, sched, limiter)
	d.Dispatch(ctx, deliveryID)

	dlv, err := q.GetDeliveryByID(ctx, uuidFromString(t, deliveryID))
	require.NoError(t, err)
	assert.Equal(t, models.DeliveryStatusDelivered, dlv.Status)
	require.NotNil(t, dlv.LastHttpStatus)
	assert.Equal(t, int32(200), *dlv.LastHttpStatus)
	assert.Equal(t, int32(1), dlv.AttemptNumber)
}

func TestDispatchIntegration_5xxRetry(t *testing.T) {
	pool := testutil.NewPool(t)
	_, q := testutil.NewTestTx(t, pool)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	deliveryID := seedDelivery(t, ctx, q, srv.URL, 10)
	rdb, sched, limiter := newIntegrationComponents(t)
	d := worker.NewDispatcher(q, sched, limiter)
	d.Dispatch(ctx, deliveryID)

	dlv, err := q.GetDeliveryByID(ctx, uuidFromString(t, deliveryID))
	require.NoError(t, err)
	assert.Equal(t, models.DeliveryStatusFailed, dlv.Status)
	require.NotNil(t, dlv.LastHttpStatus)
	assert.Equal(t, int32(502), *dlv.LastHttpStatus)
	assert.True(t, dlv.NextAttemptAt.Valid, "next_attempt_at should be set for retry")

	// Delivery should appear in the retry zset
	score := rdb.ZScore(ctx, queue.RetryZSet, deliveryID).Val()
	assert.Greater(t, score, float64(0), "delivery should be in zset:retry")
}

func TestDispatchIntegration_404DeadLetter(t *testing.T) {
	pool := testutil.NewPool(t)
	_, q := testutil.NewTestTx(t, pool)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	deliveryID := seedDelivery(t, ctx, q, srv.URL, 10)
	_, sched, limiter := newIntegrationComponents(t)
	d := worker.NewDispatcher(q, sched, limiter)
	d.Dispatch(ctx, deliveryID)

	dlv, err := q.GetDeliveryByID(ctx, uuidFromString(t, deliveryID))
	require.NoError(t, err)
	assert.Equal(t, models.DeliveryStatusDeadLettered, dlv.Status)
	require.NotNil(t, dlv.LastHttpStatus)
	assert.Equal(t, int32(404), *dlv.LastHttpStatus)
}

func TestDispatchIntegration_RateLimit(t *testing.T) {
	pool := testutil.NewPool(t)
	_, q := testutil.NewTestTx(t, pool)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rdb, sched, limiter := newIntegrationComponents(t)

	// 3 deliveries to the SAME endpoint (same endpoint ID = same rate limit window)
	ids := seedNDeliveries(t, ctx, q, srv.URL, 1, 3)
	id1, id2, id3 := ids[0], ids[1], ids[2]

	d := worker.NewDispatcher(q, sched, limiter)
	d.Dispatch(ctx, id1)
	d.Dispatch(ctx, id2)
	d.Dispatch(ctx, id3)

	statuses := map[models.DeliveryStatus]int{}
	for _, id := range []string{id1, id2, id3} {
		dlv, err := q.GetDeliveryByID(ctx, uuidFromString(t, id))
		require.NoError(t, err)
		statuses[dlv.Status]++
	}
	assert.Equal(t, 1, statuses[models.DeliveryStatusDelivered], "exactly 1 delivery should succeed with limit=1")
	assert.Equal(t, 2, statuses[models.DeliveryStatusPending], "2 deliveries should be throttled (status still pending)")

	zcard := rdb.ZCard(ctx, queue.RetryZSet).Val()
	assert.Equal(t, int64(2), zcard, "2 throttled deliveries should be in zset:retry")
}
