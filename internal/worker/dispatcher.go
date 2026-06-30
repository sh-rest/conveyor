package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sh-rest/conveyor/internal/models"
	"github.com/sh-rest/conveyor/internal/signing"
)

const maxAttempts = 7

// DispatcherQuerier is the DB interface the dispatcher depends on.
type DispatcherQuerier interface {
	GetDeliveryWithDetails(ctx context.Context, id pgtype.UUID) (models.GetDeliveryWithDetailsRow, error)
	UpdateDeliveryInFlight(ctx context.Context, id pgtype.UUID) (models.Delivery, error)
	UpdateDeliverySuccess(ctx context.Context, arg models.UpdateDeliverySuccessParams) (models.Delivery, error)
	UpdateDeliveryFailed(ctx context.Context, arg models.UpdateDeliveryFailedParams) (models.Delivery, error)
}

// RetryScheduler schedules a delivery for a future retry attempt.
type RetryScheduler interface {
	Schedule(ctx context.Context, deliveryID string, at time.Time) error
}

// RateLimiter checks whether a delivery is within the endpoint's RPS limit.
type RateLimiter interface {
	Allow(ctx context.Context, endpointID, deliveryID string, limitRPS int32) (bool, error)
}

type Dispatcher struct {
	q           DispatcherQuerier
	scheduler   RetryScheduler
	rateLimiter RateLimiter
	httpClient  *http.Client
}

func NewDispatcher(q DispatcherQuerier, scheduler RetryScheduler, rateLimiter RateLimiter) *Dispatcher {
	return &Dispatcher{
		q:           q,
		scheduler:   scheduler,
		rateLimiter: rateLimiter,
		httpClient:  &http.Client{}, // timeout is set per-request from endpoint.timeout_ms
	}
}

// Dispatch executes one delivery attempt and updates the DB with the outcome.
// Called by worker goroutines. Panics are caught by the pool's safeDispatch wrapper.
func (d *Dispatcher) Dispatch(ctx context.Context, deliveryID string) {
	var id pgtype.UUID
	if err := id.Scan(deliveryID); err != nil {
		slog.Error("invalid delivery id in stream", "delivery_id", deliveryID, "err", err)
		return
	}

	details, err := d.q.GetDeliveryWithDetails(ctx, id)
	if err != nil {
		slog.Error("failed to load delivery details", "delivery_id", deliveryID, "err", err)
		return
	}

	if !details.IsActive {
		slog.Info("skipping delivery — endpoint inactive", "delivery_id", deliveryID)
		return
	}

	// Rate limit check must happen before UpdateDeliveryInFlight so that throttled
	// deliveries are re-scheduled without touching attempt_number (Option C).
	endpointKey := fmt.Sprintf("%x", details.EndpointID.Bytes)
	allowed, err := d.rateLimiter.Allow(ctx, endpointKey, deliveryID, details.RateLimitRps)
	if err != nil {
		// Fail open: a Redis blip should not stop deliveries.
		slog.Warn("rate limit check failed, allowing delivery", "delivery_id", deliveryID, "err", err)
	} else if !allowed {
		nextAt := time.Now().Add(time.Second)
		if err := d.scheduler.Schedule(ctx, deliveryID, nextAt); err != nil {
			slog.Error("failed to reschedule throttled delivery", "delivery_id", deliveryID, "err", err)
		}
		slog.Info("delivery throttled — rescheduled in 1s",
			"delivery_id", deliveryID,
			"rps_limit", details.RateLimitRps,
		)
		return
	}

	if _, err := d.q.UpdateDeliveryInFlight(ctx, id); err != nil {
		slog.Error("failed to mark delivery in_flight", "delivery_id", deliveryID, "err", err)
		return
	}

	// Sign the payload and make the HTTP request
	signature := signing.Sign(details.Secret, details.Payload)
	timeout := time.Duration(details.TimeoutMs) * time.Millisecond

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, details.Url, bytes.NewReader(details.Payload))
	if err != nil {
		d.handleFailure(ctx, id, deliveryID, details, 0, "", "failed to build request: "+err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Conveyor-Signature", signature)
	req.Header.Set("X-Conveyor-Event-Type", details.EventType)

	resp, err := d.httpClient.Do(req)
	if err != nil {
		d.handleFailure(ctx, id, deliveryID, details, 0, "", err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	statusCode := int32(resp.StatusCode)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		d.q.UpdateDeliverySuccess(ctx, models.UpdateDeliverySuccessParams{ //nolint
			ID:             id,
			LastHttpStatus: &statusCode,
		})
		slog.Info("delivery succeeded", "delivery_id", deliveryID, "http_status", statusCode)
		return
	}

	d.handleFailure(ctx, id, deliveryID, details, statusCode, string(body), "")
}

func (d *Dispatcher) handleFailure(
	ctx context.Context,
	id pgtype.UUID,
	deliveryID string,
	details models.GetDeliveryWithDetailsRow,
	httpStatus int32,
	respBody string,
	errMsg string,
) {
	var httpPtr *int32
	if httpStatus > 0 {
		httpPtr = &httpStatus
	}
	var bodyPtr *string
	if respBody != "" {
		bodyPtr = &respBody
	}
	var errPtr *string
	if errMsg != "" {
		errPtr = &errMsg
	}

	// Permanent failures: 4xx (except 408 timeout and 429 rate-limited) → dead-letter immediately
	isPermanent := httpStatus >= 400 && httpStatus < 500 && httpStatus != 408 && httpStatus != 429
	isExhausted := int(details.AttemptNumber) >= maxAttempts-1

	if isPermanent || isExhausted {
		d.q.UpdateDeliveryFailed(ctx, models.UpdateDeliveryFailedParams{ //nolint
			ID:               id,
			Status:           models.DeliveryStatusDeadLettered,
			LastHttpStatus:   httpPtr,
			LastResponseBody: bodyPtr,
			LastError:        errPtr,
			NextAttemptAt:    pgtype.Timestamptz{},
		})
		slog.Warn("delivery dead-lettered",
			"delivery_id", deliveryID,
			"attempt", details.AttemptNumber,
			"http_status", httpStatus,
			"permanent", isPermanent,
		)
		return
	}

	// Transient failure: schedule retry with exponential backoff + full jitter
	delay := nextDelay(int(details.AttemptNumber))
	nextAt := time.Now().Add(delay)

	d.q.UpdateDeliveryFailed(ctx, models.UpdateDeliveryFailedParams{ //nolint
		ID:               id,
		Status:           models.DeliveryStatusFailed,
		LastHttpStatus:   httpPtr,
		LastResponseBody: bodyPtr,
		LastError:        errPtr,
		NextAttemptAt:    pgtype.Timestamptz{Time: nextAt, Valid: true},
	})

	if err := d.scheduler.Schedule(ctx, deliveryID, nextAt); err != nil {
		slog.Error("failed to schedule retry — DB next_attempt_at is the fallback",
			"delivery_id", deliveryID, "err", err)
	}

	slog.Info("delivery failed, retry scheduled",
		"delivery_id", deliveryID,
		"attempt", details.AttemptNumber,
		"next_at", nextAt.Format(time.RFC3339),
		"http_status", httpStatus,
	)
}

// nextDelay returns a retry delay using exponential backoff with full jitter.
// Full jitter (AWS recommendation) breaks synchronisation across concurrent failures.
func nextDelay(attempt int) time.Duration {
	ceiling := float64(1000) * math.Pow(2, float64(attempt))
	if ceiling > 3_600_000 {
		ceiling = 3_600_000 // cap at 1 hour in ms
	}
	return time.Duration(rand.Float64()*ceiling) * time.Millisecond
}
