package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	chi "github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sh-rest/conveyor/internal/api/middleware"
	"github.com/sh-rest/conveyor/internal/models"
	"github.com/sh-rest/conveyor/internal/respond"
)

// EventQuerier is the DB interface this handler depends on.
type EventQuerier interface {
	CreateEvent(ctx context.Context, arg models.CreateEventParams) (models.Event, error)
	GetEventByID(ctx context.Context, arg models.GetEventByIDParams) (models.Event, error)
	GetEventByIdempotencyKey(ctx context.Context, arg models.GetEventByIdempotencyKeyParams) (models.Event, error)
	ListEventsByProject(ctx context.Context, arg models.ListEventsByProjectParams) ([]models.Event, error)
	ListActiveEndpointsByProject(ctx context.Context, projectID pgtype.UUID) ([]models.Endpoint, error)
	CreateDelivery(ctx context.Context, arg models.CreateDeliveryParams) (models.Delivery, error)
	ListDeliveriesByEvent(ctx context.Context, eventID pgtype.UUID) ([]models.Delivery, error)
	ResetDelivery(ctx context.Context, id pgtype.UUID) (models.Delivery, error)
}

// Enqueuer pushes a delivery ID into the Redis Stream for the worker.
type Enqueuer interface {
	Enqueue(ctx context.Context, deliveryID string) error
}

type EventHandler struct {
	q        EventQuerier
	producer Enqueuer
}

func NewEventHandler(q EventQuerier, producer Enqueuer) *EventHandler {
	return &EventHandler{q: q, producer: producer}
}

type ingestRequest struct {
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
	Headers        json.RawMessage `json:"headers"`
}

// eventResponse renders payload and headers as JSON objects, not base64.
// Go's json.Marshal encodes []byte as base64 — using json.RawMessage avoids that.
type eventResponse struct {
	ID             string          `json:"id"`
	ProjectID      string          `json:"project_id"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	Headers        json.RawMessage `json:"headers"`
	IngestedAt     string          `json:"ingested_at"`
}

func toEventResponse(e models.Event) eventResponse {
	return eventResponse{
		ID:             uuidToString(e.ID),
		ProjectID:      uuidToString(e.ProjectID),
		IdempotencyKey: e.IdempotencyKey,
		EventType:      e.EventType,
		Payload:        json.RawMessage(e.Payload),
		Headers:        json.RawMessage(e.Headers),
		IngestedAt:     e.IngestedAt.Time.Format("2006-01-02T15:04:05.999999Z07:00"),
	}
}

func (h *EventHandler) Ingest(w http.ResponseWriter, r *http.Request) {
	var req ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respond.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.EventType == "" {
		respond.Error(w, http.StatusBadRequest, "event_type is required")
		return
	}
	if len(req.Payload) == 0 {
		respond.Error(w, http.StatusBadRequest, "payload is required")
		return
	}

	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	// Idempotency check: if the caller provided a key, return the existing event
	// if we've already processed it. The DB unique partial index is a second safety net
	// for concurrent requests with the same key.
	if req.IdempotencyKey != "" {
		existing, err := h.q.GetEventByIdempotencyKey(r.Context(), models.GetEventByIdempotencyKeyParams{
			ProjectID:      project.ID,
			IdempotencyKey: &req.IdempotencyKey,
		})
		if err == nil {
			respond.JSON(w, http.StatusOK, toEventResponse(existing))
			return
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			respond.Error(w, http.StatusInternalServerError, "failed to check idempotency key")
			return
		}
		// pgx.ErrNoRows → not seen before, continue
	}

	headers := req.Headers
	if len(headers) == 0 {
		headers = json.RawMessage("{}")
	}

	event, err := h.q.CreateEvent(r.Context(), models.CreateEventParams{
		ProjectID:      project.ID,
		IdempotencyKey: nullableString(req.IdempotencyKey),
		EventType:      req.EventType,
		Payload:        req.Payload,
		Headers:        headers,
	})
	if err != nil {
		// Unique constraint violation: concurrent request with same idempotency key won the race.
		// Fetch and return the existing event rather than erroring.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && req.IdempotencyKey != "" {
			existing, fetchErr := h.q.GetEventByIdempotencyKey(r.Context(), models.GetEventByIdempotencyKeyParams{
				ProjectID:      project.ID,
				IdempotencyKey: &req.IdempotencyKey,
			})
			if fetchErr == nil {
				respond.JSON(w, http.StatusOK, toEventResponse(existing))
				return
			}
		}
		respond.Error(w, http.StatusInternalServerError, "failed to create event")
		return
	}

	// Fan out: one delivery per active endpoint.
	endpoints, err := h.q.ListActiveEndpointsByProject(r.Context(), project.ID)
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to fetch endpoints")
		return
	}

	for _, ep := range endpoints {
		delivery, err := h.q.CreateDelivery(r.Context(), models.CreateDeliveryParams{
			EventID:    event.ID,
			EndpointID: ep.ID,
		})
		if err != nil {
			slog.Error("failed to create delivery", "event_id", uuidToString(event.ID), "endpoint_id", uuidToString(ep.ID), "err", err)
			continue
		}
		if err := h.producer.Enqueue(r.Context(), uuidToString(delivery.ID)); err != nil {
			slog.Error("failed to enqueue delivery", "delivery_id", uuidToString(delivery.ID), "err", err)
			// Not fatal — the scheduler will find this delivery via next_attempt_at
		}
	}

	respond.JSON(w, http.StatusAccepted, toEventResponse(event))
}

func (h *EventHandler) Get(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var id pgtype.UUID
	if err := id.Scan(chi.URLParam(r, "id")); err != nil {
		respond.Error(w, http.StatusBadRequest, "invalid event id")
		return
	}

	event, err := h.q.GetEventByID(r.Context(), models.GetEventByIDParams{
		ID: id, ProjectID: project.ID,
	})
	if err != nil {
		respond.Error(w, http.StatusNotFound, "event not found")
		return
	}

	respond.JSON(w, http.StatusOK, toEventResponse(event))
}

func (h *EventHandler) List(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	limit := int32(20)
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.ParseInt(l, 10, 32); err == nil && n > 0 && n <= 100 {
			limit = int32(n)
		}
	}

	// cursor is the UUID of the last event on the previous page
	var cursor pgtype.UUID
	if c := r.URL.Query().Get("cursor"); c != "" {
		_ = cursor.Scan(c) // invalid cursor is silently ignored — returns from beginning
	}

	events, err := h.q.ListEventsByProject(r.Context(), models.ListEventsByProjectParams{
		ProjectID: project.ID,
		Column2:   r.URL.Query().Get("event_type"),
		Column3:   cursor,
		Limit:     limit,
	})
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to list events")
		return
	}

	resp := make([]eventResponse, len(events))
	for i, e := range events {
		resp[i] = toEventResponse(e)
	}

	var nextCursor string
	if len(events) == int(limit) {
		nextCursor = resp[len(resp)-1].ID
	}

	respond.JSON(w, http.StatusOK, map[string]any{
		"data":   resp,
		"cursor": nextCursor,
	})
}

func (h *EventHandler) ListDeliveries(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var id pgtype.UUID
	if err := id.Scan(chi.URLParam(r, "id")); err != nil {
		respond.Error(w, http.StatusBadRequest, "invalid event id")
		return
	}

	// Verify the event belongs to this project before listing its deliveries
	if _, err := h.q.GetEventByID(r.Context(), models.GetEventByIDParams{
		ID: id, ProjectID: project.ID,
	}); err != nil {
		respond.Error(w, http.StatusNotFound, "event not found")
		return
	}

	deliveries, err := h.q.ListDeliveriesByEvent(r.Context(), id)
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to list deliveries")
		return
	}

	respond.JSON(w, http.StatusOK, map[string]any{"data": deliveries})
}

func (h *EventHandler) Replay(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var id pgtype.UUID
	if err := id.Scan(chi.URLParam(r, "id")); err != nil {
		respond.Error(w, http.StatusBadRequest, "invalid event id")
		return
	}

	if _, err := h.q.GetEventByID(r.Context(), models.GetEventByIDParams{
		ID: id, ProjectID: project.ID,
	}); err != nil {
		respond.Error(w, http.StatusNotFound, "event not found")
		return
	}

	deliveries, err := h.q.ListDeliveriesByEvent(r.Context(), id)
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to list deliveries")
		return
	}

	var queued int
	for _, d := range deliveries {
		if _, err := h.q.ResetDelivery(r.Context(), d.ID); err != nil {
			slog.Error("failed to reset delivery for replay", "delivery_id", uuidToString(d.ID), "err", err)
			continue
		}
		if err := h.producer.Enqueue(r.Context(), uuidToString(d.ID)); err != nil {
			slog.Error("failed to enqueue replay delivery", "delivery_id", uuidToString(d.ID), "err", err)
			continue
		}
		queued++
	}

	respond.JSON(w, http.StatusAccepted, map[string]any{"queued": queued})
}
