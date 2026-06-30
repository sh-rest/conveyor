package handler

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"time"

	chi "github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sh-rest/conveyor/internal/api/middleware"
	"github.com/sh-rest/conveyor/internal/models"
	"github.com/sh-rest/conveyor/internal/respond"
	"github.com/sh-rest/conveyor/internal/signing"
)

type EndpointQuerier interface {
	CreateEndpoint(ctx context.Context, arg models.CreateEndpointParams) (models.Endpoint, error)
	GetEndpointByID(ctx context.Context, arg models.GetEndpointByIDParams) (models.Endpoint, error)
	ListEndpointsByProject(ctx context.Context, projectID pgtype.UUID) ([]models.Endpoint, error)
	UpdateEndpoint(ctx context.Context, arg models.UpdateEndpointParams) (models.Endpoint, error)
	DeleteEndpoint(ctx context.Context, arg models.DeleteEndpointParams) error
}

type EndpointHandler struct {
	q EndpointQuerier
}

func NewEndpointHandler(q EndpointQuerier) *EndpointHandler {
	return &EndpointHandler{q: q}
}

type createEndpointRequest struct {
	URL          string `json:"url"`
	Description  string `json:"description"`
	RateLimitRPS int32  `json:"rate_limit_rps"`
	TimeoutMs    int32  `json:"timeout_ms"`
}

func (h *EndpointHandler) Create(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var req createEndpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		respond.Error(w, http.StatusBadRequest, "url is required")
		return
	}

	if req.RateLimitRPS == 0 {
		req.RateLimitRPS = 10
	}
	if req.TimeoutMs == 0 {
		req.TimeoutMs = 5000
	}

	secret, err := generateSecret()
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to generate secret")
		return
	}

	endpoint, err := h.q.CreateEndpoint(r.Context(), models.CreateEndpointParams{
		ProjectID:    project.ID,
		Url:          req.URL,
		Description:  nullableString(req.Description),
		Secret:       secret,
		RateLimitRps: req.RateLimitRPS,
		TimeoutMs:    req.TimeoutMs,
	})
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to create endpoint")
		return
	}

	respond.JSON(w, http.StatusCreated, endpoint)
}

func (h *EndpointHandler) List(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	endpoints, err := h.q.ListEndpointsByProject(r.Context(), project.ID)
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to list endpoints")
		return
	}

	respond.JSON(w, http.StatusOK, map[string]any{"data": endpoints})
}

func (h *EndpointHandler) Get(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var id pgtype.UUID
	if err := id.Scan(chi.URLParam(r, "id")); err != nil {
		respond.Error(w, http.StatusBadRequest, "invalid endpoint id")
		return
	}

	endpoint, err := h.q.GetEndpointByID(r.Context(), models.GetEndpointByIDParams{
		ID: id, ProjectID: project.ID,
	})
	if err != nil {
		respond.Error(w, http.StatusNotFound, "endpoint not found")
		return
	}

	respond.JSON(w, http.StatusOK, endpoint)
}

func (h *EndpointHandler) Delete(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var id pgtype.UUID
	if err := id.Scan(chi.URLParam(r, "id")); err != nil {
		respond.Error(w, http.StatusBadRequest, "invalid endpoint id")
		return
	}

	if err := h.q.DeleteEndpoint(r.Context(), models.DeleteEndpointParams{
		ID: id, ProjectID: project.ID,
	}); err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to delete endpoint")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type updateEndpointRequest struct {
	URL          *string `json:"url"`
	Description  *string `json:"description"`
	IsActive     *bool   `json:"is_active"`
	RateLimitRPS *int32  `json:"rate_limit_rps"`
	TimeoutMs    *int32  `json:"timeout_ms"`
}

func (h *EndpointHandler) Update(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var id pgtype.UUID
	if err := id.Scan(chi.URLParam(r, "id")); err != nil {
		respond.Error(w, http.StatusBadRequest, "invalid endpoint id")
		return
	}

	var req updateEndpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respond.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	var url *string
	if req.URL != nil {
		url = req.URL
	}

	endpoint, err := h.q.UpdateEndpoint(r.Context(), models.UpdateEndpointParams{
		ID:           id,
		ProjectID:    project.ID,
		Url:          url,
		Description:  req.Description,
		IsActive:     req.IsActive,
		RateLimitRps: req.RateLimitRPS,
		TimeoutMs:    req.TimeoutMs,
	})
	if err != nil {
		respond.Error(w, http.StatusNotFound, "endpoint not found")
		return
	}

	respond.JSON(w, http.StatusOK, endpoint)
}

type testResult struct {
	EndpointID string `json:"endpoint_id"`
	HTTPStatus int    `json:"http_status"`
	Body       string `json:"body"`
	Delivered  bool   `json:"delivered"`
	DurationMs int64  `json:"duration_ms"`
}

func (h *EndpointHandler) Test(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var id pgtype.UUID
	if err := id.Scan(chi.URLParam(r, "id")); err != nil {
		respond.Error(w, http.StatusBadRequest, "invalid endpoint id")
		return
	}

	endpoint, err := h.q.GetEndpointByID(r.Context(), models.GetEndpointByIDParams{
		ID: id, ProjectID: project.ID,
	})
	if err != nil {
		respond.Error(w, http.StatusNotFound, "endpoint not found")
		return
	}

	payload := []byte(`{"test":true,"source":"conveyor-test"}`)
	signature := signing.Sign(endpoint.Secret, payload)
	timeout := time.Duration(endpoint.TimeoutMs) * time.Millisecond

	reqCtx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	start := time.Now()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint.Url, bytes.NewReader(payload))
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to build request")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Conveyor-Signature", signature)
	req.Header.Set("X-Conveyor-Event-Type", "test")

	client := &http.Client{}
	resp, err := client.Do(req)
	elapsed := time.Since(start).Milliseconds()
	if err != nil {
		respond.JSON(w, http.StatusOK, testResult{
			EndpointID: uuidToString(endpoint.ID),
			Delivered:  false,
			DurationMs: elapsed,
			Body:       err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	respond.JSON(w, http.StatusOK, testResult{
		EndpointID: uuidToString(endpoint.ID),
		HTTPStatus: resp.StatusCode,
		Body:       string(body),
		Delivered:  resp.StatusCode >= 200 && resp.StatusCode < 300,
		DurationMs: elapsed,
	})
}

func generateSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "whsec_" + hex.EncodeToString(b), nil
}
