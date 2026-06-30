package handler

import (
	"context"
	"net/http"
	"strconv"

	"github.com/sh-rest/conveyor/internal/api/middleware"
	"github.com/sh-rest/conveyor/internal/models"
	"github.com/sh-rest/conveyor/internal/respond"
)

type MetricsQuerier interface {
	DeliveryMetricsSummary(ctx context.Context, arg models.DeliveryMetricsSummaryParams) (models.DeliveryMetricsSummaryRow, error)
}

type MetricsHandler struct {
	q MetricsQuerier
}

func NewMetricsHandler(q MetricsQuerier) *MetricsHandler {
	return &MetricsHandler{q: q}
}

func (h *MetricsHandler) Summary(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	// ?hours=24 (default) — look back window
	hours := "24"
	if h := r.URL.Query().Get("hours"); h != "" {
		if n, err := strconv.ParseInt(h, 10, 64); err == nil && n > 0 && n <= 720 {
			hours = strconv.FormatInt(n, 10)
		}
	}

	summary, err := h.q.DeliveryMetricsSummary(r.Context(), models.DeliveryMetricsSummaryParams{
		ProjectID: project.ID,
		Column2:   ptrString(hours),
	})
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to fetch metrics")
		return
	}

	respond.JSON(w, http.StatusOK, map[string]any{
		"window_hours":  hours,
		"delivered":     summary.Delivered,
		"failed":        summary.Failed,
		"dead_lettered": summary.DeadLettered,
		"pending":       summary.Pending,
		"total":         summary.Total,
	})
}

func ptrString(s string) *string { return &s }
