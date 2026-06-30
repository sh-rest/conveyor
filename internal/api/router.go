package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/sh-rest/conveyor/internal/api/handler"
	"github.com/sh-rest/conveyor/internal/api/middleware"
	"github.com/sh-rest/conveyor/internal/models"
)

type Handlers struct {
	Project  *handler.ProjectHandler
	Endpoint *handler.EndpointHandler
	Event    *handler.EventHandler
	Delivery *handler.DeliveryHandler
	Metrics  *handler.MetricsHandler
	Health   *handler.HealthHandler
	Verify   *handler.VerifyHandler
}

func NewRouter(h Handlers, q *models.Queries) http.Handler {
	r := chi.NewRouter()

	r.Use(chimiddleware.RequestID)
	r.Use(chimiddleware.RealIP)
	r.Use(chimiddleware.Logger)
	r.Use(chimiddleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	r.Get("/readyz", h.Health.Readyz)

	r.Post("/v1/projects", h.Project.Create)

	r.Group(func(r chi.Router) {
		r.Use(middleware.Auth(q))

		// Projects
		r.Get("/v1/projects/me", h.Project.Me)
		r.Post("/v1/projects/me/rotate-key", h.Project.RotateKey)

		// Endpoints
		r.Post("/v1/endpoints", h.Endpoint.Create)
		r.Get("/v1/endpoints", h.Endpoint.List)
		r.Get("/v1/endpoints/{id}", h.Endpoint.Get)
		r.Patch("/v1/endpoints/{id}", h.Endpoint.Update)
		r.Delete("/v1/endpoints/{id}", h.Endpoint.Delete)
		r.Post("/v1/endpoints/{id}/test", h.Endpoint.Test)

		// Events
		r.Post("/v1/events", h.Event.Ingest)
		r.Get("/v1/events", h.Event.List)
		r.Get("/v1/events/{id}", h.Event.Get)
		r.Get("/v1/events/{id}/deliveries", h.Event.ListDeliveries)
		r.Post("/v1/events/{id}/replay", h.Event.Replay)

		// Deliveries
		r.Get("/v1/deliveries/dead-letter", h.Delivery.ListDeadLettered)
		r.Post("/v1/deliveries/{id}/replay", h.Delivery.Replay)
		r.Get("/v1/deliveries/{id}/attempts", h.Delivery.ListAttempts)

		// Metrics
		r.Get("/v1/metrics/summary", h.Metrics.Summary)

		// Signature verification
		r.Post("/v1/verify", h.Verify.Verify)
	})

	return r
}
