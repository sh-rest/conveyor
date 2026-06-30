package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/sh-rest/conveyor/internal/models"
	"github.com/sh-rest/conveyor/internal/respond"
)

type contextKey string

const projectCtxKey contextKey = "project"

// Querier is the subset of DB methods the auth middleware needs.
// Using an interface here (not the concrete *models.Queries) makes
// this middleware testable without a real database.
type Querier interface {
	GetProjectByAPIKey(ctx context.Context, apiKey string) (models.Project, error)
}

func Auth(q Querier) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				respond.Error(w, http.StatusUnauthorized, "missing Authorization header")
				return
			}

			// Expect: "Bearer whk_live_xxxx"
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || parts[0] != "Bearer" {
				respond.Error(w, http.StatusUnauthorized, "invalid Authorization format, expected: Bearer <key>")
				return
			}

			project, err := q.GetProjectByAPIKey(r.Context(), parts[1])
			if err != nil {
				respond.Error(w, http.StatusUnauthorized, "invalid API key")
				return
			}

			// Store project in request context so handlers can access it
			ctx := context.WithValue(r.Context(), projectCtxKey, project)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ProjectFromContext retrieves the authenticated project from the request context.
// Call this in any handler that's behind the Auth middleware.
func ProjectFromContext(ctx context.Context) (models.Project, bool) {
	p, ok := ctx.Value(projectCtxKey).(models.Project)
	return p, ok
}
