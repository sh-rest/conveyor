package handler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/sh-rest/conveyor/internal/api/middleware"
	"github.com/sh-rest/conveyor/internal/respond"
	"github.com/sh-rest/conveyor/internal/models"
)

// ProjectQuerier is the DB interface this handler depends on.
type ProjectQuerier interface {
	CreateProject(ctx context.Context, arg models.CreateProjectParams) (models.Project, error)
	GetProjectByAPIKey(ctx context.Context, apiKey string) (models.Project, error)
	UpdateProjectAPIKey(ctx context.Context, arg models.UpdateProjectAPIKeyParams) (models.Project, error)
}

type ProjectHandler struct {
	q         ProjectQuerier
	keyPrefix string
}

func NewProjectHandler(q ProjectQuerier, keyPrefix string) *ProjectHandler {
	return &ProjectHandler{q: q, keyPrefix: keyPrefix}
}

type createProjectRequest struct {
	Name string `json:"name"`
}

func (h *ProjectHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req createProjectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		respond.Error(w, http.StatusBadRequest, "name is required")
		return
	}

	apiKey, err := generateAPIKey(h.keyPrefix)
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to generate API key")
		return
	}

	project, err := h.q.CreateProject(r.Context(), models.CreateProjectParams{
		Name:   req.Name,
		ApiKey: apiKey,
	})
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to create project")
		return
	}

	respond.JSON(w, http.StatusCreated, project)
}

func (h *ProjectHandler) Me(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	respond.JSON(w, http.StatusOK, project)
}

func (h *ProjectHandler) RotateKey(w http.ResponseWriter, r *http.Request) {
	project, ok := middleware.ProjectFromContext(r.Context())
	if !ok {
		respond.Error(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	newKey, err := generateAPIKey(h.keyPrefix)
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to generate key")
		return
	}

	updated, err := h.q.UpdateProjectAPIKey(r.Context(), models.UpdateProjectAPIKeyParams{
		ID:     project.ID,
		ApiKey: newKey,
	})
	if err != nil {
		respond.Error(w, http.StatusInternalServerError, "failed to rotate key")
		return
	}

	respond.JSON(w, http.StatusOK, updated)
}

// generateAPIKey produces a cryptographically random key like "whk_live_a3f9c2..."
// crypto/rand is used (not math/rand) — never use math/rand for secrets.
func generateAPIKey(prefix string) (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(b), nil
}
