package handler

import (
	"encoding/json"
	"net/http"

	"github.com/sh-rest/conveyor/internal/respond"
	"github.com/sh-rest/conveyor/internal/signing"
)

type VerifyHandler struct{}

func NewVerifyHandler() *VerifyHandler { return &VerifyHandler{} }

func (h *VerifyHandler) Verify(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Payload   string `json:"payload"`
		Signature string `json:"signature"`
		Secret    string `json:"secret"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respond.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Payload == "" || req.Signature == "" || req.Secret == "" {
		respond.Error(w, http.StatusBadRequest, "payload, signature, and secret are required")
		return
	}

	valid := signing.Verify(req.Secret, []byte(req.Payload), req.Signature)
	respond.JSON(w, http.StatusOK, map[string]bool{"valid": valid})
}
