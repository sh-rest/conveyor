package handler_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sh-rest/conveyor/internal/api/handler"
	"github.com/sh-rest/conveyor/internal/signing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerify(t *testing.T) {
	h := handler.NewVerifyHandler()

	call := func(t *testing.T, body map[string]string) *httptest.ResponseRecorder {
		t.Helper()
		b, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/v1/verify", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.Verify(w, req)
		return w
	}

	t.Run("valid signature", func(t *testing.T) {
		payload := `{"order_id":42}`
		secret := "my-secret"
		sig := signing.Sign(secret, []byte(payload))

		w := call(t, map[string]string{"payload": payload, "signature": sig, "secret": secret})
		require.Equal(t, http.StatusOK, w.Code)
		var resp map[string]bool
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.True(t, resp["valid"])
	})

	t.Run("wrong secret", func(t *testing.T) {
		payload := `{"order_id":42}`
		sig := signing.Sign("correct-secret", []byte(payload))

		w := call(t, map[string]string{"payload": payload, "signature": sig, "secret": "wrong-secret"})
		require.Equal(t, http.StatusOK, w.Code)
		var resp map[string]bool
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.False(t, resp["valid"])
	})

	t.Run("tampered payload", func(t *testing.T) {
		sig := signing.Sign("secret", []byte("original"))

		w := call(t, map[string]string{"payload": "tampered", "signature": sig, "secret": "secret"})
		require.Equal(t, http.StatusOK, w.Code)
		var resp map[string]bool
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.False(t, resp["valid"])
	})

	t.Run("bad signature format", func(t *testing.T) {
		w := call(t, map[string]string{"payload": "data", "signature": "not-a-sig", "secret": "secret"})
		require.Equal(t, http.StatusOK, w.Code)
		var resp map[string]bool
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.False(t, resp["valid"])
	})

	t.Run("missing payload", func(t *testing.T) {
		w := call(t, map[string]string{"signature": "sha256=abc", "secret": "secret"})
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("missing signature", func(t *testing.T) {
		w := call(t, map[string]string{"payload": "data", "secret": "secret"})
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("missing secret", func(t *testing.T) {
		w := call(t, map[string]string{"payload": "data", "signature": "sha256=abc"})
		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("content-type is application/json", func(t *testing.T) {
		payload := `{}`
		sig := signing.Sign("s", []byte(payload))
		w := call(t, map[string]string{"payload": payload, "signature": sig, "secret": "s"})
		assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	})
}
