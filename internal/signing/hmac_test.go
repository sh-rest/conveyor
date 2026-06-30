package signing_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/sh-rest/conveyor/internal/signing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var hexPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func TestSign(t *testing.T) {
	cases := []struct {
		name   string
		secret string
		body   []byte
	}{
		{"format", "secret", []byte("hello world")},
		{"empty_body", "secret", []byte{}},
		{"empty_secret", "", []byte("payload")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sig := signing.Sign(tc.secret, tc.body)
			require.True(t, strings.HasPrefix(sig, "sha256="), "must start with sha256=")
			hex := strings.TrimPrefix(sig, "sha256=")
			assert.True(t, hexPattern.MatchString(hex), "hex part must be 64 lowercase hex chars, got %q", hex)
		})
	}

	t.Run("deterministic", func(t *testing.T) {
		a := signing.Sign("key", []byte("body"))
		b := signing.Sign("key", []byte("body"))
		assert.Equal(t, a, b)
	})

	t.Run("different_secrets", func(t *testing.T) {
		body := []byte("payload")
		assert.NotEqual(t, signing.Sign("keyA", body), signing.Sign("keyB", body))
	})

	t.Run("different_bodies", func(t *testing.T) {
		assert.NotEqual(t, signing.Sign("key", []byte("a")), signing.Sign("key", []byte("b")))
	})
}

func TestVerify(t *testing.T) {
	secret := "test-secret"
	body := []byte("test payload")

	t.Run("valid", func(t *testing.T) {
		assert.True(t, signing.Verify(secret, body, signing.Sign(secret, body)))
	})

	t.Run("wrong_secret", func(t *testing.T) {
		assert.False(t, signing.Verify("wrong", body, signing.Sign(secret, body)))
	})

	t.Run("tampered_body", func(t *testing.T) {
		sig := signing.Sign(secret, []byte("original"))
		assert.False(t, signing.Verify(secret, []byte("tampered"), sig))
	})

	t.Run("bad_format", func(t *testing.T) {
		assert.False(t, signing.Verify(secret, body, "not-a-valid-signature"))
	})

	t.Run("wrong_sig_same_length", func(t *testing.T) {
		wrong := []string{
			"sha256=0000000000000000000000000000000000000000000000000000000000000000",
			"sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"sha256=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		}
		for _, w := range wrong {
			assert.False(t, signing.Verify(secret, body, w), "should reject %q", w)
		}
	})
}
