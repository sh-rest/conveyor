package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// Sign produces an HMAC-SHA256 signature of body using secret.
// Format: "sha256=<hex>" — matches Stripe/GitHub convention.
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// Verify checks a signature in constant time to prevent timing attacks.
// Never use == for comparing signatures.
func Verify(secret string, body []byte, signature string) bool {
	expected := Sign(secret, body)
	return hmac.Equal([]byte(expected), []byte(signature))
}
