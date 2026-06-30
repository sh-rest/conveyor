package handler

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

// nullableString converts a Go string to *string for pgx nullable columns.
// Empty string is treated as NULL.
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// uuidToString formats a pgtype.UUID as a standard UUID string.
func uuidToString(u pgtype.UUID) string {
	b := u.Bytes
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
