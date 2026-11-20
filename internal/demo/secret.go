package demo

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
)

// redactedText is all a Secret ever shows of itself.
const redactedText = "[redacted]"

// Secret holds the edge secret. Printing it with any fmt verb, logging it
// through slog, or encoding it as JSON or text yields "[redacted]". The value
// sits behind a *string, so even where fmt cannot call these methods (a Config
// in an unexported field of a struct being printed, or a verb fmt rejects) it
// shows only an address: fmt expands a pointer to a struct, slice, array or
// map, but never a pointer to a string.
// Matches compares a candidate in constant time; Reveal is the one accessor
// that returns the value. The zero Secret is unset and matches nothing.
type Secret struct {
	box *string
}

// NewSecret wraps s.
func NewSecret(s string) Secret {
	if s == "" {
		return Secret{}
	}
	return Secret{box: &s}
}

// IsSet reports whether the secret holds a non-empty value.
func (s Secret) IsSet() bool { return s.box != nil && *s.box != "" }

// Reveal returns the secret's value. It exists for the edge-secret check
// only; prefer Matches, and never pass the result anywhere else.
func (s Secret) Reveal() string { return s.value() }

func (s Secret) value() string {
	if s.box == nil {
		return ""
	}
	return *s.box
}

// Matches reports, in constant time, whether candidate equals the secret. An
// unset secret matches nothing, not even "".
func (s Secret) Matches(candidate string) bool {
	if !s.IsSet() {
		return false
	}
	want := sha256.Sum256([]byte(*s.box))
	got := sha256.Sum256([]byte(candidate))
	return subtle.ConstantTimeCompare(want[:], got[:]) == 1
}

// Equal reports whether both secrets hold the same value.
func (s Secret) Equal(o Secret) bool {
	return subtle.ConstantTimeCompare([]byte(s.value()), []byte(o.value())) == 1
}

// String implements fmt.Stringer.
func (s Secret) String() string { return redactedText }

// GoString implements fmt.GoStringer.
func (s Secret) GoString() string { return redactedText }

// Format implements fmt.Formatter for every verb and flag.
func (s Secret) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, redactedText) }

// LogValue implements slog.LogValuer.
func (s Secret) LogValue() slog.Value { return slog.StringValue(redactedText) }

// MarshalJSON implements json.Marshaler.
func (s Secret) MarshalJSON() ([]byte, error) { return json.Marshal(redactedText) }

// MarshalText implements encoding.TextMarshaler.
func (s Secret) MarshalText() ([]byte, error) { return []byte(redactedText), nil }
