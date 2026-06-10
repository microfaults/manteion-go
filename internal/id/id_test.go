package id

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestNew_PrefixAndUUIDv7(t *testing.T) {
	got := New("exp")
	if !strings.HasPrefix(got, "exp-") {
		t.Fatalf("id %q missing prefix", got)
	}
	u, err := uuid.Parse(strings.TrimPrefix(got, "exp-"))
	if err != nil {
		t.Fatalf("suffix of %q is not a uuid: %v", got, err)
	}
	if u.Version() != 7 {
		t.Errorf("uuid version = %d, want 7", u.Version())
	}
}

func TestNew_TimeOrdered(t *testing.T) {
	a := New("rule")
	b := New("rule")
	if !(a < b) {
		t.Errorf("uuidv7 ids should sort by mint order: %q !< %q", a, b)
	}
}
