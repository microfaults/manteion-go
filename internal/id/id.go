// Package id mints the platform's entity identifiers:
// "{prefix}-{uuidv7}" stored as TEXT.
//
// The prefix is type-discriminating (exp-, phase-, rule-, spec-, comp-,
// wf-, atk-, atkres-, fc-, policy-, anchor-, fevt-) so ids are self-describing in
// logs and URLs; UUIDv7 makes them time-ordered, so primary-key indexes
// stay append-local and creation order sorts lexicographically. Ids that
// reference zeus-minted objects (zeus attack/run ids) are opaque TEXT and
// never minted here.
package id

import "github.com/google/uuid"

// New returns "{prefix}-{uuidv7}", e.g. "exp-0190c2f4-95b4-7cc3-…".
func New(prefix string) string {
	u, err := uuid.NewV7()
	if err != nil {
		// Only on entropy failure; v4 keeps uniqueness (loses ordering).
		u = uuid.New()
	}
	return prefix + "-" + u.String()
}
