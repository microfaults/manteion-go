package api

import (
	"net/http"
	"strconv"
)

// Pagination defaults shared by all list endpoints. Handlers honor
// these via parsePagination + writePage so the UI sees a stable envelope.
//
//	GET /api/v1/<resource>?limit=20&offset=0[&...filters...]
//
//	{
//	  "data": [...],
//	  "page": { "offset": 0, "limit": 20, "total": 142 }
//	}
//
// Unbounded reads are intentionally not allowed — `limit` is clamped to
// [1, defaultMaxPageLimit] so a single client can't pull millions of
// rows in one shot. Cursor pagination can be layered on later if needed.
const (
	defaultPageLimit = 20
	defaultMaxLimit  = 200
)

// PageEnvelope is the shape returned by every paginated list handler.
type PageEnvelope[T any] struct {
	Data []T  `json:"data"`
	Page Page `json:"page"`
}

// Page describes the current slice within the resource.
type Page struct {
	Offset int `json:"offset"`
	Limit  int `json:"limit"`
	Total  int `json:"total"`
}

// parsePagination reads `limit` and `offset` query params and clamps them
// to the supported range. Invalid integer values fall back to defaults.
func parsePagination(r *http.Request) (limit, offset int) {
	limit = defaultPageLimit
	offset = 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			limit = v
		}
	}
	if raw := r.URL.Query().Get("offset"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
			offset = v
		}
	}
	if limit > defaultMaxLimit {
		limit = defaultMaxLimit
	}
	if limit < 1 {
		limit = defaultPageLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

// writePage emits the standard page envelope. data must be a non-nil
// slice (use an empty slice for "no results" so the wire JSON is `[]`
// rather than `null`).
func writePage[T any](w http.ResponseWriter, status int, data []T, total, limit, offset int) {
	if data == nil {
		data = []T{}
	}
	writeJSON(w, status, PageEnvelope[T]{
		Data: data,
		Page: Page{Offset: offset, Limit: limit, Total: total},
	})
}
