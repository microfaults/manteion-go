// Error envelope used across all manteion HTTP handlers.
// swag annotations reference api.ErrorResponse for 4xx/5xx documentation.
//
// Standard shape (subject to future migration to RFC 9457 problem+json
// per manteion-ui/docs/API-NEEDED.md §C.5).
package api

// ErrorResponse is the JSON body returned for all error responses.
//
// Example: {"error": "rule: invalid mode \"foo\""}
type ErrorResponse struct {
	Error string `json:"error" example:"validation failed"`
}
