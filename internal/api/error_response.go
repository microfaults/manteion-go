// Error envelope used across all manteion HTTP handlers.
// swag annotations reference api.ErrorResponse for 4xx/5xx documentation.
//
// Standard shape (subject to future migration to RFC 9457 problem+json
// per manteion-ui/docs/API-NEEDED.md §C.5).
//
// The experiment and phase handlers add a machine-readable "code" (and
// code-specific fields) on top of "error" — see writeErrorCode and the code
// constants. "error" is always present and is what expctl and the UI's
// generic path read; "code" is what the UI branches on.
package api

import (
	"errors"
	"net/http"

	"manteion-go/internal/orchestrator"
	"manteion-go/internal/store"
)

// ErrorResponse is the JSON body returned for all error responses.
//
// Example: {"error": "rule: invalid mode \"foo\""}
type ErrorResponse struct {
	Error string `json:"error" example:"validation failed"`
}

// Error codes carried in the envelope's "code" field by the experiment and
// phase handlers, with the status they ride on.
const (
	codeBadJSON         = "bad_json"         // 400: the body is not valid JSON
	codeValidation      = "validation"       // 422: well-formed, but the request or plan is invalid
	codeNotFound        = "not_found"        // 404
	codeConflict        = "conflict"         // 409: unique collision (experiment id, phase name/position)
	codeInvalidState    = "invalid_state"    // 409: lifecycle refusal; "status" carries the current status
	codeServiceOverlap  = "service_overlap"  // 409: admission control; "running_experiment_id", "services"
	codeZeusUnreachable = "zeus_unreachable" // 502: transport failure talking to zeus during a start
	codeInternal        = "internal"         // 500
)

// writeErrorCode writes the coded envelope: {"error": message, "code": code,
// ...extra}. The envelope's own keys win over extras.
func writeErrorCode(w http.ResponseWriter, status int, code, message string, extra map[string]any) {
	body := make(map[string]any, len(extra)+2)
	for k, v := range extra {
		body[k] = v
	}
	body["error"] = message
	body["code"] = code
	writeJSON(w, status, body)
}

// errorEnvelope classifies an error from the store or the orchestrator into
// the envelope's (status, code, extra). The typed orchestrator errors come
// first, then the store sentinels, then the validation tags; anything else is
// 500 internal.
func errorEnvelope(err error) (status int, code string, extra map[string]any) {
	var overlap *orchestrator.ErrServiceOverlap
	var state *orchestrator.ErrInvalidState
	var zeusDown *orchestrator.ErrZeusUnreachable
	switch {
	case errors.As(err, &overlap):
		return http.StatusConflict, codeServiceOverlap, map[string]any{
			"running_experiment_id": overlap.RunningExperimentID,
			"services":              nilToEmpty(overlap.Services),
		}
	case errors.As(err, &state):
		return http.StatusConflict, codeInvalidState, map[string]any{"status": state.Current}
	case errors.As(err, &zeusDown):
		return http.StatusBadGateway, codeZeusUnreachable, nil
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound, codeNotFound, nil
	case errors.Is(err, store.ErrConflict):
		return http.StatusConflict, codeConflict, nil
	case errors.Is(err, store.ErrValidation), errors.Is(err, orchestrator.ErrValidation):
		return http.StatusUnprocessableEntity, codeValidation, nil
	}
	return http.StatusInternalServerError, codeInternal, nil
}

// writeErrorFor writes the envelope for an error from a store or orchestrator
// call made on behalf of resource ("experiment", "phase"): a missing row is
// "<resource> not found", a classified refusal surfaces its own text, and an
// unclassified one is 500 with unexpected as the message — the lifecycle
// handlers pass err.Error() (an operator needs to see why a start failed),
// the CRUD handlers a generic line with the cause in the log.
func writeErrorFor(w http.ResponseWriter, err error, resource, unexpected string) {
	status, code, extra := errorEnvelope(err)
	msg := err.Error()
	switch code {
	case codeNotFound:
		msg = resource + " not found"
	case codeInternal:
		msg = unexpected
	}
	writeErrorCode(w, status, code, msg, extra)
}

// writeMappedError classifies err and writes the envelope with err's text as
// the message. The lifecycle handlers have always surfaced the orchestrator's
// message verbatim — an operator needs to see why a start was refused — so
// the 500 fallback keeps it too.
func writeMappedError(w http.ResponseWriter, err error) {
	status, code, extra := errorEnvelope(err)
	writeErrorCode(w, status, code, err.Error(), extra)
}
