package orchestrator

import (
	"errors"
	"fmt"
	"net/url"
)

// Typed refusals. The API maps these onto its error envelope (see
// internal/api/error_response.go); everything else the orchestrator returns
// is either a wrapped store sentinel (ErrNotFound) or an unexpected failure.
// Every message here is the log line the untyped error used to carry.

// ErrServiceOverlap is the admission-control refusal (INV-5, MANT-5): the
// candidate experiment's service footprint intersects a running
// experiment's. RunningExperimentID names the holder; Services is the sorted
// intersection. The API maps it to 409 service_overlap.
type ErrServiceOverlap struct {
	ExperimentID        string   // the experiment that was refused
	RunningExperimentID string   // the running experiment holding the services
	Services            []string // shared services, sorted
}

func (e *ErrServiceOverlap) Error() string {
	return fmt.Sprintf("experiment %q cannot start: services %v overlap running experiment %q; "+
		"wait for it to finish or stop it", e.ExperimentID, e.Services, e.RunningExperimentID)
}

// ErrInvalidState is a lifecycle refusal: the experiment or phase is not in a
// status the requested transition accepts (start needs planned, pause needs
// running, a phase start needs pending or paused, ...). Current is the status
// observed — "" only when the row could not be re-read after a lost CAS race
// — and Wanted names what the operation needs. Msg is the log line. The API
// maps it to 409 invalid_state and surfaces Current as "status".
type ErrInvalidState struct {
	Kind    string // "experiment" or "phase"
	ID      string
	Current string
	Wanted  string
	Msg     string
}

func (e *ErrInvalidState) Error() string { return e.Msg }

// invalidState builds an ErrInvalidState whose message is exactly msg.
func invalidState(kind, id, current, wanted, msg string) *ErrInvalidState {
	return &ErrInvalidState{Kind: kind, ID: id, Current: current, Wanted: wanted, Msg: msg}
}

// ErrZeusUnreachable wraps a transport-level failure talking to zeus
// (connection refused, DNS, timeout) during a phase start. A zeus response —
// even a 5xx — is a rejection, not "unreachable", and stays a plain error.
// The API maps it to 502 zeus_unreachable.
type ErrZeusUnreachable struct {
	Op  string // what was being attempted, e.g. `register workflow "wf-1" in zeus`
	Err error  // the zeus client error, wrapping the *url.Error
}

func (e *ErrZeusUnreachable) Error() string { return e.Op + ": " + e.Err.Error() }
func (e *ErrZeusUnreachable) Unwrap() error { return e.Err }

// zeusError wraps a zeus client error under op: ErrZeusUnreachable when the
// cause is a transport failure (the client wraps http.Client's *url.Error
// with %w), a plain "<op>: <err>" otherwise.
func zeusError(op string, err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return &ErrZeusUnreachable{Op: op, Err: err}
	}
	return fmt.Errorf("%s: %w", op, err)
}

// ErrValidation tags a refusal caused by the request or the plan itself — an
// unknown final status, an experiment with no phases — rather than by state:
// the caller must change what it asked for, not wait. errors.Is(err,
// ErrValidation) reports it; the API maps it to 422 validation.
var ErrValidation = errors.New("orchestrator: validation")

// validationError tags err with ErrValidation without altering its text.
func validationError(err error) error { return &taggedError{err: err, tag: ErrValidation} }

// taggedError is a message-preserving wrapper: Error and Unwrap are the
// wrapped error's; Is additionally matches tag.
type taggedError struct {
	err error
	tag error
}

func (e *taggedError) Error() string        { return e.err.Error() }
func (e *taggedError) Unwrap() error        { return e.err }
func (e *taggedError) Is(target error) bool { return target == e.tag }
