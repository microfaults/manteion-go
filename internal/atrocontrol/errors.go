package atrocontrol

import (
	"fmt"
	"time"
)

type FanoutResult struct {
	Targeted []string
	OK       []string
	Failed   []PerInstanceError
	Duration time.Duration
}

func (r FanoutResult) AllSucceeded() bool { return len(r.Failed) == 0 }
func (r FanoutResult) AnySucceeded() bool { return len(r.OK) > 0 }

type PerInstanceError struct {
	InstanceID string
	Address    string
	Err        error
}

func (e PerInstanceError) Error() string {
	return fmt.Sprintf("instance %s (%s): %v", e.InstanceID, e.Address, e.Err)
}
