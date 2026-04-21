package atrocontrol

import (
	"context"
	"sync"
	"time"
)

type target struct {
	instanceID string
	address    string
}

type opFn func(ctx context.Context, t target) error

func fanout(ctx context.Context, targets []target, op opFn, cfg callOpts) FanoutResult {
	start := time.Now()
	result := FanoutResult{
		Targeted: make([]string, len(targets)),
	}
	for i, t := range targets {
		result.Targeted[i] = t.instanceID
	}

	sem := make(chan struct{}, cfg.concurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, t := range targets {
		wg.Add(1)
		go func(t target) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			opCtx := ctx
			if cfg.timeout > 0 {
				var cancel context.CancelFunc
				opCtx, cancel = context.WithTimeout(ctx, cfg.timeout)
				defer cancel()
			}

			err := op(opCtx, t)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				result.Failed = append(result.Failed, PerInstanceError{
					InstanceID: t.instanceID,
					Address:    t.address,
					Err:        err,
				})
			} else {
				result.OK = append(result.OK, t.instanceID)
			}
		}(t)
	}

	wg.Wait()
	result.Duration = time.Since(start)
	return result
}
