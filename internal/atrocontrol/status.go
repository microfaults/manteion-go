package atrocontrol

import (
	"context"
	"sync"

	atroposdk "atropos-go"
)

type ServiceStatus struct {
	Service   string
	Instances []InstanceStatus
}

type InstanceStatus struct {
	InstanceID string
	Address    string
	Fault      *atroposdk.FaultStatus
	Rules      []atroposdk.StaticRule
	CacheBox   *atroposdk.CacheBoxStats
	Err        error
}

func (c *Controller) StatusByService(ctx context.Context, service string, opts ...CallOption) (ServiceStatus, error) {
	co := c.resolveCallOpts(opts)
	targets, err := c.resolveTargets(ctx, service, co.filter)
	if err != nil {
		return ServiceStatus{}, err
	}

	statuses := make([]InstanceStatus, len(targets))
	var wg sync.WaitGroup
	sem := make(chan struct{}, co.concurrency)

	for i, t := range targets {
		statuses[i] = InstanceStatus{InstanceID: t.instanceID, Address: t.address}
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			opCtx := ctx
			if co.timeout > 0 {
				var cancel context.CancelFunc
				opCtx, cancel = context.WithTimeout(ctx, co.timeout)
				defer cancel()
			}

			fault, faultErr := c.tx.GetFault(opCtx, t.address)
			if faultErr == nil {
				statuses[i].Fault = &fault
			}

			rules, rulesErr := c.tx.GetRules(opCtx, t.address)
			if rulesErr == nil {
				statuses[i].Rules = rules
			}

			stats, statsErr := c.tx.GetCacheBoxStats(opCtx, t.address)
			if statsErr == nil {
				statuses[i].CacheBox = &stats
			}

			// Report the first error encountered, if any.
			for _, e := range []error{faultErr, rulesErr, statsErr} {
				if e != nil {
					statuses[i].Err = e
					break
				}
			}
		}(i, t)
	}

	wg.Wait()
	return ServiceStatus{Service: service, Instances: statuses}, nil
}
