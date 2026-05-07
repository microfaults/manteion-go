package atrocontrol

import (
	"context"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

// PreloadEntries fans out a batch of cache entries to all live SDK instances
// of the given service.
func (c *Controller) PreloadEntries(ctx context.Context, service string, entries []atroposdk.CacheBoxWireEntry, opts ...CallOption) (FanoutResult, error) {
	co := c.resolveCallOpts(opts)

	targets, err := c.resolveTargets(ctx, service, co.filter)
	if err != nil {
		return FanoutResult{}, err
	}

	result := fanout(ctx, targets, func(ctx context.Context, t target) error {
		return c.tx.PostCacheEntries(ctx, t.address, entries)
	}, co)

	c.logFanout("preload_entries", service, co.runID, result)
	return result, nil
}
