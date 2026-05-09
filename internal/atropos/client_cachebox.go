package atropos

import (
	"context"
	"encoding/json"
	"net/http"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

func (c *Client) PostCacheBoxDelay(ctx context.Context, addr string, req atroposdk.DelayRequest) error {
	_, err := c.doExpectStatus(ctx, http.MethodPost, addr+"/admin/cachebox/delay", req, http.StatusNoContent)
	return err
}

func (c *Client) GetCacheBoxStats(ctx context.Context, addr string) (atroposdk.CacheBoxStats, error) {
	url := addr + "/admin/cachebox"
	data, err := c.doExpectStatus(ctx, http.MethodGet, url, nil, http.StatusOK)
	if err != nil {
		return atroposdk.CacheBoxStats{}, err
	}
	var stats atroposdk.CacheBoxStats
	if err := json.Unmarshal(data, &stats); err != nil {
		return atroposdk.CacheBoxStats{}, &TransportError{Method: "GET", URL: url, Err: err}
	}
	return stats, nil
}

func (c *Client) ClearCacheBox(ctx context.Context, addr string) error {
	_, err := c.doExpectStatus(ctx, http.MethodDelete, addr+"/admin/cachebox", nil, http.StatusNoContent)
	return err
}

func (c *Client) PostCacheEntries(ctx context.Context, addr string, entries []atroposdk.CacheBoxWireEntry) error {
	_, err := c.doExpectStatus(ctx, http.MethodPost, addr+"/admin/cachebox/entries", entries, http.StatusNoContent)
	return err
}
