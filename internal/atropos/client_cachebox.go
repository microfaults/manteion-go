package atropos

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

// GetCacheBoxStats reads an SDK instance's cache-box counters (GET /admin/cachebox).
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

// GetCacheBoxFidelity pulls the SDK's W6 fidelity snapshot for (exp, phase) —
// the authoritative per-instance counters manteion reads at the drain-timeout
// fallback (MANT-2) and at verdict time (MANT-6).
func (c *Client) GetCacheBoxFidelity(ctx context.Context, addr, experimentID, phaseID string) (atroposdk.FidelitySnapshot, error) {
	u := addr + "/cachebox/fidelity?experiment_id=" + url.QueryEscape(experimentID) + "&phase_id=" + url.QueryEscape(phaseID)
	resp, err := c.do(ctx, http.MethodGet, u, nil)
	if err != nil {
		return atroposdk.FidelitySnapshot{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return atroposdk.FidelitySnapshot{}, &HTTPError{Method: http.MethodGet, URL: u, Status: resp.StatusCode, Body: string(data)}
	}
	var snap atroposdk.FidelitySnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return atroposdk.FidelitySnapshot{}, &TransportError{Method: http.MethodGet, URL: u, Err: err}
	}
	return snap, nil
}
