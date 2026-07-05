package atropos

import (
	"context"
	"encoding/json"
	"io"
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

// --- Staged preload protocol (wire spec §W4) ---

// PreloadBegin starts a staged preload for (experiment_id, phase_id) on one SDK
// instance, clearing any prior staging. A 413 (too_large) or 409 (unsupported
// strategy) surfaces as an *HTTPError.
func (c *Client) PreloadBegin(ctx context.Context, addr string, req atroposdk.PreloadBeginRequest) error {
	_, err := c.doExpectStatus(ctx, http.MethodPost, addr+"/cachebox/preload/begin", req, http.StatusOK)
	return err
}

// PreloadChunk stages one chunk of entries (idempotent by chunk_seq) and returns
// the running staged total.
func (c *Client) PreloadChunk(ctx context.Context, addr string, req atroposdk.PreloadChunkRequest) (atroposdk.PreloadChunkResponse, error) {
	url := addr + "/cachebox/preload/chunk"
	data, err := c.doExpectStatus(ctx, http.MethodPost, url, req, http.StatusOK)
	if err != nil {
		return atroposdk.PreloadChunkResponse{}, err
	}
	var resp atroposdk.PreloadChunkResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return atroposdk.PreloadChunkResponse{}, &TransportError{Method: http.MethodPost, URL: url, Err: err}
	}
	return resp, nil
}

// PreloadCommit verifies the staged set against (total_entries, checksum) and,
// on match, atomically swaps it in as the live replay set. Both the 200 (match)
// and 409 (mismatch) responses carry a body; only a non-{200,409} status or a
// transport failure is an error. The caller inspects PreloadCommitResponse.OK.
func (c *Client) PreloadCommit(ctx context.Context, addr string, req atroposdk.PreloadCommitRequest) (atroposdk.PreloadCommitResponse, error) {
	url := addr + "/cachebox/preload/commit"
	resp, err := c.do(ctx, http.MethodPost, url, req)
	if err != nil {
		return atroposdk.PreloadCommitResponse{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		return atroposdk.PreloadCommitResponse{}, &HTTPError{Method: http.MethodPost, URL: url, Status: resp.StatusCode, Body: string(data)}
	}
	var out atroposdk.PreloadCommitResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return atroposdk.PreloadCommitResponse{}, &TransportError{Method: http.MethodPost, URL: url, Err: err}
	}
	return out, nil
}

// PreloadAbort drops any staged (not-yet-committed) entries for the pair.
func (c *Client) PreloadAbort(ctx context.Context, addr string, req atroposdk.PreloadAbortRequest) error {
	_, err := c.doExpectStatus(ctx, http.MethodPost, addr+"/cachebox/preload/abort", req, http.StatusOK)
	return err
}
