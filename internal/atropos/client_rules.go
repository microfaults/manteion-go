package atropos

import (
	"context"
	"encoding/json"
	"net/http"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

// GetRules reads the rules an SDK instance has installed (GET /admin/rules).
func (c *Client) GetRules(ctx context.Context, addr string) ([]atroposdk.StaticRule, error) {
	url := addr + "/admin/rules"
	data, err := c.doExpectStatus(ctx, http.MethodGet, url, nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	var rules []atroposdk.StaticRule
	if err := json.Unmarshal(data, &rules); err != nil {
		return nil, &TransportError{Method: "GET", URL: url, Err: err}
	}
	return rules, nil
}
