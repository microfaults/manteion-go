package atropos

import (
	"context"
	"encoding/json"
	"net/http"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

func (c *Client) PostRules(ctx context.Context, addr string, rules []atroposdk.StaticRule) error {
	_, err := c.doExpectStatus(ctx, http.MethodPost, addr+"/admin/rules", rules, http.StatusNoContent)
	return err
}

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
