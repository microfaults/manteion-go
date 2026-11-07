package atropos

import (
	"context"
	"encoding/json"
	"net/http"

	atroposdk "git.ucsc.edu/microfaults/atropos-go"
)

// GetFault reads an SDK instance's admin fault slots (GET /admin/fault).
func (c *Client) GetFault(ctx context.Context, addr string) (atroposdk.FaultStatus, error) {
	url := addr + "/admin/fault"
	data, err := c.doExpectStatus(ctx, http.MethodGet, url, nil, http.StatusOK)
	if err != nil {
		return atroposdk.FaultStatus{}, err
	}
	var status atroposdk.FaultStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return atroposdk.FaultStatus{}, &TransportError{Method: "GET", URL: url, Err: err}
	}
	return status, nil
}
