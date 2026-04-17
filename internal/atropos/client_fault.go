package atropos

import (
	"context"
	"encoding/json"
	"net/http"

	atroposdk "atropos-go"
)

func (c *Client) PostFault(ctx context.Context, addr string, req atroposdk.FaultRequest) (atroposdk.FaultStatus, error) {
	url := addr + "/admin/fault"
	data, err := c.doExpectStatus(ctx, http.MethodPost, url, req, http.StatusCreated)
	if err != nil {
		return atroposdk.FaultStatus{}, err
	}
	var status atroposdk.FaultStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return atroposdk.FaultStatus{}, &TransportError{Method: "POST", URL: url, Err: err}
	}
	return status, nil
}

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

func (c *Client) DeleteFault(ctx context.Context, addr string) error {
	_, err := c.doExpectStatus(ctx, http.MethodDelete, addr+"/admin/fault", nil, http.StatusOK)
	return err
}
