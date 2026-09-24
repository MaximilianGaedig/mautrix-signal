// mautrix-signal - A Matrix-Signal puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package signalmeow

import (
	"context"
	"errors"
	"net/http"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/web"
)

// CallingRelay matches Signal-Server's TurnToken response. URLsWithIPs are
// server-composed relay URLs whose hostname has already been resolved.
type CallingRelay struct {
	Username    string   `json:"username"`
	Password    string   `json:"password"`
	TTLSeconds  int64    `json:"ttl"`
	URLs        []string `json:"urls"`
	URLsWithIPs []string `json:"urlsWithIps"`
	Hostname    string   `json:"hostname"`
}

type CallingRelaysResponse struct {
	Relays []CallingRelay `json:"relays"`
}

// GetCallingRelays calls Signal-Server's authenticated GET /v2/calling/relays
// endpoint (CallRoutingControllerV2). Credentials are returned to the caller
// and must not be logged.
func (cli *Client) GetCallingRelays(ctx context.Context) (*CallingRelaysResponse, error) {
	if cli.AuthedWS == nil {
		return nil, errors.New("authenticated Signal websocket is not connected")
	}
	resp, err := cli.AuthedWS.SendRequest(ctx, http.MethodGet, "/v2/calling/relays", nil, nil)
	if err != nil {
		return nil, err
	}
	var relays CallingRelaysResponse
	if err = web.DecodeWSResponseBody(ctx, &relays, resp); err != nil {
		return nil, err
	}
	return &relays, nil
}
