// mautrix-signal - A Matrix-Signal puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package signalmeow

import (
	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/ringrtc"
)

func parseCallMessage(message *signalpb.CallMessage, info events.MessageInfo, timestamp uint64) []*events.Call {
	decoded := ringrtc.DecodeCallMessage(message)
	out := make([]*events.Call, 0, len(decoded))
	for _, parsed := range decoded {
		event := &events.Call{
			Info: info, Timestamp: timestamp,
			IsRinging:           parsed.MessageType == ringrtc.MessageOffer,
			ID:                  parsed.ID,
			Direction:           events.CallDirectionIncoming,
			MessageType:         events.CallMessageType(parsed.MessageType),
			Type:                events.CallType(parsed.Type),
			DestinationDeviceID: parsed.DestinationDeviceID,
			HangupType:          parsed.HangupType,
			HangupDeviceID:      parsed.HangupDeviceID,
			OpaqueUrgency:       parsed.OpaqueUrgency,
			OpaqueLength:        parsed.OpaqueLength,
			Offer:               parsed.Offer,
			Answer:              parsed.Answer,
			ICECandidate:        parsed.ICECandidate,
			ParseError:          parsed.ParseError,
		}
		out = append(out, event)
	}
	return out
}
