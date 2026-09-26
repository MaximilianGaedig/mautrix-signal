// mautrix-signal - A Matrix-Signal puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package ringrtc

import (
	"context"

	"github.com/pion/rtp"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	RTPDataPayloadType = 101
	RTPDataSSRC        = 0xD
)

// SendAccepted sends RingRTC's encrypted RTP-data acceptance handshake. The
// peer does not consider a signaling answer sufficient to accept a call.
func (l *MediaLeg) SendAccepted(callID uint64, sequence uint16) error {
	writer, err := l.RTPWriter(RTPDataPayloadType, RTPDataSSRC)
	if err != nil {
		return err
	}
	return writer.WriteRTP(&rtp.Packet{Header: rtp.Header{
		Version: 2, SequenceNumber: sequence, Timestamp: uint32(sequence),
	}, Payload: encodeAccepted(callID, uint64(sequence))})
}

// WaitAccepted waits for a RingRTC callee's encrypted RTP-data acceptance.
func (l *MediaLeg) WaitAccepted(ctx context.Context, callID uint64) error {
	reader, err := l.RTPReader(RTPDataSSRC)
	if err != nil {
		return err
	}
	for ctx.Err() == nil {
		packet, _, readErr := reader.ReadRTP()
		if readErr != nil {
			return readErr
		}
		if packet.PayloadType != RTPDataPayloadType {
			continue
		}
		acceptedID, ok := decodeAccepted(packet.Payload)
		if ok && acceptedID == callID {
			return nil
		}
	}
	return ctx.Err()
}

func encodeAccepted(callID, sequence uint64) []byte {
	accepted := protowire.AppendTag(nil, 1, protowire.VarintType)
	accepted = protowire.AppendVarint(accepted, callID)
	message := protowire.AppendTag(nil, 1, protowire.BytesType)
	message = protowire.AppendBytes(message, accepted)
	message = protowire.AppendTag(message, 4, protowire.VarintType)
	return protowire.AppendVarint(message, sequence)
}

func decodeAccepted(message []byte) (uint64, bool) {
	for len(message) > 0 {
		num, typ, n := protowire.ConsumeTag(message)
		if n < 0 {
			return 0, false
		}
		message = message[n:]
		if num == 1 && typ == protowire.BytesType {
			accepted, consumed := protowire.ConsumeBytes(message)
			if consumed < 0 {
				return 0, false
			}
			field, fieldType, tagSize := protowire.ConsumeTag(accepted)
			if tagSize < 0 || field != 1 || fieldType != protowire.VarintType {
				return 0, false
			}
			callID, valueSize := protowire.ConsumeVarint(accepted[tagSize:])
			return callID, valueSize >= 0
		}
		consumed := protowire.ConsumeFieldValue(num, typ, message)
		if consumed < 0 {
			return 0, false
		}
		message = message[consumed:]
	}
	return 0, false
}
