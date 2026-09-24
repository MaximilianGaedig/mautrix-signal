// mautrix-signal - A Matrix-Signal puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Package ringrtc decodes the opaque 1:1 call signalling payloads produced by
// Signal's RingRTC library. The wire definitions are from RingRTC's
// protobuf/protobuf/signaling.proto (Offer, Answer, IceCandidate and
// ConnectionParametersV4). Keeping the parser here avoids treating the opaque
// bytes as SDP and makes the protocol boundary explicit.
package ringrtc

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

type VideoCodecType int32

const (
	VideoCodecVP8                     VideoCodecType = 8
	VideoCodecVP9                     VideoCodecType = 9
	VideoCodecH264ConstrainedBaseline VideoCodecType = 40
	VideoCodecH264ConstrainedHigh     VideoCodecType = 46
)

type VideoCodec struct {
	Type VideoCodecType
}

type ConnectionParametersV4 struct {
	PublicKey             []byte
	ICEUfrag              string
	ICEPwd                string
	ReceiveVideoCodecs    []VideoCodec
	MaxBitrateBPS         uint64
	EncodeOnlyVideoCodecs []VideoCodec
	DecodeOnlyVideoCodecs []VideoCodec
}

type Offer struct {
	V4 *ConnectionParametersV4
}

type Answer struct {
	V4 *ConnectionParametersV4
}

type IceCandidateV3 struct {
	SDP string
}

type SocketAddr struct {
	IP   []byte
	Port uint32
}

type IceCandidate struct {
	AddedV3 *IceCandidateV3
	Removed *SocketAddr
}

func DecodeOffer(data []byte) (*Offer, error) {
	v4, err := decodeVersionedConnectionParameters(data)
	return &Offer{V4: v4}, err
}

// EncodeOffer and EncodeAnswer produce the opaque payloads used by RingRTC's
// Signal CallMessage. The wire layout is defined in
// ringrtc/protobuf/protobuf/signaling.proto.
func EncodeOffer(offer *Offer) []byte {
	if offer == nil || offer.V4 == nil {
		return nil
	}
	return appendBytesField(nil, 4, encodeConnectionParametersV4(offer.V4))
}

func EncodeAnswer(answer *Answer) []byte {
	if answer == nil || answer.V4 == nil {
		return nil
	}
	return appendBytesField(nil, 4, encodeConnectionParametersV4(answer.V4))
}

func DecodeAnswer(data []byte) (*Answer, error) {
	v4, err := decodeVersionedConnectionParameters(data)
	return &Answer{V4: v4}, err
}

func decodeVersionedConnectionParameters(data []byte) (*ConnectionParametersV4, error) {
	var out *ConnectionParametersV4
	err := consumeMessage(data, func(number protowire.Number, typ protowire.Type, value []byte, _ uint64) error {
		if number != 4 {
			return nil
		}
		if typ != protowire.BytesType {
			return wrongWireType(number, typ, protowire.BytesType)
		}
		var err error
		out, err = decodeConnectionParametersV4(value)
		return err
	})
	return out, err
}

func decodeConnectionParametersV4(data []byte) (*ConnectionParametersV4, error) {
	out := &ConnectionParametersV4{}
	err := consumeMessage(data, func(number protowire.Number, typ protowire.Type, bytesValue []byte, varintValue uint64) error {
		switch number {
		case 1:
			if typ != protowire.BytesType {
				return wrongWireType(number, typ, protowire.BytesType)
			}
			out.PublicKey = append([]byte(nil), bytesValue...)
		case 2:
			if typ != protowire.BytesType {
				return wrongWireType(number, typ, protowire.BytesType)
			}
			out.ICEUfrag = string(bytesValue)
		case 3:
			if typ != protowire.BytesType {
				return wrongWireType(number, typ, protowire.BytesType)
			}
			out.ICEPwd = string(bytesValue)
		case 4, 6, 7:
			if typ != protowire.BytesType {
				return wrongWireType(number, typ, protowire.BytesType)
			}
			codec, err := decodeVideoCodec(bytesValue)
			if err != nil {
				return err
			}
			switch number {
			case 4:
				out.ReceiveVideoCodecs = append(out.ReceiveVideoCodecs, codec)
			case 6:
				out.EncodeOnlyVideoCodecs = append(out.EncodeOnlyVideoCodecs, codec)
			case 7:
				out.DecodeOnlyVideoCodecs = append(out.DecodeOnlyVideoCodecs, codec)
			}
		case 5:
			if typ != protowire.VarintType {
				return wrongWireType(number, typ, protowire.VarintType)
			}
			out.MaxBitrateBPS = varintValue
		}
		return nil
	})
	return out, err
}

func encodeConnectionParametersV4(params *ConnectionParametersV4) []byte {
	var out []byte
	if len(params.PublicKey) > 0 {
		out = appendBytesField(out, 1, params.PublicKey)
	}
	if params.ICEUfrag != "" {
		out = appendBytesField(out, 2, []byte(params.ICEUfrag))
	}
	if params.ICEPwd != "" {
		out = appendBytesField(out, 3, []byte(params.ICEPwd))
	}
	for _, codec := range params.ReceiveVideoCodecs {
		out = appendBytesField(out, 4, encodeVideoCodec(codec))
	}
	if params.MaxBitrateBPS != 0 {
		out = appendVarintField(out, 5, params.MaxBitrateBPS)
	}
	for _, codec := range params.EncodeOnlyVideoCodecs {
		out = appendBytesField(out, 6, encodeVideoCodec(codec))
	}
	for _, codec := range params.DecodeOnlyVideoCodecs {
		out = appendBytesField(out, 7, encodeVideoCodec(codec))
	}
	return out
}

func encodeVideoCodec(codec VideoCodec) []byte {
	return appendVarintField(nil, 1, uint64(codec.Type))
}

func decodeVideoCodec(data []byte) (VideoCodec, error) {
	var out VideoCodec
	err := consumeMessage(data, func(number protowire.Number, typ protowire.Type, _ []byte, value uint64) error {
		if number != 1 {
			return nil
		}
		if typ != protowire.VarintType {
			return wrongWireType(number, typ, protowire.VarintType)
		}
		out.Type = VideoCodecType(value)
		return nil
	})
	return out, err
}

func DecodeIceCandidate(data []byte) (*IceCandidate, error) {
	out := &IceCandidate{}
	err := consumeMessage(data, func(number protowire.Number, typ protowire.Type, value []byte, _ uint64) error {
		if number != 2 && number != 3 {
			return nil
		}
		if typ != protowire.BytesType {
			return wrongWireType(number, typ, protowire.BytesType)
		}
		var err error
		if number == 2 {
			out.AddedV3, err = decodeIceCandidateV3(value)
		} else {
			out.Removed, err = decodeSocketAddr(value)
		}
		return err
	})
	return out, err
}

func EncodeIceCandidate(candidate *IceCandidate) []byte {
	if candidate == nil {
		return nil
	}
	var out []byte
	if candidate.AddedV3 != nil {
		out = appendBytesField(out, 2, appendBytesField(nil, 1, []byte(candidate.AddedV3.SDP)))
	}
	if candidate.Removed != nil {
		var removed []byte
		if len(candidate.Removed.IP) > 0 {
			removed = appendBytesField(removed, 1, candidate.Removed.IP)
		}
		if candidate.Removed.Port != 0 {
			removed = appendVarintField(removed, 2, uint64(candidate.Removed.Port))
		}
		out = appendBytesField(out, 3, removed)
	}
	return out
}

func decodeIceCandidateV3(data []byte) (*IceCandidateV3, error) {
	out := &IceCandidateV3{}
	err := consumeMessage(data, func(number protowire.Number, typ protowire.Type, value []byte, _ uint64) error {
		if number == 1 {
			if typ != protowire.BytesType {
				return wrongWireType(number, typ, protowire.BytesType)
			}
			out.SDP = string(value)
		}
		return nil
	})
	return out, err
}

func decodeSocketAddr(data []byte) (*SocketAddr, error) {
	out := &SocketAddr{}
	err := consumeMessage(data, func(number protowire.Number, typ protowire.Type, bytesValue []byte, varintValue uint64) error {
		switch number {
		case 1:
			if typ != protowire.BytesType {
				return wrongWireType(number, typ, protowire.BytesType)
			}
			out.IP = append([]byte(nil), bytesValue...)
		case 2:
			if typ != protowire.VarintType {
				return wrongWireType(number, typ, protowire.VarintType)
			}
			out.Port = uint32(varintValue)
		}
		return nil
	})
	return out, err
}

type fieldConsumer func(protowire.Number, protowire.Type, []byte, uint64) error

func consumeMessage(data []byte, consume fieldConsumer) error {
	for len(data) > 0 {
		number, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]
		var bytesValue []byte
		var varintValue uint64
		var consumed int
		switch typ {
		case protowire.BytesType:
			bytesValue, consumed = protowire.ConsumeBytes(data)
		case protowire.VarintType:
			varintValue, consumed = protowire.ConsumeVarint(data)
		default:
			consumed = protowire.ConsumeFieldValue(number, typ, data)
		}
		if consumed < 0 {
			return protowire.ParseError(consumed)
		}
		if err := consume(number, typ, bytesValue, varintValue); err != nil {
			return err
		}
		data = data[consumed:]
	}
	return nil
}

func wrongWireType(number protowire.Number, got, want protowire.Type) error {
	return fmt.Errorf("ringrtc field %d has wire type %d, want %d", number, got, want)
}

func appendBytesField(dst []byte, number protowire.Number, value []byte) []byte {
	dst = protowire.AppendTag(dst, number, protowire.BytesType)
	return protowire.AppendBytes(dst, value)
}

func appendVarintField(dst []byte, number protowire.Number, value uint64) []byte {
	dst = protowire.AppendTag(dst, number, protowire.VarintType)
	return protowire.AppendVarint(dst, value)
}
