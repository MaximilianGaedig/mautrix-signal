package ringrtc

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

func bytesField(dst []byte, number protowire.Number, value []byte) []byte {
	dst = protowire.AppendTag(dst, number, protowire.BytesType)
	return protowire.AppendBytes(dst, value)
}

func varintField(dst []byte, number protowire.Number, value uint64) []byte {
	dst = protowire.AppendTag(dst, number, protowire.VarintType)
	return protowire.AppendVarint(dst, value)
}

func TestDecodeOfferV4(t *testing.T) {
	codec := varintField(nil, 1, uint64(VideoCodecVP8))
	params := bytesField(nil, 1, bytes.Repeat([]byte{0x42}, 32))
	params = bytesField(params, 2, []byte("ufrag-secret"))
	params = bytesField(params, 3, []byte("password-secret"))
	params = bytesField(params, 4, codec)
	params = varintField(params, 5, 2_000_000)
	params = bytesField(params, 99, []byte("future field"))
	offer, err := DecodeOffer(bytesField(nil, 4, params))
	if err != nil {
		t.Fatal(err)
	}
	if offer.V4 == nil || len(offer.V4.PublicKey) != 32 || offer.V4.ICEUfrag != "ufrag-secret" || offer.V4.ICEPwd != "password-secret" {
		t.Fatalf("unexpected connection parameters: %+v", offer.V4)
	}
	if len(offer.V4.ReceiveVideoCodecs) != 1 || offer.V4.ReceiveVideoCodecs[0].Type != VideoCodecVP8 {
		t.Fatalf("unexpected codecs: %+v", offer.V4.ReceiveVideoCodecs)
	}
	if offer.V4.MaxBitrateBPS != 2_000_000 {
		t.Fatalf("unexpected bitrate: %d", offer.V4.MaxBitrateBPS)
	}
}

func TestDecodeIceCandidate(t *testing.T) {
	added := bytesField(nil, 1, []byte("candidate:1 1 udp 1 192.0.2.1 1234 typ host"))
	candidate, err := DecodeIceCandidate(bytesField(nil, 2, added))
	if err != nil {
		t.Fatal(err)
	}
	if candidate.AddedV3 == nil || candidate.AddedV3.SDP == "" || candidate.Removed != nil {
		t.Fatalf("unexpected added candidate: %+v", candidate)
	}
	removed := bytesField(nil, 1, []byte{192, 0, 2, 1})
	removed = varintField(removed, 2, 1234)
	candidate, err = DecodeIceCandidate(bytesField(nil, 3, removed))
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Removed == nil || len(candidate.Removed.IP) != 4 || candidate.Removed.Port != 1234 {
		t.Fatalf("unexpected removed candidate: %+v", candidate)
	}
}

func TestDecodeRejectsWrongWireType(t *testing.T) {
	params := varintField(nil, 1, 1)
	if _, err := DecodeAnswer(bytesField(nil, 4, params)); err == nil {
		t.Fatal("expected a wire type error")
	}
}

func TestSignalingEncodeRoundTrip(t *testing.T) {
	params := &ConnectionParametersV4{
		PublicKey: bytes.Repeat([]byte{0x23}, 32), ICEUfrag: "local-ufrag", ICEPwd: "local-password",
		ReceiveVideoCodecs: []VideoCodec{{Type: VideoCodecVP8}}, MaxBitrateBPS: 1_500_000,
		EncodeOnlyVideoCodecs: []VideoCodec{{Type: VideoCodecVP9}},
		DecodeOnlyVideoCodecs: []VideoCodec{{Type: VideoCodecH264ConstrainedBaseline}},
	}
	answer, err := DecodeAnswer(EncodeAnswer(&Answer{V4: params}))
	if err != nil {
		t.Fatal(err)
	}
	if answer.V4 == nil || !bytes.Equal(answer.V4.PublicKey, params.PublicKey) || answer.V4.ICEUfrag != params.ICEUfrag || answer.V4.ICEPwd != params.ICEPwd || answer.V4.MaxBitrateBPS != params.MaxBitrateBPS {
		t.Fatalf("answer round trip mismatch: %+v", answer.V4)
	}
	if len(answer.V4.ReceiveVideoCodecs) != 1 || len(answer.V4.EncodeOnlyVideoCodecs) != 1 || len(answer.V4.DecodeOnlyVideoCodecs) != 1 {
		t.Fatalf("answer codec round trip mismatch: %+v", answer.V4)
	}
	offer, err := DecodeOffer(EncodeOffer(&Offer{V4: params}))
	if err != nil || offer.V4 == nil || offer.V4.ICEUfrag != params.ICEUfrag {
		t.Fatalf("offer round trip mismatch: %+v, %v", offer, err)
	}
	candidate := &IceCandidate{
		AddedV3: &IceCandidateV3{SDP: "candidate:1 1 udp 1 192.0.2.1 1234 typ host"},
		Removed: &SocketAddr{IP: []byte{192, 0, 2, 1}, Port: 1234},
	}
	decodedCandidate, err := DecodeIceCandidate(EncodeIceCandidate(candidate))
	if err != nil || decodedCandidate.AddedV3 == nil || decodedCandidate.AddedV3.SDP != candidate.AddedV3.SDP || decodedCandidate.Removed == nil || !bytes.Equal(decodedCandidate.Removed.IP, candidate.Removed.IP) || decodedCandidate.Removed.Port != candidate.Removed.Port {
		t.Fatalf("candidate round trip mismatch: %+v, %v", decodedCandidate, err)
	}
}
