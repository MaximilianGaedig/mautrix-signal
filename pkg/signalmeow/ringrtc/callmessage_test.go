package ringrtc

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

func TestParseCallMessage(t *testing.T) {
	params := protowire.AppendTag(nil, 1, protowire.BytesType)
	params = protowire.AppendBytes(params, make([]byte, 32))
	offerOpaque := protowire.AppendTag(nil, 4, protowire.BytesType)
	offerOpaque = protowire.AppendBytes(offerOpaque, params)
	message := &signalpb.CallMessage{
		Offer: &signalpb.CallMessage_Offer{
			Id: proto.Uint64(42), Type: signalpb.CallMessage_Offer_OFFER_AUDIO_CALL.Enum(), Opaque: offerOpaque,
		},
		IceUpdate: []*signalpb.CallMessage_IceUpdate{{Id: proto.Uint64(42), Opaque: []byte{0x12, 0x00}}},
		Hangup: &signalpb.CallMessage_Hangup{
			Id: proto.Uint64(42), Type: signalpb.CallMessage_Hangup_HANGUP_DECLINED.Enum(), DeviceId: proto.Uint32(7),
		},
		DestinationDeviceId: proto.Uint32(8),
	}
	parsed := DecodeCallMessage(message)
	if len(parsed) != 3 {
		t.Fatalf("got %d events", len(parsed))
	}
	if parsed[0].MessageType != MessageOffer || parsed[0].Offer.V4 == nil {
		t.Fatalf("unexpected offer: %+v", parsed[0])
	}
	if parsed[1].MessageType != MessageICE || parsed[1].ICECandidate == nil {
		t.Fatalf("unexpected ICE update: %+v", parsed[1])
	}
	if parsed[2].MessageType != MessageHangup || parsed[2].HangupDeviceID != 7 {
		t.Fatalf("unexpected hangup: %+v", parsed[2])
	}
	for _, event := range parsed {
		if event.ID != 42 || event.DestinationDeviceID != 8 {
			t.Fatalf("lost common metadata: %+v", event)
		}
	}
}
