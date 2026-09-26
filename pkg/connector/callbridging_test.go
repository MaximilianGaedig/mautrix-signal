package connector

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/ringrtc"
)

func TestSignalCallEventContentVersionIsString(t *testing.T) {
	content := signalCallEventContent(&event.CallHangupEventContent{
		BaseCallEventContent: event.BaseCallEventContent{CallID: "call", PartyID: "party", Version: "1"},
		Reason:               event.CallHangupUserHangup,
	})
	data, err := json.Marshal(content)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"version":"1"`) || !strings.Contains(string(data), `"call_id":"call"`) {
		t.Fatalf("unexpected content: %s", data)
	}
}

func TestSignalVideoCodec(t *testing.T) {
	video := &events.Call{Type: events.CallTypeVideo, Offer: &ringrtc.Offer{V4: &ringrtc.ConnectionParametersV4{
		ReceiveVideoCodecs: []ringrtc.VideoCodec{{Type: ringrtc.VideoCodecVP9}, {Type: ringrtc.VideoCodecH264ConstrainedHigh}, {Type: ringrtc.VideoCodecVP8}},
	}}}
	if got := signalVideoCodec(video); got != webrtc.MimeTypeVP8 {
		t.Fatalf("expected RingRTC-compatible VP8, got %q", got)
	}
	video.Offer.V4.ReceiveVideoCodecs = []ringrtc.VideoCodec{{Type: ringrtc.VideoCodecVP8}}
	if got := signalVideoCodec(video); got != webrtc.MimeTypeVP8 {
		t.Fatalf("expected VP8, got %q", got)
	}
	video.Offer.V4.EncodeOnlyVideoCodecs = []ringrtc.VideoCodec{{Type: ringrtc.VideoCodecVP9}, {Type: ringrtc.VideoCodecVP8}}
	video.Offer.V4.DecodeOnlyVideoCodecs = []ringrtc.VideoCodec{{Type: ringrtc.VideoCodecVP8}}
	if got := signalVideoCodec(video); got != webrtc.MimeTypeVP8 {
		t.Fatalf("expected asymmetric intersection VP8, got %q", got)
	}
	video.Offer.V4.DecodeOnlyVideoCodecs = []ringrtc.VideoCodec{{Type: ringrtc.VideoCodecH264ConstrainedBaseline}}
	if got := signalVideoCodec(video); got != "" {
		t.Fatalf("expected no asymmetric intersection, got %q", got)
	}
	video.Offer.V4.EncodeOnlyVideoCodecs = nil
	video.Offer.V4.ReceiveVideoCodecs = []ringrtc.VideoCodec{{Type: ringrtc.VideoCodecVP8}}
	video.Offer.V4.DecodeOnlyVideoCodecs = []ringrtc.VideoCodec{{Type: ringrtc.VideoCodecVP8}}
	if got := signalVideoCodec(video); got != webrtc.MimeTypeVP8 {
		t.Fatalf("expected independent legacy encode fallback, got %q", got)
	}
	video.Type = events.CallTypeAudio
	if got := signalVideoCodec(video); got != "" {
		t.Fatalf("audio call unexpectedly selected %q", got)
	}
}

func TestSignalCallControlResponsesAreBroadcast(t *testing.T) {
	hangup := signalHangupMessage(42)
	if hangup.GetHangup().GetId() != 42 || hangup.GetHangup().GetType() != 0 {
		t.Fatalf("unexpected hangup: %+v", hangup.GetHangup())
	}
	if hangup.DestinationDeviceId != nil {
		t.Fatal("local hangup must be broadcast, not device-targeted")
	}
	busy := signalBusyMessage(43)
	if busy.GetBusy().GetId() != 43 || busy.DestinationDeviceId != nil {
		t.Fatalf("unexpected busy response: %+v", busy)
	}
	accepted := signalAcceptedMessage(44, 9)
	if accepted.GetHangup().GetId() != 44 || accepted.GetHangup().GetType() != signalpb.CallMessage_Hangup_HANGUP_ACCEPTED || accepted.GetHangup().GetDeviceId() != 9 || accepted.DestinationDeviceId != nil {
		t.Fatalf("unexpected accepted-elsewhere response: %+v", accepted)
	}
}

func TestSignalAnswerAndICEAreDeviceTargeted(t *testing.T) {
	params := &ringrtc.ConnectionParametersV4{
		ICEUfrag: "ufrag", ICEPwd: "password", PublicKey: make([]byte, 32), MaxBitrateBPS: 2_000_000,
	}
	answer := signalAnswerMessage(44, 7, params)
	if answer.GetDestinationDeviceId() != 7 || answer.GetAnswer().GetId() != 44 {
		t.Fatalf("unexpected answer targeting: %+v", answer)
	}
	decodedAnswer, err := ringrtc.DecodeAnswer(answer.GetAnswer().GetOpaque())
	if err != nil {
		t.Fatal(err)
	}
	if decodedAnswer.V4 == nil || decodedAnswer.V4.ICEUfrag != params.ICEUfrag || decodedAnswer.V4.MaxBitrateBPS != params.MaxBitrateBPS {
		t.Fatalf("unexpected encoded answer: %+v", decodedAnswer)
	}

	device := uint32(8)
	ice := signalICEMessage(45, &device, "candidate:test")
	if ice.GetDestinationDeviceId() != 8 || len(ice.GetIceUpdate()) != 1 || ice.GetIceUpdate()[0].GetId() != 45 {
		t.Fatalf("unexpected ICE targeting: %+v", ice)
	}
	decodedICE, err := ringrtc.DecodeIceCandidate(ice.GetIceUpdate()[0].GetOpaque())
	if err != nil {
		t.Fatal(err)
	}
	if decodedICE.AddedV3 == nil || decodedICE.AddedV3.SDP != "candidate:test" {
		t.Fatalf("unexpected encoded ICE update: %+v", decodedICE)
	}
	broadcastICE := signalICEMessage(45, nil, "candidate:test")
	if broadcastICE.DestinationDeviceId != nil {
		t.Fatal("caller ICE must be broadcast to all callee devices")
	}
}

func TestSignalOfferIsBroadcastAndEncoded(t *testing.T) {
	params := &ringrtc.ConnectionParametersV4{
		ICEUfrag: "offer-ufrag", ICEPwd: "offer-password", PublicKey: make([]byte, 32),
		ReceiveVideoCodecs: []ringrtc.VideoCodec{{Type: ringrtc.VideoCodecVP8}},
	}
	offer := signalOfferMessage(46, events.CallTypeVideo, params)
	if offer.DestinationDeviceId != nil || offer.GetOffer().GetId() != 46 || offer.GetOffer().GetType() != signalpb.CallMessage_Offer_OFFER_VIDEO_CALL {
		t.Fatalf("unexpected offer: %+v", offer)
	}
	decoded, err := ringrtc.DecodeOffer(offer.GetOffer().GetOpaque())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.V4 == nil || decoded.V4.ICEUfrag != params.ICEUfrag || len(decoded.V4.ReceiveVideoCodecs) != 1 {
		t.Fatalf("unexpected encoded offer: %+v", decoded)
	}
}
