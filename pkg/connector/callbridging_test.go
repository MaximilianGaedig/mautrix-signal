package connector

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
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
	if got := signalVideoCodec(video); got != webrtc.MimeTypeH264 {
		t.Fatalf("expected first supported codec H264, got %q", got)
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
	video.Type = events.CallTypeAudio
	if got := signalVideoCodec(video); got != "" {
		t.Fatalf("audio call unexpectedly selected %q", got)
	}
}

func TestSignalRingingOnlyResponsesAreBroadcast(t *testing.T) {
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

	ice := signalICEMessage(45, 8, "candidate:test")
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
}
