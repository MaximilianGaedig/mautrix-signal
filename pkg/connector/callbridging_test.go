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
