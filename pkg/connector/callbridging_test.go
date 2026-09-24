package connector

import (
	"encoding/json"
	"strings"
	"testing"

	"maunium.net/go/mautrix/event"
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
