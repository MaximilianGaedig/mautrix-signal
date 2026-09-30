package connector

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

func TestPollTerminate(t *testing.T) {
	dm := pollTerminate(1234, 5678)
	if dm.GetPollTerminate().GetTargetSentTimestamp() != 1234 || dm.GetTimestamp() != 5678 {
		t.Fatalf("message = %+v", dm)
	}
	// What we send must be something a receiving bridge treats as the poll's end.
	evt := &Bv2ChatEvent{ChatEvent: &events.ChatEvent{Info: events.MessageInfo{Sender: pinSender}, Event: proto.Clone(dm).(*signalpb.DataMessage)}}
	if evt.GetType() != bridgev2.RemoteEventMessage {
		t.Errorf("type = %s", evt.GetType())
	}
}
