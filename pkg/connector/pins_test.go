package connector

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

var (
	pinAuthor = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	pinSender = uuid.MustParse("22222222-2222-4222-8222-222222222222")
)

func pinEvent(dm *signalpb.DataMessage) *Bv2ChatEvent {
	return &Bv2ChatEvent{ChatEvent: &events.ChatEvent{Info: events.MessageInfo{Sender: pinSender}, Event: dm}}
}

func TestIncomingPins(t *testing.T) {
	target := signalid.MakeMessageID(pinAuthor, 1234)
	for _, tc := range []struct {
		name string
		dm   *signalpb.DataMessage
		want bridgev2.PinChange
	}{
		{"pin", &signalpb.DataMessage{
			Timestamp: proto.Uint64(5678),
			// Clients newer than our protocol definition mark pins as requiring a newer version.
			RequiredProtocolVersion: proto.Uint32(uint32(signalpb.DataMessage_CURRENT) + 1),
			PinMessage: &signalpb.DataMessage_PinMessage{
				TargetAuthorAciBinary: pinAuthor[:],
				TargetSentTimestamp:   proto.Uint64(1234),
				PinDuration:           &signalpb.DataMessage_PinMessage_PinDurationSeconds{PinDurationSeconds: 86400},
			},
		}, bridgev2.PinChange{MessageID: target, Pinned: true}},
		{"unpin", &signalpb.DataMessage{
			Timestamp: proto.Uint64(5678),
			UnpinMessage: &signalpb.DataMessage_UnpinMessage{
				TargetAuthorAciBinary: pinAuthor[:],
				TargetSentTimestamp:   proto.Uint64(1234),
			},
		}, bridgev2.PinChange{MessageID: target, Pinned: false}},
		{"pin of own message", &signalpb.DataMessage{
			Timestamp:  proto.Uint64(5678),
			PinMessage: &signalpb.DataMessage_PinMessage{TargetSentTimestamp: proto.Uint64(1234)},
		}, bridgev2.PinChange{MessageID: signalid.MakeMessageID(pinSender, 1234), Pinned: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evt := pinEvent(tc.dm)
			if typ := evt.GetType(); typ != bridgev2.RemoteEventChatInfoChange {
				t.Fatalf("type = %s, want chat info change", typ)
			}
			change, err := evt.GetChatInfoChange(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if pc := change.ChatInfo.PinChanges; len(pc) != 1 || pc[0] != tc.want {
				t.Fatalf("pin changes = %+v, want %+v", pc, tc.want)
			}
		})
	}
}

func TestPinDataMessage(t *testing.T) {
	pin := pinDataMessage(pinAuthor, 1234, true, 5678)
	if uuid.UUID(pin.GetPinMessage().GetTargetAuthorAciBinary()) != pinAuthor ||
		pin.GetPinMessage().GetTargetSentTimestamp() != 1234 || pin.GetTimestamp() != 5678 {
		t.Fatalf("pin = %+v", pin)
	}
	if !pin.GetPinMessage().GetPinDurationForever() {
		t.Error("pins from Matrix must not expire")
	}
	if pin.UnpinMessage != nil {
		t.Error("pin also carries an unpin")
	}
	unpin := pinDataMessage(pinAuthor, 1234, false, 5678)
	if unpin.PinMessage != nil || uuid.UUID(unpin.GetUnpinMessage().GetTargetAuthorAciBinary()) != pinAuthor ||
		unpin.GetUnpinMessage().GetTargetSentTimestamp() != 1234 {
		t.Fatalf("unpin = %+v", unpin)
	}
	// What we send must come back as the same change.
	for _, dm := range []*signalpb.DataMessage{pin, unpin} {
		got := pinChange(dm, pinSender).ChatInfo.PinChanges[0]
		if got.MessageID != signalid.MakeMessageID(pinAuthor, 1234) || got.Pinned != (dm.PinMessage != nil) {
			t.Errorf("round trip = %+v", got)
		}
	}
}
