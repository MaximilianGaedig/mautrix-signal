package connector

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

func TestPollEndIsAMessage(t *testing.T) {
	evt := pinEvent(&signalpb.DataMessage{
		Timestamp:               proto.Uint64(5678),
		RequiredProtocolVersion: proto.Uint32(uint32(signalpb.DataMessage_POLLS)),
		PollTerminate:           &signalpb.DataMessage_PollTerminate{TargetSentTimestamp: proto.Uint64(1234)},
	})
	if typ := evt.GetType(); typ != bridgev2.RemoteEventMessage {
		t.Fatalf("type = %s, want message", typ)
	}
}
