package connector

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-signal/pkg/signalid"
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

func TestStoryEventIsAMessageWithStoryID(t *testing.T) {
	evt := pinEvent(nil)
	evt.Event = &signalpb.StoryEvent{
		Story:     &signalpb.StoryMessage{},
		Timestamp: 4242,
	}
	if typ := evt.GetType(); typ != bridgev2.RemoteEventMessage {
		t.Fatalf("type = %s, want message", typ)
	}
	if want := signalid.MakeMessageID(pinSender, 4242); evt.GetID() != want {
		t.Fatalf("id = %q, want %q", evt.GetID(), want)
	}
}

func TestStoryReactionType(t *testing.T) {
	dm := &signalpb.DataMessage{
		Timestamp: proto.Uint64(5678),
		Reaction: &signalpb.DataMessage_Reaction{
			Emoji:                 proto.String("x"),
			TargetAuthorAciBinary: pinAuthor[:],
			TargetSentTimestamp:   proto.Uint64(1234),
		},
		StoryContext: &signalpb.DataMessage_StoryContext{AuthorAciBinary: pinAuthor[:], SentTimestamp: proto.Uint64(1234)},
	}
	evt := pinEvent(dm)
	if typ := evt.GetType(); typ != bridgev2.RemoteEventReaction {
		t.Fatalf("bridged story: type = %s, want reaction", typ)
	}
	if want := signalid.MakeMessageID(pinAuthor, 1234); evt.GetTargetMessage() != want {
		t.Fatalf("target = %q, want %q", evt.GetTargetMessage(), want)
	}
	evt.storyReactionUnbridged = true
	if typ := evt.GetType(); typ != bridgev2.RemoteEventMessage {
		t.Fatalf("unbridged story: type = %s, want message", typ)
	}
}
