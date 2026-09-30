package msgconv

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-signal/pkg/msgconv/signalfmt"
	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

var (
	storyAuthor = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	storyOwn    = uuid.MustParse("33333333-3333-4333-8333-333333333333")
)

func testConverter(bridged bool) *MessageConverter {
	return &MessageConverter{
		SignalFmtParams: &signalfmt.FormatParams{
			GetUserInfo: func(ctx context.Context, u uuid.UUID) signalfmt.UserInfo {
				if u == storyAuthor {
					return signalfmt.UserInfo{Name: "Alice Example"}
				}
				return signalfmt.UserInfo{}
			},
		},
		storyExists: func(ctx context.Context, id networkid.MessageID) bool {
			return bridged && id == signalid.MakeMessageID(storyAuthor, 4242)
		},
	}
}

func storyReplyDM(body string) *signalpb.DataMessage {
	return &signalpb.DataMessage{
		Timestamp: proto.Uint64(9000),
		Body:      proto.String(body),
		StoryContext: &signalpb.DataMessage_StoryContext{
			AuthorAciBinary: storyAuthor[:],
			SentTimestamp:   proto.Uint64(4242),
		},
	}
}

func TestTextStoryToMatrix(t *testing.T) {
	story := &signalpb.StoryMessage{
		AllowsReplies: proto.Bool(true),
		Attachment: &signalpb.StoryMessage_TextAttachment{TextAttachment: &signalpb.TextAttachment{
			Text:                proto.String("hello story"),
			TextStyle:           signalpb.TextAttachment_BOLD.Enum(),
			TextForegroundColor: proto.Uint32(0xffffffff),
			Background:          &signalpb.TextAttachment_Color{Color: 0xff112233},
		}},
	}
	cm := testConverter(false).StoryToMatrix(context.Background(), nil, nil, nil, &signalpb.StoryEvent{Story: story, Timestamp: 4242})
	if len(cm.Parts) != 1 {
		t.Fatalf("got %d parts, want 1", len(cm.Parts))
	}
	part := cm.Parts[0]
	if want := StoryPrefix + ": hello story"; part.Content.Body != want {
		t.Errorf("body = %q, want %q", part.Content.Body, want)
	}
	if !strings.Contains(part.Content.Body, "24 hours") {
		t.Errorf("body doesn't mention the 24 hour expiry")
	}
	meta := part.DBMetadata.(*signalid.MessageMetadata)
	if !meta.IsStory {
		t.Errorf("metadata doesn't mark the message as a story")
	}
	extra := part.Extra["fi.mau.signal.story"].(map[string]any)
	if extra["background_color"] != "#ff112233" || extra["text_color"] != "#ffffffff" || extra["text_style"] != "BOLD" || extra["allows_replies"] != true {
		t.Errorf("unexpected story extra: %v", extra)
	}
}

func TestStoryReplyLinksToBridgedStory(t *testing.T) {
	cm := testConverter(true).ToMatrix(context.Background(), nil, nil, storyAuthor, nil, storyReplyDM("nice one"), nil)
	if cm.ReplyTo == nil || cm.ReplyTo.MessageID != signalid.MakeMessageID(storyAuthor, 4242) {
		t.Fatalf("reply to = %+v, want the story message", cm.ReplyTo)
	}
	if cm.Parts[0].Content.Body != "nice one" {
		t.Errorf("body = %q, want it unchanged", cm.Parts[0].Content.Body)
	}
}

func TestStoryReplyToUnknownStoryGetsPrefix(t *testing.T) {
	cm := testConverter(false).ToMatrix(context.Background(), nil, nil, storyAuthor, nil, storyReplyDM("nice one"), nil)
	if cm.ReplyTo != nil {
		t.Errorf("reply to = %+v, want none", cm.ReplyTo)
	}
	if want := "Replied to a story by Alice Example: nice one"; cm.Parts[0].Content.Body != want {
		t.Errorf("body = %q, want %q", cm.Parts[0].Content.Body, want)
	}
}

func TestStoryReactionNotice(t *testing.T) {
	dm := &signalpb.DataMessage{
		Timestamp: proto.Uint64(9000),
		Reaction: &signalpb.DataMessage_Reaction{
			Emoji:                 proto.String("\U0001F44D"),
			TargetAuthorAciBinary: storyAuthor[:],
			TargetSentTimestamp:   proto.Uint64(4242),
		},
		StoryContext: &signalpb.DataMessage_StoryContext{AuthorAciBinary: storyAuthor[:], SentTimestamp: proto.Uint64(4242)},
	}
	if !IsStoryReaction(dm) {
		t.Fatal("not recognized as a story reaction")
	}
	cm := testConverter(false).ToMatrix(context.Background(), nil, nil, storyAuthor, nil, dm, nil)
	if len(cm.Parts) != 1 || cm.Parts[0].Content.Body != "Reacted \U0001F44D to a story by Alice Example" {
		t.Fatalf("unexpected notice: %+v", cm.Parts)
	}
}

func TestMatrixReplyToStoryIsStoryReply(t *testing.T) {
	story := &database.Message{
		ID:       signalid.MakeMessageID(storyAuthor, 4242),
		Metadata: &signalid.MessageMetadata{IsStory: true},
	}
	ctxt := StoryContextFor(story, storyOwn)
	if ctxt == nil || uuid.UUID(ctxt.AuthorAciBinary) != storyAuthor || ctxt.GetSentTimestamp() != 4242 {
		t.Fatalf("unexpected story context: %v", ctxt)
	}
	normal := &database.Message{ID: story.ID, Metadata: &signalid.MessageMetadata{}}
	if StoryContextFor(normal, storyOwn) != nil {
		t.Error("normal message got a story context")
	}
	own := &database.Message{ID: signalid.MakeMessageID(storyOwn, 1), Metadata: &signalid.MessageMetadata{IsStory: true}}
	if StoryContextFor(own, storyOwn) != nil {
		t.Error("own story got a story context")
	}
}
