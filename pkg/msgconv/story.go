package msgconv

import (
	"context"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-signal/pkg/msgconv/signalfmt"
	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

// StoryPrefix marks a bridged story. Stories vanish from Signal after a day, but the Matrix
// message stays, so say that in the message itself.
const StoryPrefix = "Story (expires on Signal after 24 hours)"

const storyLifetime = 24 * time.Hour

func argbColor(c uint32) string {
	return fmt.Sprintf("#%08x", c)
}

func storyExtra(story *signalpb.StoryMessage, ts uint64) map[string]any {
	extra := map[string]any{
		"allows_replies": story.GetAllowsReplies(),
		"expires_at":     time.UnixMilli(int64(ts)).Add(storyLifetime).UnixMilli(),
	}
	if ta := story.GetTextAttachment(); ta != nil {
		extra["text_style"] = ta.GetTextStyle().String()
		if ta.TextForegroundColor != nil {
			extra["text_color"] = argbColor(ta.GetTextForegroundColor())
		}
		if ta.TextBackgroundColor != nil {
			extra["text_background_color"] = argbColor(ta.GetTextBackgroundColor())
		}
		switch bg := ta.GetBackground().(type) {
		case *signalpb.TextAttachment_Color:
			extra["background_color"] = argbColor(bg.Color)
		case *signalpb.TextAttachment_Gradient_:
			colors := make([]string, 0, len(bg.Gradient.GetColors()))
			for _, c := range bg.Gradient.GetColors() {
				colors = append(colors, argbColor(c))
			}
			if len(colors) == 0 && bg.Gradient.StartColor != nil {
				colors = []string{argbColor(bg.Gradient.GetStartColor()), argbColor(bg.Gradient.GetEndColor())}
			}
			extra["background_gradient"] = map[string]any{
				"colors":    colors,
				"positions": bg.Gradient.GetPositions(),
				"angle":     bg.Gradient.GetAngle(),
			}
		}
	}
	return map[string]any{"fi.mau.signal.story": extra}
}

// StoryToMatrix converts a story into an ordinary message. The story's message ID is the same
// (author, sent timestamp) pair as for every other Signal message, so replies can point to it.
func (mc *MessageConverter) StoryToMatrix(
	ctx context.Context,
	client *signalmeow.Client,
	portal *bridgev2.Portal,
	intent bridgev2.MatrixAPI,
	ev *signalpb.StoryEvent,
) *bridgev2.ConvertedMessage {
	ctx = context.WithValue(ctx, contextKeyClient, client)
	ctx = context.WithValue(ctx, contextKeyPortal, portal)
	ctx = context.WithValue(ctx, contextKeyIntent, intent)
	story := ev.Story
	cm := &bridgev2.ConvertedMessage{}
	hasMedia := false
	switch {
	case story.GetTextAttachment() != nil:
		ta := story.GetTextAttachment()
		text := ta.GetText()
		if url := ta.GetPreview().GetUrl(); url != "" && !strings.Contains(text, url) {
			if text != "" {
				text += "\n"
			}
			text += url
		}
		content := signalfmt.Parse(ctx, text, story.GetBodyRanges(), mc.SignalFmtParams)
		prefixContent(content, StoryPrefix+": ")
		cm.Parts = append(cm.Parts, &bridgev2.ConvertedMessagePart{
			Type:    event.EventMessage,
			Content: content,
			Extra:   storyExtra(story, ev.Timestamp),
		})
	case story.GetFileAttachment() != nil:
		hasMedia = true
		att := story.GetFileAttachment()
		media := mc.convertAttachmentToMatrix(ctx, 0, att, nil)
		media.Extra = storyExtra(story, ev.Timestamp)
		caption := signalfmt.Parse(ctx, att.GetCaption(), story.GetBodyRanges(), mc.SignalFmtParams)
		if att.GetCaption() == "" {
			caption.Body = StoryPrefix
		} else {
			prefixContent(caption, StoryPrefix+": ")
		}
		cm.Parts = append(cm.Parts,
			media,
			&bridgev2.ConvertedMessagePart{Type: event.EventMessage, Content: caption},
		)
	default:
		cm.Parts = append(cm.Parts, &bridgev2.ConvertedMessagePart{
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType: event.MsgNotice,
				Body:    StoryPrefix + ": the bridge does not support this kind of story yet.",
			},
			Extra: storyExtra(story, ev.Timestamp),
		})
	}
	cm.MergeCaption()
	for i, part := range cm.Parts {
		part.ID = signalid.MakeMessagePartID(i)
		part.DBMetadata = &signalid.MessageMetadata{ContainsAttachments: hasMedia, IsStory: true}
	}
	return cm
}

// prefixContent puts a plain prefix in front of a text message body, and in front of the HTML
// body if there is one.
func prefixContent(content *event.MessageEventContent, prefix string) {
	content.Body = prefix + content.Body
	if content.FormattedBody != "" {
		content.FormattedBody = html.EscapeString(prefix) + content.FormattedBody
	}
}

func (mc *MessageConverter) storyBridged(ctx context.Context, id networkid.MessageID) bool {
	if mc.storyExists != nil {
		return mc.storyExists(ctx, id)
	}
	if mc.Bridge == nil {
		return false
	}
	msg, err := mc.Bridge.DB.Message.GetFirstPartByID(ctx, getPortal(ctx).Receiver, id)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to check if replied-to story was bridged")
		return false
	}
	return msg != nil
}

// storyDescription returns "your story", "a story by X" or "a story".
func (mc *MessageConverter) storyDescription(ctx context.Context, author, ownACI uuid.UUID) string {
	if author != uuid.Nil && author == ownACI {
		return "your story"
	}
	if author != uuid.Nil && mc.SignalFmtParams != nil && mc.SignalFmtParams.GetUserInfo != nil {
		if name := mc.SignalFmtParams.GetUserInfo(ctx, author).Name; name != "" {
			return "a story by " + name
		}
	}
	return "a story"
}

// applyStoryContext handles a DataMessage answering a story. If the story was bridged, the
// message becomes a Matrix reply to it. Otherwise a visible prefix keeps the context.
func (mc *MessageConverter) applyStoryContext(ctx context.Context, cm *bridgev2.ConvertedMessage, sc *signalpb.DataMessage_StoryContext, ownACI uuid.UUID) {
	author, err := signalmeow.ParseStringOrBinaryUUID(sc.GetAuthorAci(), sc.GetAuthorAciBinary())
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to parse story author ACI")
		return
	}
	id := signalid.MakeMessageID(author, sc.GetSentTimestamp())
	if mc.storyBridged(ctx, id) {
		cm.ReplyTo = &networkid.MessageOptionalPartID{MessageID: id}
		return
	}
	prefix := "Replied to " + mc.storyDescription(ctx, author, ownACI)
	for _, part := range cm.Parts {
		switch part.Content.MsgType {
		case event.MsgText, event.MsgNotice, event.MsgEmote:
			prefixContent(part.Content, prefix+": ")
			return
		}
	}
	cm.Parts = append(cm.Parts, &bridgev2.ConvertedMessagePart{
		Type:    event.EventMessage,
		Content: &event.MessageEventContent{MsgType: event.MsgNotice, Body: prefix},
	})
}

// storyReactionNotice describes a reaction to a story that wasn't bridged, so there is
// nothing to attach a Matrix reaction to.
func (mc *MessageConverter) storyReactionNotice(ctx context.Context, dm *signalpb.DataMessage, ownACI uuid.UUID) *bridgev2.ConvertedMessagePart {
	author, _ := signalmeow.ParseStringOrBinaryUUID(dm.GetStoryContext().GetAuthorAci(), dm.GetStoryContext().GetAuthorAciBinary())
	return &bridgev2.ConvertedMessagePart{
		Type: event.EventMessage,
		Content: &event.MessageEventContent{
			MsgType: event.MsgNotice,
			Body:    fmt.Sprintf("Reacted %s to %s", dm.GetReaction().GetEmoji(), mc.storyDescription(ctx, author, ownACI)),
		},
	}
}

// IsStoryReaction returns true for a reaction that answers a story.
func IsStoryReaction(dm *signalpb.DataMessage) bool {
	return dm.GetReaction() != nil && dm.GetStoryContext() != nil
}

// StoryMessageID returns the ID of the story that a DataMessage answers, or "".
func StoryMessageID(dm *signalpb.DataMessage) networkid.MessageID {
	sc := dm.GetStoryContext()
	if sc == nil {
		return ""
	}
	author, err := signalmeow.ParseStringOrBinaryUUID(sc.GetAuthorAci(), sc.GetAuthorAciBinary())
	if err != nil {
		return ""
	}
	return signalid.MakeMessageID(author, sc.GetSentTimestamp())
}

// StoryBridged reports whether the story a DataMessage answers is in the database.
func (mc *MessageConverter) StoryBridged(ctx context.Context, portal *bridgev2.Portal, dm *signalpb.DataMessage) bool {
	id := StoryMessageID(dm)
	if id == "" {
		return false
	}
	return mc.storyBridged(context.WithValue(ctx, contextKeyPortal, portal), id)
}
