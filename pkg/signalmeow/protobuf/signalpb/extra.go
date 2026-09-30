package signalpb

type BodyRangeAssociatedValue = isBodyRange_AssociatedValue

type ChatEventContent interface {
	isChatEventContent()
}

func (*DataMessage) isChatEventContent()   {}
func (*TypingMessage) isChatEventContent() {}
func (*EditMessage) isChatEventContent()   {}

// StoryEvent is a story posted by someone, delivered as a chat event.
// StoryMessage itself carries no timestamp, so the sent timestamp is stored next to it.
// Replies to the story reference this timestamp in their StoryContext.
type StoryEvent struct {
	Story     *StoryMessage
	Timestamp uint64
}

func (*StoryEvent) isChatEventContent() {}
