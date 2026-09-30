package connector

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

var _ bridgev2.PinHandlingNetworkAPI = (*SignalClient)(nil)

func isPinChange(dm *signalpb.DataMessage) bool {
	return dm.PinMessage != nil || dm.UnpinMessage != nil
}

// pinChange turns a pin or unpin into the pinned-state change it stands for. A target without an
// author is the sender's own message, as with reactions.
func pinChange(dm *signalpb.DataMessage, sender uuid.UUID) *bridgev2.ChatInfoChange {
	var authorBinary []byte
	var sentTS uint64
	pinned := dm.PinMessage != nil
	if pinned {
		authorBinary, sentTS = dm.PinMessage.GetTargetAuthorAciBinary(), dm.PinMessage.GetTargetSentTimestamp()
	} else {
		authorBinary, sentTS = dm.UnpinMessage.GetTargetAuthorAciBinary(), dm.UnpinMessage.GetTargetSentTimestamp()
	}
	author := sender
	if len(authorBinary) == 16 {
		author = uuid.UUID(authorBinary)
	}
	return &bridgev2.ChatInfoChange{
		ChatInfo: &bridgev2.ChatInfo{
			PinChanges: []bridgev2.PinChange{{MessageID: signalid.MakeMessageID(author, sentTS), Pinned: pinned}},
		},
	}
}

// pinDataMessage builds the message that pins or unpins a message for everyone. Matrix pins don't
// expire, so neither do the pins sent from it.
func pinDataMessage(author uuid.UUID, sentTS uint64, pinned bool, ts uint64) *signalpb.DataMessage {
	dm := &signalpb.DataMessage{Timestamp: proto.Uint64(ts)}
	if pinned {
		dm.PinMessage = &signalpb.DataMessage_PinMessage{
			TargetAuthorAciBinary: author[:],
			TargetSentTimestamp:   proto.Uint64(sentTS),
			PinDuration:           &signalpb.DataMessage_PinMessage_PinDurationForever{PinDurationForever: true},
		}
	} else {
		dm.UnpinMessage = &signalpb.DataMessage_UnpinMessage{
			TargetAuthorAciBinary: author[:],
			TargetSentTimestamp:   proto.Uint64(sentTS),
		}
	}
	return dm
}

func (s *SignalClient) HandleMatrixPin(ctx context.Context, msg *bridgev2.MatrixPin) error {
	author, sentTS, err := signalid.ParseMessageID(msg.TargetMessage.ID)
	if err != nil {
		return fmt.Errorf("failed to parse target message ID: %w", err)
	}
	ts := getTimestampForEvent(msg.InputTransactionID, msg.Event, msg.OrigSender)
	return s.sendMessage(ctx, msg.Portal.ID, signalmeow.WrapDataMessage(pinDataMessage(author, sentTS, msg.Pinned, ts)))
}
