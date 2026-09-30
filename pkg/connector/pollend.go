package connector

import (
	"context"
	"fmt"

	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

var _ bridgev2.PollEndHandlingNetworkAPI = (*SignalClient)(nil)

// pollTerminate is the message that ends the poll sent at pollTS; like the poll, it needs clients that know
// polls.
func pollTerminate(pollTS, ts uint64) *signalpb.DataMessage {
	return &signalpb.DataMessage{
		Timestamp:               ptr.Ptr(ts),
		PollTerminate:           &signalpb.DataMessage_PollTerminate{TargetSentTimestamp: ptr.Ptr(pollTS)},
		RequiredProtocolVersion: ptr.Ptr(uint32(signalpb.DataMessage_POLLS)),
	}
}

// HandleMatrixPollEnd ends the poll on Signal. Only its author can: Signal clients ignore anyone else's end.
func (s *SignalClient) HandleMatrixPollEnd(ctx context.Context, msg *bridgev2.MatrixPollEnd) error {
	author, pollTS, err := signalid.ParseMessageID(msg.Poll.ID)
	if err != nil {
		return err
	}
	if author != s.Client.Store.ACI {
		return fmt.Errorf("only the poll's author can end it on Signal")
	}
	ts := getTimestampForEvent(msg.InputTransactionID, msg.Event, msg.OrigSender)
	return s.sendMessage(ctx, msg.Portal.ID, signalmeow.WrapDataMessage(pollTerminate(pollTS, ts)))
}
