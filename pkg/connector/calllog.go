package connector

import (
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/calllog"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

// callLogger turns Signal's 1:1 call signalling, and what the user's other devices report about their calls,
// into one editable timeline entry per call.
type callLogger struct {
	log       *calllog.Log
	self      uuid.UUID
	portalKey func(chatID string) networkid.PortalKey
	sender    func(uuid.UUID) bridgev2.EventSender
	// bridged holds the calls that call bridging carries as a real Matrix call, which are not logged as well.
	bridged sync.Map
}

func newCallLogger(self uuid.UUID, portalKey func(string) networkid.PortalKey, sender func(uuid.UUID) bridgev2.EventSender) *callLogger {
	return &callLogger{log: calllog.New(), self: self, portalKey: portalKey, sender: sender}
}

func callKey(id uint64) string {
	return strconv.FormatUint(id, 10)
}

// markBridged records that call bridging handles this call, so the log leaves it alone.
func (cl *callLogger) markBridged(id uint64) {
	cl.bridged.Store(id, struct{}{})
}

func (cl *callLogger) isBridged(id uint64) bool {
	_, ok := cl.bridged.Load(id)
	return ok
}

func nonNil(evts ...bridgev2.RemoteEvent) []bridgev2.RemoteEvent {
	out := evts[:0]
	for _, evt := range evts {
		if evt != nil {
			out = append(out, evt)
		}
	}
	return out
}

// fromSignal is what a call signalling message from another user (or one of the user's own devices) changes.
func (cl *callLogger) fromSignal(evt *events.Call) []bridgev2.RemoteEvent {
	if cl.isBridged(evt.ID) {
		return nil
	}
	key := callKey(evt.ID)
	at := time.UnixMilli(int64(evt.Timestamp))
	switch evt.MessageType {
	case events.CallMessageOffer:
		return nonNil(cl.log.Start(key, calllog.Call{
			Portal:  cl.portalKey(evt.Info.ChatID),
			Caller:  cl.sender(evt.Info.Sender),
			Video:   evt.Type == events.CallTypeVideo,
			Started: at,
		}))
	case events.CallMessageAnswer:
		return nonNil(cl.log.Answer(key, at))
	case events.CallMessageBusy:
		return nonNil(cl.log.Decline(key, at))
	case events.CallMessageHangup:
		switch evt.HangupType {
		case signalpb.CallMessage_Hangup_HANGUP_ACCEPTED:
			// Another device of the user picked the call up.
			return nonNil(cl.log.Answer(key, at))
		case signalpb.CallMessage_Hangup_HANGUP_DECLINED, signalpb.CallMessage_Hangup_HANGUP_BUSY,
			signalpb.CallMessage_Hangup_HANGUP_NEED_PERMISSION:
			return nonNil(cl.log.Decline(key, at))
		default:
			return nonNil(cl.log.End(key, at))
		}
	}
	return nil
}

// fromSync is what a call event from one of the user's other devices changes. Calls that device placed were
// never signalled to this one, so they are logged from the report itself.
func (cl *callLogger) fromSync(evt *events.CallSync) []bridgev2.RemoteEvent {
	raw := evt.Raw
	if raw.GetType() != signalpb.SyncMessage_CallEvent_AUDIO_CALL && raw.GetType() != signalpb.SyncMessage_CallEvent_VIDEO_CALL {
		// Group and ad-hoc calls have no ring to log.
		return nil
	}
	if raw.GetEvent() != signalpb.SyncMessage_CallEvent_ACCEPTED && raw.GetEvent() != signalpb.SyncMessage_CallEvent_NOT_ACCEPTED {
		return nil
	}
	if cl.isBridged(raw.GetCallId()) {
		return nil
	}
	peer, err := libsignalgo.ServiceIDFromBytes(raw.GetConversationId())
	if err != nil {
		return nil
	}
	outgoing := raw.GetDirection() == signalpb.SyncMessage_CallEvent_OUTGOING
	caller := peer.UUID
	if outgoing {
		caller = cl.self
	}
	at := time.UnixMilli(int64(raw.GetTimestamp()))
	if raw.GetTimestamp() == 0 {
		at = time.UnixMilli(int64(evt.Timestamp))
	}
	key := callKey(raw.GetCallId())
	start := cl.log.Start(key, calllog.Call{
		Portal:   cl.portalKey(peer.String()),
		Caller:   cl.sender(caller),
		Video:    raw.GetType() == signalpb.SyncMessage_CallEvent_VIDEO_CALL,
		Outgoing: outgoing,
		Started:  at,
	})
	var out []bridgev2.RemoteEvent
	out = append(out, start)
	if raw.GetEvent() == signalpb.SyncMessage_CallEvent_ACCEPTED {
		out = append(out, cl.log.Answer(key, at))
		if start != nil {
			// Nothing tells us when a call we never saw ringing ended, so don't leave it "in progress".
			out = append(out, cl.log.End(key, at))
		}
	} else if outgoing {
		out = append(out, cl.log.End(key, at))
	} else {
		out = append(out, cl.log.Decline(key, at))
	}
	return nonNil(out...)
}
