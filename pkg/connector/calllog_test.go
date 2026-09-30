package connector

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/calllog"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

var (
	testSelf = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	testPeer = uuid.MustParse("22222222-2222-4222-8222-222222222222")
)

func testCallLogger() *callLogger {
	return newCallLogger(testSelf,
		func(chatID string) networkid.PortalKey { return networkid.PortalKey{ID: networkid.PortalID(chatID)} },
		func(u uuid.UUID) bridgev2.EventSender {
			return bridgev2.EventSender{IsFromMe: u == testSelf, Sender: networkid.UserID(u.String())}
		})
}

const t0 = 1_700_000_000_000

func callSignal(id uint64, typ events.CallMessageType, ms uint64) *events.Call {
	return &events.Call{
		Info:        events.MessageInfo{Sender: testPeer, ChatID: testPeer.String()},
		Timestamp:   t0 + ms,
		ID:          id,
		Direction:   events.CallDirectionIncoming,
		MessageType: typ,
	}
}

// texts is what each queued event shows the call as.
func texts(t *testing.T, evts []bridgev2.RemoteEvent) (out []string) {
	t.Helper()
	for _, evt := range evts {
		msg, ok := evt.(*simplevent.Message[*calllog.Call])
		if !ok {
			t.Fatalf("unexpected event %T", evt)
		}
		out = append(out, msg.Data.Text())
	}
	return
}

func eq(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestOfferAnswerHangupIsOneEditedEntry(t *testing.T) {
	cl := testCallLogger()
	offer := callSignal(7, events.CallMessageOffer, 0)
	offer.Type = events.CallTypeVideo
	start := cl.fromSignal(offer)
	eq(t, texts(t, start), "Incoming video call")
	if start[0].GetType() != bridgev2.RemoteEventMessage {
		t.Fatalf("offer should post a message, got %s", start[0].GetType())
	}

	accepted := callSignal(7, events.CallMessageHangup, 2000)
	accepted.HangupType = signalpb.CallMessage_Hangup_HANGUP_ACCEPTED
	ans := cl.fromSignal(accepted)
	eq(t, texts(t, ans), "Video call in progress")
	if ans[0].GetType() != bridgev2.RemoteEventEdit {
		t.Fatalf("answer should edit, got %s", ans[0].GetType())
	}

	end := cl.fromSignal(callSignal(7, events.CallMessageHangup, 65000))
	eq(t, texts(t, end), "Video call, 1:03")
}

func TestUnansweredHangupIsMissed(t *testing.T) {
	cl := testCallLogger()
	cl.fromSignal(callSignal(8, events.CallMessageOffer, 0))
	eq(t, texts(t, cl.fromSignal(callSignal(8, events.CallMessageHangup, 1000))), "Missed voice call")
}

func TestDeclinedAndBusy(t *testing.T) {
	cl := testCallLogger()
	cl.fromSignal(callSignal(9, events.CallMessageOffer, 0))
	declined := callSignal(9, events.CallMessageHangup, 1000)
	declined.HangupType = signalpb.CallMessage_Hangup_HANGUP_DECLINED
	eq(t, texts(t, cl.fromSignal(declined)), "Declined voice call")

	cl.fromSignal(callSignal(10, events.CallMessageOffer, 0))
	eq(t, texts(t, cl.fromSignal(callSignal(10, events.CallMessageBusy, 1000))), "Declined voice call")
}

func TestICEAndOpaqueChangeNothing(t *testing.T) {
	cl := testCallLogger()
	cl.fromSignal(callSignal(11, events.CallMessageOffer, 0))
	if got := cl.fromSignal(callSignal(11, events.CallMessageICE, 10)); len(got) != 0 {
		t.Fatalf("ICE produced %d events", len(got))
	}
}

func syncEvt(id uint64, typ signalpb.SyncMessage_CallEvent_Type, dir signalpb.SyncMessage_CallEvent_Direction, ev signalpb.SyncMessage_CallEvent_Event, ms uint64) *events.CallSync {
	return &events.CallSync{Timestamp: t0 + ms, Raw: &signalpb.SyncMessage_CallEvent{
		ConversationId: testPeer[:],
		CallId:         proto.Uint64(id),
		Timestamp:      proto.Uint64(t0 + ms),
		Type:           typ.Enum(),
		Direction:      dir.Enum(),
		Event:          ev.Enum(),
	}}
}

func TestSyncDeclineUpdatesTheRingingEntry(t *testing.T) {
	cl := testCallLogger()
	cl.fromSignal(callSignal(12, events.CallMessageOffer, 0))
	got := cl.fromSync(syncEvt(12, signalpb.SyncMessage_CallEvent_AUDIO_CALL, signalpb.SyncMessage_CallEvent_INCOMING, signalpb.SyncMessage_CallEvent_NOT_ACCEPTED, 3000))
	eq(t, texts(t, got), "Declined voice call")
	if got[0].GetType() != bridgev2.RemoteEventEdit {
		t.Fatalf("should edit the existing entry, got %s", got[0].GetType())
	}
}

func TestSyncAnswerOnOtherDeviceUpdatesTheEntry(t *testing.T) {
	cl := testCallLogger()
	cl.fromSignal(callSignal(13, events.CallMessageOffer, 0))
	got := cl.fromSync(syncEvt(13, signalpb.SyncMessage_CallEvent_AUDIO_CALL, signalpb.SyncMessage_CallEvent_INCOMING, signalpb.SyncMessage_CallEvent_ACCEPTED, 3000))
	eq(t, texts(t, got), "Voice call in progress")
}

func TestSyncOfCallPlacedElsewhereIsLogged(t *testing.T) {
	cl := testCallLogger()
	got := cl.fromSync(syncEvt(14, signalpb.SyncMessage_CallEvent_VIDEO_CALL, signalpb.SyncMessage_CallEvent_OUTGOING, signalpb.SyncMessage_CallEvent_ACCEPTED, 0))
	eq(t, texts(t, got), "Outgoing video call", "Video call in progress", "Video call ended")
	msg := got[0].(*simplevent.Message[*calllog.Call])
	if !msg.Data.Outgoing || !msg.GetSender().IsFromMe {
		t.Fatalf("placed by the user: %+v", msg.Data)
	}

	got = cl.fromSync(syncEvt(15, signalpb.SyncMessage_CallEvent_AUDIO_CALL, signalpb.SyncMessage_CallEvent_OUTGOING, signalpb.SyncMessage_CallEvent_NOT_ACCEPTED, 0))
	eq(t, texts(t, got), "Outgoing voice call", "Cancelled voice call")
}

func TestSyncIgnoresGroupCallsAndDeletes(t *testing.T) {
	cl := testCallLogger()
	if got := cl.fromSync(syncEvt(16, signalpb.SyncMessage_CallEvent_GROUP_CALL, signalpb.SyncMessage_CallEvent_INCOMING, signalpb.SyncMessage_CallEvent_ACCEPTED, 0)); len(got) != 0 {
		t.Fatalf("group call produced %d events", len(got))
	}
	if got := cl.fromSync(syncEvt(17, signalpb.SyncMessage_CallEvent_AUDIO_CALL, signalpb.SyncMessage_CallEvent_INCOMING, signalpb.SyncMessage_CallEvent_DELETE, 0)); len(got) != 0 {
		t.Fatalf("delete produced %d events", len(got))
	}
}

func TestBridgedCallsAreNotLoggedTwice(t *testing.T) {
	cl := testCallLogger()
	cl.markBridged(18)
	if got := cl.fromSignal(callSignal(18, events.CallMessageOffer, 0)); len(got) != 0 {
		t.Fatalf("bridged offer logged %d events", len(got))
	}
	if got := cl.fromSync(syncEvt(18, signalpb.SyncMessage_CallEvent_AUDIO_CALL, signalpb.SyncMessage_CallEvent_INCOMING, signalpb.SyncMessage_CallEvent_ACCEPTED, 0)); len(got) != 0 {
		t.Fatalf("bridged sync logged %d events", len(got))
	}
}

func TestIsBridgeableOffer(t *testing.T) {
	if isBridgeableOffer(callSignal(1, events.CallMessageOffer, 0)) {
		t.Fatal("an offer without connection parameters is not bridgeable")
	}
}

func TestGroupCallNotice(t *testing.T) {
	for started, want := range map[bool]string{true: "Started a group call", false: "Group call ended"} {
		conv, err := convertGroupCallEvent(context.Background(), nil, nil, &events.Call{IsRinging: started})
		if err != nil {
			t.Fatal(err)
		}
		if got := conv.Parts[0].Content.Body; got != want {
			t.Errorf("started=%v: got %q, want %q", started, got, want)
		}
	}
}
