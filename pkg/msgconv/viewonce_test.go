package msgconv

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

func mediaMessage() *signalpb.DataMessage {
	return &signalpb.DataMessage{Attachments: []*signalpb.AttachmentPointer{{ContentType: proto.String("image/jpeg")}}}
}

func once() *event.BeeperViewLimitedMedia {
	return &event.BeeperViewLimitedMedia{Type: "count", Count: 1}
}

func TestViewOnceMarksAPhotoOrVideo(t *testing.T) {
	for _, msgType := range []event.MessageType{event.MsgImage, event.MsgVideo} {
		dm := mediaMessage()
		dm.Quote = &signalpb.DataMessage_Quote{Id: proto.Uint64(1)}
		dm.Preview = []*signalpb.Preview{{Url: proto.String("https://example.com")}}
		if err := applyViewOnce(dm, &event.MessageEventContent{MsgType: msgType, BeeperViewLimited: once()}); err != nil {
			t.Fatal(err)
		}
		if !dm.GetIsViewOnce() {
			t.Fatalf("%s: not marked view-once", msgType)
		}
		if dm.GetRequiredProtocolVersion() != uint32(signalpb.DataMessage_VIEW_ONCE_VIDEO) {
			t.Fatalf("%s: required protocol version %d", msgType, dm.GetRequiredProtocolVersion())
		}
		// Signal clients show a view-once message with a quote or a preview as unsupported.
		if dm.Quote != nil || dm.Preview != nil || len(dm.Attachments) != 1 {
			t.Fatalf("%s: not a bare attachment: %+v", msgType, dm)
		}
	}
}

func TestOrdinaryMediaIsNotViewOnce(t *testing.T) {
	dm := mediaMessage()
	dm.Body = proto.String("caption")
	dm.Quote = &signalpb.DataMessage_Quote{Id: proto.Uint64(1)}
	if err := applyViewOnce(dm, &event.MessageEventContent{MsgType: event.MsgImage}); err != nil {
		t.Fatal(err)
	}
	if dm.IsViewOnce != nil || dm.RequiredProtocolVersion != nil || dm.Quote == nil || dm.GetBody() != "caption" {
		t.Fatalf("ordinary media changed: %+v", dm)
	}
}

func TestViewOnceRefusesWhatSignalCannotShow(t *testing.T) {
	twoAttachments := mediaMessage()
	twoAttachments.Attachments = append(twoAttachments.Attachments, &signalpb.AttachmentPointer{})
	captioned := mediaMessage()
	captioned.Body = proto.String("look")
	cases := []struct {
		name    string
		dm      *signalpb.DataMessage
		content *event.MessageEventContent
		want    error
	}{
		{"file", mediaMessage(), &event.MessageEventContent{MsgType: event.MsgFile, BeeperViewLimited: once()}, ErrViewOnceNotMedia},
		{"voice message", mediaMessage(), &event.MessageEventContent{MsgType: event.MsgAudio, BeeperViewLimited: once()}, ErrViewOnceNotMedia},
		{"gif", mediaMessage(), &event.MessageEventContent{MsgType: event.MsgVideo, Info: &event.FileInfo{MauGIF: true}, BeeperViewLimited: once()}, ErrViewOnceNotMedia},
		{"two attachments", twoAttachments, &event.MessageEventContent{MsgType: event.MsgImage, BeeperViewLimited: once()}, ErrViewOnceNotMedia},
		{"caption", captioned, &event.MessageEventContent{MsgType: event.MsgImage, BeeperViewLimited: once()}, bridgev2.ErrCaptionsNotAllowed},
		{"two views", mediaMessage(), &event.MessageEventContent{MsgType: event.MsgImage, BeeperViewLimited: &event.BeeperViewLimitedMedia{Type: "count", Count: 2}}, bridgev2.ErrUnsupportedViewLimitedType},
	}
	for _, c := range cases {
		if err := applyViewOnce(c.dm, c.content); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
		if c.dm.IsViewOnce != nil {
			t.Errorf("%s: marked view-once anyway", c.name)
		}
	}
}
