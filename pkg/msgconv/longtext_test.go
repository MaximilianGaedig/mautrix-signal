package msgconv

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

func fakeUpload(got *[]byte) func(context.Context, []byte) (*signalpb.AttachmentPointer, error) {
	return func(_ context.Context, data []byte) (*signalpb.AttachmentPointer, error) {
		*got = data
		return &signalpb.AttachmentPointer{}, nil
	}
}

func TestShortTextIsUntouched(t *testing.T) {
	dm := &signalpb.DataMessage{Body: proto.String("hello")}
	var up []byte
	if err := attachLongText(context.Background(), dm, fakeUpload(&up)); err != nil {
		t.Fatal(err)
	}
	if dm.GetBody() != "hello" || len(dm.Attachments) != 0 || up != nil {
		t.Fatalf("short text changed: %+v", dm)
	}
}

func TestLongTextBecomesBodyPlusAttachment(t *testing.T) {
	// 3-byte runes so that a byte cut lands inside a code point.
	full := strings.Repeat("€", 1500) // 4500 bytes
	dm := &signalpb.DataMessage{
		Body: proto.String(full),
		BodyRanges: []*signalpb.BodyRange{
			{Start: proto.Uint32(0), Length: proto.Uint32(5)},
			{Start: proto.Uint32(600), Length: proto.Uint32(500)}, // runs past the cut
			{Start: proto.Uint32(1000), Length: proto.Uint32(10)}, // wholly past the cut
		},
	}
	var up []byte
	if err := attachLongText(context.Background(), dm, fakeUpload(&up)); err != nil {
		t.Fatal(err)
	}
	if string(up) != full {
		t.Fatal("attachment does not carry the full text")
	}
	if len(dm.GetBody()) > MaxSignalBodyBytes || !utf8.ValidString(dm.GetBody()) || !strings.HasPrefix(full, dm.GetBody()) {
		t.Fatalf("bad body: %d bytes valid=%v", len(dm.GetBody()), utf8.ValidString(dm.GetBody()))
	}
	if n := utf8.RuneCountInString(dm.GetBody()); n != 682 {
		t.Fatalf("expected 682 runes in body, got %d", n)
	}
	if len(dm.Attachments) != 1 || dm.Attachments[0].GetContentType() != "text/x-signal-plain" {
		t.Fatalf("bad attachments: %+v", dm.Attachments)
	}
	if len(dm.BodyRanges) != 2 || dm.BodyRanges[1].GetLength() != 82 {
		t.Fatalf("ranges not clipped to the body: %+v", dm.BodyRanges)
	}
}

func TestTextOverAttachmentLimitIsRejected(t *testing.T) {
	dm := &signalpb.DataMessage{Body: proto.String(strings.Repeat("a", MaxLongTextBytes+1))}
	var up []byte
	if err := attachLongText(context.Background(), dm, fakeUpload(&up)); err == nil {
		t.Fatal("expected an error")
	}
}
