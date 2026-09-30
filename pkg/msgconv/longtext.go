package msgconv

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

// These mirror Signal-Desktop's ts/util/longAttachment.std.ts: a message body is at most 2 KiB, and
// anything longer is sent as the first 2 KiB plus the whole text as a text/x-signal-plain attachment of at
// most 64 KiB.
const (
	MaxSignalBodyBytes  = 2 * 1024
	MaxLongTextBytes    = 64 * 1024
	longTextContentType = "text/x-signal-plain"
)

// ErrLongTextTooLong is returned for a text that does not fit in a long-text attachment either.
var ErrLongTextTooLong = fmt.Errorf("message is too long for Signal (limit %d bytes)", MaxLongTextBytes)

// trimBody cuts body to at most limit bytes without splitting a code point (or leaving a dangling
// zero-width joiner).
func trimBody(body string, limit int) string {
	if len(body) <= limit {
		return body
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(body[cut]) {
		cut--
	}
	trimmed := strings.TrimRight(body[:cut], "‍")
	if trimmed == "" {
		return "￾"
	}
	return trimmed
}

// clipRanges keeps the body ranges that start inside a body of length utf16Len (in UTF-16 code units)
// and shortens the ones that run past its end.
func clipRanges(ranges []*signalpb.BodyRange, utf16Len uint32) []*signalpb.BodyRange {
	var out []*signalpb.BodyRange
	for _, r := range ranges {
		if r.GetStart() >= utf16Len {
			continue
		}
		if r.GetStart()+r.GetLength() > utf16Len {
			r = proto.Clone(r).(*signalpb.BodyRange)
			r.Length = proto.Uint32(utf16Len - r.GetStart())
		}
		out = append(out, r)
	}
	return out
}

// splitLongText returns the body to put in the Signal message and, when body was too long for one, the full
// text to attach as a long-text attachment.
func splitLongText(body string, ranges []*signalpb.BodyRange) (trimmed string, trimmedRanges []*signalpb.BodyRange, full []byte, err error) {
	if len(body) <= MaxSignalBodyBytes {
		return body, ranges, nil, nil
	}
	if len(body) > MaxLongTextBytes {
		return "", nil, nil, ErrLongTextTooLong
	}
	trimmed = trimBody(body, MaxSignalBodyBytes)
	trimmedRanges = clipRanges(ranges, uint32(len(utf16.Encode([]rune(trimmed)))))
	return trimmed, trimmedRanges, []byte(body), nil
}

// attachLongText makes dm carry its body as Signal-Desktop does when the body is too long.
func attachLongText(ctx context.Context, dm *signalpb.DataMessage, upload func(context.Context, []byte) (*signalpb.AttachmentPointer, error)) error {
	if dm.Body == nil {
		return nil
	}
	trimmed, ranges, full, err := splitLongText(dm.GetBody(), dm.BodyRanges)
	if err != nil {
		return err
	}
	if full == nil {
		return nil
	}
	att, err := upload(ctx, full)
	if err != nil {
		return fmt.Errorf("failed to upload long text: %w", err)
	}
	att.ContentType = proto.String(longTextContentType)
	att.FileName = proto.String(fmt.Sprintf("long-message-%d.txt", time.Now().UnixMilli()))
	dm.Body = proto.String(trimmed)
	dm.BodyRanges = ranges
	dm.Attachments = append(dm.Attachments, att)
	return nil
}
