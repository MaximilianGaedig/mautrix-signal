package ringrtc

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

func TestAcceptedRTPDataEncoding(t *testing.T) {
	encoded := encodeAccepted(0xCA111D, 7)
	acceptedID, ok := decodeAccepted(encoded)
	if !ok || acceptedID != 0xCA111D {
		t.Fatalf("unexpected accepted message: id=%x ok=%t", acceptedID, ok)
	}
	accepted := protowire.AppendTag(nil, 1, protowire.VarintType)
	accepted = protowire.AppendVarint(accepted, 0xCA111D)
	want := protowire.AppendTag(nil, 1, protowire.BytesType)
	want = protowire.AppendBytes(want, accepted)
	want = protowire.AppendTag(want, 4, protowire.VarintType)
	want = protowire.AppendVarint(want, 7)
	if !bytes.Equal(encoded, want) {
		t.Fatalf("unexpected protobuf wire encoding: %x", encoded)
	}
	if _, ok = decodeAccepted([]byte{0x0a, 0xff}); ok {
		t.Fatal("malformed accepted message was decoded")
	}
}
