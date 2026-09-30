package connector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

func TestReceiptTypeMapping(t *testing.T) {
	for typ, want := range map[signalpb.ReceiptMessage_Type]bridgev2.RemoteEventType{
		signalpb.ReceiptMessage_DELIVERY: bridgev2.RemoteEventDeliveryReceipt,
		signalpb.ReceiptMessage_READ:     bridgev2.RemoteEventReadReceipt,
		signalpb.ReceiptMessage_VIEWED:   bridgev2.RemoteEventReadReceipt,
	} {
		if got := (&Bv2Receipt{Type: typ}).GetType(); got != want {
			t.Errorf("%s: got %v, want %v", typ, got, want)
		}
	}
}
