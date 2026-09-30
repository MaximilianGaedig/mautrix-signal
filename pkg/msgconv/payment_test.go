package msgconv

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

func notification(note *string) *signalpb.DataMessage_Payment {
	return &signalpb.DataMessage_Payment{Item: &signalpb.DataMessage_Payment_Notification_{
		Notification: &signalpb.DataMessage_Payment_Notification{
			Transaction: &signalpb.DataMessage_Payment_Notification_MobileCoin_{
				MobileCoin: &signalpb.DataMessage_Payment_Notification_MobileCoin{Receipt: []byte{1, 2, 3}},
			},
			Note: note,
		},
	}}
}

func TestPaymentText(t *testing.T) {
	activation := func(typ signalpb.DataMessage_Payment_Activation_Type) *signalpb.DataMessage_Payment {
		return &signalpb.DataMessage_Payment{Item: &signalpb.DataMessage_Payment_Activation_{
			Activation: &signalpb.DataMessage_Payment_Activation{Type: typ.Enum()},
		}}
	}
	for name, tc := range map[string]struct {
		in   *signalpb.DataMessage_Payment
		want string
	}{
		"no note":   {notification(nil), "Sent a payment"},
		"note":      {notification(proto.String("for lunch")), "Sent a payment: for lunch"},
		"request":   {activation(signalpb.DataMessage_Payment_Activation_REQUEST), "Asked to activate payments"},
		"activated": {activation(signalpb.DataMessage_Payment_Activation_ACTIVATED), "Activated payments"},
	} {
		if got := paymentText(tc.in); got != tc.want {
			t.Errorf("%s: got %q, want %q", name, got, tc.want)
		}
	}
}

func TestGiftBadgeText(t *testing.T) {
	if got := giftBadgeText(false); got != "Sent you a gift badge" {
		t.Errorf("got %q", got)
	}
	if got := giftBadgeText(true); got != "Sent a gift badge" {
		t.Errorf("got %q", got)
	}
}

func TestPaymentAndGiftBadgeParts(t *testing.T) {
	mc := &MessageConverter{}
	part := mc.convertPaymentToMatrix(context.Background(), notification(proto.String("thanks")))
	if part.Content.Body != "Sent a payment: thanks" {
		t.Errorf("payment part body %q", part.Content.Body)
	}
	gift := mc.convertGiftBadgeToMatrix(context.Background(), &signalpb.DataMessage_GiftBadge{}, false)
	if gift.Content.Body != "Sent you a gift badge" {
		t.Errorf("gift part body %q", gift.Content.Body)
	}
}
