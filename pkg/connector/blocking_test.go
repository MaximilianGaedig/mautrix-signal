package connector

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
)

type fakeBlocker struct {
	calls map[networkid.UserID]bool
	fail  networkid.UserID
}

func (f *fakeBlocker) SetGhostBlocked(_ context.Context, id networkid.UserID, blocked bool) error {
	if id == f.fail {
		return errors.New("boom")
	}
	f.calls[id] = blocked
	return nil
}

func TestBlockChangesBlockAndUnblockGhosts(t *testing.T) {
	added := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	removed := uuid.MustParse("44444444-4444-4444-8444-444444444444")
	f := &fakeBlocker{calls: map[networkid.UserID]bool{}}
	log := zerolog.Nop()
	if !applyBlockChanges(context.Background(), f, &log, &events.BlockChanges{Blocked: []uuid.UUID{added}, Unblocked: []uuid.UUID{removed}}) {
		t.Fatal("expected success")
	}
	if len(f.calls) != 2 || !f.calls[networkid.UserID(added.String())] || f.calls[networkid.UserID(removed.String())] {
		t.Fatalf("unexpected changes: %v", f.calls)
	}
}

func TestBlockChangesReportFailure(t *testing.T) {
	a := uuid.MustParse("33333333-3333-4333-8333-333333333333")
	b := uuid.MustParse("44444444-4444-4444-8444-444444444444")
	f := &fakeBlocker{calls: map[networkid.UserID]bool{}, fail: networkid.UserID(a.String())}
	log := zerolog.Nop()
	if applyBlockChanges(context.Background(), f, &log, &events.BlockChanges{Blocked: []uuid.UUID{a, b}}) {
		t.Fatal("expected failure to be reported")
	}
	if !f.calls[networkid.UserID(b.String())] {
		t.Fatal("one failure must not stop the rest")
	}
}
