package connector

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
)

var _ bridgev2.UserBlockingNetworkAPI = (*SignalClient)(nil)

// ghostBlocker mirrors a block made on Signal into Matrix. *bridgev2.UserLogin is one.
type ghostBlocker interface {
	SetGhostBlocked(ctx context.Context, ghostID networkid.UserID, blocked bool) error
}

// applyBlockChanges ignores the ghosts of the people the user blocked on Signal and stops ignoring the ones
// they unblocked. It returns false if any change failed, so that the event is retried.
func applyBlockChanges(ctx context.Context, blocker ghostBlocker, log *zerolog.Logger, changes *events.BlockChanges) bool {
	ok := true
	apply := func(acis []uuid.UUID, blocked bool) {
		for _, aci := range acis {
			if err := blocker.SetGhostBlocked(ctx, signalid.MakeUserID(aci), blocked); err != nil {
				log.Err(err).Stringer("aci", aci).Bool("blocked", blocked).Msg("Failed to mirror Signal block into the ignore list")
				ok = false
			}
		}
	}
	apply(changes.Blocked, true)
	apply(changes.Unblocked, false)
	return ok
}

// HandleMatrixBlock blocks or unblocks the person a ghost stands for on Signal, where the user ignoring
// or un-ignoring the ghost on Matrix is what triggers it.
func (s *SignalClient) HandleMatrixBlock(ctx context.Context, ghost *bridgev2.Ghost, blocked bool) error {
	aci, err := signalid.ParseUserID(ghost.ID)
	if err != nil {
		return fmt.Errorf("can't block %s: %w", ghost.ID, err)
	} else if aci == s.Client.Store.ACI {
		return fmt.Errorf("can't block yourself")
	}
	return s.Client.SetContactBlocked(ctx, aci, blocked)
}
