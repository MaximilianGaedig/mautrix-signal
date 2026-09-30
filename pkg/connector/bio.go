package connector

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/google/uuid"
	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
)

// bioProfileKey is the extra profile field (MSC4440) all bridges put a person's bio in.
const bioProfileKey = "gay.fomx.biography"

// bioText is a person's About text with their About emoji in front of it.
func bioText(profile types.Profile) string {
	about := strings.TrimSpace(profile.About)
	emoji := strings.TrimSpace(profile.AboutEmoji)
	if emoji == "" {
		return about
	} else if about == "" {
		return emoji
	}
	return emoji + " " + about
}

// bioExtraProfile is the extra profile fields that carry the person's bio. A bio that was removed is written
// as null, which clears the field, but only for a ghost that has one to clear.
func bioExtraProfile(profile types.Profile, hadBio bool) database.ExtraProfile {
	text := bioText(profile)
	if text == "" {
		if !hadBio {
			return nil
		}
		return database.ExtraProfile{bioProfileKey: json.RawMessage("null")}
	}
	var extra database.ExtraProfile
	_ = extra.Set(bioProfileKey, map[string]any{"m.text": []map[string]string{{"body": text}}})
	return extra
}

// ghostHasBio is whether the ghost already carries a bio in its extra profile.
func (s *SignalClient) ghostHasBio(ctx context.Context, contact *types.Recipient) bool {
	if contact.ACI == uuid.Nil {
		return false
	}
	ghost, err := s.Main.Bridge.GetExistingGhostByID(ctx, signalid.MakeUserID(contact.ACI))
	if err != nil || ghost == nil {
		return false
	}
	raw, ok := ghost.ExtraProfile[bioProfileKey]
	return ok && string(raw) != "null"
}
