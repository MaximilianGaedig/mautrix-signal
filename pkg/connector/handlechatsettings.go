// mautrix-signal - A Matrix-signal puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
)

var (
	_ bridgev2.MuteHandlingNetworkAPI         = (*SignalClient)(nil)
	_ bridgev2.MarkedUnreadHandlingNetworkAPI = (*SignalClient)(nil)
	_ bridgev2.TagHandlingNetworkAPI          = (*SignalClient)(nil)
)

func chatRefForPortal(portal *bridgev2.Portal) (signalmeow.ChatRef, string, error) {
	userID, groupID, err := signalid.ParsePortalID(portal.ID)
	if err != nil {
		return signalmeow.ChatRef{}, "", fmt.Errorf("failed to parse portal id: %w", err)
	}
	if groupID != "" {
		return signalmeow.ChatRef{GroupID: groupID}, string(groupID), nil
	}
	return signalmeow.ChatRef{ServiceID: userID}, userID.String(), nil
}

// muteToStorage converts a Matrix mute time to Signal's mutedUntilTimestamp.
func muteToStorage(mutedUntil time.Time, now time.Time) uint64 {
	if mutedUntil.Equal(event.MutedForever) {
		return types.MuteForeverProto
	} else if mutedUntil.After(now) {
		return uint64(mutedUntil.UnixMilli())
	}
	return 0
}

func (s *SignalClient) updateChatSettings(ctx context.Context, portal *bridgev2.Portal, change signalmeow.ChatSettingsChange) error {
	chat, chatID, err := chatRefForPortal(portal)
	if err != nil {
		return err
	}
	err = s.Client.UpdateChatSettings(ctx, chat, change)
	if errors.Is(err, signalmeow.ErrTooManyPinnedChats) {
		return fmt.Errorf("Signal only allows %d pinned chats: %w", signalmeow.MaxPinnedChats, err)
	} else if err != nil {
		return err
	}
	// The write is now part of what the storage service says, so it isn't a change to bridge back.
	if settings, ok := s.Client.ChatSettings(chatID); ok {
		if meta, ok := portal.Metadata.(*signalid.PortalMetadata); ok && (meta.Settings == nil || *meta.Settings != settings) {
			meta.Settings = &settings
			if err = portal.Save(ctx); err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("Failed to save portal after updating chat settings")
			}
		}
	}
	return nil
}

func (s *SignalClient) HandleMute(ctx context.Context, msg *bridgev2.MatrixMute) error {
	mutedUntil := muteToStorage(msg.Content.GetMutedUntilTime(), time.Now())
	return s.updateChatSettings(ctx, msg.Portal, signalmeow.ChatSettingsChange{MutedUntil: &mutedUntil})
}

func (s *SignalClient) HandleMarkedUnread(ctx context.Context, msg *bridgev2.MatrixMarkedUnread) error {
	return s.updateChatSettings(ctx, msg.Portal, signalmeow.ChatSettingsChange{MarkedUnread: &msg.Content.Unread})
}

func (s *SignalClient) HandleRoomTag(ctx context.Context, msg *bridgev2.MatrixRoomTag) error {
	change := tagsToSettingsChange(s.chatSettingsTags(), msg.PrevContent, msg.Content)
	if change == nil {
		return nil
	}
	return s.updateChatSettings(ctx, msg.Portal, *change)
}

// tagsToSettingsChange finds out which of Signal's pinned and archived states the Matrix tag change asks for.
// Only tags that were added or removed count, so unrelated tag changes never touch the chat in Signal.
func tagsToSettingsChange(tags chatSettingsTags, prev, cur *event.TagEventContent) *signalmeow.ChatSettingsChange {
	has := func(content *event.TagEventContent, tag event.RoomTag) bool {
		if content == nil {
			return false
		}
		_, ok := content.Tags[tag]
		return ok
	}
	var change signalmeow.ChatSettingsChange
	if tags.Pinned != "" && has(prev, tags.Pinned) != has(cur, tags.Pinned) {
		change.Pinned = ptr.Ptr(has(cur, tags.Pinned))
	}
	if tags.Archive != "" && tags.Archive != tags.Pinned && has(prev, tags.Archive) != has(cur, tags.Archive) {
		change.Archived = ptr.Ptr(has(cur, tags.Archive))
	}
	// Signal doesn't keep archived chats pinned. If both are asked for at once, pinning wins.
	if change.Pinned != nil && *change.Pinned && tags.Archive != "" {
		change.Archived = ptr.Ptr(false)
	} else if change.Archived != nil && *change.Archived && tags.Pinned != "" {
		change.Pinned = ptr.Ptr(false)
	}
	if change.Pinned == nil && change.Archived == nil {
		return nil
	}
	return &change
}
