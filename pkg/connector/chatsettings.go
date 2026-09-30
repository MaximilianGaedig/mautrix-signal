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
	"math"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
)

// chatSettingsTags are the room tags used for Signal's pinned and archived chats. Empty means not bridged.
type chatSettingsTags struct {
	Pinned  event.RoomTag
	Archive event.RoomTag
}

func (s *SignalClient) chatSettingsTags() chatSettingsTags {
	return chatSettingsTags{Pinned: s.Main.Config.PinnedTag, Archive: s.Main.Config.ArchiveTag}
}

// tagFor returns the room tag that represents the chat settings, or an empty tag if none applies.
func (t chatSettingsTags) tagFor(cs types.ChatSettings) event.RoomTag {
	if cs.Pinned && t.Pinned != "" {
		return t.Pinned
	} else if cs.Archived && t.Archive != "" {
		return t.Archive
	}
	return ""
}

// effectiveMute normalizes the raw mute value so that all forever mutes and all expired mutes compare equal.
func effectiveMute(cs types.ChatSettings, now time.Time) uint64 {
	if cs.MutedForever() {
		return math.MaxUint64
	} else if _, muted := cs.MuteExpiry(now); !muted {
		return 0
	}
	return cs.MutedUntil
}

func muteTime(cs types.ChatSettings, now time.Time) time.Time {
	if cs.MutedForever() {
		return event.MutedForever
	} else if until, muted := cs.MuteExpiry(now); muted {
		return until
	}
	return bridgev2.Unmuted
}

type chatSettingsPlan struct {
	UserLocal  *bridgev2.UserLocalPortalInfo
	MarkUnread *bool
}

func (p chatSettingsPlan) isEmpty() bool {
	return p.UserLocal == nil && p.MarkUnread == nil
}

// planChatSettings decides which parts of the settings need to be sent to Matrix.
// Only values that differ from what was last seen are included. A nil prev means all defaults.
func planChatSettings(prev *types.ChatSettings, cur types.ChatSettings, tags chatSettingsTags, now time.Time) (plan chatSettingsPlan) {
	old := ptr.Val(prev)
	local := &bridgev2.UserLocalPortalInfo{}
	if effectiveMute(old, now) != effectiveMute(cur, now) {
		local.MutedUntil = ptr.Ptr(muteTime(cur, now))
	}
	if oldTag, newTag := tags.tagFor(old), tags.tagFor(cur); oldTag != newTag {
		local.Tag = ptr.Ptr(newTag)
	}
	if local.MutedUntil != nil || local.Tag != nil {
		plan.UserLocal = local
	}
	if old.MarkedUnread != cur.MarkedUnread {
		plan.MarkUnread = ptr.Ptr(cur.MarkedUnread)
	}
	return
}

// initialUserLocal is the mute and tag state to apply when a room is created.
// Defaults are left out so that creating a room doesn't touch the user's Matrix-side settings.
func initialUserLocal(cur types.ChatSettings, tags chatSettingsTags, now time.Time) *bridgev2.UserLocalPortalInfo {
	local := &bridgev2.UserLocalPortalInfo{}
	if effectiveMute(cur, now) != 0 {
		local.MutedUntil = ptr.Ptr(muteTime(cur, now))
	}
	if tag := tags.tagFor(cur); tag != "" {
		local.Tag = ptr.Ptr(tag)
	}
	if local.MutedUntil == nil && local.Tag == nil {
		return nil
	}
	return local
}

func settingsMetaUpdater(settings types.ChatSettings) bridgev2.ExtraUpdater[*bridgev2.Portal] {
	return func(ctx context.Context, portal *bridgev2.Portal) bool {
		meta := portal.Metadata.(*signalid.PortalMetadata)
		if meta.Settings != nil && *meta.Settings == settings {
			return false
		}
		meta.Settings = &settings
		return true
	}
}

// addChatSettings adds the last seen storage service mute and tag to chat info that is used to create or resync a room.
func (s *SignalClient) addChatSettings(info *bridgev2.ChatInfo, chatID string) {
	settings, ok := s.Client.ChatSettings(chatID)
	if !ok {
		return
	}
	if info.UserLocal == nil {
		info.UserLocal = initialUserLocal(settings, s.chatSettingsTags(), time.Now())
	}
	info.ExtraUpdates = bridgev2.MergeExtraUpdaters(info.ExtraUpdates, func(ctx context.Context, portal *bridgev2.Portal) bool {
		// The unread flag can't be part of chat info, so keep whatever was last sent as a separate event.
		// If it was never sent, the next storage sync notices the difference and sends it.
		stored := settings
		stored.MarkedUnread = false
		if meta := portal.Metadata.(*signalid.PortalMetadata); meta.Settings != nil {
			stored.MarkedUnread = meta.Settings.MarkedUnread
		}
		return settingsMetaUpdater(stored)(ctx, portal)
	})
}

func (s *SignalClient) handleSignalChatSettings(evt *events.ChatSettings) {
	log := s.UserLogin.Log.With().Str("action", "handle chat settings").Logger()
	ctx := log.WithContext(s.Main.Bridge.BackgroundCtx)
	for _, entry := range evt.Chats {
		s.bridgeChatSettings(ctx, entry)
	}
}

func (s *SignalClient) bridgeChatSettings(ctx context.Context, entry events.ChatSettingsEntry) {
	log := zerolog.Ctx(ctx).With().Str("chat_id", entry.ChatID).Logger()
	key := s.makePortalKey(entry.ChatID)
	portal, err := s.Main.Bridge.GetExistingPortalByKey(ctx, key)
	if err != nil {
		log.Err(err).Msg("Failed to get portal to bridge chat settings")
		return
	} else if portal == nil || portal.MXID == "" {
		// Settings of chats without a room are applied when the room is created
		return
	}
	meta, ok := portal.Metadata.(*signalid.PortalMetadata)
	if !ok {
		return
	}
	plan := planChatSettings(meta.Settings, entry.Settings, s.chatSettingsTags(), time.Now())
	if plan.isEmpty() {
		return
	}
	log.Debug().
		Bool("mute_or_tag", plan.UserLocal != nil).
		Any("marked_unread", plan.MarkUnread).
		Msg("Bridging chat settings from Signal storage service")
	now := time.Now()
	res := s.UserLogin.QueueRemoteEvent(&simplevent.ChatInfoChange{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatInfoChange,
			PortalKey: key,
			Timestamp: now,
			LogContext: func(c zerolog.Context) zerolog.Context {
				return c.Str("action", "bridge chat settings")
			},
		},
		ChatInfoChange: &bridgev2.ChatInfoChange{
			ChatInfo: &bridgev2.ChatInfo{
				UserLocal:    plan.UserLocal,
				ExtraUpdates: settingsMetaUpdater(entry.Settings),
			},
		},
	})
	if !res.Success {
		return
	}
	if plan.MarkUnread != nil {
		s.UserLogin.QueueRemoteEvent(&simplevent.MarkUnread{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventMarkUnread,
				PortalKey: key,
				Timestamp: now,
				LogContext: func(c zerolog.Context) zerolog.Context {
					return c.Str("action", "bridge marked unread")
				},
			},
			Unread: *plan.MarkUnread,
		})
	}
}
