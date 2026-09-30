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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
)

var (
	testNow  = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	testTags = chatSettingsTags{Pinned: event.RoomTagFavourite, Archive: event.RoomTagLowPriority}
)

func TestPlanChatSettings_Mute(t *testing.T) {
	future := testNow.Add(3 * time.Hour)
	past := testNow.Add(-3 * time.Hour)
	tests := []struct {
		name string
		prev *types.ChatSettings
		cur  types.ChatSettings
		want *time.Time // nil = no mute update
	}{
		{"forever", nil, types.ChatSettings{MutedUntil: types.MuteForeverProto}, &event.MutedForever},
		{"javascript max date is forever too", nil, types.ChatSettings{MutedUntil: 8_640_000_000_000_000}, &event.MutedForever},
		{"until", nil, types.ChatSettings{MutedUntil: uint64(future.UnixMilli())}, &future},
		{"none, nothing seen before", nil, types.ChatSettings{}, nil},
		{"expired, nothing seen before", nil, types.ChatSettings{MutedUntil: uint64(past.UnixMilli())}, nil},
		{"unmuted", &types.ChatSettings{MutedUntil: types.MuteForeverProto}, types.ChatSettings{}, &bridgev2.Unmuted},
		{"same forever mute", &types.ChatSettings{MutedUntil: types.MuteForeverProto}, types.ChatSettings{MutedUntil: types.MuteForeverProto}, nil},
		{"forever with different raw value", &types.ChatSettings{MutedUntil: types.MuteForeverProto}, types.ChatSettings{MutedUntil: 8_640_000_000_000_001}, nil},
		{"mute extended", &types.ChatSettings{MutedUntil: uint64(future.UnixMilli())}, types.ChatSettings{MutedUntil: types.MuteForeverProto}, &event.MutedForever},
		{"mute expired by itself", &types.ChatSettings{MutedUntil: uint64(past.UnixMilli())}, types.ChatSettings{}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := planChatSettings(tt.prev, tt.cur, testTags, testNow)
			if tt.want == nil {
				assert.True(t, plan.UserLocal == nil || plan.UserLocal.MutedUntil == nil, "unexpected mute update: %+v", plan.UserLocal)
				return
			}
			require.NotNil(t, plan.UserLocal)
			require.NotNil(t, plan.UserLocal.MutedUntil)
			assert.True(t, plan.UserLocal.MutedUntil.Equal(*tt.want), "got %s, want %s", plan.UserLocal.MutedUntil, tt.want)
			assert.Nil(t, plan.UserLocal.Tag)
			assert.Nil(t, plan.MarkUnread)
		})
	}
}

func TestPlanChatSettings_Tags(t *testing.T) {
	tag := func(p chatSettingsPlan) *event.RoomTag {
		if p.UserLocal == nil {
			return nil
		}
		return p.UserLocal.Tag
	}
	t.Run("pinned", func(t *testing.T) {
		plan := planChatSettings(nil, types.ChatSettings{Pinned: true}, testTags, testNow)
		require.NotNil(t, tag(plan))
		assert.Equal(t, event.RoomTagFavourite, *tag(plan))
		assert.Nil(t, plan.UserLocal.MutedUntil)
	})
	t.Run("archived", func(t *testing.T) {
		plan := planChatSettings(nil, types.ChatSettings{Archived: true}, testTags, testNow)
		require.NotNil(t, tag(plan))
		assert.Equal(t, event.RoomTagLowPriority, *tag(plan))
	})
	t.Run("pinned wins over archived", func(t *testing.T) {
		plan := planChatSettings(nil, types.ChatSettings{Pinned: true, Archived: true}, testTags, testNow)
		assert.Equal(t, event.RoomTagFavourite, *tag(plan))
	})
	t.Run("unpinned removes the tag", func(t *testing.T) {
		plan := planChatSettings(&types.ChatSettings{Pinned: true}, types.ChatSettings{}, testTags, testNow)
		require.NotNil(t, tag(plan))
		assert.Equal(t, event.RoomTag(""), *tag(plan))
	})
	t.Run("pinned to archived swaps the tag", func(t *testing.T) {
		plan := planChatSettings(&types.ChatSettings{Pinned: true}, types.ChatSettings{Archived: true}, testTags, testNow)
		assert.Equal(t, event.RoomTagLowPriority, *tag(plan))
	})
	t.Run("unchanged", func(t *testing.T) {
		plan := planChatSettings(&types.ChatSettings{Pinned: true}, types.ChatSettings{Pinned: true}, testTags, testNow)
		assert.True(t, plan.isEmpty())
	})
	t.Run("archive not configured is left alone", func(t *testing.T) {
		noArchive := chatSettingsTags{Pinned: event.RoomTagFavourite}
		assert.True(t, planChatSettings(nil, types.ChatSettings{Archived: true}, noArchive, testNow).isEmpty())
		assert.True(t, planChatSettings(&types.ChatSettings{Archived: true}, types.ChatSettings{}, noArchive, testNow).isEmpty())
	})
	t.Run("pin not configured is left alone", func(t *testing.T) {
		assert.True(t, planChatSettings(nil, types.ChatSettings{Pinned: true}, chatSettingsTags{}, testNow).isEmpty())
	})
}

func TestPlanChatSettings_Unread(t *testing.T) {
	plan := planChatSettings(nil, types.ChatSettings{MarkedUnread: true}, testTags, testNow)
	require.NotNil(t, plan.MarkUnread)
	assert.True(t, *plan.MarkUnread)
	assert.Nil(t, plan.UserLocal)

	plan = planChatSettings(&types.ChatSettings{MarkedUnread: true}, types.ChatSettings{}, testTags, testNow)
	require.NotNil(t, plan.MarkUnread)
	assert.False(t, *plan.MarkUnread)

	assert.True(t, planChatSettings(&types.ChatSettings{MarkedUnread: true}, types.ChatSettings{MarkedUnread: true}, testTags, testNow).isEmpty())
	assert.True(t, planChatSettings(nil, types.ChatSettings{}, testTags, testNow).isEmpty())
}

func TestInitialUserLocal(t *testing.T) {
	assert.Nil(t, initialUserLocal(types.ChatSettings{}, testTags, testNow), "defaults don't touch the room")
	assert.Nil(t, initialUserLocal(types.ChatSettings{MutedUntil: uint64(testNow.Add(-time.Hour).UnixMilli())}, testTags, testNow))
	local := initialUserLocal(types.ChatSettings{MutedUntil: types.MuteForeverProto, Pinned: true}, testTags, testNow)
	require.NotNil(t, local)
	assert.True(t, local.MutedUntil.Equal(event.MutedForever))
	assert.Equal(t, event.RoomTagFavourite, *local.Tag)
}

func TestSettingsMetaUpdater(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{}}
	portal.Metadata = &signalid.PortalMetadata{}
	settings := types.ChatSettings{MutedUntil: 5, Pinned: true}
	assert.True(t, settingsMetaUpdater(settings)(context.Background(), portal))
	assert.Equal(t, &settings, portal.Metadata.(*signalid.PortalMetadata).Settings)
	assert.False(t, settingsMetaUpdater(settings)(context.Background(), portal), "no save if nothing changed")
}
