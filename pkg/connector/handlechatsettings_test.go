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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
)

func tagContent(tags ...event.RoomTag) *event.TagEventContent {
	content := &event.TagEventContent{Tags: event.Tags{}}
	for _, tag := range tags {
		content.Tags[tag] = event.TagMetadata{}
	}
	return content
}

func TestMuteToStorage(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	forever := (&event.BeeperMuteEventContent{MutedUntil: -1}).GetMutedUntilTime()
	assert.Equal(t, types.MuteForeverProto, muteToStorage(forever, now))
	later := now.Add(2 * time.Hour)
	assert.Equal(t, uint64(later.UnixMilli()), muteToStorage(later, now))
	assert.Zero(t, muteToStorage(now.Add(-time.Hour), now), "an expired mute is an unmute")
	assert.Zero(t, muteToStorage((&event.BeeperMuteEventContent{}).GetMutedUntilTime(), now))
}

func TestTagsToSettingsChange(t *testing.T) {
	t.Run("favourite added pins", func(t *testing.T) {
		change := tagsToSettingsChange(testTags, tagContent(), tagContent(event.RoomTagFavourite))
		require.NotNil(t, change)
		require.NotNil(t, change.Pinned)
		assert.True(t, *change.Pinned)
		require.NotNil(t, change.Archived, "a pinned chat can't be archived")
		assert.False(t, *change.Archived)
	})
	t.Run("favourite removed unpins", func(t *testing.T) {
		change := tagsToSettingsChange(testTags, tagContent(event.RoomTagFavourite), tagContent())
		require.NotNil(t, change)
		assert.False(t, *change.Pinned)
		assert.Nil(t, change.Archived)
	})
	t.Run("nil previous content counts as no tags", func(t *testing.T) {
		change := tagsToSettingsChange(testTags, nil, tagContent(event.RoomTagFavourite))
		require.NotNil(t, change)
		assert.True(t, *change.Pinned)
	})
	t.Run("archive tag archives and unpins", func(t *testing.T) {
		change := tagsToSettingsChange(testTags, tagContent(event.RoomTagFavourite), tagContent(event.RoomTagLowPriority))
		require.NotNil(t, change)
		assert.True(t, *change.Archived)
		assert.False(t, *change.Pinned)
	})
	t.Run("archive tag removed unarchives", func(t *testing.T) {
		change := tagsToSettingsChange(testTags, tagContent(event.RoomTagLowPriority), tagContent())
		require.NotNil(t, change)
		assert.False(t, *change.Archived)
		assert.Nil(t, change.Pinned)
	})
	t.Run("pin wins if both are asked for at once", func(t *testing.T) {
		change := tagsToSettingsChange(testTags, tagContent(), tagContent(event.RoomTagFavourite, event.RoomTagLowPriority))
		require.NotNil(t, change)
		assert.True(t, *change.Pinned)
		assert.False(t, *change.Archived)
	})
	t.Run("unrelated tag changes do nothing", func(t *testing.T) {
		assert.Nil(t, tagsToSettingsChange(testTags, tagContent(), tagContent("u.work")))
		// A favourite that was already there and stays there isn't a change either
		assert.Nil(t, tagsToSettingsChange(testTags, tagContent(event.RoomTagFavourite), tagContent(event.RoomTagFavourite, "u.work")))
		assert.Nil(t, tagsToSettingsChange(testTags, tagContent(), tagContent()))
	})
	t.Run("unconfigured tags do nothing", func(t *testing.T) {
		noArchive := chatSettingsTags{Pinned: event.RoomTagFavourite}
		assert.Nil(t, tagsToSettingsChange(noArchive, tagContent(), tagContent(event.RoomTagLowPriority)))
		change := tagsToSettingsChange(noArchive, tagContent(), tagContent(event.RoomTagFavourite))
		require.NotNil(t, change)
		assert.True(t, *change.Pinned)
		assert.Nil(t, change.Archived, "archive state is left alone when archiving isn't bridged")
		assert.Nil(t, tagsToSettingsChange(chatSettingsTags{}, tagContent(), tagContent(event.RoomTagFavourite)))
	})
}
