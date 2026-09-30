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

package types

import "time"

// MuteForeverProto is what Signal clients write to mutedUntilTimestamp for an indefinite mute
// (Int64 max, see MuteExpiration.ALWAYS_PROTO in Signal-Desktop).
const MuteForeverProto uint64 = 1<<63 - 1

// maxSafeDateMillis is the largest timestamp a JavaScript Date can hold.
// Signal-Desktop treats any stored mute at or above it as "muted forever".
const maxSafeDateMillis uint64 = 8_640_000_000_000_000

// ChatSettings is the per-chat state the storage service keeps for a contact or group.
type ChatSettings struct {
	// MutedUntil is the raw mutedUntilTimestamp in milliseconds. 0 means not muted.
	MutedUntil   uint64 `json:"muted_until,omitempty"`
	Archived     bool   `json:"archived,omitempty"`
	MarkedUnread bool   `json:"marked_unread,omitempty"`
	// Pinned comes from the account record's pinned conversation list, not from the chat's own record.
	Pinned bool `json:"pinned,omitempty"`
}

// MutedForever returns true if the mute never expires.
func (cs ChatSettings) MutedForever() bool {
	return cs.MutedUntil >= maxSafeDateMillis
}

// MuteExpiry returns the time the mute ends. The bool is false if the chat is not muted at the given time.
func (cs ChatSettings) MuteExpiry(now time.Time) (until time.Time, muted bool) {
	if cs.MutedUntil == 0 || cs.MutedForever() {
		return time.Time{}, cs.MutedForever()
	}
	until = time.UnixMilli(int64(cs.MutedUntil))
	return until, until.After(now)
}
