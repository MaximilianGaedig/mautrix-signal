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

package signalmeow

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
)

// All keys and IDs in these tests are made up.
var (
	testStorageKey = bytes.Repeat([]byte{0x11}, 32)
	testIKM        = bytes.Repeat([]byte{0x22}, 32)
	testGroupKey   = bytes.Repeat([]byte{0x33}, 32)
	testGroupKey2  = bytes.Repeat([]byte{0x44}, 32)
	testACI        = uuid.MustParse("11111111-2222-3333-4444-555555555555")
	testACI2       = uuid.MustParse("66666666-7777-8888-9999-000000000000")
)

// unknownFieldBytes is a protobuf field that no message in the schema defines.
func unknownFieldBytes() []byte {
	b := protowire.AppendTag(nil, 999, protowire.BytesType)
	return protowire.AppendString(b, "future field")
}

func contactStorageRecord(aci uuid.UUID, mutate func(*signalpb.ContactRecord)) *signalpb.StorageRecord {
	contact := &signalpb.ContactRecord{Aci: aci.String(), GivenName: "Test", E164: "+15550100", ProfileKey: bytes.Repeat([]byte{7}, 32)}
	contact.ProtoReflect().SetUnknown(unknownFieldBytes())
	if mutate != nil {
		mutate(contact)
	}
	return &signalpb.StorageRecord{Record: &signalpb.StorageRecord_Contact{Contact: contact}}
}

func groupStorageRecord(key []byte, mutate func(*signalpb.GroupV2Record)) *signalpb.StorageRecord {
	group := &signalpb.GroupV2Record{MasterKey: key, Blocked: true}
	group.ProtoReflect().SetUnknown(unknownFieldBytes())
	if mutate != nil {
		mutate(group)
	}
	return &signalpb.StorageRecord{Record: &signalpb.StorageRecord_GroupV2{GroupV2: group}}
}

func accountStorageRecord(pins ...*signalpb.AccountRecord_PinnedConversation) *signalpb.StorageRecord {
	account := &signalpb.AccountRecord{ProfileKey: bytes.Repeat([]byte{9}, 32), PinnedConversations: pins, ReadReceipts: true}
	account.ProtoReflect().SetUnknown(unknownFieldBytes())
	return &signalpb.StorageRecord{Record: &signalpb.StorageRecord_Account{Account: account}}
}

func pinContact(sid libsignalgo.ServiceID) *signalpb.AccountRecord_PinnedConversation {
	return &signalpb.AccountRecord_PinnedConversation{Identifier: &signalpb.AccountRecord_PinnedConversation_Contact_{
		Contact: &signalpb.AccountRecord_PinnedConversation_Contact{ServiceId: sid.String()},
	}}
}

func pinGroup(key []byte) *signalpb.AccountRecord_PinnedConversation {
	return &signalpb.AccountRecord_PinnedConversation{Identifier: &signalpb.AccountRecord_PinnedConversation_GroupMasterKey{GroupMasterKey: key}}
}

func mustGroupID(t *testing.T, key []byte) types.GroupIdentifier {
	t.Helper()
	id, err := groupIDFromMasterKeyBytes(key)
	require.NoError(t, err)
	return id
}

func TestChatSettingsHelpers(t *testing.T) {
	t.Run("record settings", func(t *testing.T) {
		c := &signalpb.ContactRecord{MutedUntilTimestamp: 5, Archived: true, MarkedUnread: true}
		assert.Equal(t, types.ChatSettings{MutedUntil: 5, Archived: true, MarkedUnread: true}, contactRecordSettings(c))
		g := &signalpb.GroupV2Record{MutedUntilTimestamp: types.MuteForeverProto}
		assert.True(t, groupRecordSettings(g).MutedForever())
		assert.False(t, types.ChatSettings{MutedUntil: 1900000000000}.MutedForever())
	})
	t.Run("pinned ids", func(t *testing.T) {
		sid := libsignalgo.NewACIServiceID(testACI)
		pni := libsignalgo.NewPNIServiceID(testACI2)
		account := accountStorageRecord(
			pinContact(sid),
			&signalpb.AccountRecord_PinnedConversation{Identifier: &signalpb.AccountRecord_PinnedConversation_Contact_{
				Contact: &signalpb.AccountRecord_PinnedConversation_Contact{ServiceIdBinary: pni.Bytes()},
			}},
			pinGroup(testGroupKey),
			pinGroup([]byte("bad key length")),
		).GetAccount()
		pinned := pinnedChatIDs(account, groupIDFromMasterKeyBytes)
		assert.Len(t, pinned, 3)
		assert.Contains(t, pinned, sid.String())
		assert.Contains(t, pinned, pni.String())
		assert.Contains(t, pinned, string(mustGroupID(t, testGroupKey)))
	})
	t.Run("event has pinned chats without records", func(t *testing.T) {
		cli := &Client{}
		cli.recordChatSettings("chat-a", types.ChatSettings{MutedUntil: 9})
		cli.recordPinnedChats(map[string]struct{}{"chat-a": {}, "chat-b": {}})
		evt := cli.chatSettingsEvent()
		require.Len(t, evt.Chats, 2)
		assert.Equal(t, "chat-a", evt.Chats[0].ChatID)
		assert.Equal(t, types.ChatSettings{MutedUntil: 9, Pinned: true}, evt.Chats[0].Settings)
		assert.Equal(t, types.ChatSettings{Pinned: true}, evt.Chats[1].Settings)
		got, ok := cli.ChatSettings("chat-b")
		assert.True(t, ok)
		assert.True(t, got.Pinned)
		_, ok = cli.ChatSettings("chat-c")
		assert.False(t, ok)
	})
}
