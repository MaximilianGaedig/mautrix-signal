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
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
)

func manifestFor(version uint64, ikm []byte, ids map[string]signalpb.ManifestRecord_Identifier_Type, order []string) *signalpb.ManifestRecord {
	m := &signalpb.ManifestRecord{Version: version, SourceDevice: 1, RecordIkm: ikm}
	for _, id := range order {
		m.Identifiers = append(m.Identifiers, &signalpb.ManifestRecord_Identifier{Raw: []byte(id), Type: ids[id]})
	}
	return m
}

func b64(id string) string { return base64.StdEncoding.EncodeToString([]byte(id)) }

func snapshotOf(records map[string]*signalpb.StorageRecord, order []string) *storageSnapshot {
	snap := &storageSnapshot{Manifest: &signalpb.ManifestRecord{Version: 5}}
	for _, id := range order {
		rec := records[id]
		var typ signalpb.ManifestRecord_Identifier_Type
		switch rec.GetRecord().(type) {
		case *signalpb.StorageRecord_Contact:
			typ = signalpb.ManifestRecord_Identifier_CONTACT
		case *signalpb.StorageRecord_GroupV2:
			typ = signalpb.ManifestRecord_Identifier_GROUPV2
		case *signalpb.StorageRecord_Account:
			typ = signalpb.ManifestRecord_Identifier_ACCOUNT
		}
		snap.Records = append(snap.Records, &DecryptedStorageRecord{ItemType: typ, StorageID: b64(id), StorageRecord: rec})
	}
	return snap
}

func requireUnknownField(t *testing.T, m proto.Message) {
	t.Helper()
	assert.Equal(t, unknownFieldBytes(), []byte(m.ProtoReflect().GetUnknown()), "unknown fields must survive")
}

func TestEncryptStorageRecordRoundTrip(t *testing.T) {
	for name, ikm := range map[string][]byte{"legacy key derivation": nil, "recordIkm": testIKM} {
		t.Run(name, func(t *testing.T) {
			record := contactStorageRecord(testACI, func(c *signalpb.ContactRecord) { c.MutedUntilTimestamp = 12345 })
			id := bytes.Repeat([]byte{0xab}, 16)
			encrypted, err := encryptStorageRecord(testStorageKey, ikm, id, record)
			require.NoError(t, err)
			// This is the same function the read path uses
			decrypted, _, err := decryptStorageItem(testStorageKey, ikm, &signalpb.StorageItem{Key: id, Value: encrypted})
			require.NoError(t, err)
			assert.True(t, proto.Equal(record, decrypted))
			// A different ID derives a different key
			_, _, err = decryptStorageItem(testStorageKey, ikm, &signalpb.StorageItem{Key: bytes.Repeat([]byte{0xac}, 16), Value: encrypted})
			assert.ErrorIs(t, err, errStorageItemDecrypt)
			// Encrypting twice never gives the same ciphertext
			encrypted2, err := encryptStorageRecord(testStorageKey, ikm, id, record)
			require.NoError(t, err)
			assert.NotEqual(t, encrypted, encrypted2)
		})
	}
}

func TestEncryptStorageManifestRoundTrip(t *testing.T) {
	manifest := manifestFor(41, testIKM, map[string]signalpb.ManifestRecord_Identifier_Type{"aaaaaaaaaaaaaaaa": signalpb.ManifestRecord_Identifier_CONTACT}, []string{"aaaaaaaaaaaaaaaa"})
	encrypted, err := encryptStorageManifest(testStorageKey, manifest)
	require.NoError(t, err)
	assert.Equal(t, uint64(41), encrypted.GetVersion())
	decrypted, err := decryptStorageManifest(testStorageKey, encrypted)
	require.NoError(t, err)
	assert.True(t, proto.Equal(manifest, decrypted))
}

func TestBuildStorageWrite(t *testing.T) {
	types_ := map[string]signalpb.ManifestRecord_Identifier_Type{
		"contact-one-0001": signalpb.ManifestRecord_Identifier_CONTACT,
		"contact-two-0002": signalpb.ManifestRecord_Identifier_CONTACT,
		"group-one--0003":  signalpb.ManifestRecord_Identifier_GROUPV2,
		"account-rec-0004": signalpb.ManifestRecord_Identifier_ACCOUNT,
	}
	order := []string{"contact-one-0001", "contact-two-0002", "group-one--0003", "account-rec-0004"}
	manifest := manifestFor(41, testIKM, types_, order)
	manifest.ProtoReflect().SetUnknown(unknownFieldBytes())
	newRecord := contactStorageRecord(testACI, func(c *signalpb.ContactRecord) { c.MutedUntilTimestamp = 99 })

	built, err := buildStorageWrite(testStorageKey, manifest, 3, []storageEdit{{OldID: b64("contact-two-0002"), Record: newRecord}})
	require.NoError(t, err)

	// Exactly one item is inserted and only the replaced one is deleted
	require.Len(t, built.Op.InsertItem, 1)
	require.Len(t, built.Op.DeleteKey, 1)
	assert.False(t, built.Op.ClearAll)
	assert.Equal(t, []byte("contact-two-0002"), built.Op.DeleteKey[0])
	newID := built.Op.InsertItem[0].Key
	assert.Len(t, newID, 16)
	assert.NotEqual(t, []byte("contact-two-0002"), newID)

	// The manifest is version + 1 and decrypts with the manifest key of the new version
	assert.Equal(t, uint64(42), built.Op.Manifest.GetVersion())
	decManifest, err := decryptStorageManifest(testStorageKey, built.Op.Manifest)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), decManifest.GetVersion())
	assert.Equal(t, uint32(3), decManifest.GetSourceDevice())
	assert.Equal(t, testIKM, decManifest.GetRecordIkm())
	requireUnknownField(t, decManifest)
	require.Len(t, decManifest.Identifiers, 4)
	// Old ID replaced in place, everything else identical
	assert.Equal(t, []byte("contact-one-0001"), decManifest.Identifiers[0].Raw)
	assert.Equal(t, newID, decManifest.Identifiers[1].Raw)
	assert.Equal(t, signalpb.ManifestRecord_Identifier_CONTACT, decManifest.Identifiers[1].Type)
	assert.Equal(t, []byte("group-one--0003"), decManifest.Identifiers[2].Raw)
	assert.Equal(t, []byte("account-rec-0004"), decManifest.Identifiers[3].Raw)
	for _, ident := range decManifest.Identifiers {
		assert.NotEqual(t, []byte("contact-two-0002"), ident.Raw)
	}
	// The input manifest wasn't modified
	assert.Equal(t, uint64(41), manifest.Version)
	assert.Equal(t, []byte("contact-two-0002"), manifest.Identifiers[1].Raw)

	// The inserted item decrypts with the read code, under its new ID
	decRecord, _, err := decryptStorageItem(testStorageKey, testIKM, built.Op.InsertItem[0])
	require.NoError(t, err)
	assert.True(t, proto.Equal(newRecord, decRecord))
	require.Len(t, built.NewRecords, 1)
	assert.Equal(t, base64.StdEncoding.EncodeToString(newID), built.NewRecords[0].StorageID)

	// Unknown record and duplicate edits are refused instead of writing something partial
	_, err = buildStorageWrite(testStorageKey, manifest, 3, []storageEdit{{OldID: b64("nonexistent-id-00"), Record: newRecord}})
	assert.Error(t, err)
	_, err = buildStorageWrite(testStorageKey, manifest, 3, []storageEdit{{OldID: b64("contact-two-0002"), Record: newRecord}, {OldID: b64("contact-two-0002"), Record: newRecord}})
	assert.Error(t, err)
	_, err = buildStorageWrite(testStorageKey, manifest, 3, nil)
	assert.Error(t, err)
}

func TestApplyChatSettingsChange_Contact(t *testing.T) {
	sid := libsignalgo.NewACIServiceID(testACI)
	forever := types.MuteForeverProto
	until := uint64(1900000000000)
	yes, no := true, false
	tests := []struct {
		name   string
		change ChatSettingsChange
		check  func(t *testing.T, c *signalpb.ContactRecord)
	}{
		{"mute forever", ChatSettingsChange{MutedUntil: &forever}, func(t *testing.T, c *signalpb.ContactRecord) {
			assert.Equal(t, forever, c.MutedUntilTimestamp)
		}},
		{"mute until", ChatSettingsChange{MutedUntil: &until}, func(t *testing.T, c *signalpb.ContactRecord) {
			assert.Equal(t, until, c.MutedUntilTimestamp)
		}},
		{"unmute", ChatSettingsChange{MutedUntil: ptrUint64(0)}, func(t *testing.T, c *signalpb.ContactRecord) {
			assert.Zero(t, c.MutedUntilTimestamp)
		}},
		{"archive", ChatSettingsChange{Archived: &yes}, func(t *testing.T, c *signalpb.ContactRecord) {
			assert.True(t, c.Archived)
		}},
		{"unarchive", ChatSettingsChange{Archived: &no}, func(t *testing.T, c *signalpb.ContactRecord) {
			assert.False(t, c.Archived)
		}},
		{"mark unread", ChatSettingsChange{MarkedUnread: &yes}, func(t *testing.T, c *signalpb.ContactRecord) {
			assert.True(t, c.MarkedUnread)
		}},
		{"mark read", ChatSettingsChange{MarkedUnread: &no}, func(t *testing.T, c *signalpb.ContactRecord) {
			assert.False(t, c.MarkedUnread)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Start with every flag set the opposite way round from what the test wants, so that each one is a change
			orig := contactStorageRecord(testACI, func(c *signalpb.ContactRecord) {
				c.MutedUntilTimestamp = 777
				c.Archived = tt.change.Archived != nil && !*tt.change.Archived
				c.MarkedUnread = tt.change.MarkedUnread != nil && !*tt.change.MarkedUnread
				if tt.change.MutedUntil != nil && *tt.change.MutedUntil == 0 {
					c.MutedUntilTimestamp = 777
				}
			})
			origCopy := proto.Clone(orig).(*signalpb.StorageRecord)
			other := contactStorageRecord(testACI2, nil)
			snap := snapshotOf(map[string]*signalpb.StorageRecord{"contact-one-0001": orig, "contact-two-0002": other}, []string{"contact-two-0002", "contact-one-0001"})
			edits, err := applyChatSettingsChange(snap, ChatRef{ServiceID: sid}, tt.change)
			require.NoError(t, err)
			require.Len(t, edits, 1)
			assert.Equal(t, b64("contact-one-0001"), edits[0].OldID, "only the record of that contact is replaced")
			updated := edits[0].Record.GetContact()
			tt.check(t, updated)
			requireUnknownField(t, updated)
			// Nothing but the requested field differs
			want := proto.Clone(orig.GetContact()).(*signalpb.ContactRecord)
			if tt.change.MutedUntil != nil {
				want.MutedUntilTimestamp = *tt.change.MutedUntil
			}
			if tt.change.Archived != nil {
				want.Archived = *tt.change.Archived
			}
			if tt.change.MarkedUnread != nil {
				want.MarkedUnread = *tt.change.MarkedUnread
			}
			assert.True(t, proto.Equal(want, updated), "other fields changed: %v vs %v", want, updated)
			assert.True(t, proto.Equal(origCopy, orig), "the input record must not be mutated")
		})
	}
}

func ptrUint64(v uint64) *uint64 { return &v }

func TestApplyChatSettingsChange_NoChangeAndMissing(t *testing.T) {
	sid := libsignalgo.NewACIServiceID(testACI)
	yes := true
	snap := snapshotOf(map[string]*signalpb.StorageRecord{
		"contact-one-0001": contactStorageRecord(testACI, func(c *signalpb.ContactRecord) { c.Archived = true }),
	}, []string{"contact-one-0001"})
	edits, err := applyChatSettingsChange(snap, ChatRef{ServiceID: sid}, ChatSettingsChange{Archived: &yes})
	require.NoError(t, err)
	assert.Empty(t, edits, "setting a value that is already set writes nothing")

	_, err = applyChatSettingsChange(snap, ChatRef{ServiceID: libsignalgo.NewACIServiceID(testACI2)}, ChatSettingsChange{Archived: &yes})
	assert.ErrorIs(t, err, ErrNoStorageRecord)
	// PNI doesn't match an ACI record
	_, err = applyChatSettingsChange(snap, ChatRef{ServiceID: libsignalgo.NewPNIServiceID(testACI)}, ChatSettingsChange{Archived: &yes})
	assert.ErrorIs(t, err, ErrNoStorageRecord)
}

func TestApplyChatSettingsChange_Group(t *testing.T) {
	forever := types.MuteForeverProto
	yes := true
	group := groupStorageRecord(testGroupKey, nil)
	snap := snapshotOf(map[string]*signalpb.StorageRecord{
		"group-one--0003":  group,
		"group-two--0004":  groupStorageRecord(testGroupKey2, nil),
		"contact-one-0001": contactStorageRecord(testACI, nil),
	}, []string{"contact-one-0001", "group-two--0004", "group-one--0003"})
	edits, err := applyChatSettingsChange(snap, ChatRef{GroupID: mustGroupID(t, testGroupKey)}, ChatSettingsChange{MutedUntil: &forever, MarkedUnread: &yes})
	require.NoError(t, err)
	require.Len(t, edits, 1)
	assert.Equal(t, b64("group-one--0003"), edits[0].OldID)
	updated := edits[0].Record.GetGroupV2()
	assert.Equal(t, forever, updated.MutedUntilTimestamp)
	assert.True(t, updated.MarkedUnread)
	assert.False(t, updated.Archived)
	assert.True(t, updated.Blocked, "unrelated fields stay")
	assert.Equal(t, testGroupKey, updated.MasterKey)
	requireUnknownField(t, updated)
}

func TestApplyChatSettingsChange_Pin(t *testing.T) {
	sid := libsignalgo.NewACIServiceID(testACI)
	sid2 := libsignalgo.NewACIServiceID(testACI2)
	yes, no := true, false
	records := func(pins ...*signalpb.AccountRecord_PinnedConversation) *storageSnapshot {
		return snapshotOf(map[string]*signalpb.StorageRecord{
			"contact-one-0001": contactStorageRecord(testACI, nil),
			"account-rec-0004": accountStorageRecord(pins...),
		}, []string{"contact-one-0001", "account-rec-0004"})
	}

	t.Run("pin appends and only replaces the account record", func(t *testing.T) {
		edits, err := applyChatSettingsChange(records(pinContact(sid2), pinGroup(testGroupKey)), ChatRef{ServiceID: sid}, ChatSettingsChange{Pinned: &yes})
		require.NoError(t, err)
		require.Len(t, edits, 1)
		assert.Equal(t, b64("account-rec-0004"), edits[0].OldID)
		account := edits[0].Record.GetAccount()
		require.Len(t, account.PinnedConversations, 3)
		assert.True(t, pinMatchesChat(account.PinnedConversations[0], ChatRef{ServiceID: sid2}))
		assert.True(t, pinMatchesChat(account.PinnedConversations[1], ChatRef{GroupID: mustGroupID(t, testGroupKey)}))
		last := account.PinnedConversations[2].GetContact()
		assert.Equal(t, sid.String(), last.ServiceId)
		assert.Equal(t, "+15550100", last.E164)
		assert.True(t, account.ReadReceipts, "other account fields stay")
		requireUnknownField(t, account)
	})
	t.Run("unpin removes only that chat", func(t *testing.T) {
		edits, err := applyChatSettingsChange(records(pinContact(sid2), pinContact(sid), pinGroup(testGroupKey)), ChatRef{ServiceID: sid}, ChatSettingsChange{Pinned: &no})
		require.NoError(t, err)
		require.Len(t, edits, 1)
		account := edits[0].Record.GetAccount()
		require.Len(t, account.PinnedConversations, 2)
		assert.True(t, pinMatchesChat(account.PinnedConversations[0], ChatRef{ServiceID: sid2}))
		assert.True(t, pinMatchesChat(account.PinnedConversations[1], ChatRef{GroupID: mustGroupID(t, testGroupKey)}))
		requireUnknownField(t, account)
	})
	t.Run("already in the wanted state writes nothing", func(t *testing.T) {
		edits, err := applyChatSettingsChange(records(pinContact(sid)), ChatRef{ServiceID: sid}, ChatSettingsChange{Pinned: &yes})
		require.NoError(t, err)
		assert.Empty(t, edits)
		edits, err = applyChatSettingsChange(records(pinContact(sid2)), ChatRef{ServiceID: sid}, ChatSettingsChange{Pinned: &no})
		require.NoError(t, err)
		assert.Empty(t, edits)
	})
	t.Run("limit of four", func(t *testing.T) {
		full := records(pinContact(sid2), pinGroup(testGroupKey), pinGroup(testGroupKey2), &signalpb.AccountRecord_PinnedConversation{
			Identifier: &signalpb.AccountRecord_PinnedConversation_ReleaseNotes_{ReleaseNotes: &signalpb.AccountRecord_PinnedConversation_ReleaseNotes{}},
		})
		_, err := applyChatSettingsChange(full, ChatRef{ServiceID: sid}, ChatSettingsChange{Pinned: &yes})
		assert.ErrorIs(t, err, ErrTooManyPinnedChats)
		// unpinning is still fine when full
		edits, err := applyChatSettingsChange(full, ChatRef{ServiceID: sid2}, ChatSettingsChange{Pinned: &no})
		require.NoError(t, err)
		require.Len(t, edits, 1)
		assert.Len(t, edits[0].Record.GetAccount().PinnedConversations, 3)
	})
	t.Run("binary encoding is followed", func(t *testing.T) {
		binPin := &signalpb.AccountRecord_PinnedConversation{Identifier: &signalpb.AccountRecord_PinnedConversation_Contact_{
			Contact: &signalpb.AccountRecord_PinnedConversation_Contact{ServiceIdBinary: sid2.Bytes()},
		}}
		edits, err := applyChatSettingsChange(records(binPin), ChatRef{ServiceID: sid}, ChatSettingsChange{Pinned: &yes})
		require.NoError(t, err)
		require.Len(t, edits, 1)
		added := edits[0].Record.GetAccount().PinnedConversations[1].GetContact()
		assert.Equal(t, sid.Bytes(), added.ServiceIdBinary)
		assert.Empty(t, added.ServiceId)
		// A binary pin is found again for unpinning
		edits, err = applyChatSettingsChange(records(binPin), ChatRef{ServiceID: sid2}, ChatSettingsChange{Pinned: &no})
		require.NoError(t, err)
		require.Len(t, edits, 1)
		assert.Empty(t, edits[0].Record.GetAccount().PinnedConversations)
	})
	t.Run("group pin", func(t *testing.T) {
		edits, err := applyChatSettingsChange(records(), ChatRef{GroupID: mustGroupID(t, testGroupKey), GroupMasterKey: testGroupKey}, ChatSettingsChange{Pinned: &yes})
		require.NoError(t, err)
		require.Len(t, edits, 1)
		assert.Equal(t, testGroupKey, edits[0].Record.GetAccount().PinnedConversations[0].GetGroupMasterKey())
		_, err = applyChatSettingsChange(records(), ChatRef{GroupID: mustGroupID(t, testGroupKey)}, ChatSettingsChange{Pinned: &yes})
		assert.Error(t, err, "can't pin a group without its master key")
	})
	t.Run("pin plus mute is one write of two records", func(t *testing.T) {
		forever := types.MuteForeverProto
		edits, err := applyChatSettingsChange(records(), ChatRef{ServiceID: sid}, ChatSettingsChange{Pinned: &yes, MutedUntil: &forever})
		require.NoError(t, err)
		require.Len(t, edits, 2)
	})
}

type fakeStorage struct {
	t          *testing.T
	manifest   *signalpb.ManifestRecord
	records    map[string]*signalpb.StorageRecord // by base64 ID
	conflicts  int
	writes     []*signalpb.WriteOperation
	manifestFn func(*fakeStorage) // called on every manifest fetch
	fetches    int
}

func (f *fakeStorage) fetchManifest(ctx context.Context) (*signalpb.ManifestRecord, error) {
	f.fetches++
	if f.manifestFn != nil {
		f.manifestFn(f)
	}
	return proto.Clone(f.manifest).(*signalpb.ManifestRecord), nil
}

func (f *fakeStorage) fetchRecords(ctx context.Context, ikm []byte, ids map[string]signalpb.ManifestRecord_Identifier_Type) ([]*DecryptedStorageRecord, error) {
	var out []*DecryptedStorageRecord
	for id, typ := range ids {
		out = append(out, &DecryptedStorageRecord{ItemType: typ, StorageID: id, StorageRecord: proto.Clone(f.records[id]).(*signalpb.StorageRecord)})
	}
	return out, nil
}

func (f *fakeStorage) writeStorage(ctx context.Context, op *signalpb.WriteOperation) (bool, error) {
	f.writes = append(f.writes, op)
	if f.conflicts > 0 {
		f.conflicts--
		return true, nil
	}
	return false, nil
}

func newFakeStorage(t *testing.T) *fakeStorage {
	ids := map[string]signalpb.ManifestRecord_Identifier_Type{
		"contact-one-0001": signalpb.ManifestRecord_Identifier_CONTACT,
		"account-rec-0004": signalpb.ManifestRecord_Identifier_ACCOUNT,
	}
	return &fakeStorage{
		t:        t,
		manifest: manifestFor(10, testIKM, ids, []string{"contact-one-0001", "account-rec-0004"}),
		records: map[string]*signalpb.StorageRecord{
			b64("contact-one-0001"): contactStorageRecord(testACI, nil),
			b64("account-rec-0004"): accountStorageRecord(),
		},
	}
}

func TestModifyStorage(t *testing.T) {
	sid := libsignalgo.NewACIServiceID(testACI)
	yes := true
	change := ChatSettingsChange{MarkedUnread: &yes}
	run := func(f *fakeStorage) (*builtStorageWrite, error) {
		return modifyStorage(context.Background(), f, testStorageKey, 2, change.wantedRecordTypes(ChatRef{ServiceID: sid}), func(snap *storageSnapshot) ([]storageEdit, error) {
			return applyChatSettingsChange(snap, ChatRef{ServiceID: sid}, change)
		})
	}

	t.Run("plain write", func(t *testing.T) {
		f := newFakeStorage(t)
		built, err := run(f)
		require.NoError(t, err)
		require.NotNil(t, built)
		require.Len(t, f.writes, 1)
		assert.Equal(t, uint64(11), f.writes[0].Manifest.Version)
	})
	t.Run("retries on conflict with a fresh manifest", func(t *testing.T) {
		f := newFakeStorage(t)
		f.conflicts = 2
		// Another device bumps the manifest between our attempts
		f.manifestFn = func(f *fakeStorage) {
			if f.fetches > 1 {
				f.manifest.Version++
			}
		}
		built, err := run(f)
		require.NoError(t, err)
		require.NotNil(t, built)
		require.Len(t, f.writes, 3)
		assert.Equal(t, uint64(11), f.writes[0].Manifest.Version)
		assert.Equal(t, uint64(12), f.writes[1].Manifest.Version)
		assert.Equal(t, uint64(13), f.writes[2].Manifest.Version)
		assert.Equal(t, 3, f.fetches)
	})
	t.Run("gives up after repeated conflicts", func(t *testing.T) {
		f := newFakeStorage(t)
		f.conflicts = 100
		_, err := run(f)
		assert.True(t, errors.Is(err, ErrStorageConflict))
		assert.Len(t, f.writes, maxStorageWriteRetries)
	})
	t.Run("no write if nothing changes", func(t *testing.T) {
		f := newFakeStorage(t)
		f.records[b64("contact-one-0001")] = contactStorageRecord(testACI, func(c *signalpb.ContactRecord) { c.MarkedUnread = true })
		built, err := run(f)
		require.NoError(t, err)
		assert.Nil(t, built)
		assert.Empty(t, f.writes)
	})
}
