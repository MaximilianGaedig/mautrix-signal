package signalmeow

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

var testACI3 = uuid.MustParse("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")

func TestBlockedListChanges(t *testing.T) {
	list := &signalpb.SyncMessage_Blocked{
		Acis:       []string{testACI.String()},
		AcisBinary: [][]byte{testACI.NodeID(), testACI2[:]}, // the first entry is malformed and ignored
		BlockedAcis: []*signalpb.SyncMessage_Blocked_BlockedAci{
			{AciBinary: testACI2[:]}, // repeated
		},
		Numbers: []string{"+15550100"},
	}
	// testACI3 was blocked before and is not on the list any more; testACI stays blocked
	changes := blockedListChanges(list, []uuid.UUID{testACI, testACI3})
	assert.ElementsMatch(t, []uuid.UUID{testACI, testACI2}, changes.Blocked, "everyone on the list, once")
	assert.Equal(t, []uuid.UUID{testACI3}, changes.Unblocked, "only who was dropped from the list")
}

func TestBlockedListChangesEmptyListUnblocksEveryone(t *testing.T) {
	changes := blockedListChanges(&signalpb.SyncMessage_Blocked{}, []uuid.UUID{testACI})
	assert.Empty(t, changes.Blocked)
	assert.Equal(t, []uuid.UUID{testACI}, changes.Unblocked)
}

func TestApplyBlockedChange(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	chat := ChatRef{ServiceID: libsignalgo.NewACIServiceID(testACI)}
	snap := snapshotOfContacts(map[string]uuid.UUID{"contact-one-0001": testACI, "contact-two-0002": testACI2})

	edits, err := applyBlockedChange(snap, chat, true, nil, now)
	require.NoError(t, err)
	require.Len(t, edits, 1)
	assert.Equal(t, b64("contact-one-0001"), edits[0].OldID)
	updated := edits[0].Record.GetContact()
	assert.True(t, updated.Blocked)
	assert.Equal(t, uint64(now.UnixMilli()), updated.BlockedAtTimestamp)
	assert.Equal(t, "Test", updated.GivenName, "other fields stay")
	requireUnknownField(t, updated)
	assert.False(t, snap.Records[0].StorageRecord.GetContact().Blocked, "the input record is not mutated")

	// blocking an already blocked contact writes nothing, unblocking one that isn't blocked neither
	snap.Records[0].StorageRecord.GetContact().Blocked = true
	edits, err = applyBlockedChange(snap, chat, true, nil, now)
	require.NoError(t, err)
	assert.Empty(t, edits)
	edits, err = applyBlockedChange(snap, chat, false, nil, now)
	require.NoError(t, err)
	require.Len(t, edits, 1)
	assert.False(t, edits[0].Record.GetContact().Blocked)
	assert.Zero(t, edits[0].Record.GetContact().BlockedAtTimestamp)
	edits, err = applyBlockedChange(snap, ChatRef{ServiceID: libsignalgo.NewACIServiceID(testACI3)}, false, nil, now)
	require.NoError(t, err)
	assert.Empty(t, edits)
}

func TestApplyBlockedChangeAddsMissingContact(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	chat := ChatRef{ServiceID: libsignalgo.NewACIServiceID(testACI3)}
	snap := snapshotOfContacts(map[string]uuid.UUID{"contact-one-0001": testACI})
	key := []byte{1, 2, 3}
	edits, err := applyBlockedChange(snap, chat, true, key, now)
	require.NoError(t, err)
	require.Len(t, edits, 1)
	assert.Empty(t, edits[0].OldID)
	assert.Equal(t, signalpb.ManifestRecord_Identifier_CONTACT, edits[0].InsertType)
	c := edits[0].Record.GetContact()
	assert.Equal(t, testACI3.String(), c.Aci)
	assert.True(t, c.Blocked)
	assert.Equal(t, key, c.ProfileKey)

	// New records follow the ACI encoding of the existing ones
	snap.Records[0].StorageRecord.GetContact().Aci = ""
	snap.Records[0].StorageRecord.GetContact().AciBinary = testACI[:]
	edits, err = applyBlockedChange(snap, chat, true, nil, now)
	require.NoError(t, err)
	assert.Equal(t, testACI3[:], edits[0].Record.GetContact().AciBinary)
	assert.Empty(t, edits[0].Record.GetContact().Aci)

	_, err = applyBlockedChange(snap, ChatRef{ServiceID: libsignalgo.NewPNIServiceID(testACI3)}, true, nil, now)
	assert.Error(t, err, "a PNI can't be blocked")
}

func TestBuildStorageWriteInsertsNewRecord(t *testing.T) {
	types_ := map[string]signalpb.ManifestRecord_Identifier_Type{"contact-one-0001": signalpb.ManifestRecord_Identifier_CONTACT}
	manifest := manifestFor(41, testIKM, types_, []string{"contact-one-0001"})
	record := contactStorageRecord(testACI3, nil)
	built, err := buildStorageWrite(testStorageKey, manifest, 3, []storageEdit{{Record: record, InsertType: signalpb.ManifestRecord_Identifier_CONTACT}})
	require.NoError(t, err)
	require.Len(t, built.Op.InsertItem, 1)
	assert.Empty(t, built.Op.DeleteKey, "nothing is deleted when a record is only added")
	newManifest, err := decryptStorageManifest(testStorageKey, built.Op.Manifest)
	require.NoError(t, err)
	require.Len(t, newManifest.Identifiers, 2)
	assert.Equal(t, []byte("contact-one-0001"), newManifest.Identifiers[0].Raw)
	assert.Equal(t, built.Op.InsertItem[0].Key, newManifest.Identifiers[1].Raw)
	decrypted, _, err := decryptStorageItem(testStorageKey, newManifest.RecordIkm, built.Op.InsertItem[0])
	require.NoError(t, err)
	assert.Equal(t, testACI3.String(), decrypted.GetContact().Aci)
}

func snapshotOfContacts(byID map[string]uuid.UUID) *storageSnapshot {
	records := map[string]*signalpb.StorageRecord{}
	var order []string
	for _, id := range []string{"contact-one-0001", "contact-two-0002"} {
		if aci, ok := byID[id]; ok {
			records[id] = contactStorageRecord(aci, nil)
			order = append(order, id)
		}
	}
	return snapshotOf(records, order)
}
