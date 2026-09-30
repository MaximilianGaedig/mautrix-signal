package signalmeow

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

// blockedListACIs is every ACI a blocked list sync message names.
func blockedListACIs(list *signalpb.SyncMessage_Blocked) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{})
	var out []uuid.UUID
	add := func(aci uuid.UUID, err error) {
		if err != nil || aci == uuid.Nil {
			return
		}
		if _, dupe := seen[aci]; !dupe {
			seen[aci] = struct{}{}
			out = append(out, aci)
		}
	}
	for _, aci := range list.GetAcis() {
		add(uuid.Parse(aci))
	}
	for _, aci := range list.GetAcisBinary() {
		add(uuid.FromBytes(aci))
	}
	for _, aci := range list.GetBlockedAcis() {
		add(uuid.FromBytes(aci.GetAciBinary()))
	}
	return out
}

// blockedListChanges is what a full blocked list, given the ACIs blocked until now, blocks and unblocks.
// Everyone on the list is reported as blocked, so that an entry the bridge missed is picked up too.
func blockedListChanges(list *signalpb.SyncMessage_Blocked, previously []uuid.UUID) *events.BlockChanges {
	blocked := blockedListACIs(list)
	changes := &events.BlockChanges{Blocked: blocked}
	for _, aci := range previously {
		if !slices.Contains(blocked, aci) {
			changes.Unblocked = append(changes.Unblocked, aci)
		}
	}
	return changes
}

// handleBlockedSync handles the full blocked list the user's other devices send. Only the events are
// produced: the blocked flags of the recipients come from the storage service, which is authoritative.
func (cli *Client) handleBlockedSync(ctx context.Context, list *signalpb.SyncMessage_Blocked) bool {
	previously, err := cli.Store.RecipientStore.LoadBlockedACIs(ctx)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to load blocked recipients for blocked list sync")
		return false
	}
	changes := blockedListChanges(list, previously)
	if len(changes.Blocked) == 0 && len(changes.Unblocked) == 0 {
		return true
	}
	return cli.handleEvent(changes)
}

// applyBlockedChange edits the contact record of a chat, or adds one when the chat has none and is being blocked.
func applyBlockedChange(snap *storageSnapshot, chat ChatRef, blocked bool, profileKey []byte, now time.Time) ([]storageEdit, error) {
	if chat.ServiceID.Type != libsignalgo.ServiceIDTypeACI {
		return nil, fmt.Errorf("can only block people by their ACI")
	}
	rec := findChatRecord(snap, chat)
	if rec == nil {
		if !blocked {
			return nil, nil
		}
		contact := &signalpb.ContactRecord{Blocked: true, BlockedAtTimestamp: uint64(now.UnixMilli()), ProfileKey: bytes.Clone(profileKey)}
		if contactRecordsUseBinaryACIs(snap) {
			contact.AciBinary = chat.ServiceID.UUID[:]
		} else {
			contact.Aci = chat.ServiceID.UUID.String()
		}
		return []storageEdit{{
			InsertType: signalpb.ManifestRecord_Identifier_CONTACT,
			Record:     &signalpb.StorageRecord{Record: &signalpb.StorageRecord_Contact{Contact: contact}},
		}}, nil
	}
	if rec.StorageRecord.GetContact().GetBlocked() == blocked {
		return nil, nil
	}
	updated := cloneStorageRecord(rec.StorageRecord)
	contact := updated.GetContact()
	contact.Blocked = blocked
	if blocked {
		contact.BlockedAtTimestamp = uint64(now.UnixMilli())
	} else {
		contact.BlockedAtTimestamp = 0
	}
	return []storageEdit{{OldID: rec.StorageID, Record: updated}}, nil
}

// contactRecordsUseBinaryACIs follows whichever ACI encoding the account's contact records already use.
func contactRecordsUseBinaryACIs(snap *storageSnapshot) bool {
	for _, rec := range snap.Records {
		if c := rec.StorageRecord.GetContact(); c != nil && c.GetAci() == "" && len(c.GetAciBinary()) > 0 {
			return true
		}
	}
	return false
}

// SetContactBlocked blocks or unblocks a person by changing the blocked flag of their contact record in the
// storage service, which is where Signal clients keep the block list, and tells the user's other devices to
// fetch it.
func (cli *Client) SetContactBlocked(ctx context.Context, aci uuid.UUID, blocked bool) error {
	if len(cli.Store.MasterKey) == 0 {
		return ErrNoStorageMasterKey
	}
	log := zerolog.Ctx(ctx).With().Str("action", "set contact blocked").Stringer("aci", aci).Bool("blocked", blocked).Logger()
	ctx = log.WithContext(ctx)
	var profileKey []byte
	if key, err := cli.Store.RecipientStore.LoadProfileKey(ctx, aci); err == nil && key != nil {
		profileKey = key.Slice()
	}
	chat := ChatRef{ServiceID: libsignalgo.NewACIServiceID(aci)}
	cli.storageSyncLock.Lock()
	defer cli.storageSyncLock.Unlock()
	api := &clientStorageAPI{cli: cli, storageKey: deriveStorageServiceKey(cli.Store.MasterKey)}
	built, err := modifyStorage(ctx, api, api.storageKey, uint32(cli.Store.DeviceID), []signalpb.ManifestRecord_Identifier_Type{signalpb.ManifestRecord_Identifier_CONTACT}, func(snap *storageSnapshot) ([]storageEdit, error) {
		return applyBlockedChange(snap, chat, blocked, profileKey, time.Now())
	})
	if err != nil {
		return err
	} else if built == nil {
		log.Debug().Msg("Storage service already has the requested block state")
		return nil
	}
	err = cli.Store.DoContactTxn(ctx, func(ctx context.Context) error {
		return cli.processStorageInTxn(ctx, &StorageUpdate{
			Version:             built.Op.GetManifest().GetVersion(),
			NewRecords:          built.NewRecords,
			NoChatSettingsEvent: true,
		})
	})
	if err != nil {
		log.Err(err).Msg("Failed to process own storage write")
	}
	cli.sendStorageManifestChanged(ctx)
	return nil
}
