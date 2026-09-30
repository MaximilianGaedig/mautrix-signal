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
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/random"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/web"
)

var (
	ErrNoStorageRecord    = errors.New("no storage service record for this chat")
	ErrTooManyPinnedChats = errors.New("too many pinned chats")
	ErrStorageConflict    = errors.New("storage service manifest kept changing")
	ErrNoStorageMasterKey = errors.New("no storage master key")
	errStorageNothingToDo = errors.New("no storage changes")
)

const (
	storageItemIDLength    = 16
	maxStorageWriteRetries = 5
)

// MaxPinnedChats is the maximum number of pinned chats Signal allows.
const MaxPinnedChats = 4

// storageEdit replaces one existing storage record with a new version of it, or adds a new record.
type storageEdit struct {
	// OldID is the base64 storage ID of the record that is being replaced. It is empty for a new record.
	OldID  string
	Record *signalpb.StorageRecord
	// InsertType is the type of the new record when OldID is empty.
	InsertType signalpb.ManifestRecord_Identifier_Type
}

func cloneStorageRecord(record *signalpb.StorageRecord) *signalpb.StorageRecord {
	return proto.Clone(record).(*signalpb.StorageRecord)
}

// storageSnapshot is the decrypted state of the parts of the storage service that a change needs to look at.
type storageSnapshot struct {
	Manifest *signalpb.ManifestRecord
	Records  []*DecryptedStorageRecord
}

func encryptStorageBlob(key, plaintext []byte) ([]byte, error) {
	nonce := random.Bytes(NONCE_LENGTH)
	ciphertext, err := AesgcmEncrypt(key, nonce, plaintext)
	if err != nil {
		return nil, err
	}
	return append(nonce, ciphertext...), nil
}

// encryptStorageRecord is the reverse of the decryption in fetchStorageRecords.
func encryptStorageRecord(storageKey, recordIKM, id []byte, record *signalpb.StorageRecord) ([]byte, error) {
	plaintext, err := proto.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal storage record: %w", err)
	}
	itemKey := deriveStorageItemKey(storageKey, recordIKM, id, base64.StdEncoding.EncodeToString(id))
	return encryptStorageBlob(itemKey, plaintext)
}

// encryptStorageManifest is the reverse of the decryption in fetchStorageManifest.
func encryptStorageManifest(storageKey []byte, manifest *signalpb.ManifestRecord) (*signalpb.StorageManifest, error) {
	plaintext, err := proto.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest record: %w", err)
	}
	value, err := encryptStorageBlob(deriveStorageManifestKey(storageKey, manifest.GetVersion()), plaintext)
	if err != nil {
		return nil, err
	}
	return &signalpb.StorageManifest{Version: manifest.Version, Value: value}, nil
}

type builtStorageWrite struct {
	Op *signalpb.WriteOperation
	// NewRecords are the replacement records with their new storage IDs.
	NewRecords []*DecryptedStorageRecord
}

// buildStorageWrite creates the write operation that swaps the edited records for new versions with fresh random IDs.
// The new manifest has version + 1, and is the old one with only those identifiers swapped.
// Nothing else is inserted or deleted, except for the records edits add.
func buildStorageWrite(storageKey []byte, manifest *signalpb.ManifestRecord, sourceDevice uint32, edits []storageEdit) (*builtStorageWrite, error) {
	if len(edits) == 0 {
		return nil, errStorageNothingToDo
	}
	newManifest := proto.Clone(manifest).(*signalpb.ManifestRecord)
	newManifest.Version = manifest.GetVersion() + 1
	newManifest.SourceDevice = sourceDevice
	recordIKM := manifest.GetRecordIkm()
	op := &signalpb.WriteOperation{}
	out := &builtStorageWrite{Op: op}
	replaced := make(map[string]struct{}, len(edits))
	for _, edit := range edits {
		if edit.OldID == "" {
			newID := random.Bytes(storageItemIDLength)
			encrypted, err := encryptStorageRecord(storageKey, recordIKM, newID, edit.Record)
			if err != nil {
				return nil, err
			}
			newManifest.Identifiers = append(newManifest.Identifiers, &signalpb.ManifestRecord_Identifier{Raw: newID, Type: edit.InsertType})
			op.InsertItem = append(op.InsertItem, &signalpb.StorageItem{Key: newID, Value: encrypted})
			out.NewRecords = append(out.NewRecords, &DecryptedStorageRecord{
				ItemType:      edit.InsertType,
				StorageID:     base64.StdEncoding.EncodeToString(newID),
				StorageRecord: edit.Record,
			})
			continue
		}
		if _, dupe := replaced[edit.OldID]; dupe {
			return nil, fmt.Errorf("storage record %s edited twice", edit.OldID)
		}
		replaced[edit.OldID] = struct{}{}
		oldRaw, err := base64.StdEncoding.DecodeString(edit.OldID)
		if err != nil {
			return nil, fmt.Errorf("invalid storage ID %q: %w", edit.OldID, err)
		}
		found := false
		for _, ident := range newManifest.Identifiers {
			if !bytes.Equal(ident.GetRaw(), oldRaw) {
				continue
			}
			newID := random.Bytes(storageItemIDLength)
			encrypted, err := encryptStorageRecord(storageKey, recordIKM, newID, edit.Record)
			if err != nil {
				return nil, err
			}
			ident.Raw = newID
			op.InsertItem = append(op.InsertItem, &signalpb.StorageItem{Key: newID, Value: encrypted})
			op.DeleteKey = append(op.DeleteKey, oldRaw)
			out.NewRecords = append(out.NewRecords, &DecryptedStorageRecord{
				ItemType:      ident.GetType(),
				StorageID:     base64.StdEncoding.EncodeToString(newID),
				StorageRecord: edit.Record,
			})
			found = true
			break
		}
		if !found {
			return nil, fmt.Errorf("storage record %s is not in the manifest", edit.OldID)
		}
	}
	var err error
	op.Manifest, err = encryptStorageManifest(storageKey, newManifest)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// storageAPI is the part of the storage service that modifyStorage talks to.
type storageAPI interface {
	// fetchManifest returns the latest decrypted manifest.
	fetchManifest(ctx context.Context) (*signalpb.ManifestRecord, error)
	fetchRecords(ctx context.Context, recordIKM []byte, ids map[string]signalpb.ManifestRecord_Identifier_Type) ([]*DecryptedStorageRecord, error)
	// writeStorage returns conflict=true if the manifest version was outdated.
	writeStorage(ctx context.Context, op *signalpb.WriteOperation) (conflict bool, err error)
}

// modifyStorage reads the current manifest and the records of the wanted types, asks mutate for the edits to make,
// and writes them as version + 1. On a version conflict it starts over from a fresh manifest.
// It returns nil, nil if mutate had nothing to change.
func modifyStorage(
	ctx context.Context,
	api storageAPI,
	storageKey []byte,
	sourceDevice uint32,
	wanted []signalpb.ManifestRecord_Identifier_Type,
	mutate func(*storageSnapshot) ([]storageEdit, error),
) (*builtStorageWrite, error) {
	log := zerolog.Ctx(ctx)
	for attempt := 0; attempt < maxStorageWriteRetries; attempt++ {
		manifest, err := api.fetchManifest(ctx)
		if err != nil {
			return nil, err
		} else if manifest == nil {
			return nil, errors.New("storage service returned no manifest")
		}
		ids := make(map[string]signalpb.ManifestRecord_Identifier_Type)
		for _, ident := range manifest.GetIdentifiers() {
			for _, typ := range wanted {
				if ident.GetType() == typ {
					ids[base64.StdEncoding.EncodeToString(ident.GetRaw())] = typ
				}
			}
		}
		records, err := api.fetchRecords(ctx, manifest.GetRecordIkm(), ids)
		if err != nil {
			return nil, err
		}
		edits, err := mutate(&storageSnapshot{Manifest: manifest, Records: records})
		if err != nil {
			return nil, err
		} else if len(edits) == 0 {
			return nil, nil
		}
		built, err := buildStorageWrite(storageKey, manifest, sourceDevice, edits)
		if err != nil {
			return nil, err
		}
		conflict, err := api.writeStorage(ctx, built.Op)
		if err != nil {
			return nil, err
		} else if !conflict {
			log.Debug().
				Uint64("new_version", built.Op.GetManifest().GetVersion()).
				Int("replaced_records", len(edits)).
				Msg("Wrote storage service update")
			return built, nil
		}
		log.Debug().
			Int("attempt", attempt+1).
			Uint64("attempted_version", built.Op.GetManifest().GetVersion()).
			Msg("Storage service version conflict, refetching")
	}
	return nil, ErrStorageConflict
}

type clientStorageAPI struct {
	cli        *Client
	storageKey []byte
}

func (a *clientStorageAPI) fetchManifest(ctx context.Context) (*signalpb.ManifestRecord, error) {
	return a.cli.fetchStorageManifest(ctx, a.storageKey, 0)
}

func (a *clientStorageAPI) fetchRecords(ctx context.Context, recordIKM []byte, ids map[string]signalpb.ManifestRecord_Identifier_Type) ([]*DecryptedStorageRecord, error) {
	records, missing, err := a.cli.fetchStorageRecords(ctx, a.storageKey, recordIKM, ids)
	if err != nil {
		return nil, err
	}
	if len(missing) > 0 {
		zerolog.Ctx(ctx).Warn().Int("missing_records", len(missing)).Msg("Some storage records couldn't be fetched or decrypted")
	}
	return records, nil
}

func (a *clientStorageAPI) writeStorage(ctx context.Context, op *signalpb.WriteOperation) (bool, error) {
	creds, err := a.cli.getStorageCredentials(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to fetch credentials: %w", err)
	}
	body, err := proto.Marshal(op)
	if err != nil {
		return false, fmt.Errorf("failed to marshal write operation: %w", err)
	}
	resp, err := web.SendHTTPRequest(ctx, web.StorageHostname, http.MethodPut, "/v1/storage", &web.HTTPReqOpt{
		Username:    &creds.Username,
		Password:    &creds.Password,
		Body:        body,
		ContentType: web.ContentTypeProtobuf,
	})
	defer web.CloseBody(resp)
	if err != nil {
		return false, fmt.Errorf("failed to write storage: %w", err)
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		return false, nil
	case http.StatusConflict:
		// The response body is the current manifest, but we refetch it anyway
		_, _ = io.Copy(io.Discard, resp.Body)
		return true, nil
	default:
		return false, fmt.Errorf("unexpected status code %d writing storage", resp.StatusCode)
	}
}

// ChatRef identifies a chat whose storage record should be changed.
type ChatRef struct {
	// ServiceID is set for 1:1 chats.
	ServiceID libsignalgo.ServiceID
	// GroupID is set for groups.
	GroupID types.GroupIdentifier
	// GroupMasterKey is required for pinning a group.
	GroupMasterKey []byte
}

// ChatSettingsChange lists the fields to change. Nil fields are left as they are.
type ChatSettingsChange struct {
	// MutedUntil is in milliseconds, 0 to unmute, or types.MuteForeverProto.
	MutedUntil   *uint64
	Archived     *bool
	MarkedUnread *bool
	Pinned       *bool
}

func (c ChatSettingsChange) needsChatRecord() bool {
	return c.MutedUntil != nil || c.Archived != nil || c.MarkedUnread != nil
}

func (c ChatSettingsChange) wantedRecordTypes(chat ChatRef) []signalpb.ManifestRecord_Identifier_Type {
	var wanted []signalpb.ManifestRecord_Identifier_Type
	if c.needsChatRecord() || (c.Pinned != nil && chat.GroupID == "") {
		if chat.GroupID != "" {
			wanted = append(wanted, signalpb.ManifestRecord_Identifier_GROUPV2)
		} else {
			wanted = append(wanted, signalpb.ManifestRecord_Identifier_CONTACT)
		}
	}
	if c.Pinned != nil {
		wanted = append(wanted, signalpb.ManifestRecord_Identifier_ACCOUNT)
	}
	return wanted
}

func recordMatchesChat(record *signalpb.StorageRecord, chat ChatRef) bool {
	switch data := record.GetRecord().(type) {
	case *signalpb.StorageRecord_Contact:
		if chat.GroupID != "" || chat.ServiceID.IsEmpty() {
			return false
		}
		aci, _ := ParseStringOrBinaryUUID(data.Contact.GetAci(), data.Contact.GetAciBinary())
		pni, _ := ParseStringOrBinaryUUID(data.Contact.GetPni(), data.Contact.GetPniBinary())
		if chat.ServiceID.Type == libsignalgo.ServiceIDTypeACI {
			return aci == chat.ServiceID.UUID
		}
		return pni == chat.ServiceID.UUID
	case *signalpb.StorageRecord_GroupV2:
		if chat.GroupID == "" {
			return false
		}
		groupID, err := groupIDFromMasterKeyBytes(data.GroupV2.GetMasterKey())
		return err == nil && groupID == chat.GroupID
	}
	return false
}

func findChatRecord(snap *storageSnapshot, chat ChatRef) *DecryptedStorageRecord {
	for _, rec := range snap.Records {
		if recordMatchesChat(rec.StorageRecord, chat) {
			return rec
		}
	}
	return nil
}

func findAccountRecord(snap *storageSnapshot) *DecryptedStorageRecord {
	for _, rec := range snap.Records {
		if _, ok := rec.StorageRecord.GetRecord().(*signalpb.StorageRecord_Account); ok {
			return rec
		}
	}
	return nil
}

// applyChatFields changes the wanted fields on a copy of a contact or group record.
// The record is cloned as a whole, so unknown fields survive.
func applyChatFields(record *signalpb.StorageRecord, change ChatSettingsChange) (*signalpb.StorageRecord, bool) {
	updated := proto.Clone(record).(*signalpb.StorageRecord)
	var mutedUntil *uint64
	var archived, markedUnread *bool
	switch data := updated.GetRecord().(type) {
	case *signalpb.StorageRecord_Contact:
		mutedUntil, archived, markedUnread = &data.Contact.MutedUntilTimestamp, &data.Contact.Archived, &data.Contact.MarkedUnread
	case *signalpb.StorageRecord_GroupV2:
		mutedUntil, archived, markedUnread = &data.GroupV2.MutedUntilTimestamp, &data.GroupV2.Archived, &data.GroupV2.MarkedUnread
	default:
		return nil, false
	}
	changed := false
	if change.MutedUntil != nil && *mutedUntil != *change.MutedUntil {
		*mutedUntil = *change.MutedUntil
		changed = true
	}
	if change.Archived != nil && *archived != *change.Archived {
		*archived = *change.Archived
		changed = true
	}
	if change.MarkedUnread != nil && *markedUnread != *change.MarkedUnread {
		*markedUnread = *change.MarkedUnread
		changed = true
	}
	return updated, changed
}

func pinMatchesChat(pin *signalpb.AccountRecord_PinnedConversation, chat ChatRef) bool {
	switch id := pin.GetIdentifier().(type) {
	case *signalpb.AccountRecord_PinnedConversation_Contact_:
		sid, ok := pinnedContactServiceID(id.Contact)
		return ok && chat.GroupID == "" && sid == chat.ServiceID
	case *signalpb.AccountRecord_PinnedConversation_GroupMasterKey:
		if chat.GroupID == "" {
			return false
		}
		groupID, err := groupIDFromMasterKeyBytes(id.GroupMasterKey)
		return err == nil && groupID == chat.GroupID
	}
	return false
}

func newPinEntry(chat ChatRef, contactRecord *signalpb.ContactRecord, existing []*signalpb.AccountRecord_PinnedConversation) (*signalpb.AccountRecord_PinnedConversation, error) {
	if chat.GroupID != "" {
		if len(chat.GroupMasterKey) != libsignalgo.GroupMasterKeyLength {
			return nil, fmt.Errorf("can't pin group without its master key")
		}
		return &signalpb.AccountRecord_PinnedConversation{
			Identifier: &signalpb.AccountRecord_PinnedConversation_GroupMasterKey{GroupMasterKey: bytes.Clone(chat.GroupMasterKey)},
		}, nil
	}
	// Follow whichever encoding the account already uses: Signal is migrating to the binary form.
	binary := contactRecord != nil && contactRecord.GetAci() == "" && len(contactRecord.GetAciBinary()) > 0
	for _, pin := range existing {
		if c := pin.GetContact(); c != nil && c.GetServiceId() == "" && len(c.GetServiceIdBinary()) > 0 {
			binary = true
		}
	}
	contact := &signalpb.AccountRecord_PinnedConversation_Contact{}
	if binary {
		contact.ServiceIdBinary = chat.ServiceID.Bytes()
	} else {
		contact.ServiceId = chat.ServiceID.String()
	}
	if contactRecord != nil {
		contact.E164 = contactRecord.GetE164()
	}
	return &signalpb.AccountRecord_PinnedConversation{
		Identifier: &signalpb.AccountRecord_PinnedConversation_Contact_{Contact: contact},
	}, nil
}

// applyPinChange returns a copy of the account record with the chat added to or removed from the pinned list.
func applyPinChange(record *signalpb.StorageRecord, chat ChatRef, pinned bool, contactRecord *signalpb.ContactRecord) (*signalpb.StorageRecord, bool, error) {
	updated := proto.Clone(record).(*signalpb.StorageRecord)
	account := updated.GetAccount()
	if account == nil {
		return nil, false, fmt.Errorf("record is not an account record")
	}
	isPinned := false
	var kept []*signalpb.AccountRecord_PinnedConversation
	for _, pin := range account.PinnedConversations {
		if pinMatchesChat(pin, chat) {
			isPinned = true
			if pinned {
				kept = append(kept, pin)
			}
		} else {
			kept = append(kept, pin)
		}
	}
	if isPinned == pinned {
		return updated, false, nil
	}
	if pinned {
		if len(kept) >= MaxPinnedChats {
			return nil, false, ErrTooManyPinnedChats
		}
		entry, err := newPinEntry(chat, contactRecord, account.PinnedConversations)
		if err != nil {
			return nil, false, err
		}
		kept = append(kept, entry)
	}
	account.PinnedConversations = kept
	return updated, true, nil
}

// applyChatSettingsChange turns a settings change into record edits. It returns no edits if nothing would change.
func applyChatSettingsChange(snap *storageSnapshot, chat ChatRef, change ChatSettingsChange) ([]storageEdit, error) {
	var edits []storageEdit
	chatRecord := findChatRecord(snap, chat)
	if change.needsChatRecord() {
		if chatRecord == nil {
			return nil, ErrNoStorageRecord
		}
		if updated, changed := applyChatFields(chatRecord.StorageRecord, change); changed {
			edits = append(edits, storageEdit{OldID: chatRecord.StorageID, Record: updated})
		}
	}
	if change.Pinned != nil {
		account := findAccountRecord(snap)
		if account == nil {
			return nil, fmt.Errorf("%w: no account record", ErrNoStorageRecord)
		}
		var contactRecord *signalpb.ContactRecord
		if chatRecord != nil {
			contactRecord = chatRecord.StorageRecord.GetContact()
		}
		if updated, changed, err := applyPinChange(account.StorageRecord, chat, *change.Pinned, contactRecord); err != nil {
			return nil, err
		} else if changed {
			edits = append(edits, storageEdit{OldID: account.StorageID, Record: updated})
		}
	}
	return edits, nil
}

// UpdateChatSettings changes the mute, archive, unread or pinned state of one chat in the storage service.
// Only the given fields of the one contact or group record (and the account record for pins) are touched.
func (cli *Client) UpdateChatSettings(ctx context.Context, chat ChatRef, change ChatSettingsChange) error {
	if len(cli.Store.MasterKey) == 0 {
		return ErrNoStorageMasterKey
	}
	log := zerolog.Ctx(ctx).With().Str("action", "update chat settings").Logger()
	ctx = log.WithContext(ctx)
	if change.Pinned != nil && *change.Pinned && chat.GroupID != "" && chat.GroupMasterKey == nil {
		masterKey, err := cli.Store.GroupStore.MasterKeyFromGroupIdentifier(ctx, chat.GroupID)
		if err != nil {
			return fmt.Errorf("failed to get group master key: %w", err)
		}
		mk := masterKeyToBytes(masterKey)
		chat.GroupMasterKey = mk[:]
	}
	cli.storageSyncLock.Lock()
	defer cli.storageSyncLock.Unlock()
	api := &clientStorageAPI{cli: cli, storageKey: deriveStorageServiceKey(cli.Store.MasterKey)}
	built, err := modifyStorage(ctx, api, api.storageKey, uint32(cli.Store.DeviceID), change.wantedRecordTypes(chat), func(snap *storageSnapshot) ([]storageEdit, error) {
		return applyChatSettingsChange(snap, chat, change)
	})
	if err != nil {
		return err
	} else if built == nil {
		log.Debug().Msg("Storage service already has the requested chat settings")
		return nil
	}
	err = cli.Store.DoContactTxn(ctx, func(ctx context.Context) error {
		return cli.processStorageInTxn(ctx, &StorageUpdate{
			Version:    built.Op.GetManifest().GetVersion(),
			NewRecords: built.NewRecords,
			// The caller updates the portal itself, an event would only race with it
			NoChatSettingsEvent: true,
		})
	})
	if err != nil {
		log.Err(err).Msg("Failed to process own storage write")
	}
	cli.sendStorageManifestChanged(ctx)
	return nil
}

// sendStorageManifestChanged tells the other devices to fetch the new storage manifest, like Signal-Desktop does.
func (cli *Client) sendStorageManifestChanged(ctx context.Context) {
	_, err := cli.sendContent(ctx, cli.Store.ACIServiceID(), uint64(time.Now().UnixMilli()), WrapSyncMessage(&signalpb.SyncMessage{
		Content: &signalpb.SyncMessage_FetchLatest_{
			FetchLatest: &signalpb.SyncMessage_FetchLatest{
				Type: signalpb.SyncMessage_FetchLatest_STORAGE_MANIFEST.Enum(),
			},
		},
	}), 0, false, nil, nil)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to send storage manifest fetch latest notice to other devices")
	}
}
