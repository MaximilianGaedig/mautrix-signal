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
	"errors"
	"maps"
	"slices"

	"github.com/google/uuid"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
)

var errInvalidMasterKeyLength = errors.New("invalid group master key length")

// contactChatID returns the portal ID used for the 1:1 chat with a contact record.
// The ACI is preferred, the PNI is only used for contacts that have no ACI yet.
func contactChatID(aci, pni uuid.UUID) string {
	if aci != uuid.Nil {
		return libsignalgo.NewACIServiceID(aci).String()
	} else if pni != uuid.Nil {
		return libsignalgo.NewPNIServiceID(pni).String()
	}
	return ""
}

func contactRecordSettings(contact *signalpb.ContactRecord) types.ChatSettings {
	return types.ChatSettings{
		MutedUntil:   contact.GetMutedUntilTimestamp(),
		Archived:     contact.GetArchived(),
		MarkedUnread: contact.GetMarkedUnread(),
	}
}

func groupRecordSettings(group *signalpb.GroupV2Record) types.ChatSettings {
	return types.ChatSettings{
		MutedUntil:   group.GetMutedUntilTimestamp(),
		Archived:     group.GetArchived(),
		MarkedUnread: group.GetMarkedUnread(),
	}
}

// pinnedContactServiceID parses the service ID of a pinned contact entry, accepting both the string and binary forms.
func pinnedContactServiceID(contact *signalpb.AccountRecord_PinnedConversation_Contact) (libsignalgo.ServiceID, bool) {
	if len(contact.GetServiceIdBinary()) > 0 {
		if sid, err := libsignalgo.ServiceIDFromBytes(contact.GetServiceIdBinary()); err == nil && !sid.IsEmpty() {
			return sid, true
		}
	}
	if contact.GetServiceId() != "" {
		if sid, err := libsignalgo.ServiceIDFromString(contact.GetServiceId()); err == nil && !sid.IsEmpty() {
			return sid, true
		}
	}
	return libsignalgo.EmptyServiceID, false
}

// pinnedChatIDs returns the portal IDs of the chats listed in the account record's pinned conversations.
func pinnedChatIDs(account *signalpb.AccountRecord, groupIDFromMasterKey func([]byte) (types.GroupIdentifier, error)) map[string]struct{} {
	pinned := make(map[string]struct{})
	for _, pin := range account.GetPinnedConversations() {
		switch id := pin.GetIdentifier().(type) {
		case *signalpb.AccountRecord_PinnedConversation_Contact_:
			if sid, ok := pinnedContactServiceID(id.Contact); ok {
				pinned[sid.String()] = struct{}{}
			}
		case *signalpb.AccountRecord_PinnedConversation_GroupMasterKey:
			if groupID, err := groupIDFromMasterKey(id.GroupMasterKey); err == nil {
				pinned[string(groupID)] = struct{}{}
			}
		}
	}
	return pinned
}

func groupIDFromMasterKeyBytes(masterKey []byte) (types.GroupIdentifier, error) {
	if len(masterKey) != libsignalgo.GroupMasterKeyLength {
		return "", errInvalidMasterKeyLength
	}
	return groupIdentifierFromMasterKey(masterKeyFromBytes(libsignalgo.GroupMasterKey(masterKey)))
}

func (cli *Client) recordChatSettings(chatID string, settings types.ChatSettings) {
	cli.chatSettingsLock.Lock()
	defer cli.chatSettingsLock.Unlock()
	if cli.chatSettings == nil {
		cli.chatSettings = make(map[string]types.ChatSettings)
	}
	cli.chatSettings[chatID] = settings
}

func (cli *Client) recordPinnedChats(pinned map[string]struct{}) {
	cli.chatSettingsLock.Lock()
	defer cli.chatSettingsLock.Unlock()
	cli.chatPinned = pinned
}

// ChatSettings returns the last seen storage service settings for a chat, keyed by portal ID.
func (cli *Client) ChatSettings(chatID string) (types.ChatSettings, bool) {
	cli.chatSettingsLock.RLock()
	defer cli.chatSettingsLock.RUnlock()
	settings, ok := cli.chatSettings[chatID]
	_, pinned := cli.chatPinned[chatID]
	settings.Pinned = pinned
	return settings, ok || pinned
}

func (cli *Client) chatSettingsEvent() *events.ChatSettings {
	cli.chatSettingsLock.RLock()
	defer cli.chatSettingsLock.RUnlock()
	ids := make(map[string]struct{}, len(cli.chatSettings)+len(cli.chatPinned))
	for id := range cli.chatSettings {
		ids[id] = struct{}{}
	}
	for id := range cli.chatPinned {
		ids[id] = struct{}{}
	}
	evt := &events.ChatSettings{Chats: make([]events.ChatSettingsEntry, 0, len(ids))}
	for _, id := range slices.Sorted(maps.Keys(ids)) {
		settings := cli.chatSettings[id]
		_, settings.Pinned = cli.chatPinned[id]
		evt.Chats = append(evt.Chats, events.ChatSettingsEntry{ChatID: id, Settings: settings})
	}
	return evt
}
