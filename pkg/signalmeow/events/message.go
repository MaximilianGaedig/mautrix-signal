// mautrix-signal - A Matrix-signal puppeting bridge.
// Copyright (C) 2024 Tulir Asokan
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

package events

import (
	"github.com/google/uuid"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/ringrtc"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/types"
)

type SignalEvent interface {
	isSignalEvent()
}

func (*ChatEvent) isSignalEvent()              {}
func (*DecryptionError) isSignalEvent()        {}
func (*Receipt) isSignalEvent()                {}
func (*ReadSelf) isSignalEvent()               {}
func (*Call) isSignalEvent()                   {}
func (*ContactList) isSignalEvent()            {}
func (*ACIFound) isSignalEvent()               {}
func (*DeleteForMe) isSignalEvent()            {}
func (*MessageRequestResponse) isSignalEvent() {}
func (*QueueEmpty) isSignalEvent()             {}
func (*LoggedOut) isSignalEvent()              {}

type MessageInfo struct {
	Sender uuid.UUID
	ChatID string

	GroupRevision   uint32
	ServerTimestamp uint64
}

type ChatEvent struct {
	Info  MessageInfo
	Event signalpb.ChatEventContent
}

type DecryptionError struct {
	Sender    uuid.UUID
	Err       error
	Timestamp uint64
}

type Receipt struct {
	Sender  uuid.UUID
	Content *signalpb.ReceiptMessage
}

type ReadSelf struct {
	Timestamp uint64
	Messages  []*signalpb.SyncMessage_Read
}

type CallDirection string

const (
	CallDirectionIncoming CallDirection = "incoming"
	CallDirectionOutgoing CallDirection = "outgoing"
)

type CallMessageType string

const (
	CallMessageOffer  CallMessageType = "offer"
	CallMessageAnswer CallMessageType = "answer"
	CallMessageICE    CallMessageType = "ice_update"
	CallMessageBusy   CallMessageType = "busy"
	CallMessageHangup CallMessageType = "hangup"
	CallMessageOpaque CallMessageType = "opaque"
)

type CallType string

const (
	CallTypeAudio CallType = "audio"
	CallTypeVideo CallType = "video"
)

type Call struct {
	Info                MessageInfo
	Timestamp           uint64
	IsRinging           bool
	ID                  uint64
	Direction           CallDirection
	MessageType         CallMessageType
	Type                CallType
	DestinationDeviceID uint32
	HangupType          signalpb.CallMessage_Hangup_Type
	HangupDeviceID      uint32
	OpaqueUrgency       signalpb.CallMessage_Opaque_Urgency
	OpaqueLength        int
	Offer               *ringrtc.Offer
	Answer              *ringrtc.Answer
	ICECandidate        *ringrtc.IceCandidate
	ParseError          string
}

func (c *Call) ConnectionParameters() *ringrtc.ConnectionParametersV4 {
	if c.Offer != nil {
		return c.Offer.V4
	}
	if c.Answer != nil {
		return c.Answer.V4
	}
	return nil
}

type ContactList struct {
	Contacts []*types.Recipient
	IsFromDB bool
}

type ACIFound struct {
	PNI libsignalgo.ServiceID
	ACI libsignalgo.ServiceID
}

type DeleteForMe struct {
	Timestamp uint64
	*signalpb.SyncMessage_DeleteForMe
}

type MessageRequestResponse struct {
	Timestamp uint64
	ThreadACI uuid.UUID
	GroupID   *libsignalgo.GroupIdentifier
	Type      signalpb.SyncMessage_MessageRequestResponse_Type
	Raw       *signalpb.SyncMessage_MessageRequestResponse
}

type QueueEmpty struct{}

type LoggedOut struct{ Error error }
