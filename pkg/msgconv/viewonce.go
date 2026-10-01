// mautrix-signal - A Matrix-Signal puppeting bridge.
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

package msgconv

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
)

// ViewOnce is the com.beeper.view_limited value of a Matrix media event that may be opened once. Signal has
// no other kind of view limit, so it is the only one the room features list and the only one accepted here.
var ViewOnce = event.BeeperViewLimitedMedia{Type: "count", Count: 1}

// ErrViewOnceNotMedia is returned for a view-once event that is not exactly one photo or video.
var ErrViewOnceNotMedia error = bridgev2.WrapErrorInStatus(errors.New("only a single photo or video can be sent as view-once on Signal")).WithErrorAsMessage().WithIsCertain(true).WithSendNotice(true).WithErrorReason(event.MessageStatusUnsupported)

// applyViewOnce marks an already converted media message as view-once if the Matrix event asks for it.
//
// Signal clients treat a view-once message as valid only if it is one image or video with no text, quote or
// link preview (Signal-Desktop's isValidTapToView), and show anything else as an unsupported message. A
// caption is therefore refused rather than silently lost, while the reply quote, which is only decoration,
// is dropped.
func applyViewOnce(dm *signalpb.DataMessage, content *event.MessageEventContent) error {
	limit := content.BeeperViewLimited
	if limit == nil {
		return nil
	}
	if *limit != ViewOnce {
		return fmt.Errorf("%w: Signal media can only be limited to one view", bridgev2.ErrUnsupportedViewLimitedType)
	}
	isPhotoOrVideo := content.MsgType == event.MsgImage || content.MsgType == event.MsgVideo
	if !isPhotoOrVideo || content.GetInfo().MauGIF || len(dm.GetAttachments()) != 1 {
		return ErrViewOnceNotMedia
	}
	if dm.GetBody() != "" {
		return fmt.Errorf("%w on view-once media", bridgev2.ErrCaptionsNotAllowed)
	}
	dm.IsViewOnce = proto.Bool(true)
	dm.Quote = nil
	dm.Preview = nil
	// Signal-Android raises the required version the same way, so that a client too old to honour the limit
	// shows an update notice instead of the media.
	dm.RequiredProtocolVersion = proto.Uint32(max(dm.GetRequiredProtocolVersion(), uint32(signalpb.DataMessage_VIEW_ONCE_VIDEO)))
	return nil
}
