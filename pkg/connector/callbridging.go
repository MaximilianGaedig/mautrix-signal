// mautrix-signal - A Matrix-Signal puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package connector

// Signal call bridging for bidirectional 1:1 audio/video calls over RingRTC's
// ICE/static-SRTP media transport.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pion/rtcp"
	"github.com/pion/stun/v4"
	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
	"go.mau.fi/util/ptr"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/callbridge"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-signal/pkg/libsignalgo"
	"go.mau.fi/mautrix-signal/pkg/signalid"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/events"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/protobuf/signalpb"
	"go.mau.fi/mautrix-signal/pkg/signalmeow/ringrtc"
)

const signalCallInviteLifetime = 60 * time.Second

var signalMatrixCallEventTypes = []event.Type{
	event.CallInvite, event.CallCandidates, event.CallAnswer, event.CallReject,
	event.CallSelectAnswer, event.CallNegotiate, event.CallHangup,
}

// registerCallEventHandlers hooks m.call.* into the Matrix event processor;
// bridgev2 itself drops them. This is the same hook used by mautrix-meta.
func (s *SignalConnector) registerCallEventHandlers() {
	mx, ok := s.Bridge.Matrix.(*matrix.Connector)
	if !ok || mx.EventProcessor == nil {
		s.Bridge.Log.Warn().Msg("Matrix connector doesn't expose an event processor, call bridging can't receive m.call events")
		return
	}
	for _, evtType := range signalMatrixCallEventTypes {
		mx.EventProcessor.On(evtType, s.handleMatrixCallEvent)
	}
}

func (s *SignalConnector) handleMatrixCallEvent(ctx context.Context, evt *event.Event) {
	if !s.Config.CallBridging || s.Bridge.IsGhostMXID(evt.Sender) || evt.Sender == s.Bridge.Bot.GetMXID() {
		return
	}
	log := s.Bridge.Log.With().
		Str("action", "handle matrix call event").
		Str("event_type", evt.Type.Type).
		Stringer("event_id", evt.ID).
		Stringer("room_id", evt.RoomID).
		Logger()
	ctx = log.WithContext(ctx)
	portal, err := s.Bridge.GetPortalByMXID(ctx, evt.RoomID)
	if err != nil || portal == nil {
		return
	}
	login, err := s.Bridge.GetExistingUserLoginByID(ctx, portal.Receiver)
	if err != nil || login == nil {
		user, userErr := s.Bridge.GetUserByMXID(ctx, evt.Sender)
		if userErr != nil || user == nil {
			return
		}
		login = user.GetDefaultLogin()
	}
	if login == nil || login.UserMXID != evt.Sender {
		log.Debug().Msg("Ignoring call event from a user without a login for this portal")
		return
	}
	client, ok := login.Client.(*SignalClient)
	if !ok || client.Client == nil {
		return
	}
	client.callBridge.handleMatrixEvent(ctx, portal, evt)
}

type signalCallBridge struct {
	client *SignalClient
	lock   sync.Mutex
	active *signalCallSession
}

type signalCallSession struct {
	bridge *signalCallBridge
	ctx    context.Context
	cancel context.CancelFunc
	log    zerolog.Logger
	lock   sync.Mutex

	portal          *bridgev2.Portal
	ghost           bridgev2.MatrixAPI
	peer            libsignalgo.ServiceID
	signalID        uint64
	sourceDevice    uint32
	incoming        bool
	callType        events.CallType
	videoCodec      string
	videoCodecID    ringrtc.VideoCodecType
	offer           *ringrtc.Offer
	mxCallID        string
	mxParty         string
	mxLocalSDP      string
	mxLeg           *callbridge.Leg
	signalLeg       *ringrtc.MediaLeg
	matrixVideoSSRC uint32
	pendingKeyframe bool
	pendingICE      []signalRemoteCandidate
	answering       bool

	endOnce sync.Once
}

type signalRemoteCandidate struct {
	device uint32
	sdp    string
}

func (s *SignalClient) handleSignalCall(evt *events.Call) {
	switch evt.MessageType {
	case events.CallMessageOffer:
		if (evt.Type != events.CallTypeAudio && evt.Type != events.CallTypeVideo) || evt.Offer == nil || evt.Offer.V4 == nil || evt.ParseError != "" {
			return
		}
		go s.callBridge.startIncoming(context.WithoutCancel(s.Main.Bridge.BackgroundCtx), evt)
	case events.CallMessageICE:
		if evt.ICECandidate != nil && evt.ParseError == "" {
			s.callBridge.handleRemoteICE(evt)
		}
	case events.CallMessageAnswer:
		if evt.Answer != nil && evt.Answer.V4 != nil && evt.ParseError == "" {
			s.callBridge.handleRemoteAnswer(evt)
		}
	case events.CallMessageHangup, events.CallMessageBusy:
		s.callBridge.handleRemoteEnd(evt)
	}
}

func (cb *signalCallBridge) startIncoming(ctx context.Context, evt *events.Call) {
	peer := libsignalgo.NewACIServiceID(evt.Info.Sender)
	cb.lock.Lock()
	active := cb.active
	if active != nil {
		cb.lock.Unlock()
		if active.signalID != evt.ID || active.peer != peer {
			// RingRTC core/platform.rs documents Busy as broadcast to all devices.
			if err := cb.sendBusy(ctx, peer, evt.ID); err != nil {
				cb.client.UserLogin.Log.Warn().Err(err).Msg("Failed to send Signal busy response")
			}
		}
		return
	}
	cb.lock.Unlock()

	portal, err := cb.client.Main.Bridge.GetExistingPortalByKey(ctx, cb.client.makeDMPortalKey(peer))
	if err != nil || portal == nil || portal.MXID == "" {
		cb.client.UserLogin.Log.Warn().Err(err).Msg("Can't ring Matrix for Signal call without an existing DM portal")
		return
	}
	ghost, err := cb.client.Main.Bridge.GetGhostByID(ctx, signalid.MakeUserID(evt.Info.Sender))
	if err != nil || ghost == nil {
		cb.client.UserLogin.Log.Warn().Err(err).Msg("Can't get Signal caller ghost")
		return
	}

	sessionCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	videoCodec, videoCodecID := signalVideoSelection(evt)
	session := &signalCallSession{
		bridge: cb, ctx: sessionCtx, cancel: cancel,
		portal: portal, ghost: ghost.Intent, peer: peer, signalID: evt.ID, sourceDevice: evt.Info.SourceDeviceID,
		incoming: true, callType: evt.Type, videoCodec: videoCodec, videoCodecID: videoCodecID, offer: evt.Offer,
		mxCallID: uuid.NewString(), mxParty: "bridge-" + uuid.NewString()[:8],
	}
	if session.callType == events.CallTypeVideo && session.videoCodec == "" {
		cancel()
		cb.client.UserLogin.Log.Warn().Msg("Can't ring Matrix for Signal video call without a mutually supported relay codec")
		return
	}
	session.log = cb.client.UserLogin.Log.With().
		Str("component", "call bridge").
		Uint64("signal_call_id", evt.ID).
		Str("portal_id", string(portal.ID)).
		Str("mx_call_id", session.mxCallID).
		Logger()

	cb.lock.Lock()
	if cb.active != nil {
		cb.lock.Unlock()
		cancel()
		if err = cb.sendBusy(ctx, peer, evt.ID); err != nil {
			session.log.Warn().Err(err).Msg("Failed to send Signal busy response")
		}
		return
	}
	cb.active = session
	cb.lock.Unlock()

	if err = session.ringMatrix(); err != nil {
		session.log.Error().Err(err).Msg("Failed to ring Matrix for incoming Signal call")
		session.end(signalCallEndFailed, true)
		return
	}
	session.log.Info().Uint32("source_device_id", evt.Info.SourceDeviceID).Msg("Ringing Matrix for incoming Signal call")
	time.AfterFunc(signalCallInviteLifetime, func() {
		if session.ctx.Err() == nil {
			session.log.Info().Msg("Nobody answered the Signal call in Matrix")
			session.end(signalCallEndTimeout, true)
		}
	})
}

func (s *signalCallSession) matrixICEServers() []webrtc.ICEServer {
	asIntent, ok := s.ghost.(*matrix.ASIntent)
	if !ok {
		return nil
	}
	resp, err := asIntent.Matrix.TurnServer(s.ctx)
	if err != nil {
		s.log.Warn().Err(err).Msg("Failed to fetch homeserver TURN server, using no relay on the Matrix leg")
		return nil
	} else if len(resp.URIs) == 0 {
		return nil
	}
	s.log.Debug().Int("url_count", len(resp.URIs)).Msg("Got homeserver TURN server")
	return []webrtc.ICEServer{{URLs: resp.URIs, Username: resp.Username, Credential: resp.Password}}
}

func (s *signalCallSession) ringMatrix() error {
	leg, err := callbridge.NewLeg(callbridge.LegConfig{
		Name: "matrix", ICEServers: s.matrixICEServers(), VideoCodec: s.videoCodec, Log: s.log,
	})
	if err != nil {
		return err
	}
	s.lock.Lock()
	if s.ctx.Err() != nil {
		s.lock.Unlock()
		leg.Close()
		return s.ctx.Err()
	}
	s.mxLeg = leg
	s.lock.Unlock()
	if _, err = leg.CreateOffer(); err != nil {
		leg.Close()
		return err
	}
	offer := leg.WaitGathering(s.ctx, 3*time.Second)
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	_, err = s.ghost.SendMessage(s.ctx, s.portal.MXID, event.CallInvite, signalCallEventContent(&event.CallInviteEventContent{
		BaseCallEventContent: s.baseMatrixContent(),
		Lifetime:             int(signalCallInviteLifetime / time.Millisecond),
		Offer:                event.CallData{SDP: offer, Type: event.CallDataTypeOffer},
	}), nil)
	return err
}

// signalVideoCodec picks a codec that callbridge can relay without transcoding.
// RingRTC signaling.proto defines receive_video_codecs as the legacy
// bidirectional set. Newer peers send encode_only and decode_only sets; a pure
// RTP relay requires an intersection between them.
func signalVideoCodec(evt *events.Call) string {
	mime, _ := signalVideoSelection(evt)
	return mime
}

func signalVideoSelection(evt *events.Call) (string, ringrtc.VideoCodecType) {
	if evt == nil || evt.Type != events.CallTypeVideo || evt.Offer == nil || evt.Offer.V4 == nil {
		return "", 0
	}
	params := evt.Offer.V4
	encode := params.EncodeOnlyVideoCodecs
	decode := params.DecodeOnlyVideoCodecs
	if len(encode) == 0 {
		encode = params.ReceiveVideoCodecs
	}
	if len(decode) == 0 {
		decode = params.ReceiveVideoCodecs
	}
	// The current RingRTC WebRTC build fixes VP8 to PT 108. Matrix's shared
	// leg also supports VP8, so this is the codec we can relay without decode.
	if hasSignalVideoCodec(encode, ringrtc.VideoCodecVP8) && hasSignalVideoCodec(decode, ringrtc.VideoCodecVP8) {
		return webrtc.MimeTypeVP8, ringrtc.VideoCodecVP8
	}
	return "", 0
}

func hasSignalVideoCodec(codecs []ringrtc.VideoCodec, typ ringrtc.VideoCodecType) bool {
	for _, codec := range codecs {
		if codec.Type == typ {
			return true
		}
	}
	return false
}

func (s *signalCallSession) baseMatrixContent() event.BaseCallEventContent {
	return event.BaseCallEventContent{CallID: s.mxCallID, PartyID: s.mxParty, Version: "1"}
}

// signalCallEventContent keeps version 1 as a JSON string, as required by
// Matrix VoIP and strict Ruma clients. CallVersion otherwise serializes it as
// a number.
func signalCallEventContent(parsed any) *event.Content {
	raw := map[string]any{}
	if data, err := json.Marshal(parsed); err == nil {
		_ = json.Unmarshal(data, &raw)
	}
	if version, ok := raw["version"]; ok && fmt.Sprint(version) != "0" {
		raw["version"] = fmt.Sprint(version)
	}
	return &event.Content{Raw: raw}
}

func parseSignalCallContent[T any](evt *event.Event) (*T, bool) {
	if parsed, ok := evt.Content.Parsed.(*T); ok {
		return parsed, true
	}
	var out T
	if err := json.Unmarshal(evt.Content.VeryRaw, &out); err != nil {
		return nil, false
	}
	return &out, true
}

func (cb *signalCallBridge) handleMatrixEvent(ctx context.Context, portal *bridgev2.Portal, evt *event.Event) {
	if evt.Type == event.CallInvite {
		inv, ok := parseSignalCallContent[event.CallInviteEventContent](evt)
		if ok {
			go cb.startOutgoing(context.WithoutCancel(ctx), portal, inv)
		}
		return
	}
	var base *event.BaseCallEventContent
	if evt.Type == event.CallAnswer {
		if answer, ok := parseSignalCallContent[event.CallAnswerEventContent](evt); ok {
			base = &answer.BaseCallEventContent
		}
	} else if parsed, ok := parseSignalCallContent[event.BaseCallEventContent](evt); ok {
		base = parsed
	}
	if base == nil {
		return
	}
	cb.lock.Lock()
	session := cb.active
	cb.lock.Unlock()
	if session == nil || session.mxCallID != base.CallID || session.portal.MXID != portal.MXID || base.PartyID == session.mxParty {
		return
	}
	switch evt.Type {
	case event.CallReject, event.CallHangup:
		session.log.Info().Str("event_type", evt.Type.Type).Msg("Matrix user ended Signal call")
		go session.end(signalCallEndLocalDecline, true)
	case event.CallAnswer:
		if !session.incoming {
			return
		}
		answer, ok := parseSignalCallContent[event.CallAnswerEventContent](evt)
		if !ok || answer.Answer.SDP == "" {
			go session.end(signalCallEndFailed, true)
			return
		}
		go session.answerIncoming(answer.Answer.SDP, answer.PartyID)
	case event.CallCandidates:
		candidates, ok := parseSignalCallContent[event.CallCandidatesEventContent](evt)
		if !ok {
			return
		}
		session.addMatrixCandidates(candidates.Candidates)
	}
}

func (s *signalCallSession) addMatrixCandidates(candidates []event.CallCandidate) {
	s.lock.Lock()
	leg := s.mxLeg
	s.lock.Unlock()
	if leg == nil {
		return
	}
	for _, candidate := range candidates {
		if candidate.SDPMLineIndex < 0 || candidate.SDPMLineIndex > 65535 {
			s.log.Debug().Msg("Rejected Matrix ICE candidate with invalid media line index")
			continue
		}
		mid := candidate.SDPMID
		line := uint16(candidate.SDPMLineIndex)
		if err := leg.AddCandidate(webrtc.ICECandidateInit{
			Candidate: candidate.Candidate, SDPMid: &mid, SDPMLineIndex: &line,
		}); err != nil {
			// Candidate errors can contain addresses, so do not attach the error.
			s.log.Debug().Msg("Rejected malformed Matrix ICE candidate")
		}
	}
}

func (cb *signalCallBridge) startOutgoing(ctx context.Context, portal *bridgev2.Portal, inv *event.CallInviteEventContent) {
	if portal.OtherUserID == "" || inv == nil || inv.Offer.SDP == "" {
		return
	}
	peer, err := signalid.ParseUserIDAsServiceID(portal.OtherUserID)
	if err != nil || peer.Type != libsignalgo.ServiceIDTypeACI {
		return
	}
	ghost, err := cb.client.Main.Bridge.GetGhostByID(ctx, portal.OtherUserID)
	if err != nil || ghost == nil {
		return
	}
	cb.lock.Lock()
	if cb.active != nil {
		cb.lock.Unlock()
		cb.rejectMatrixCall(ctx, ghost.Intent, portal, inv, "user_busy")
		return
	}
	callUUID := uuid.New()
	callID := binary.BigEndian.Uint64(callUUID[:8])
	if callID == 0 {
		callID = 1
	}
	sessionCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	callType := events.CallTypeAudio
	videoCodec := ""
	videoCodecID := ringrtc.VideoCodecType(0)
	if callbridge.SendsVideo(inv.Offer.SDP) {
		callType = events.CallTypeVideo
		videoCodec = webrtc.MimeTypeVP8
		videoCodecID = ringrtc.VideoCodecVP8
	}
	s := &signalCallSession{
		bridge: cb, ctx: sessionCtx, cancel: cancel, portal: portal, ghost: ghost.Intent, peer: peer,
		signalID: callID, callType: callType, videoCodec: videoCodec, videoCodecID: videoCodecID,
		mxCallID: inv.CallID, mxParty: "bridge-" + uuid.NewString()[:8],
	}
	s.log = cb.client.UserLogin.Log.With().Str("component", "call bridge").Uint64("signal_call_id", callID).
		Str("portal_id", string(portal.ID)).Str("mx_call_id", s.mxCallID).Logger()
	cb.active = s
	cb.lock.Unlock()

	mxLeg, err := callbridge.NewLeg(callbridge.LegConfig{
		Name: "matrix", ICEServers: s.matrixICEServers(), VideoCodec: videoCodec, Log: s.log,
	})
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to create Matrix leg for outgoing Signal call")
		s.end(signalCallEndFailed, false)
		return
	}
	s.lock.Lock()
	s.mxLeg = mxLeg
	s.lock.Unlock()
	if _, err = mxLeg.AnswerOffer(inv.Offer.SDP); err != nil {
		s.log.Warn().Msg("Rejected malformed outgoing Matrix call offer")
		s.end(signalCallEndFailed, false)
		return
	}
	s.mxLocalSDP = mxLeg.WaitGathering(s.ctx, 3*time.Second)
	if s.ctx.Err() != nil {
		return
	}
	iceServers, err := s.signalICEServers()
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to prepare Signal calling relays")
		s.end(signalCallEndFailed, false)
		return
	}
	callerIdentity, calleeIdentity, err := s.identityKeys()
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to load Signal call identity keys")
		s.end(signalCallEndFailed, false)
		return
	}
	signalLeg, err := ringrtc.NewOutgoingMediaLeg(ringrtc.OutgoingMediaConfig{
		CallerIdentityKey: callerIdentity, CalleeIdentityKey: calleeIdentity,
		ICEServers: iceServers, VideoCodec: videoCodecID, OnCandidate: s.onLocalSignalCandidate,
	})
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to create outgoing Signal media leg")
		s.end(signalCallEndFailed, false)
		return
	}
	s.lock.Lock()
	s.signalLeg = signalLeg
	s.lock.Unlock()
	if err = cb.sendCallMessage(s.ctx, peer, signalOfferMessage(callID, callType, signalLeg.Parameters)); err != nil {
		s.log.Error().Err(err).Msg("Failed to send Signal call offer")
		s.end(signalCallEndFailed, false)
		return
	}
	s.log.Info().Bool("video", callType == events.CallTypeVideo).Msg("Ringing Signal for outgoing Matrix call")
	if err = signalLeg.GatherCandidates(); err != nil {
		s.log.Error().Err(err).Msg("Failed to gather Signal ICE candidates")
		s.end(signalCallEndFailed, true)
		return
	}
	lifetime := signalCallInviteLifetime
	if inv.Lifetime > 0 {
		lifetime = min(time.Duration(inv.Lifetime)*time.Millisecond, 2*signalCallInviteLifetime)
	}
	time.AfterFunc(lifetime, func() {
		if s.ctx.Err() == nil {
			s.log.Info().Msg("Signal peer didn't answer")
			s.end(signalCallEndTimeout, true)
		}
	})
}

func (cb *signalCallBridge) rejectMatrixCall(ctx context.Context, ghost bridgev2.MatrixAPI, portal *bridgev2.Portal, inv *event.CallInviteEventContent, reason event.CallHangupReason) {
	_, _ = ghost.SendMessage(ctx, portal.MXID, event.CallHangup, signalCallEventContent(&event.CallHangupEventContent{
		BaseCallEventContent: event.BaseCallEventContent{CallID: inv.CallID, PartyID: "bridge-busy", Version: "1"}, Reason: reason,
	}), nil)
}

func (s *signalCallSession) onLocalSignalCandidate(candidate string) {
	if err := s.sendSignalICE(candidate); err != nil && s.ctx.Err() == nil {
		s.log.Warn().Err(err).Msg("Failed to send local Signal ICE candidate")
	}
}

func (cb *signalCallBridge) handleRemoteICE(evt *events.Call) {
	cb.lock.Lock()
	session := cb.active
	cb.lock.Unlock()
	if session == nil || session.signalID != evt.ID || session.peer.UUID != evt.Info.Sender || evt.ICECandidate.AddedV3 == nil {
		return
	}
	session.lock.Lock()
	if session.sourceDevice != 0 && session.sourceDevice != evt.Info.SourceDeviceID {
		session.lock.Unlock()
		return
	}
	leg := session.signalLeg
	if leg == nil || (!session.incoming && session.sourceDevice == 0) {
		session.pendingICE = append(session.pendingICE, signalRemoteCandidate{device: evt.Info.SourceDeviceID, sdp: evt.ICECandidate.AddedV3.SDP})
		session.lock.Unlock()
		return
	}
	session.lock.Unlock()
	if err := leg.AddRemoteCandidate(evt.ICECandidate.AddedV3.SDP); err != nil {
		// Candidate errors can contain addresses, so do not attach the error.
		session.log.Debug().Msg("Rejected malformed remote Signal ICE candidate")
	}
}

func (cb *signalCallBridge) handleRemoteAnswer(evt *events.Call) {
	cb.lock.Lock()
	s := cb.active
	cb.lock.Unlock()
	if s == nil || s.incoming || s.signalID != evt.ID || s.peer.UUID != evt.Info.Sender || evt.Info.SourceDeviceID == 0 {
		return
	}
	s.lock.Lock()
	if s.answering || s.ctx.Err() != nil {
		s.lock.Unlock()
		return
	}
	s.answering = true
	s.sourceDevice = evt.Info.SourceDeviceID
	leg := s.signalLeg
	pendingRemote := s.pendingICE
	s.pendingICE = nil
	s.lock.Unlock()
	if leg == nil || evt.Answer == nil || evt.Answer.V4 == nil {
		s.end(signalCallEndFailed, true)
		return
	}
	if err := leg.SetRemoteAnswer(evt.Answer.V4); err != nil {
		s.log.Warn().Msg("Rejected malformed Signal call answer")
		s.end(signalCallEndFailed, true)
		return
	}
	for _, candidate := range pendingRemote {
		if candidate.device != s.sourceDevice {
			continue
		}
		if err := leg.AddRemoteCandidate(candidate.sdp); err != nil {
			s.log.Debug().Msg("Rejected malformed queued Signal ICE candidate")
		}
	}
	if err := leg.Connect(s.ctx); err != nil {
		s.log.Error().Err(err).Msg("Failed to connect outgoing Signal media leg")
		s.end(signalCallEndFailed, true)
		return
	}
	if err := leg.WaitAccepted(s.ctx, s.signalID); err != nil {
		s.log.Error().Err(err).Msg("Failed to receive Signal call acceptance")
		s.end(signalCallEndFailed, true)
		return
	}
	if err := cb.sendCallMessage(s.ctx, s.peer, signalAcceptedMessage(s.signalID, s.sourceDevice)); err != nil {
		s.log.Warn().Err(err).Msg("Failed to dismiss Signal ringing on other devices")
	}
	_, err := s.ghost.SendMessage(s.ctx, s.portal.MXID, event.CallAnswer, signalCallEventContent(&event.CallAnswerEventContent{
		BaseCallEventContent: s.baseMatrixContent(),
		Answer:               event.CallData{SDP: s.mxLocalSDP, Type: event.CallDataTypeAnswer},
	}), nil)
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to answer outgoing Matrix call")
		s.end(signalCallEndFailed, true)
		return
	}
	s.log.Info().Bool("video", s.callType == events.CallTypeVideo).Msg("Outgoing Signal and Matrix media legs connected")
	s.startAudioRelays()
	if s.callType == events.CallTypeVideo {
		s.startVideoRelays()
	}
	s.startRTCPRelay()
}

func (s *signalCallSession) answerIncoming(matrixAnswer, matrixParty string) {
	s.lock.Lock()
	if s.answering || s.ctx.Err() != nil {
		s.lock.Unlock()
		return
	}
	s.answering = true
	mxLeg := s.mxLeg
	s.lock.Unlock()
	if mxLeg == nil || s.offer == nil || s.offer.V4 == nil {
		s.end(signalCallEndFailed, true)
		return
	}
	if err := mxLeg.SetAnswer(matrixAnswer); err != nil {
		s.log.Warn().Msg("Rejected malformed Matrix call answer")
		s.end(signalCallEndFailed, true)
		return
	}
	_, err := s.ghost.SendMessage(s.ctx, s.portal.MXID, event.CallSelectAnswer, signalCallEventContent(&event.CallSelectAnswerEventContent{
		BaseCallEventContent: s.baseMatrixContent(), SelectedPartyID: matrixParty,
	}), nil)
	if err != nil {
		s.log.Warn().Err(err).Msg("Failed to select Matrix call answer")
	}
	iceServers, err := s.signalICEServers()
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to prepare Signal calling relays")
		s.end(signalCallEndFailed, true)
		return
	}
	callerIdentity, calleeIdentity, err := s.identityKeys()
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to load Signal call identity keys")
		s.end(signalCallEndFailed, true)
		return
	}
	signalLeg, err := ringrtc.NewIncomingMediaLeg(ringrtc.IncomingMediaConfig{
		Remote: s.offer.V4, CallerIdentityKey: callerIdentity, CalleeIdentityKey: calleeIdentity,
		ICEServers: iceServers, VideoCodec: s.videoCodecID,
		OnCandidate: s.onLocalSignalCandidate,
	})
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to create Signal media leg")
		s.end(signalCallEndFailed, true)
		return
	}
	s.lock.Lock()
	s.signalLeg = signalLeg
	pendingICE := s.pendingICE
	s.pendingICE = nil
	s.lock.Unlock()
	for _, candidate := range pendingICE {
		if candidate.device != s.sourceDevice {
			continue
		}
		if err = signalLeg.AddRemoteCandidate(candidate.sdp); err != nil {
			s.log.Debug().Msg("Rejected malformed queued Signal ICE candidate")
		}
	}
	if err = s.sendSignalAnswer(signalLeg.Parameters); err != nil {
		s.log.Error().Err(err).Msg("Failed to answer Signal call")
		s.end(signalCallEndFailed, true)
		return
	}
	if err = signalLeg.GatherCandidates(); err != nil {
		s.log.Error().Err(err).Msg("Failed to gather Signal ICE candidates")
		s.end(signalCallEndFailed, true)
		return
	}
	if err = signalLeg.Connect(s.ctx); err != nil {
		s.log.Error().Err(err).Msg("Failed to connect Signal media leg")
		s.end(signalCallEndFailed, true)
		return
	}
	if err = signalLeg.SendAccepted(s.signalID, 1); err != nil {
		s.log.Error().Err(err).Msg("Failed to accept Signal media connection")
		s.end(signalCallEndFailed, true)
		return
	}
	go s.repeatSignalAccepted()
	s.log.Info().Bool("video", s.callType == events.CallTypeVideo).Msg("Signal and Matrix media legs connected")
	s.startAudioRelays()
	if s.callType == events.CallTypeVideo {
		s.startVideoRelays()
	}
	s.startRTCPRelay()
}

func (s *signalCallSession) identityKeys() (caller, callee []byte, err error) {
	local, err := s.bridge.client.Client.Store.ACIIdentityKeyPair.GetPublicKey().Serialize()
	if err != nil {
		return nil, nil, err
	}
	peerIdentity, err := s.bridge.client.Client.Store.IdentityKeyStore.GetIdentityKey(s.ctx, s.peer)
	if err != nil {
		return nil, nil, err
	} else if peerIdentity == nil {
		return nil, nil, errors.New("peer Signal identity key is missing")
	}
	peer, err := peerIdentity.Serialize()
	if err != nil {
		return nil, nil, err
	}
	if s.incoming {
		return peer, local, nil
	}
	return local, peer, nil
}

func (s *signalCallSession) signalICEServers() ([]*stun.URI, error) {
	relays, err := s.bridge.client.Client.GetCallingRelays(s.ctx)
	if err != nil {
		return nil, err
	}
	var out []*stun.URI
	for _, relay := range relays.Relays {
		urls := relay.URLs
		if len(urls) == 0 {
			urls = relay.URLsWithIPs
		}
		parsed, parseErr := ringrtc.ParseICEServerURLs(urls, relay.Username, relay.Password)
		if parseErr != nil {
			return nil, parseErr
		}
		out = append(out, parsed...)
	}
	return out, nil
}

func (s *signalCallSession) sendSignalAnswer(params *ringrtc.ConnectionParametersV4) error {
	return s.bridge.sendCallMessage(s.ctx, s.peer, signalAnswerMessage(s.signalID, s.sourceDevice, params))
}

func (s *signalCallSession) sendSignalICE(candidate string) error {
	var destinationDevice *uint32
	if s.incoming {
		destinationDevice = ptr.Ptr(s.sourceDevice)
	}
	return s.bridge.sendCallMessage(s.ctx, s.peer, signalICEMessage(s.signalID, destinationDevice, candidate))
}

func signalAnswerMessage(callID uint64, destinationDevice uint32, params *ringrtc.ConnectionParametersV4) *signalpb.CallMessage {
	answer := &signalpb.CallMessage_Answer{Id: ptr.Ptr(callID), Opaque: ringrtc.EncodeAnswer(&ringrtc.Answer{V4: params})}
	return &signalpb.CallMessage{Answer: answer, DestinationDeviceId: ptr.Ptr(destinationDevice)}
}

func signalOfferMessage(callID uint64, callType events.CallType, params *ringrtc.ConnectionParametersV4) *signalpb.CallMessage {
	offerType := signalpb.CallMessage_Offer_OFFER_AUDIO_CALL
	if callType == events.CallTypeVideo {
		offerType = signalpb.CallMessage_Offer_OFFER_VIDEO_CALL
	}
	return &signalpb.CallMessage{Offer: &signalpb.CallMessage_Offer{
		Id: ptr.Ptr(callID), Type: &offerType, Opaque: ringrtc.EncodeOffer(&ringrtc.Offer{V4: params}),
	}}
}

func signalICEMessage(callID uint64, destinationDevice *uint32, candidate string) *signalpb.CallMessage {
	iceUpdate := &signalpb.CallMessage_IceUpdate{
		Id:     ptr.Ptr(callID),
		Opaque: ringrtc.EncodeIceCandidate(&ringrtc.IceCandidate{AddedV3: &ringrtc.IceCandidateV3{SDP: candidate}}),
	}
	return &signalpb.CallMessage{IceUpdate: []*signalpb.CallMessage_IceUpdate{iceUpdate}, DestinationDeviceId: destinationDevice}
}

func (s *signalCallSession) repeatSignalAccepted() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for sequence := uint16(2); ; sequence++ {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.signalLeg.SendAccepted(s.signalID, sequence); err != nil && s.ctx.Err() == nil {
				s.log.Debug().Msg("Signal acceptance heartbeat stopped")
				return
			}
		}
	}
}

func (s *signalCallSession) startAudioRelays() {
	var localSSRC, remoteSSRC uint32 = ringrtc.CallerAudioSSRC, ringrtc.CalleeAudioSSRC
	if s.incoming {
		localSSRC, remoteSSRC = ringrtc.CalleeAudioSSRC, ringrtc.CallerAudioSSRC
	}
	writer, err := s.signalLeg.RTPWriter(ringrtc.OpusPayloadType, localSSRC)
	if err != nil {
		s.end(signalCallEndFailed, true)
		return
	}
	go func() {
		track, trackErr := s.mxLeg.RemoteTrack(s.ctx)
		if trackErr == nil {
			trackErr = callbridge.Relay(s.ctx, track, uint8(track.PayloadType()), writer, &callbridge.RelayStats{}, s.log)
		}
		if trackErr != nil && s.ctx.Err() == nil {
			s.log.Warn().Err(trackErr).Msg("Matrix to Signal audio relay stopped")
			s.end(signalCallEndFailed, true)
		}
	}()
	go func() {
		reader, trackErr := s.signalLeg.RTPReader(remoteSSRC)
		if trackErr == nil {
			trackErr = callbridge.Relay(s.ctx, reader, ringrtc.OpusPayloadType, s.mxLeg.Local, &callbridge.RelayStats{}, s.log)
		}
		if trackErr != nil && s.ctx.Err() == nil {
			s.log.Warn().Err(trackErr).Msg("Signal to Matrix audio relay stopped")
			s.end(signalCallEndFailed, true)
		}
	}()
}

func (s *signalCallSession) startVideoRelays() {
	var localSSRC, remoteSSRC uint32 = ringrtc.CallerVideoSSRC, ringrtc.CalleeVideoSSRC
	if s.incoming {
		localSSRC, remoteSSRC = ringrtc.CalleeVideoSSRC, ringrtc.CallerVideoSSRC
	}
	writer, err := s.signalLeg.RTPWriter(ringrtc.VP8PayloadType, localSSRC)
	if err != nil || s.mxLeg.LocalVideo == nil {
		s.end(signalCallEndFailed, true)
		return
	}
	go func() {
		track, trackErr := s.mxLeg.RemoteVideoTrack(s.ctx)
		if trackErr == nil {
			s.lock.Lock()
			s.matrixVideoSSRC = uint32(track.SSRC())
			pendingKeyframe := s.pendingKeyframe
			s.pendingKeyframe = false
			s.lock.Unlock()
			if pendingKeyframe {
				s.mxLeg.RequestKeyframe(track.SSRC())
			}
			trackErr = callbridge.RelayVideo(s.ctx, track, uint8(track.PayloadType()), writer, &callbridge.RelayStats{}, s.log)
		}
		if trackErr != nil && s.ctx.Err() == nil {
			s.log.Warn().Err(trackErr).Msg("Matrix to Signal video relay stopped")
			s.end(signalCallEndFailed, true)
		}
	}()
	go func() {
		reader, trackErr := s.signalLeg.RTPReader(remoteSSRC)
		if trackErr == nil {
			trackErr = callbridge.RelayVideo(s.ctx, reader, ringrtc.VP8PayloadType, s.mxLeg.LocalVideo, &callbridge.RelayStats{}, s.log)
		}
		if trackErr != nil && s.ctx.Err() == nil {
			s.log.Warn().Err(trackErr).Msg("Signal to Matrix video relay stopped")
			s.end(signalCallEndFailed, true)
		}
	}()
}

func (s *signalCallSession) startRTCPRelay() {
	localVideoSSRC, remoteVideoSSRC := uint32(ringrtc.CallerVideoSSRC), uint32(ringrtc.CalleeVideoSSRC)
	if s.incoming {
		localVideoSSRC, remoteVideoSSRC = ringrtc.CalleeVideoSSRC, ringrtc.CallerVideoSSRC
	}
	if s.callType == events.CallTypeVideo {
		s.mxLeg.OnKeyframeRequest(func() {
			_ = s.signalLeg.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{SenderSSRC: localVideoSSRC, MediaSSRC: remoteVideoSSRC}})
		})
	}
	go func() {
		err := s.signalLeg.HandleRTCP(s.ctx, func(packets []rtcp.Packet) {
			if s.callType != events.CallTypeVideo {
				return
			}
			for _, packet := range packets {
				switch packet.(type) {
				case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
					// RingRTC asks for a keyframe from the Matrix video sender.
					go s.requestMatrixKeyframe()
				}
			}
		})
		if err != nil && s.ctx.Err() == nil {
			s.log.Debug().Msg("Signal RTCP reader stopped")
		}
	}()
}

func (s *signalCallSession) requestMatrixKeyframe() {
	s.lock.Lock()
	ssrc := s.matrixVideoSSRC
	if ssrc == 0 {
		s.pendingKeyframe = true
	}
	s.lock.Unlock()
	if ssrc != 0 {
		s.mxLeg.RequestKeyframe(webrtc.SSRC(ssrc))
	}
}

func (cb *signalCallBridge) handleRemoteEnd(evt *events.Call) {
	cb.lock.Lock()
	session := cb.active
	cb.lock.Unlock()
	if session == nil || session.signalID != evt.ID || session.peer.UUID != evt.Info.Sender {
		return
	}
	reason := signalCallEndRemoteHangup
	if evt.MessageType == events.CallMessageBusy || evt.HangupType == signalpb.CallMessage_Hangup_HANGUP_BUSY || evt.HangupType == signalpb.CallMessage_Hangup_HANGUP_DECLINED {
		reason = signalCallEndRemoteBusy
	} else if session.incoming && evt.HangupType == signalpb.CallMessage_Hangup_HANGUP_ACCEPTED {
		reason = signalCallEndAnsweredElsewhere
	} else if !session.incoming && evt.HangupType == signalpb.CallMessage_Hangup_HANGUP_ACCEPTED {
		return
	}
	go session.end(reason, false)
}

type signalCallEnd int

const (
	signalCallEndRemoteHangup signalCallEnd = iota
	signalCallEndRemoteBusy
	signalCallEndAnsweredElsewhere
	signalCallEndLocalDecline
	signalCallEndTimeout
	signalCallEndFailed
	signalCallEndBridgeShutdown
)

func (cb *signalCallBridge) stop() {
	cb.lock.Lock()
	session := cb.active
	cb.lock.Unlock()
	if session != nil {
		session.end(signalCallEndBridgeShutdown, false)
	}
}

func (s *signalCallSession) end(reason signalCallEnd, notifySignal bool) {
	s.endOnce.Do(func() {
		s.log.Info().Int("end_reason", int(reason)).Msg("Ending Signal ringing session")
		sendCtx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), 5*time.Second)
		defer cancel()
		if notifySignal {
			if err := s.bridge.sendHangup(sendCtx, s.peer, s.signalID); err != nil {
				s.log.Warn().Err(err).Msg("Failed to send Signal hangup")
			}
		}
		if reason != signalCallEndLocalDecline {
			matrixReason := event.CallHangupUserHangup
			switch reason {
			case signalCallEndRemoteBusy:
				matrixReason = "user_busy"
			case signalCallEndAnsweredElsewhere:
				matrixReason = "answered_elsewhere"
			case signalCallEndTimeout:
				matrixReason = event.CallHangupInviteTimeout
			case signalCallEndFailed, signalCallEndBridgeShutdown:
				matrixReason = event.CallHangupUnknownError
			}
			_, err := s.ghost.SendMessage(sendCtx, s.portal.MXID, event.CallHangup, signalCallEventContent(&event.CallHangupEventContent{
				BaseCallEventContent: s.baseMatrixContent(), Reason: matrixReason,
			}), nil)
			if err != nil {
				s.log.Warn().Err(err).Msg("Failed to send Matrix call hangup")
			}
		}
		s.cancel()
		s.lock.Lock()
		mxLeg := s.mxLeg
		signalLeg := s.signalLeg
		s.lock.Unlock()
		if mxLeg != nil {
			mxLeg.Close()
		}
		if signalLeg != nil {
			_ = signalLeg.Close()
		}
		s.bridge.lock.Lock()
		if s.bridge.active == s {
			s.bridge.active = nil
		}
		s.bridge.lock.Unlock()
	})
}

func (cb *signalCallBridge) sendHangup(ctx context.Context, peer libsignalgo.ServiceID, callID uint64) error {
	// RingRTC core/call_manager.rs handle_hangup sends Hangup::Normal for a
	// local hangup. core/signaling.rs documents hangups as broadcast, so no
	// destinationDeviceId is set.
	return cb.sendCallMessage(ctx, peer, signalHangupMessage(callID))
}

func (cb *signalCallBridge) sendBusy(ctx context.Context, peer libsignalgo.ServiceID, callID uint64) error {
	return cb.sendCallMessage(ctx, peer, signalBusyMessage(callID))
}

func signalHangupMessage(callID uint64) *signalpb.CallMessage {
	hangupType := signalpb.CallMessage_Hangup_HANGUP_NORMAL
	return &signalpb.CallMessage{Hangup: &signalpb.CallMessage_Hangup{Id: ptr.Ptr(callID), Type: &hangupType}}
}

func signalBusyMessage(callID uint64) *signalpb.CallMessage {
	return &signalpb.CallMessage{Busy: &signalpb.CallMessage_Busy{Id: ptr.Ptr(callID)}}
}

func signalAcceptedMessage(callID uint64, winningDevice uint32) *signalpb.CallMessage {
	hangupType := signalpb.CallMessage_Hangup_HANGUP_ACCEPTED
	return &signalpb.CallMessage{Hangup: &signalpb.CallMessage_Hangup{
		Id: ptr.Ptr(callID), Type: &hangupType, DeviceId: ptr.Ptr(winningDevice),
	}}
}

func (cb *signalCallBridge) sendCallMessage(ctx context.Context, peer libsignalgo.ServiceID, message *signalpb.CallMessage) error {
	result := cb.client.Client.SendMessage(ctx, peer, &signalpb.Content{
		Content: &signalpb.Content_CallMessage{CallMessage: message},
	})
	if !result.WasSuccessful {
		return result.Error
	}
	return nil
}
