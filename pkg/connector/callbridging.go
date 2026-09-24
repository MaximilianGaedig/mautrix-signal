// mautrix-signal - A Matrix-Signal puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package connector

// Signal call bridging: ring Matrix for incoming 1:1 audio/video calls and
// bridge audio calls over RingRTC's ICE/static-SRTP media transport.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
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

	portal       *bridgev2.Portal
	ghost        bridgev2.MatrixAPI
	peer         libsignalgo.ServiceID
	signalID     uint64
	sourceDevice uint32
	callType     events.CallType
	videoCodec   string
	offer        *ringrtc.Offer
	mxCallID     string
	mxParty      string
	mxLeg        *callbridge.Leg
	signalLeg    *ringrtc.IncomingMediaLeg
	pendingICE   []string
	answering    bool

	endOnce sync.Once
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
	session := &signalCallSession{
		bridge: cb, ctx: sessionCtx, cancel: cancel,
		portal: portal, ghost: ghost.Intent, peer: peer, signalID: evt.ID, sourceDevice: evt.Info.SourceDeviceID,
		callType: evt.Type, videoCodec: signalVideoCodec(evt), offer: evt.Offer,
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
	if evt == nil || evt.Type != events.CallTypeVideo || evt.Offer == nil || evt.Offer.V4 == nil {
		return ""
	}
	params := evt.Offer.V4
	encode := params.EncodeOnlyVideoCodecs
	decode := params.DecodeOnlyVideoCodecs
	if len(encode) == 0 || len(decode) == 0 {
		encode = params.ReceiveVideoCodecs
		decode = params.ReceiveVideoCodecs
	}
	for _, codec := range encode {
		if !hasSignalVideoCodec(decode, codec.Type) {
			continue
		}
		switch codec.Type {
		case ringrtc.VideoCodecVP8:
			return webrtc.MimeTypeVP8
		case ringrtc.VideoCodecH264ConstrainedBaseline, ringrtc.VideoCodecH264ConstrainedHigh:
			return webrtc.MimeTypeH264
		}
	}
	return ""
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
			cb.rejectUnsupportedOutgoing(ctx, portal, inv)
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
		session.log.Info().Str("event_type", evt.Type.Type).Msg("Matrix user declined Signal call")
		go session.end(signalCallEndLocalDecline, true)
	case event.CallAnswer:
		answer, ok := parseSignalCallContent[event.CallAnswerEventContent](evt)
		if !ok || answer.Answer.SDP == "" {
			go session.end(signalCallEndFailed, true)
			return
		}
		if session.callType == events.CallTypeVideo {
			session.log.Warn().Msg("Matrix answered Signal video call before the video relay stage")
			go session.end(signalCallEndUnsupportedAnswer, true)
			return
		}
		go session.answerIncomingAudio(answer.Answer.SDP, answer.PartyID)
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

func (cb *signalCallBridge) handleRemoteICE(evt *events.Call) {
	cb.lock.Lock()
	session := cb.active
	cb.lock.Unlock()
	if session == nil || session.signalID != evt.ID || session.peer.UUID != evt.Info.Sender || evt.ICECandidate.AddedV3 == nil {
		return
	}
	session.lock.Lock()
	leg := session.signalLeg
	if leg == nil {
		session.pendingICE = append(session.pendingICE, evt.ICECandidate.AddedV3.SDP)
		session.lock.Unlock()
		return
	}
	session.lock.Unlock()
	if err := leg.AddRemoteCandidate(evt.ICECandidate.AddedV3.SDP); err != nil {
		// Candidate errors can contain addresses, so do not attach the error.
		session.log.Debug().Msg("Rejected malformed remote Signal ICE candidate")
	}
}

func (s *signalCallSession) answerIncomingAudio(matrixAnswer, matrixParty string) {
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
	localIdentity, err := s.bridge.client.Client.Store.ACIIdentityKeyPair.GetPublicKey().Serialize()
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to serialize local Signal identity key")
		s.end(signalCallEndFailed, true)
		return
	}
	peerIdentity, err := s.bridge.client.Client.Store.IdentityKeyStore.GetIdentityKey(s.ctx, s.peer)
	if err != nil || peerIdentity == nil {
		s.log.Error().Err(err).Msg("Failed to load Signal caller identity key")
		s.end(signalCallEndFailed, true)
		return
	}
	peerIdentityBytes, err := peerIdentity.Serialize()
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to serialize Signal caller identity key")
		s.end(signalCallEndFailed, true)
		return
	}
	signalLeg, err := ringrtc.NewIncomingMediaLeg(ringrtc.IncomingMediaConfig{
		Remote: s.offer.V4, CallerIdentityKey: peerIdentityBytes, CalleeIdentityKey: localIdentity,
		ICEServers: iceServers,
		OnCandidate: func(candidate string) {
			if s.ctx.Err() == nil {
				if sendErr := s.sendSignalICE(candidate); sendErr != nil {
					s.log.Warn().Err(sendErr).Msg("Failed to send local Signal ICE candidate")
				}
			}
		},
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
		if err = signalLeg.AddRemoteCandidate(candidate); err != nil {
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
	s.log.Info().Msg("Signal and Matrix audio legs connected")
	s.startAudioRelays()
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
	return s.bridge.sendCallMessage(s.ctx, s.peer, signalICEMessage(s.signalID, s.sourceDevice, candidate))
}

func signalAnswerMessage(callID uint64, destinationDevice uint32, params *ringrtc.ConnectionParametersV4) *signalpb.CallMessage {
	answer := &signalpb.CallMessage_Answer{Id: ptr.Ptr(callID), Opaque: ringrtc.EncodeAnswer(&ringrtc.Answer{V4: params})}
	return &signalpb.CallMessage{Answer: answer, DestinationDeviceId: ptr.Ptr(destinationDevice)}
}

func signalICEMessage(callID uint64, destinationDevice uint32, candidate string) *signalpb.CallMessage {
	iceUpdate := &signalpb.CallMessage_IceUpdate{
		Id:     ptr.Ptr(callID),
		Opaque: ringrtc.EncodeIceCandidate(&ringrtc.IceCandidate{AddedV3: &ringrtc.IceCandidateV3{SDP: candidate}}),
	}
	return &signalpb.CallMessage{IceUpdate: []*signalpb.CallMessage_IceUpdate{iceUpdate}, DestinationDeviceId: ptr.Ptr(destinationDevice)}
}

func (s *signalCallSession) startAudioRelays() {
	writer, err := s.signalLeg.RTPWriter(ringrtc.OpusPayloadType, 2002)
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
		reader, _, trackErr := s.signalLeg.AcceptRTP()
		if trackErr == nil {
			trackErr = callbridge.Relay(s.ctx, reader, ringrtc.OpusPayloadType, s.mxLeg.Local, &callbridge.RelayStats{}, s.log)
		}
		if trackErr != nil && s.ctx.Err() == nil {
			s.log.Warn().Err(trackErr).Msg("Signal to Matrix audio relay stopped")
			s.end(signalCallEndFailed, true)
		}
	}()
}

func (cb *signalCallBridge) rejectUnsupportedOutgoing(ctx context.Context, portal *bridgev2.Portal, inv *event.CallInviteEventContent) {
	if portal.OtherUserID == "" {
		return
	}
	ghost, err := cb.client.Main.Bridge.GetGhostByID(ctx, portal.OtherUserID)
	if err != nil || ghost == nil {
		return
	}
	_, _ = ghost.Intent.SendMessage(ctx, portal.MXID, event.CallHangup, signalCallEventContent(&event.CallHangupEventContent{
		BaseCallEventContent: event.BaseCallEventContent{CallID: inv.CallID, PartyID: "bridge-unsupported", Version: "1"},
		Reason:               event.CallHangupUnknownError,
	}), nil)
	zerolog.Ctx(ctx).Info().Msg("Outgoing Signal calls aren't enabled in the ringing-only stage")
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
	} else if evt.HangupType == signalpb.CallMessage_Hangup_HANGUP_ACCEPTED {
		reason = signalCallEndAnsweredElsewhere
	}
	go session.end(reason, false)
}

type signalCallEnd int

const (
	signalCallEndRemoteHangup signalCallEnd = iota
	signalCallEndRemoteBusy
	signalCallEndAnsweredElsewhere
	signalCallEndLocalDecline
	signalCallEndUnsupportedAnswer
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
			case signalCallEndUnsupportedAnswer, signalCallEndFailed, signalCallEndBridgeShutdown:
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

func (cb *signalCallBridge) sendCallMessage(ctx context.Context, peer libsignalgo.ServiceID, message *signalpb.CallMessage) error {
	result := cb.client.Client.SendMessage(ctx, peer, &signalpb.Content{
		Content: &signalpb.Content_CallMessage{CallMessage: message},
	})
	if !result.WasSuccessful {
		return result.Error
	}
	return nil
}
