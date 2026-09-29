// mautrix-signal - A Matrix-Signal puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package ringrtc

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/srtp/v3"
	"github.com/pion/stun/v4"
)

const (
	// RingRTC's rffi/src/constants.h fixes Opus to payload type 102.
	OpusPayloadType = 102
	VP8PayloadType  = 108
	VP9PayloadType  = 109
	CallerAudioSSRC = 1002
	CallerVideoSSRC = 1003
	CalleeAudioSSRC = 2002
	CalleeVideoSSRC = 2003
)

type IncomingMediaConfig struct {
	Remote            *ConnectionParametersV4
	CallerIdentityKey []byte
	CalleeIdentityKey []byte
	ICEServers        []*stun.URI
	VideoCodec        VideoCodecType
	OnCandidate       func(string)
	// IncludeLoopback offers 127.0.0.1 as an ICE candidate; see OutgoingMediaConfig.
	IncludeLoopback bool
}

type OutgoingMediaConfig struct {
	CallerIdentityKey []byte
	CalleeIdentityKey []byte
	ICEServers        []*stun.URI
	VideoCodec        VideoCodecType
	OnCandidate       func(string)
	// IncludeLoopback offers 127.0.0.1 as an ICE candidate. Only tests set it, so that two legs
	// can find each other on a machine with no usable interface; a real call would never want a
	// candidate the peer cannot reach.
	IncludeLoopback bool
}

// MediaLeg is the controlled ICE + static-key SRTP side of a RingRTC call.
// RingRTC disables DTLS and installs the negotiated
// AEAD_AES_256_GCM keys directly (core/connection.rs negotiate_srtp_keys), so
// this intentionally does not use pion/webrtc PeerConnection.
type MediaLeg struct {
	Agent      *ice.Agent
	Parameters *ConnectionParametersV4

	remote                                              *ConnectionParametersV4
	dial                                                bool
	localKey, remoteKey                                 SRTPKey
	privateSecret, callerIdentityKey, calleeIdentityKey []byte

	lock  sync.Mutex
	conn  net.Conn
	mux   *mediaPacketMux
	SRTP  *srtp.SessionSRTP
	SRTCP *srtp.SessionSRTCP
}

type RTPReader struct {
	stream *srtp.ReadStreamSRTP
}

type RTPWriter struct {
	stream      *srtp.WriteStreamSRTP
	payloadType uint8
	ssrc        uint32
}

func NewIncomingMediaLeg(cfg IncomingMediaConfig) (*MediaLeg, error) {
	if cfg.Remote == nil || len(cfg.Remote.PublicKey) != 32 || cfg.Remote.ICEUfrag == "" || cfg.Remote.ICEPwd == "" {
		return nil, errors.New("incomplete remote RingRTC connection parameters")
	}
	leg, secret, err := newMediaLeg(cfg.ICEServers, cfg.VideoCodec, cfg.OnCandidate, cfg.IncludeLoopback)
	if err != nil {
		return nil, err
	}
	keys, err := NegotiateSRTPKeys(secret, cfg.Remote.PublicKey, cfg.CallerIdentityKey, cfg.CalleeIdentityKey)
	clear(secret)
	if err != nil {
		_ = leg.Agent.Close()
		return nil, err
	}
	leg.remote = cfg.Remote
	leg.localKey, leg.remoteKey = keys.Answer, keys.Offer
	return leg, nil
}

func NewOutgoingMediaLeg(cfg OutgoingMediaConfig) (*MediaLeg, error) {
	leg, secret, err := newMediaLeg(cfg.ICEServers, cfg.VideoCodec, cfg.OnCandidate, cfg.IncludeLoopback)
	if err != nil {
		return nil, err
	}
	leg.dial = true
	leg.privateSecret = secret
	leg.callerIdentityKey = append([]byte(nil), cfg.CallerIdentityKey...)
	leg.calleeIdentityKey = append([]byte(nil), cfg.CalleeIdentityKey...)
	return leg, nil
}

func newMediaLeg(iceServers []*stun.URI, videoCodec VideoCodecType, onCandidate func(string), includeLoopback bool) (*MediaLeg, []byte, error) {
	privateKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate RingRTC media key: %w", err)
	}
	agentCfg := &ice.AgentConfig{
		Urls: iceServers,
		// Without this the agent gathers nothing at all - pion treats an empty NetworkTypes as
		// "no networks" rather than "every network", so every candidate list came back empty and
		// no call could ever connect.
		NetworkTypes: []ice.NetworkType{
			ice.NetworkTypeUDP4, ice.NetworkTypeUDP6,
		},
	}
	if includeLoopback {
		agentCfg.IncludeLoopback = true
	}
	agent, err := ice.NewAgent(agentCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("create RingRTC ICE agent: %w", err)
	}
	ufrag, pwd, err := agent.GetLocalUserCredentials()
	if err != nil {
		_ = agent.Close()
		return nil, nil, fmt.Errorf("get RingRTC ICE credentials: %w", err)
	}
	params := &ConnectionParametersV4{
		PublicKey: privateKey.PublicKey().Bytes(), ICEUfrag: ufrag, ICEPwd: pwd,
		// RingRTC DataMode::Normal advertises a local receive ceiling of 2 Mbps.
		MaxBitrateBPS: 2_000_000,
	}
	if videoCodec != 0 {
		codec := VideoCodec{Type: videoCodec}
		params.ReceiveVideoCodecs = []VideoCodec{codec}
		params.EncodeOnlyVideoCodecs = []VideoCodec{codec}
		params.DecodeOnlyVideoCodecs = []VideoCodec{codec}
	}
	leg := &MediaLeg{Agent: agent, Parameters: params}
	if err = agent.OnCandidate(func(candidate ice.Candidate) {
		if candidate != nil && onCandidate != nil {
			// RingRTC IceCandidate::from_v3_sdp forwards WebRTC's candidate
			// string verbatim; pion's Marshal omits the candidate: prefix.
			onCandidate("candidate:" + candidate.Marshal())
		}
	}); err != nil {
		_ = agent.Close()
		return nil, nil, fmt.Errorf("set RingRTC ICE candidate handler: %w", err)
	}
	return leg, privateKey.Bytes(), nil
}

func (l *MediaLeg) SetRemoteAnswer(remote *ConnectionParametersV4) error {
	if remote == nil || len(remote.PublicKey) != 32 || remote.ICEUfrag == "" || remote.ICEPwd == "" {
		return errors.New("incomplete remote RingRTC connection parameters")
	}
	l.lock.Lock()
	if !l.dial || len(l.privateSecret) == 0 || l.remote != nil {
		l.lock.Unlock()
		return errors.New("RingRTC media leg cannot accept an answer")
	}
	secret := l.privateSecret
	callerIdentityKey := l.callerIdentityKey
	calleeIdentityKey := l.calleeIdentityKey
	l.privateSecret, l.callerIdentityKey, l.calleeIdentityKey = nil, nil, nil
	l.lock.Unlock()
	keys, err := NegotiateSRTPKeys(secret, remote.PublicKey, callerIdentityKey, calleeIdentityKey)
	clear(secret)
	clear(callerIdentityKey)
	clear(calleeIdentityKey)
	if err != nil {
		return err
	}
	l.lock.Lock()
	l.remote = remote
	l.localKey, l.remoteKey = keys.Offer, keys.Answer
	l.lock.Unlock()
	return nil
}

func ParseICEServerURLs(urls []string, username, password string) ([]*stun.URI, error) {
	parsed := make([]*stun.URI, 0, len(urls))
	for i, raw := range urls {
		uri, err := stun.ParseURI(raw)
		if err != nil {
			return nil, fmt.Errorf("parse ICE server URL %d: %w", i, err)
		}
		uri.Username = username
		uri.Password = password
		parsed = append(parsed, uri)
	}
	return parsed, nil
}

func (l *MediaLeg) GatherCandidates() error {
	return l.Agent.GatherCandidates()
}

func (l *MediaLeg) AddRemoteCandidate(sdp string) error {
	sdp = strings.TrimPrefix(strings.TrimSpace(sdp), "a=")
	candidate, err := ice.UnmarshalCandidate(sdp)
	if err != nil {
		return fmt.Errorf("decode RingRTC ICE candidate: %w", err)
	}
	return l.Agent.AddRemoteCandidate(candidate)
}

func (l *MediaLeg) Connect(ctx context.Context) error {
	l.lock.Lock()
	remote, dial := l.remote, l.dial
	localKey, remoteKey := l.localKey, l.remoteKey
	l.lock.Unlock()
	if remote == nil || len(localKey.Key) == 0 || len(remoteKey.Key) == 0 {
		return errors.New("RingRTC media leg has no remote parameters")
	}
	var conn *ice.Conn
	var err error
	if dial {
		conn, err = l.Agent.Dial(ctx, remote.ICEUfrag, remote.ICEPwd)
	} else {
		conn, err = l.Agent.Accept(ctx, remote.ICEUfrag, remote.ICEPwd)
	}
	if err != nil {
		return fmt.Errorf("connect RingRTC ICE: %w", err)
	}
	mux := newMediaPacketMux(conn)
	keys := srtp.SessionKeys{
		LocalMasterKey: localKey.Key, LocalMasterSalt: localKey.Salt,
		RemoteMasterKey: remoteKey.Key, RemoteMasterSalt: remoteKey.Salt,
	}
	srtpSession, err := srtp.NewSessionSRTP(mux.rtp, &srtp.Config{Keys: keys, Profile: srtp.ProtectionProfileAeadAes256Gcm})
	if err != nil {
		_ = mux.Close()
		return fmt.Errorf("start RingRTC SRTP: %w", err)
	}
	remoteAudioSSRC, remoteVideoSSRC := uint32(CallerAudioSSRC), uint32(CallerVideoSSRC)
	if dial {
		remoteAudioSSRC, remoteVideoSSRC = CalleeAudioSSRC, CalleeVideoSSRC
	}
	for _, ssrc := range []uint32{RTPDataSSRC, remoteAudioSSRC, remoteVideoSSRC} {
		if _, err = srtpSession.OpenReadStream(ssrc); err != nil {
			_ = srtpSession.Close()
			_ = mux.Close()
			return fmt.Errorf("prepare RingRTC SRTP stream: %w", err)
		}
	}
	srtcpSession, err := srtp.NewSessionSRTCP(mux.rtcp, &srtp.Config{Keys: keys, Profile: srtp.ProtectionProfileAeadAes256Gcm})
	if err != nil {
		_ = srtpSession.Close()
		_ = mux.Close()
		return fmt.Errorf("start RingRTC SRTCP: %w", err)
	}
	l.lock.Lock()
	l.conn, l.mux, l.SRTP, l.SRTCP = conn, mux, srtpSession, srtcpSession
	l.lock.Unlock()
	return nil
}

func (l *MediaLeg) AcceptRTP() (*RTPReader, uint32, error) {
	l.lock.Lock()
	session := l.SRTP
	l.lock.Unlock()
	if session == nil {
		return nil, 0, errors.New("RingRTC SRTP session is not connected")
	}
	stream, ssrc, err := session.AcceptStream()
	if err != nil {
		return nil, 0, err
	}
	return &RTPReader{stream: stream}, ssrc, nil
}

// RTPReader opens the fixed RingRTC 1:1 stream identified by ssrc without
// waiting for it to produce its first packet. RingRTC assigns 1002/1003 to
// the caller's audio/video streams and 2002/2003 to the callee's streams.
func (l *MediaLeg) RTPReader(ssrc uint32) (*RTPReader, error) {
	l.lock.Lock()
	session := l.SRTP
	l.lock.Unlock()
	if session == nil {
		return nil, errors.New("RingRTC SRTP session is not connected")
	}
	stream, err := session.OpenReadStream(ssrc)
	if err != nil {
		return nil, err
	}
	return &RTPReader{stream: stream}, nil
}

func (l *MediaLeg) WriteRTCP(packets []rtcp.Packet) error {
	l.lock.Lock()
	session := l.SRTCP
	l.lock.Unlock()
	if session == nil {
		return errors.New("RingRTC SRTCP session is not connected")
	}
	raw, err := rtcp.Marshal(packets)
	if err != nil {
		return err
	}
	stream, err := session.OpenWriteStream()
	if err != nil {
		return err
	}
	_, err = stream.Write(raw)
	return err
}

// HandleRTCP drains all RingRTC SRTCP streams until the leg closes. Each RTCP
// sender gets a Pion read stream, so accepted streams are drained concurrently.
func (l *MediaLeg) HandleRTCP(ctx context.Context, handle func([]rtcp.Packet)) error {
	l.lock.Lock()
	session := l.SRTCP
	l.lock.Unlock()
	if session == nil {
		return errors.New("RingRTC SRTCP session is not connected")
	}
	for {
		stream, _, err := session.AcceptStream()
		if err != nil {
			return err
		}
		go func() {
			buf := make([]byte, 8192)
			for ctx.Err() == nil {
				n, readErr := stream.Read(buf)
				if readErr != nil {
					return
				}
				packets, unmarshalErr := rtcp.Unmarshal(buf[:n])
				if unmarshalErr == nil {
					handle(packets)
				}
			}
		}()
	}
}

func (l *MediaLeg) RTPWriter(payloadType uint8, ssrc uint32) (*RTPWriter, error) {
	l.lock.Lock()
	session := l.SRTP
	l.lock.Unlock()
	if session == nil {
		return nil, errors.New("RingRTC SRTP session is not connected")
	}
	stream, err := session.OpenWriteStream()
	if err != nil {
		return nil, err
	}
	return &RTPWriter{stream: stream, payloadType: payloadType, ssrc: ssrc}, nil
}

func (r *RTPReader) ReadRTP() (*rtp.Packet, interceptor.Attributes, error) {
	buf := make([]byte, 8192)
	n, _, err := r.stream.ReadRTP(buf)
	if err != nil {
		return nil, nil, err
	}
	packet := &rtp.Packet{}
	if err = packet.Unmarshal(buf[:n]); err != nil {
		return nil, nil, err
	}
	return packet, nil, nil
}

func (w *RTPWriter) WriteRTP(packet *rtp.Packet) error {
	header := packet.Header.Clone()
	header.PayloadType = w.payloadType
	header.SSRC = w.ssrc
	_, err := w.stream.WriteRTP(&header, packet.Payload)
	return err
}

func (l *MediaLeg) Close() error {
	l.lock.Lock()
	srtpSession, srtcpSession, mux := l.SRTP, l.SRTCP, l.mux
	secret, callerIdentityKey, calleeIdentityKey := l.privateSecret, l.callerIdentityKey, l.calleeIdentityKey
	l.SRTP, l.SRTCP, l.mux, l.conn = nil, nil, nil, nil
	l.privateSecret, l.callerIdentityKey, l.calleeIdentityKey = nil, nil, nil
	l.lock.Unlock()
	clear(secret)
	clear(callerIdentityKey)
	clear(calleeIdentityKey)
	if srtpSession != nil {
		_ = srtpSession.Close()
	}
	if srtcpSession != nil {
		_ = srtcpSession.Close()
	}
	if mux != nil {
		_ = mux.Close()
	}
	return l.Agent.Close()
}

// mediaPacketMux splits RTP and RTCP sharing one ICE component as required by
// RingRTC's rtcp-mux session descriptions. The matcher is RFC 5761 section 4.
type mediaPacketMux struct {
	conn      net.Conn
	rtp, rtcp *mediaPacketConn
	writeLock sync.Mutex
	closeOnce sync.Once
	done      chan struct{}
}

type mediaPacketConn struct {
	parent *mediaPacketMux
	read   chan []byte
}

func newMediaPacketMux(conn net.Conn) *mediaPacketMux {
	mux := &mediaPacketMux{conn: conn, done: make(chan struct{})}
	mux.rtp = &mediaPacketConn{parent: mux, read: make(chan []byte, 32)}
	mux.rtcp = &mediaPacketConn{parent: mux, read: make(chan []byte, 8)}
	go mux.readLoop()
	return mux
}

func (m *mediaPacketMux) readLoop() {
	defer close(m.done)
	for {
		buf := make([]byte, 8192)
		n, err := m.conn.Read(buf)
		if err != nil {
			return
		}
		packet := buf[:n]
		dst := m.rtp.read
		if len(packet) >= 2 && packet[1] >= 192 && packet[1] <= 223 {
			dst = m.rtcp.read
		}
		select {
		case dst <- packet:
		case <-m.done:
			return
		}
	}
}

func (m *mediaPacketMux) Close() error {
	var err error
	m.closeOnce.Do(func() { err = m.conn.Close() })
	return err
}

func (c *mediaPacketConn) Read(buf []byte) (int, error) {
	select {
	case packet := <-c.read:
		if len(packet) > len(buf) {
			return 0, io.ErrShortBuffer
		}
		return copy(buf, packet), nil
	case <-c.parent.done:
		return 0, io.EOF
	}
}

func (c *mediaPacketConn) Write(buf []byte) (int, error) {
	c.parent.writeLock.Lock()
	defer c.parent.writeLock.Unlock()
	return c.parent.conn.Write(buf)
}

func (c *mediaPacketConn) Close() error                      { return c.parent.Close() }
func (c *mediaPacketConn) LocalAddr() net.Addr               { return c.parent.conn.LocalAddr() }
func (c *mediaPacketConn) RemoteAddr() net.Addr              { return c.parent.conn.RemoteAddr() }
func (c *mediaPacketConn) SetDeadline(t time.Time) error     { return c.parent.conn.SetDeadline(t) }
func (c *mediaPacketConn) SetReadDeadline(t time.Time) error { return c.parent.conn.SetReadDeadline(t) }
func (c *mediaPacketConn) SetWriteDeadline(t time.Time) error {
	return c.parent.conn.SetWriteDeadline(t)
}
