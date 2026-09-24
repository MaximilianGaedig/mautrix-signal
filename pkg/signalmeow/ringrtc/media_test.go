package ringrtc

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/srtp/v3"
)

func TestMediaPacketMuxAndStaticSRTP(t *testing.T) {
	connA, connB := net.Pipe()
	muxA, muxB := newMediaPacketMux(connA), newMediaPacketMux(connB)
	defer muxA.Close()
	defer muxB.Close()

	keyA := bytes.Repeat([]byte{0x11}, SRTPKeySize)
	saltA := bytes.Repeat([]byte{0x12}, SRTPSaltSize)
	keyB := bytes.Repeat([]byte{0x21}, SRTPKeySize)
	saltB := bytes.Repeat([]byte{0x22}, SRTPSaltSize)
	configA := &srtp.Config{Profile: srtp.ProtectionProfileAeadAes256Gcm, Keys: srtp.SessionKeys{
		LocalMasterKey: keyA, LocalMasterSalt: saltA, RemoteMasterKey: keyB, RemoteMasterSalt: saltB,
	}}
	configB := &srtp.Config{Profile: srtp.ProtectionProfileAeadAes256Gcm, Keys: srtp.SessionKeys{
		LocalMasterKey: keyB, LocalMasterSalt: saltB, RemoteMasterKey: keyA, RemoteMasterSalt: saltA,
	}}
	sessionA, err := srtp.NewSessionSRTP(muxA.rtp, configA)
	if err != nil {
		t.Fatal(err)
	}
	defer sessionA.Close()
	sessionB, err := srtp.NewSessionSRTP(muxB.rtp, configB)
	if err != nil {
		t.Fatal(err)
	}
	defer sessionB.Close()

	writer, err := sessionA.OpenWriteStream()
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("opus-frame")
	if _, err = writer.WriteRTP(&rtp.Header{Version: 2, PayloadType: OpusPayloadType, SequenceNumber: 7, Timestamp: 960, SSRC: 2002}, payload); err != nil {
		t.Fatal(err)
	}
	reader, ssrc, err := sessionB.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	if ssrc != 2002 {
		t.Fatalf("unexpected SSRC %d", ssrc)
	}
	buf := make([]byte, 1500)
	_ = reader.SetReadDeadline(time.Now().Add(time.Second))
	n, header, err := reader.ReadRTP(buf)
	if err != nil {
		t.Fatal(err)
	}
	packet := &rtp.Packet{}
	if err = packet.Unmarshal(buf[:n]); err != nil {
		t.Fatal(err)
	}
	if header.PayloadType != OpusPayloadType || !bytes.Equal(packet.Payload, payload) {
		t.Fatalf("unexpected decrypted RTP: pt=%d payload=%x", header.PayloadType, packet.Payload)
	}

	rtcpA, err := srtp.NewSessionSRTCP(muxA.rtcp, configA)
	if err != nil {
		t.Fatal(err)
	}
	defer rtcpA.Close()
	rtcpB, err := srtp.NewSessionSRTCP(muxB.rtcp, configB)
	if err != nil {
		t.Fatal(err)
	}
	defer rtcpB.Close()
	rtcpWriter, err := rtcpA.OpenWriteStream()
	if err != nil {
		t.Fatal(err)
	}
	wantRTCP := &rtcp.PictureLossIndication{SenderSSRC: CalleeVideoSSRC, MediaSSRC: CallerVideoSSRC}
	rawRTCP, err := wantRTCP.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rtcpWriter.Write(rawRTCP); err != nil {
		t.Fatal(err)
	}
	rtcpReader, _, err := rtcpB.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = rtcpReader.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err = rtcpReader.ReadRTCP(buf)
	if err != nil {
		t.Fatal(err)
	}
	packets, err := rtcp.Unmarshal(buf[:n])
	if err != nil || len(packets) != 1 {
		t.Fatalf("unexpected decrypted RTCP: packets=%d err=%v", len(packets), err)
	}
	gotPLI, ok := packets[0].(*rtcp.PictureLossIndication)
	if !ok || gotPLI.MediaSSRC != CallerVideoSSRC {
		t.Fatalf("unexpected decrypted RTCP packet: %+v", packets[0])
	}
}
