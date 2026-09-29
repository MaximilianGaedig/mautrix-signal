// mautrix-signal - A Matrix-Signal puppeting bridge.
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

package ringrtc

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
)

/*
 * A whole Signal call, both ends of it, with no Signal.
 *
 * The other tests here check a piece at a time: the key orientation, the packet mux, the SRTP
 * profile. None of them answer the question the bridge actually depends on - whether a caller leg
 * and a callee leg, set up the way the bridge sets them up, agree well enough that audio written
 * into one comes out of the other.
 *
 * Everything real is exercised: the X25519 secrets, the HKDF key derivation with its caller/callee
 * orientation, ICE over loopback, and SRTP. What is simulated is only the signalling that would
 * have carried the connection parameters between two phones, which is a Signal message and nothing
 * more.
 */
func TestACallerAndCalleeLegCarryAudio(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Two identity keys, as two Signal accounts would have.
	callerIdentity := make([]byte, 32)
	calleeIdentity := make([]byte, 32)
	if _, err := rand.Read(callerIdentity); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(calleeIdentity); err != nil {
		t.Fatal(err)
	}

	// The agent calls back on its own goroutine, so the slices need a lock: reading them unguarded
	// is a data race, and the reader may simply never observe the appends.
	var candLock sync.Mutex
	var callerCandidates, calleeCandidates []string
	collect := func(into *[]string) func(string) {
		return func(c string) {
			candLock.Lock()
			*into = append(*into, c)
			candLock.Unlock()
		}
	}
	snapshot := func(from *[]string) []string {
		candLock.Lock()
		defer candLock.Unlock()
		return append([]string(nil), (*from)...)
	}
	caller, err := NewOutgoingMediaLeg(OutgoingMediaConfig{
		CallerIdentityKey: callerIdentity,
		CalleeIdentityKey: calleeIdentity,
		OnCandidate:       collect(&callerCandidates),
		IncludeLoopback:   true,
	})
	if err != nil {
		t.Fatalf("outgoing leg: %v", err)
	}
	defer caller.Close()

	// The callee is built from what the caller would have sent in its offer.
	callee, err := NewIncomingMediaLeg(IncomingMediaConfig{
		Remote:            caller.Parameters,
		CallerIdentityKey: callerIdentity,
		CalleeIdentityKey: calleeIdentity,
		OnCandidate:       collect(&calleeCandidates),
		IncludeLoopback:   true,
	})
	if err != nil {
		t.Fatalf("incoming leg: %v", err)
	}
	defer callee.Close()

	// And the caller learns the callee's answer, as it would from a Signal message.
	if err = caller.SetRemoteAnswer(callee.Parameters); err != nil {
		t.Fatalf("set remote answer: %v", err)
	}

	if err = caller.GatherCandidates(); err != nil {
		t.Fatalf("caller gather: %v", err)
	}
	if err = callee.GatherCandidates(); err != nil {
		t.Fatalf("callee gather: %v", err)
	}
	// Give gathering time, then trade what each found. Host candidates appear quickly on a machine
	// with a real interface; a longer window only costs time when there are none to find.
	deadlineGather := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadlineGather) {
		if len(snapshot(&callerCandidates)) > 0 && len(snapshot(&calleeCandidates)) > 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	callerFound, calleeFound := snapshot(&callerCandidates), snapshot(&calleeCandidates)
	for _, c := range callerFound {
		if err = callee.AddRemoteCandidate(c); err != nil {
			t.Logf("callee rejected a candidate: %v", err)
		}
	}
	for _, c := range calleeFound {
		if err = caller.AddRemoteCandidate(c); err != nil {
			t.Logf("caller rejected a candidate: %v", err)
		}
	}
	if len(callerFound) == 0 || len(calleeFound) == 0 {
		t.Fatalf("no ICE candidates gathered (caller %d, callee %d)", len(callerFound), len(calleeFound))
	}
	t.Logf("gathered %d caller and %d callee candidates", len(callerFound), len(calleeFound))

	connected := make(chan error, 2)
	go func() { connected <- caller.Connect(ctx) }()
	go func() { connected <- callee.Connect(ctx) }()
	for range 2 {
		if err = <-connected; err != nil {
			t.Fatalf("connect: %v", err)
		}
	}

	// The bridge writes the caller's audio under the caller's SSRC and reads the callee's under the
	// callee's, which is what these two constants are for.
	writer, err := caller.RTPWriter(OpusPayloadType, CallerAudioSSRC)
	if err != nil {
		t.Fatalf("rtp writer: %v", err)
	}
	reader, err := callee.RTPReader(CallerAudioSSRC)
	if err != nil {
		t.Fatalf("rtp reader: %v", err)
	}

	got := make(chan *rtp.Packet, 1)
	go func() {
		pkt, _, readErr := reader.ReadRTP()
		if readErr != nil {
			got <- nil
			return
		}
		got <- pkt
	}()

	payload := []byte("signal-opus-frame")
	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for i := 0; ; i++ {
		select {
		case pkt := <-got:
			if pkt == nil {
				t.Fatal("the callee leg could not read the caller's audio")
			}
			if string(pkt.Payload) != string(payload) {
				t.Errorf("payload came through as %q, want %q", pkt.Payload, payload)
			}
			t.Logf("audio crossed a real Signal media leg pair after %d packets", i+1)
			return
		case <-deadline:
			t.Fatal("no audio reached the callee leg within 15s")
		case <-ticker.C:
		}
		if err = writer.WriteRTP(&rtp.Packet{
			Header:  rtp.Header{Version: 2, SequenceNumber: uint16(i), Timestamp: uint32(i) * 960, SSRC: CallerAudioSSRC},
			Payload: payload,
		}); err != nil {
			t.Fatalf("write rtp: %v", err)
		}
	}
}
