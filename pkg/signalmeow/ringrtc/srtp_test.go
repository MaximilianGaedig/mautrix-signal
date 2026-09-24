package ringrtc

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// This vector was generated with RingRTC's exact Rust primitives and layout:
// x25519-dalek 3.0.0, hkdf 0.13.0 and sha2 0.11.0, matching RingRTC's Cargo.lock.
func TestNegotiateSRTPKeysKnownAnswer(t *testing.T) {
	localSecret := make([]byte, 32)
	for i := range localSecret {
		localSecret[i] = byte(i + 1)
	}
	remotePublic := mustDecodeHex(t, "0d799600f6ffaee2e121e6b8f7a05dc66874b51db3102d0d71f799a09cb4c461")
	callerIdentity := append([]byte{0x05}, bytes.Repeat([]byte{0x11}, 32)...)
	calleeIdentity := append([]byte{0x05}, bytes.Repeat([]byte{0x22}, 32)...)
	keys, err := NegotiateSRTPKeys(localSecret, remotePublic, callerIdentity, calleeIdentity)
	if err != nil {
		t.Fatal(err)
	}
	assertHex(t, "offer key", keys.Offer.Key, "8ee17d28805194ba8a0456fb0d28fb8aa9f3a60d4c7338f349eb3219498df1a7")
	assertHex(t, "offer salt", keys.Offer.Salt, "6694f2108c7081a3356ea745")
	assertHex(t, "answer key", keys.Answer.Key, "29eb97a0e4a87e7ef2c89554426c1a0084e2c83583693884df13255942a8594e")
	assertHex(t, "answer salt", keys.Answer.Salt, "1038c087322ee0d579496e91")
}

func TestNegotiateSRTPKeysRejectsLowOrderKey(t *testing.T) {
	if _, err := NegotiateSRTPKeys(bytes.Repeat([]byte{1}, 32), make([]byte, 32), []byte("caller"), []byte("callee")); err == nil {
		t.Fatal("expected a non-contributory remote key to be rejected")
	}
}

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

func assertHex(t *testing.T, name string, got []byte, want string) {
	t.Helper()
	if hex.EncodeToString(got) != want {
		t.Errorf("%s = %x, want %s", name, got, want)
	}
}
