package ringrtc

import (
	"crypto/sha256"
	"fmt"
	"io"

	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
)

const (
	SRTPKeySize  = 32
	SRTPSaltSize = 12
	srtpKDFInfo  = "Signal_Calling_20200807_SignallingDH_SRTPKey_KDF"
)

type SRTPKey struct {
	Key  []byte
	Salt []byte
}

type NegotiatedSRTPKeys struct {
	Offer  SRTPKey
	Answer SRTPKey
}

// NegotiateSRTPKeys reproduces RingRTC's negotiate_srtp_keys in
// src/rust/src/core/connection.rs: X25519, a zeroed 32-byte HKDF salt, and
// caller then callee identity keys appended to the fixed KDF label. RingRTC's
// AeadAes256Gcm suite defines a 32-byte key and 12-byte salt.
func NegotiateSRTPKeys(localSecret, remotePublicKey, callerIdentityKey, calleeIdentityKey []byte) (*NegotiatedSRTPKeys, error) {
	sharedSecret, err := curve25519.X25519(localSecret, remotePublicKey)
	if err != nil {
		return nil, fmt.Errorf("invalid remote SRTP public key: %w", err)
	}
	info := make([]byte, 0, len(srtpKDFInfo)+len(callerIdentityKey)+len(calleeIdentityKey))
	info = append(info, srtpKDFInfo...)
	info = append(info, callerIdentityKey...)
	info = append(info, calleeIdentityKey...)
	okm := make([]byte, 2*(SRTPKeySize+SRTPSaltSize))
	if _, err = io.ReadFull(hkdf.New(sha256.New, sharedSecret, make([]byte, 32), info), okm); err != nil {
		return nil, fmt.Errorf("derive SRTP keys: %w", err)
	}
	return &NegotiatedSRTPKeys{
		Offer: SRTPKey{
			Key:  append([]byte(nil), okm[:SRTPKeySize]...),
			Salt: append([]byte(nil), okm[SRTPKeySize:SRTPKeySize+SRTPSaltSize]...),
		},
		Answer: SRTPKey{
			Key:  append([]byte(nil), okm[SRTPKeySize+SRTPSaltSize:2*SRTPKeySize+SRTPSaltSize]...),
			Salt: append([]byte(nil), okm[2*SRTPKeySize+SRTPSaltSize:]...),
		},
	}, nil
}
