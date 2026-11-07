package service

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"
)

// VerificationKey is one identity_service public verification key. KID may be empty for
// PEM files that carry no key id.
type VerificationKey struct {
	KID string
	Key any
}

// KeySet holds the configured verification keys (the current key plus the next key used
// during rotation). Keys are distributed through deployment configuration and are never
// fetched at request time.
type KeySet struct {
	keys []VerificationKey
}

// NewKeySet builds a key set from already parsed keys.
func NewKeySet(keys ...VerificationKey) *KeySet {
	return &KeySet{keys: keys}
}

// Len reports how many keys are configured.
func (k *KeySet) Len() int {
	if k == nil {
		return 0
	}
	return len(k.keys)
}

// Select returns the candidate keys for a token: the single key matching kid when the token
// names one, otherwise every configured key (which supports rotation and keyless PEM keys).
func (k *KeySet) Select(kid string) []any {
	if k == nil {
		return nil
	}
	if kid != "" {
		for _, candidate := range k.keys {
			if candidate.KID == kid {
				return []any{candidate.Key}
			}
		}
		return nil
	}
	keys := make([]any, 0, len(k.keys))
	for _, candidate := range k.keys {
		keys = append(keys, candidate.Key)
	}
	return keys
}

// LoadKeySet builds the verification key set from the configured sources: inline JWKS (or a
// JWKS file path), the current PEM key and the next PEM key. At least one key is required.
func LoadKeySet(jwks, currentPath, nextPath string) (*KeySet, error) {
	keys := make([]VerificationKey, 0, 3)

	if trimmed := strings.TrimSpace(jwks); trimmed != "" {
		raw := []byte(trimmed)
		if !strings.HasPrefix(trimmed, "{") {
			content, err := os.ReadFile(trimmed)
			if err != nil {
				return nil, fmt.Errorf("read IDENTITY_JWKS file: %w", err)
			}
			raw = content
		}
		parsed, err := ParseJWKS(raw)
		if err != nil {
			return nil, err
		}
		keys = append(keys, parsed...)
	}

	for _, path := range []string{currentPath, nextPath} {
		if strings.TrimSpace(path) == "" {
			continue
		}
		key, err := LoadPublicKeyPEM(path)
		if err != nil {
			return nil, err
		}
		keys = append(keys, VerificationKey{KID: "", Key: key})
	}

	if len(keys) == 0 {
		return nil, errors.New("no identity verification keys configured")
	}
	return &KeySet{keys: keys}, nil
}

// LoadPublicKeyPEM reads one public key from a PEM file. PKIX, PKCS#1 and certificate PEM
// blocks are accepted.
func LoadPublicKeyPEM(path string) (any, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read verification key %s: %w", path, err)
	}
	key, err := ParsePublicKeyPEM(content)
	if err != nil {
		return nil, fmt.Errorf("parse verification key %s: %w", path, err)
	}
	return key, nil
}

// ParsePublicKeyPEM parses the first usable public key block of a PEM document.
func ParsePublicKeyPEM(content []byte) (any, error) {
	rest := content
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return nil, errors.New("no public key PEM block found")
		}
		switch block.Type {
		case "PUBLIC KEY":
			parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("parse PKIX public key: %w", err)
			}
			return parsed, nil
		case "RSA PUBLIC KEY":
			parsed, err := x509.ParsePKCS1PublicKey(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("parse PKCS#1 public key: %w", err)
			}
			return parsed, nil
		case "CERTIFICATE":
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, fmt.Errorf("parse certificate: %w", err)
			}
			return cert.PublicKey, nil
		}
	}
}

// jwk is the subset of the JSON Web Key format identity_service publishes.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// jwkSet is a JSON Web Key Set document.
type jwkSet struct {
	Keys []jwk `json:"keys"`
}

// ParseJWKS parses a JWKS document into verification keys. RSA, EC (P-256/P-384/P-521) and
// Ed25519 (OKP) keys are supported; symmetric (oct) keys are rejected because the gateway
// only accepts asymmetric signatures.
func ParseJWKS(raw []byte) ([]VerificationKey, error) {
	var set jwkSet
	if err := json.Unmarshal(raw, &set); err != nil {
		return nil, fmt.Errorf("parse JWKS: %w", err)
	}
	if len(set.Keys) == 0 {
		return nil, errors.New("JWKS contains no keys")
	}

	keys := make([]VerificationKey, 0, len(set.Keys))
	for _, key := range set.Keys {
		parsed, err := key.publicKey()
		if err != nil {
			return nil, err
		}
		keys = append(keys, VerificationKey{KID: key.Kid, Key: parsed})
	}
	return keys, nil
}

// publicKey converts one JWK into a crypto public key.
func (k jwk) publicKey() (any, error) {
	switch k.Kty {
	case "RSA":
		modulus, err := decodeBigInt(k.N)
		if err != nil {
			return nil, fmt.Errorf("JWKS RSA key %q modulus: %w", k.Kid, err)
		}
		exponent, err := decodeBigInt(k.E)
		if err != nil {
			return nil, fmt.Errorf("JWKS RSA key %q exponent: %w", k.Kid, err)
		}
		if !exponent.IsInt64() {
			return nil, fmt.Errorf("JWKS RSA key %q exponent is too large", k.Kid)
		}
		return &rsa.PublicKey{N: modulus, E: int(exponent.Int64())}, nil
	case "EC":
		curve, size, err := curveFor(k.Crv)
		if err != nil {
			return nil, err
		}
		x, err := decodeFixedBytes(k.X, size)
		if err != nil {
			return nil, fmt.Errorf("JWKS EC key %q x: %w", k.Kid, err)
		}
		y, err := decodeFixedBytes(k.Y, size)
		if err != nil {
			return nil, fmt.Errorf("JWKS EC key %q y: %w", k.Kid, err)
		}
		point := make([]byte, 1+2*size)
		point[0] = 0x04 // uncompressed point
		copy(point[1:1+size], x)
		copy(point[1+size:], y)
		// ParseUncompressedPublicKey rejects points that are not on the curve.
		publicKey, err := ecdsa.ParseUncompressedPublicKey(curve, point)
		if err != nil {
			return nil, fmt.Errorf("JWKS EC key %q is invalid: %w", k.Kid, err)
		}
		return publicKey, nil
	case "OKP":
		if k.Crv != "Ed25519" {
			return nil, fmt.Errorf("JWKS OKP key %q uses unsupported curve %q", k.Kid, k.Crv)
		}
		raw, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil {
			return nil, fmt.Errorf("JWKS OKP key %q x: %w", k.Kid, err)
		}
		if len(raw) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("JWKS OKP key %q has an invalid length", k.Kid)
		}
		return ed25519.PublicKey(raw), nil
	default:
		return nil, fmt.Errorf("JWKS key %q has unsupported kty %q", k.Kid, k.Kty)
	}
}

// curveFor maps a JWK curve name to an elliptic curve and its coordinate size in bytes.
func curveFor(name string) (elliptic.Curve, int, error) {
	switch name {
	case "P-256":
		return elliptic.P256(), 32, nil
	case "P-384":
		return elliptic.P384(), 48, nil
	case "P-521":
		return elliptic.P521(), 66, nil
	default:
		return nil, 0, fmt.Errorf("unsupported JWKS curve %q", name)
	}
}

// decodeFixedBytes decodes a base64url integer into a fixed-width, left-padded byte slice,
// which is the encoding ECDSA coordinates use in an uncompressed point.
func decodeFixedBytes(value string, size int) ([]byte, error) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || len(raw) > size {
		return nil, fmt.Errorf("coordinate must be 1..%d bytes, got %d", size, len(raw))
	}
	padded := make([]byte, size)
	copy(padded[size-len(raw):], raw)
	return padded, nil
}

// decodeBigInt decodes a base64url encoded big-endian integer.
func decodeBigInt(value string) (*big.Int, error) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, errors.New("empty integer")
	}
	return new(big.Int).SetBytes(raw), nil
}
