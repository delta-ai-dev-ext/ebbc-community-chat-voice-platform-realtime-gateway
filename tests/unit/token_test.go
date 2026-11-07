package unit

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"realtime_gateway/internal/service"
)

const (
	testIssuer   = "identity_service"
	testAudience = "realtime_gateway"
)

// testClock is the fixed injected clock of the auth tests.
var testClock = time.Date(2026, 2, 11, 10, 0, 0, 0, time.UTC)

// tokenSigner mints tokens with the test keys.
type tokenSigner struct {
	t   *testing.T
	key any
}

// mint signs a token with the given method, applying mutate to the default claims.
func (s tokenSigner) mint(method jwt.SigningMethod, kid string, mutate func(jwt.MapClaims)) string {
	s.t.Helper()

	claims := jwt.MapClaims{
		"sub":            uuidAt(13),
		"sid":            uuidAt(30),
		"iss":            testIssuer,
		"aud":            testAudience,
		"iat":            testClock.Unix(),
		"exp":            testClock.Add(10 * time.Minute).Unix(),
		"email_verified": true,
	}
	if mutate != nil {
		mutate(claims)
	}

	token := jwt.NewWithClaims(method, claims)
	if kid != "" {
		token.Header["kid"] = kid
	}
	signed, err := token.SignedString(s.key)
	require.NoError(s.t, err)
	return signed
}

// newRSAKey generates a throwaway RSA key for tests.
func newRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return key
}

// pemPublicKey encodes a public key as a PKIX PEM document.
func pemPublicKey(t *testing.T, key any) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

// writeKeyFile writes a PEM public key into a temporary file.
func writeKeyFile(t *testing.T, name string, key any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, pemPublicKey(t, key), 0o600))
	return path
}

// newTestVerifier builds a verifier trusting the given RSA public key under kid "current".
func newTestVerifier(t *testing.T, keys ...service.VerificationKey) *service.JWTVerifier {
	t.Helper()
	verifier, err := service.NewJWTVerifier(service.NewKeySet(keys...), service.VerifierOptions{
		Issuer:   testIssuer,
		Audience: testAudience,
		Clock:    func() time.Time { return testClock },
	})
	require.NoError(t, err)
	return verifier
}

func TestJWTVerifierAcceptsValidToken(t *testing.T) {
	t.Parallel()

	key := newRSAKey(t)
	verifier := newTestVerifier(t, service.VerificationKey{KID: "current", Key: &key.PublicKey})

	token := tokenSigner{t: t, key: key}.mint(jwt.SigningMethodRS256, "current", nil)

	claims, err := verifier.Verify(context.Background(), token)
	require.NoError(t, err)
	assert.Equal(t, uuidAt(13), claims.UserID)
	assert.Equal(t, uuidAt(30), claims.SessionID)
	assert.True(t, claims.EmailVerified)
	assert.Equal(t, testIssuer, claims.Issuer)
	assert.Equal(t, []string{testAudience}, claims.Audience)
	assert.True(t, testClock.Add(10*time.Minute).Equal(claims.ExpiresAt))
	assert.True(t, testClock.Equal(claims.IssuedAt))
}

func TestJWTVerifierRejectsExpiredToken(t *testing.T) {
	t.Parallel()

	key := newRSAKey(t)
	verifier := newTestVerifier(t, service.VerificationKey{KID: "current", Key: &key.PublicKey})

	token := tokenSigner{t: t, key: key}.mint(jwt.SigningMethodRS256, "current", func(claims jwt.MapClaims) {
		claims["exp"] = testClock.Add(-time.Second).Unix()
	})

	_, err := verifier.Verify(context.Background(), token)
	require.Error(t, err)
	assert.ErrorIs(t, err, service.ErrTokenExpired)
}

func TestJWTVerifierRejectsInvalidTokens(t *testing.T) {
	t.Parallel()

	key := newRSAKey(t)
	otherKey := newRSAKey(t)
	verifier := newTestVerifier(t, service.VerificationKey{KID: "current", Key: &key.PublicKey})
	signer := tokenSigner{t: t, key: key}

	cases := []struct {
		name  string
		token func() string
	}{
		{"empty", func() string { return "" }},
		{"garbage", func() string { return "not.a.jwt" }},
		{"wrong issuer", func() string {
			return signer.mint(jwt.SigningMethodRS256, "current", func(c jwt.MapClaims) { c["iss"] = "some_other_service" })
		}},
		{"wrong audience", func() string {
			return signer.mint(jwt.SigningMethodRS256, "current", func(c jwt.MapClaims) { c["aud"] = "another_service" })
		}},
		{"missing sid", func() string {
			return signer.mint(jwt.SigningMethodRS256, "current", func(c jwt.MapClaims) { delete(c, "sid") })
		}},
		{"sub not uuid", func() string {
			return signer.mint(jwt.SigningMethodRS256, "current", func(c jwt.MapClaims) { c["sub"] = "user-1" })
		}},
		{"missing exp", func() string {
			return signer.mint(jwt.SigningMethodRS256, "current", func(c jwt.MapClaims) { delete(c, "exp") })
		}},
		{"unknown kid", func() string {
			return signer.mint(jwt.SigningMethodRS256, "rotated", nil)
		}},
		{"signed by foreign key", func() string {
			return tokenSigner{t: t, key: otherKey}.mint(jwt.SigningMethodRS256, "current", nil)
		}},
		{"hmac signed", func() string {
			return tokenSigner{t: t, key: []byte("shared-secret")}.mint(jwt.SigningMethodHS256, "current", nil)
		}},
		{"alg none", func() string {
			return tokenSigner{t: t, key: jwt.UnsafeAllowNoneSignatureType}.mint(jwt.SigningMethodNone, "current", nil)
		}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := verifier.Verify(context.Background(), tc.token())
			require.Error(t, err)
			assert.ErrorIs(t, err, service.ErrInvalidToken)
			assert.NotErrorIs(t, err, service.ErrTokenExpired)
		})
	}
}

func TestJWTVerifierAcceptsEveryAsymmetricAlgorithm(t *testing.T) {
	t.Parallel()

	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	// An Ed25519 private key is the 64-byte form; jwt signs with the private part.
	rsaKey := newRSAKey(t)

	verifier := newTestVerifier(t,
		service.VerificationKey{KID: "ec", Key: &ecKey.PublicKey},
		service.VerificationKey{KID: "ed", Key: edKey.Public()},
		service.VerificationKey{KID: "rsa", Key: &rsaKey.PublicKey},
	)

	cases := []struct {
		name   string
		kid    string
		method jwt.SigningMethod
		key    any
	}{
		{"ES256", "ec", jwt.SigningMethodES256, ecKey},
		{"EdDSA", "ed", jwt.SigningMethodEdDSA, edKey},
		{"PS256", "rsa", jwt.SigningMethodPS256, rsaKey},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			token := tokenSigner{t: t, key: tc.key}.mint(tc.method, tc.kid, nil)
			claims, err := verifier.Verify(context.Background(), token)
			require.NoError(t, err)
			assert.Equal(t, uuidAt(13), claims.UserID)
		})
	}
}

func TestLoadKeySetSupportsRotation(t *testing.T) {
	t.Parallel()

	current := newRSAKey(t)
	next := newRSAKey(t)

	keySet, err := service.LoadKeySet("", writeKeyFile(t, "current.pem", &current.PublicKey), writeKeyFile(t, "next.pem", &next.PublicKey))
	require.NoError(t, err)
	require.Equal(t, 2, keySet.Len())

	verifier, err := service.NewJWTVerifier(keySet, service.VerifierOptions{
		Issuer:   testIssuer,
		Audience: testAudience,
		Clock:    func() time.Time { return testClock },
	})
	require.NoError(t, err)

	for name, key := range map[string]*rsa.PrivateKey{"current": current, "next": next} {
		token := tokenSigner{t: t, key: key}.mint(jwt.SigningMethodRS256, "", nil)
		claims, verifyErr := verifier.Verify(context.Background(), token)
		require.NoError(t, verifyErr, name)
		assert.Equal(t, uuidAt(13), claims.UserID, name)
	}
}

func TestLoadKeySetFromJWKS(t *testing.T) {
	t.Parallel()

	key := newRSAKey(t)
	jwks, err := json.Marshal(map[string]any{"keys": []any{rsaJWK("current", &key.PublicKey)}})
	require.NoError(t, err)

	keySet, err := service.LoadKeySet(string(jwks), "", "")
	require.NoError(t, err)
	require.Equal(t, 1, keySet.Len())

	verifier, err := service.NewJWTVerifier(keySet, service.VerifierOptions{
		Issuer:   testIssuer,
		Audience: testAudience,
		Clock:    func() time.Time { return testClock },
	})
	require.NoError(t, err)

	token := tokenSigner{t: t, key: key}.mint(jwt.SigningMethodRS256, "current", nil)
	_, err = verifier.Verify(context.Background(), token)
	require.NoError(t, err)
}

func TestLoadKeySetFromJWKSFile(t *testing.T) {
	t.Parallel()

	key := newRSAKey(t)
	jwks, err := json.Marshal(map[string]any{"keys": []any{rsaJWK("current", &key.PublicKey)}})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "jwks.json")
	require.NoError(t, os.WriteFile(path, jwks, 0o600))

	keySet, err := service.LoadKeySet(path, "", "")
	require.NoError(t, err)
	assert.Equal(t, 1, keySet.Len())
}

func TestParseJWKSRejectsUnsupportedKeys(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"symmetric":     `{"keys":[{"kty":"oct","kid":"sym","k":"c2VjcmV0"}]}`,
		"unknown curve": `{"keys":[{"kty":"EC","kid":"ec","crv":"P-192","x":"AA","y":"AA"}]}`,
		"off curve":     `{"keys":[{"kty":"EC","kid":"ec","crv":"P-256","x":"AA","y":"AA"}]}`,
		"bad base64":    `{"keys":[{"kty":"RSA","kid":"rsa","n":"!!","e":"AQAB"}]}`,
		"empty set":     `{"keys":[]}`,
		"not json":      `{`,
	}

	for name, document := range cases {
		document := document
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := service.ParseJWKS([]byte(document))
			require.Error(t, err)
		})
	}
}

func TestParseJWKSAcceptsECAndEd25519(t *testing.T) {
	t.Parallel()

	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	edPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	document, err := json.Marshal(map[string]any{"keys": []any{
		ecJWK("ec", &ecKey.PublicKey),
		map[string]any{"kty": "OKP", "kid": "ed", "crv": "Ed25519",
			"x": base64.RawURLEncoding.EncodeToString(edPub)},
	}})
	require.NoError(t, err)

	keys, err := service.ParseJWKS(document)
	require.NoError(t, err)
	require.Len(t, keys, 2)
	assert.IsType(t, &ecdsa.PublicKey{}, keys[0].Key)
	assert.IsType(t, ed25519.PublicKey{}, keys[1].Key)
}

func TestLoadPublicKeyPEMVariants(t *testing.T) {
	t.Parallel()

	key := newRSAKey(t)

	paths := map[string][]byte{
		"pkcs1": pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&key.PublicKey)}),
		"pkix":  pemPublicKey(t, &key.PublicKey),
	}

	for name, content := range paths {
		content := content
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), name+".pem")
			require.NoError(t, os.WriteFile(path, content, 0o600))

			parsed, err := service.LoadPublicKeyPEM(path)
			require.NoError(t, err)
			assert.IsType(t, &rsa.PublicKey{}, parsed)
		})
	}

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()
		_, err := service.LoadPublicKeyPEM(filepath.Join(t.TempDir(), "absent.pem"))
		require.Error(t, err)
	})

	t.Run("no pem block", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "junk.pem")
		require.NoError(t, os.WriteFile(path, []byte("not a pem document"), 0o600))
		_, err := service.LoadPublicKeyPEM(path)
		require.Error(t, err)
	})
}

func TestLoadKeySetRequiresKeys(t *testing.T) {
	t.Parallel()

	_, err := service.LoadKeySet("", "", "")
	require.Error(t, err)
}

func TestNewJWTVerifierValidation(t *testing.T) {
	t.Parallel()

	key := newRSAKey(t)
	empty := service.NewKeySet()

	_, err := service.NewJWTVerifier(empty, service.VerifierOptions{Issuer: testIssuer, Audience: testAudience})
	require.Error(t, err)

	keys := service.NewKeySet(service.VerificationKey{Key: &key.PublicKey})
	_, err = service.NewJWTVerifier(keys, service.VerifierOptions{Audience: testAudience})
	require.Error(t, err)

	_, err = service.NewJWTVerifier(keys, service.VerifierOptions{Issuer: testIssuer})
	require.Error(t, err)
}

// rsaJWK renders an RSA public key as a JWK member.
func rsaJWK(kid string, pub *rsa.PublicKey) map[string]any {
	return map[string]any{
		"kty": "RSA",
		"kid": kid,
		"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
	}
}

// ecJWK renders a P-256 public key as a JWK member.
func ecJWK(kid string, pub *ecdsa.PublicKey) map[string]any {
	// Bytes returns the uncompressed point 0x04 || X || Y.
	point, err := pub.Bytes()
	if err != nil {
		panic(err)
	}
	size := (len(point) - 1) / 2
	return map[string]any{
		"kty": "EC",
		"kid": kid,
		"crv": "P-256",
		"x":   base64.RawURLEncoding.EncodeToString(point[1 : 1+size]),
		"y":   base64.RawURLEncoding.EncodeToString(point[1+size:]),
	}
}
