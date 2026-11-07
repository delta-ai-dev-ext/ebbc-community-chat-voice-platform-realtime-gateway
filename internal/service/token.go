package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"realtime_gateway/internal/contracts"
)

// Token verification failures. A close-code decision maps ErrTokenExpired to 4401 and
// everything else to 4402.
var (
	// ErrInvalidToken means the token is missing, malformed, signed with an unexpected
	// algorithm or key, or carries foreign/missing claims.
	ErrInvalidToken = errors.New("invalid token")
	// ErrTokenExpired means the token signature and claims were valid but `exp` passed.
	ErrTokenExpired = errors.New("token expired")
)

// allowedAlgorithms are the asymmetric signature algorithms accepted for access tokens.
// Symmetric (HMAC) and unsecured ("none") algorithms are never accepted.
var allowedAlgorithms = []string{
	"RS256", "RS384", "RS512",
	"PS256", "PS384", "PS512",
	"ES256", "ES384", "ES512",
	"EdDSA",
}

// Claims is the verified subset of an identity_service access token.
type Claims struct {
	UserID        string
	SessionID     string
	EmailVerified bool
	Issuer        string
	Audience      []string
	IssuedAt      time.Time
	ExpiresAt     time.Time
}

// TokenVerifier verifies access tokens locally. Implementations must not perform network
// calls: identity's public keys come from configuration.
type TokenVerifier interface {
	Verify(ctx context.Context, token string) (*Claims, error)
}

// VerifierOptions configures the expected token claims and the injected clock.
type VerifierOptions struct {
	// Issuer is the required `iss` claim.
	Issuer string
	// Audience is the required `aud` claim.
	Audience string
	// Clock is the time source for `exp` validation (defaults to time.Now).
	Clock func() time.Time
	// Leeway tolerates small clock skew; zero by default so expiry is exact.
	Leeway time.Duration
}

// accessClaims mirrors the identity_service access token claims.
type accessClaims struct {
	SessionID     string `json:"sid"`
	EmailVerified bool   `json:"email_verified"`
	jwt.RegisteredClaims
}

// JWTVerifier verifies access tokens against the configured public keys, issuer, audience
// and expiry.
type JWTVerifier struct {
	keys     *KeySet
	issuer   string
	audience string
	parser   *jwt.Parser
}

// NewJWTVerifier builds a verifier. It fails when no keys are configured or when the
// expected issuer/audience are missing, so a misconfigured replica never accepts tokens.
func NewJWTVerifier(keys *KeySet, opts VerifierOptions) (*JWTVerifier, error) {
	if keys.Len() == 0 {
		return nil, errors.New("token verifier requires at least one verification key")
	}
	if strings.TrimSpace(opts.Issuer) == "" {
		return nil, errors.New("token verifier requires IDENTITY_TOKEN_ISSUER")
	}
	if strings.TrimSpace(opts.Audience) == "" {
		return nil, errors.New("token verifier requires IDENTITY_TOKEN_AUDIENCE")
	}
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}

	parser := jwt.NewParser(
		jwt.WithValidMethods(allowedAlgorithms),
		jwt.WithIssuer(opts.Issuer),
		jwt.WithAudience(opts.Audience),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(opts.Leeway),
		jwt.WithTimeFunc(clock),
	)

	return &JWTVerifier{
		keys:     keys,
		issuer:   opts.Issuer,
		audience: opts.Audience,
		parser:   parser,
	}, nil
}

// Verify validates the token signature and claims and returns the extracted `sub`/`sid`.
func (v *JWTVerifier) Verify(_ context.Context, token string) (*Claims, error) {
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("%w: empty token", ErrInvalidToken)
	}

	claims := &accessClaims{}
	parsed, err := v.parser.ParseWithClaims(token, claims, func(candidate *jwt.Token) (any, error) {
		// Defence in depth: WithValidMethods already rejects these, but a symmetric key
		// must never be accepted as an identity verification key.
		alg := candidate.Method.Alg()
		if _, isHMAC := candidate.Method.(*jwt.SigningMethodHMAC); isHMAC || alg == jwt.SigningMethodNone.Alg() {
			return nil, fmt.Errorf("%w: algorithm %q is not allowed", ErrInvalidToken, alg)
		}
		kid, _ := candidate.Header["kid"].(string)
		configured := v.keys.Select(kid)
		if len(configured) == 0 {
			return nil, fmt.Errorf("%w: no configured key matches kid %q", ErrInvalidToken, kid)
		}
		keys := make([]jwt.VerificationKey, 0, len(configured))
		for _, key := range configured {
			keys = append(keys, key)
		}
		return jwt.VerificationKeySet{Keys: keys}, nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, fmt.Errorf("%w: %v", ErrTokenExpired, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if !parsed.Valid {
		return nil, fmt.Errorf("%w: token did not validate", ErrInvalidToken)
	}

	switch {
	case claims.Subject == "":
		return nil, fmt.Errorf("%w: token has no sub claim", ErrInvalidToken)
	case !contracts.IsUUID(claims.Subject):
		return nil, fmt.Errorf("%w: token sub claim is not a user id", ErrInvalidToken)
	case claims.SessionID == "":
		return nil, fmt.Errorf("%w: token has no sid claim", ErrInvalidToken)
	case claims.ExpiresAt == nil:
		return nil, fmt.Errorf("%w: token has no exp claim", ErrInvalidToken)
	}

	result := &Claims{
		UserID:        claims.Subject,
		SessionID:     claims.SessionID,
		EmailVerified: claims.EmailVerified,
		Issuer:        claims.Issuer,
		Audience:      []string(claims.Audience),
		ExpiresAt:     claims.ExpiresAt.Time,
	}
	if claims.IssuedAt != nil {
		result.IssuedAt = claims.IssuedAt.Time
	}
	return result, nil
}
