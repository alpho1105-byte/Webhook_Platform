package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("token expired")
)

type claims struct {
	Subject   string `json:"sub"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
}

var header = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

// Signer 簽發與驗證 webhook 管理 API 使用的 HS256 JWT，subject 是使用者的 user id。
type Signer struct {
	secret []byte
	now    func() time.Time
}

func NewSigner(secret []byte) *Signer {
	return &Signer{secret: secret, now: time.Now}
}

func (s *Signer) Issue(userID string, ttl time.Duration) (string, error) {
	now := s.now()
	body, err := json.Marshal(claims{Subject: userID, IssuedAt: now.Unix(), ExpiresAt: now.Add(ttl).Unix()})
	if err != nil {
		return "", fmt.Errorf("marshal claims: %w", err)
	}
	unsigned := header + "." + base64.RawURLEncoding.EncodeToString(body)
	return unsigned + "." + s.sign(unsigned), nil
}

func (s *Signer) Verify(token string) (string, error) {
	parts := strings.Split(token, ".")
	// header 必須和簽發時完全一致，擋掉 alg=none 這類降級攻擊。
	if len(parts) != 3 || parts[0] != header {
		return "", ErrInvalidToken
	}
	if !hmac.Equal([]byte(s.sign(parts[0]+"."+parts[1])), []byte(parts[2])) {
		return "", ErrInvalidToken
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", ErrInvalidToken
	}
	var c claims
	if err := json.Unmarshal(body, &c); err != nil || c.Subject == "" {
		return "", ErrInvalidToken
	}
	if s.now().Unix() >= c.ExpiresAt {
		return "", ErrExpiredToken
	}
	return c.Subject, nil
}

func (s *Signer) sign(unsigned string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(unsigned))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
