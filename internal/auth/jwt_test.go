package auth

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestIssueAndVerify(t *testing.T) {
	s := NewSigner([]byte("secret"))
	token, err := s.Issue("alice", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if user, err := s.Verify(token); err != nil || user != "alice" {
		t.Fatalf("verify = %q, %v", user, err)
	}
}

func TestVerifyRejectsForgedAndExpiredTokens(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s := NewSigner([]byte("secret"))
	s.now = func() time.Time { return now }
	token, _ := s.Issue("alice", time.Minute)
	parts := strings.Split(token, ".")

	forged := parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"mallory","exp":9999999999}`)) + "." + parts[2]
	none := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + parts[1] + "."
	other, _ := NewSigner([]byte("other")).Issue("alice", time.Minute)
	for name, tok := range map[string]string{"forged": forged, "alg none": none, "other secret": other, "garbage": "x"} {
		if _, err := s.Verify(tok); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: err = %v", name, err)
		}
	}

	now = now.Add(2 * time.Minute)
	if _, err := s.Verify(token); !errors.Is(err, ErrExpiredToken) {
		t.Fatalf("expired: err = %v", err)
	}
}
