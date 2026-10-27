package signing

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSignAndVerify(t *testing.T) {
	secret, body, now := NewSecret(), []byte(`{"id":"evt_1","type":"invoice.paid"}`), time.Unix(1_800_000_000, 0)
	h := http.Header{}
	if err := Sign(h, secret, "evt_1", now, body); err != nil {
		t.Fatal(err)
	}
	if err := Verify(h, secret, body, now.Add(time.Minute), 5*time.Minute); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}

	cases := map[string]struct {
		header http.Header
		secret string
		body   []byte
		at     time.Time
		want   error
	}{
		"tampered body":     {h, secret, []byte(`{"id":"evt_1","type":"invoice.refunded"}`), now, ErrSignature},
		"other secret":      {h, NewSecret(), body, now, ErrSignature},
		"replayed too late": {h, secret, body, now.Add(6 * time.Minute), ErrTimestamp},
		"from the future":   {h, secret, body, now.Add(-6 * time.Minute), ErrTimestamp},
		"no headers":        {http.Header{}, secret, body, now, ErrMissingHeaders},
	}
	for name, c := range cases {
		if err := Verify(c.header, c.secret, c.body, c.at, 5*time.Minute); !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
	}
}

func TestVerifyAcceptsAnyOfSeveralSignaturesDuringRotation(t *testing.T) {
	oldSecret, newSecret, body, now := NewSecret(), NewSecret(), []byte("{}"), time.Now()
	h1, h2 := http.Header{}, http.Header{}
	Sign(h1, oldSecret, "evt_1", now, body)
	Sign(h2, newSecret, "evt_1", now, body)
	h1.Set(HeaderSignature, h1.Get(HeaderSignature)+" "+h2.Get(HeaderSignature))
	for _, s := range []string{oldSecret, newSecret} {
		if err := Verify(h1, s, body, now, time.Minute); err != nil {
			t.Fatalf("rotation: %v", err)
		}
	}
	if !strings.HasPrefix(oldSecret, "whsec_") {
		t.Fatalf("secret = %q", oldSecret)
	}
}
