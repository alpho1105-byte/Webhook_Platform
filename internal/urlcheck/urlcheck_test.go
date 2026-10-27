package urlcheck

import (
	"context"
	"errors"
	"testing"
)

func TestCheck(t *testing.T) {
	ctx := context.Background()
	prod, dev := Policy{}, Policy{AllowLoopbackHTTP: true}
	for url, want := range map[string]bool{
		"https://93.184.216.34/hooks":             true,
		"http://93.184.216.34/hooks":              false, // 只接受 https
		"https://10.1.2.3/hooks":                  false,
		"https://192.168.0.10/hooks":              false,
		"https://169.254.169.254/latest/metadata": false, // 雲端 metadata 服務
		"https://100.64.0.1/hooks":                false,
		"https://[::1]/hooks":                     false,
		"https://user:pass@93.184.216.34/hooks":   false,
		"not a url":                               false,
	} {
		if got := prod.Check(ctx, url); got != want {
			t.Errorf("prod %s: %v, want %v", url, got, want)
		}
	}
	if !dev.Check(ctx, "http://127.0.0.1:8080/hook") {
		t.Error("dev policy should allow loopback http")
	}
	if dev.Check(ctx, "http://10.0.0.8/hook") {
		t.Error("dev policy must still refuse private addresses")
	}
}

func TestDialControlBlocksPrivateAddresses(t *testing.T) {
	if err := (Policy{}).DialControl("tcp", "10.0.0.8:443", nil); !errors.Is(err, ErrBlocked) {
		t.Fatalf("private: err = %v", err)
	}
	if err := (Policy{}).DialControl("tcp", "93.184.216.34:443", nil); err != nil {
		t.Fatalf("public: err = %v", err)
	}
	if err := (Policy{AllowLoopbackHTTP: true}).DialControl("tcp", "127.0.0.1:80", nil); err != nil {
		t.Fatalf("dev loopback: err = %v", err)
	}
}
