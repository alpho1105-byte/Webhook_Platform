// Package urlcheck 決定客戶註冊的 URL 能不能被平台呼叫。
//
// webhook 平台會依客戶給的 URL 從自己的網路發出請求，若不設限，
// 客戶填一個內網位址（例如雲端的 metadata 服務 169.254.169.254）就能讓平台替他打進內網（SSRF）。
package urlcheck

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
)

var ErrBlocked = errors.New("destination address is not allowed")

type Policy struct {
	// AllowLoopbackHTTP 只給本機開發與模擬使用：允許 http://127.0.0.1 這類位址。
	AllowLoopbackHTTP bool
}

// Check 回傳 URL 是否可以註冊。正式環境只接受 https，且主機解析出的每個位址都不能落在內網。
func (p Policy) Check(ctx context.Context, raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || len(raw) > 2048 {
		return false
	}
	addrs, err := p.resolve(ctx, u.Hostname())
	if err != nil || len(addrs) == 0 {
		return false
	}
	if p.AllowLoopbackHTTP && (u.Scheme == "http" || u.Scheme == "https") && allLoopback(addrs) {
		return true
	}
	if u.Scheme != "https" {
		return false
	}
	for _, a := range addrs {
		if !public(a) {
			return false
		}
	}
	return true
}

func (p Policy) resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	if a, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil {
		return []netip.Addr{a.Unmap()}, nil
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	for i := range ips {
		ips[i] = ips[i].Unmap()
	}
	return ips, err
}

func allLoopback(addrs []netip.Addr) bool {
	for _, a := range addrs {
		if !a.IsLoopback() {
			return false
		}
	}
	return true
}

func public(a netip.Addr) bool {
	return a.IsGlobalUnicast() && !a.IsPrivate() && !a.IsLoopback() && !a.IsLinkLocalUnicast() &&
		!netip.MustParsePrefix("100.64.0.0/10").Contains(a) // carrier-grade NAT
}

// DialControl 在每次建立連線時再檢查一次實際連到的 IP。只在註冊時檢查不夠：
// DNS 可以在註冊之後改指向內網（DNS rebinding），所以投遞時也要擋。
func (p Policy) DialControl(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return err
	}
	a = a.Unmap()
	if public(a) || (p.AllowLoopbackHTTP && a.IsLoopback()) {
		return nil
	}
	return ErrBlocked
}
