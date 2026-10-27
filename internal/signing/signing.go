// Package signing 以 HMAC-SHA256 簽署 webhook，格式依照 Standard Webhooks：
// 簽署內容是 "{webhook-id}.{webhook-timestamp}.{body}"，簽章放在 webhook-signature header。
// 接收端用同一個 package 驗證，所以它同時是平台送出端與客戶接收端的 SDK。
package signing

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	HeaderID        = "Webhook-Id"
	HeaderTimestamp = "Webhook-Timestamp"
	HeaderSignature = "Webhook-Signature"
	secretPrefix    = "whsec_"
)

var (
	ErrMissingHeaders = errors.New("missing webhook headers")
	ErrTimestamp      = errors.New("timestamp outside the tolerance window")
	ErrSignature      = errors.New("no matching signature")
)

// NewSecret 產生端點註冊時交給客戶的共享密鑰。
func NewSecret() string {
	b := make([]byte, 32)
	rand.Read(b)
	return secretPrefix + base64.StdEncoding.EncodeToString(b)
}

func key(secret string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, secretPrefix))
}

func sign(k []byte, id string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, k)
	mac.Write([]byte(id + "." + strconv.FormatInt(ts, 10) + "."))
	mac.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// Sign 設定三個 header。時間戳記是送出當下的時間，每次重試都重新簽，
// 接收端才能拒絕很久以前被截取、又被重放的請求。
func Sign(h http.Header, secret, id string, now time.Time, body []byte) error {
	k, err := key(secret)
	if err != nil {
		return err
	}
	ts := now.Unix()
	h.Set(HeaderID, id)
	h.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	h.Set(HeaderSignature, sign(k, id, ts, body))
	return nil
}

// Verify 是接收端的檢查：時間戳記必須在 tolerance 內，且至少一個簽章吻合。
// 比對用 hmac.Equal（constant-time），避免攻擊者從回應時間逐字猜出簽章。
// header 可以帶多個以空白分隔的簽章，輪替密鑰期間新舊密鑰的簽章會同時出現。
func Verify(h http.Header, secret string, body []byte, now time.Time, tolerance time.Duration) error {
	id, tsRaw, sigs := h.Get(HeaderID), h.Get(HeaderTimestamp), h.Get(HeaderSignature)
	if id == "" || tsRaw == "" || sigs == "" {
		return ErrMissingHeaders
	}
	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return ErrMissingHeaders
	}
	if d := now.Sub(time.Unix(ts, 0)); d > tolerance || d < -tolerance {
		return ErrTimestamp
	}
	k, err := key(secret)
	if err != nil {
		return err
	}
	expected := []byte(sign(k, id, ts, body))
	for _, s := range strings.Fields(sigs) {
		if hmac.Equal([]byte(s), expected) {
			return nil
		}
	}
	return ErrSignature
}
