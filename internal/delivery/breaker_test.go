package delivery

import (
	"testing"
	"time"
)

func TestBreakerOpensProbesOnceAndRecovers(t *testing.T) {
	b := &breaker{threshold: 3, cooldown: time.Second}
	now := time.Unix(0, 0)
	for range 3 {
		if !b.allow(now) {
			t.Fatal("closed breaker must allow")
		}
		b.record(false, now)
	}
	if b.allow(now.Add(500 * time.Millisecond)) {
		t.Fatal("open breaker must block during the cooldown")
	}
	later := now.Add(time.Second)
	if !b.allow(later) || b.allow(later) {
		t.Fatal("after the cooldown exactly one probe goes through")
	}
	b.record(false, later) // 試探失敗：重新打開
	if b.allow(later.Add(500 * time.Millisecond)) {
		t.Fatal("a failed probe must reopen the breaker")
	}
	again := later.Add(time.Second)
	b.allow(again)
	if !b.record(true, again) {
		t.Fatal("a successful probe should report recovery")
	}
	if !b.allow(again) || !b.allow(again) {
		t.Fatal("recovered breaker must allow everything")
	}
}
