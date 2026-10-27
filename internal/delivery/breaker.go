package delivery

import "time"

type breakerState int

const (
	closed breakerState = iota
	open
	halfOpen
)

// breaker 是單一端點的 circuit breaker。連續失敗到門檻就打開：冷卻期間不送任何請求，
// 冷卻後只放一個試探請求，成功才恢復。對方已經掛了，繼續每個事件都打一次只會浪費 worker、
// 加重對方恢復時的負擔。
type breaker struct {
	threshold int
	cooldown  time.Duration
	state     breakerState
	failures  int
	openedAt  time.Time
	probing   bool
}

func (b *breaker) allow(now time.Time) bool {
	switch b.state {
	case closed:
		return true
	case open:
		if now.Sub(b.openedAt) < b.cooldown {
			return false
		}
		b.state = halfOpen
	}
	if b.probing {
		return false
	}
	b.probing = true
	return true
}

// record 記錄一次送出的結果，回傳斷路器是否剛從打開恢復成關閉。
func (b *breaker) record(ok bool, now time.Time) (recovered bool) {
	if ok {
		recovered = b.state != closed
		b.state, b.failures, b.probing = closed, 0, false
		return recovered
	}
	b.failures++
	if b.state == halfOpen || b.failures >= b.threshold {
		b.state, b.openedAt, b.probing = open, now, false
	}
	return false
}
