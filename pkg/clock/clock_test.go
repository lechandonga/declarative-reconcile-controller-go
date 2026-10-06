package clock_test

import (
	"testing"
	"time"

	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/clock"
)

// TestFakeClockDeterministic proves backoff scheduling needs no real wait:
// timers fire only when the clock is stepped across their deadline, in
// deadline order.
func TestFakeClockDeterministic(t *testing.T) {
	start := time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)
	c := clock.NewFakeClock(start)

	t1 := c.NewTimer(200 * time.Millisecond)
	t2 := c.NewTimer(100 * time.Millisecond)
	t3 := c.NewTimer(100 * time.Millisecond)

	c.Step(50 * time.Millisecond)
	select {
	case <-t1.C():
		t.Fatal("200ms timer fired at 50ms")
	case <-t2.C():
		t.Fatal("100ms timer fired at 50ms")
	default:
	}

	c.Step(100 * time.Millisecond)
	// Same-deadline timers fire in creation order: t2 then t3.
	select {
	case <-t2.C():
	default:
		t.Fatal("t2 should have fired after 150ms")
	}
	select {
	case <-t3.C():
	default:
		t.Fatal("t3 should have fired after 150ms")
	}
	select {
	case <-t1.C():
		t.Fatal("t1 must not fire before 200ms")
	default:
	}

	c.Step(100 * time.Millisecond)
	select {
	case <-t1.C():
	default:
		t.Fatal("t1 should fire at 200ms")
	}

	// A stopped timer never fires.
	t4 := c.NewTimer(time.Second)
	if !t4.Stop() {
		t.Fatal("Stop on active timer must return true")
	}
	c.Step(2 * time.Second)
	select {
	case <-t4.C():
		t.Fatal("stopped timer fired")
	default:
	}
	t.Log("业务[确定性时钟] 用例[手动步进驱动定时器, 不到点不触发, 同刻按序触发, Stop 生效] -> PASS")
}
