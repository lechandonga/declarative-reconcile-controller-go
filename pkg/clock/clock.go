// Package clock abstracts time so retry/backoff logic is deterministic in
// tests without real sleeps.
package clock

import (
	"sort"
	"sync"
	"time"
)

// Clock is the subset of time package functionality the controller needs.
type Clock interface {
	Now() time.Time
	// NewTimer returns a timer that fires after the given duration.
	NewTimer(d time.Duration) Timer
}

// Timer mirrors time.Timer.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	// Reset changes the expiry. With the fake clock it re-arms a stopped
	// or fired timer.
	Reset(d time.Duration) bool
}

// RealClock uses the wall clock.
type RealClock struct{}

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// NewTimer returns a real time.Timer.
func (RealClock) NewTimer(d time.Duration) Timer {
	return &realTimer{t: time.NewTimer(d)}
}

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time { return r.t.C }
func (r *realTimer) Stop() bool          { return r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) bool {
	if r.t.Stop() {
		// Drain only if needed; Reset semantics after Stop are fine either way
		// for our queue usage, which never has a pending value to race with.
		select {
		case <-r.t.C:
		default:
		}
	}
	r.t.Reset(d)
	return true
}

// FakeClock is a manually advanced clock. Timers fire when Step moves the
// clock to/past their deadline.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*fakeTimer]struct{}
	seq    int64
}

// NewFakeClock starts at the given epoch.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{
		now:    start,
		timers: make(map[*fakeTimer]struct{}),
	}
}

// Now returns the fake current time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer registers a timer due at now+d.
func (c *FakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	ft := &fakeTimer{
		clock:    c,
		ch:       make(chan time.Time, 1),
		deadline: c.now.Add(d),
		id:       c.seq,
	}
	c.timers[ft] = struct{}{}
	return ft
}

// Step advances the clock by d and fires every due timer. Firing order is
// deadline, then creation order, matching real time behavior.
func (c *FakeClock) Step(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	type due struct {
		ft       *fakeTimer
		deadline time.Time
	}
	var pending []due
	for ft := range c.timers {
		if !ft.stopped && !ft.deadline.After(c.now) {
			pending = append(pending, due{ft, ft.deadline})
		}
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].deadline.Equal(pending[j].deadline) {
			return pending[i].ft.id < pending[j].ft.id
		}
		return pending[i].deadline.Before(pending[j].deadline)
	})
	c.mu.Unlock()
	for _, p := range pending {
		p.ft.fire(c.Now())
	}
}

func (c *FakeClock) remove(ft *fakeTimer) {
	c.mu.Lock()
	delete(c.timers, ft)
	c.mu.Unlock()
}

type fakeTimer struct {
	clock    *FakeClock
	ch       chan time.Time
	deadline time.Time
	id       int64
	stopped  bool
	fired    bool
	mu       sync.Mutex
}

func (ft *fakeTimer) C() <-chan time.Time { return ft.ch }

func (ft *fakeTimer) Stop() bool {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	wasActive := !ft.stopped && !ft.fired
	ft.stopped = true
	if wasActive {
		ft.clock.remove(ft)
	}
	return wasActive
}

func (ft *fakeTimer) Reset(d time.Duration) bool {
	ft.mu.Lock()
	wasActive := !ft.stopped && !ft.fired
	ft.stopped = false
	ft.fired = false
	ft.mu.Unlock()
	ft.clock.mu.Lock()
	ft.deadline = ft.clock.now.Add(d)
	ft.clock.timers[ft] = struct{}{}
	ft.clock.mu.Unlock()
	return wasActive
}

func (ft *fakeTimer) fire(at time.Time) {
	ft.mu.Lock()
	if ft.stopped || ft.fired {
		ft.mu.Unlock()
		return
	}
	ft.fired = true
	ft.stopped = true
	ft.mu.Unlock()
	select {
	case ft.ch <- at:
	default:
	}
	ft.clock.remove(ft)
}
