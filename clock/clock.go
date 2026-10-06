// Package clock provides a small injectable clock so the controller's
// backoff is fully testable without real sleeps.
package clock

import (
	"sync"
	"time"
)

// Clock is the time surface used by the controller.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// PollScheduler is implemented by clocks designed for deterministic testing.
type PollScheduler interface {
	NewPollTimer() Timer
}

// Timer abstracts time.Timer.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

// RealClock uses wall-clock time.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }
func (RealClock) NewTimer(d time.Duration) Timer {
	return &realTimer{t: time.NewTimer(d)}
}

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// Fake is a manually advanced clock. Timers fire only when Advance moves
// fake time to/past their due instant.
type Fake struct {
	mu       sync.Mutex
	now      time.Time
	pending  map[*fakeTimer]struct{}
	pollStep time.Duration
}

// NewFake builds a fake clock anchored at t.
func NewFake(t time.Time) *Fake {
	return &Fake{now: t, pending: map[*fakeTimer]struct{}{}}
}

// SetPollStep sets the interval used by NewPollTimer.
func (f *Fake) SetPollStep(d time.Duration) {
	f.mu.Lock()
	f.pollStep = d
	f.mu.Unlock()
}

// Now returns the fake current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NewTimer creates a timer due after d of fake time.
func (f *Fake) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{fake: f, due: f.now.Add(d), ch: make(chan time.Time, 1)}
	f.pending[t] = struct{}{}
	return t
}

// NewPollTimer creates a periodic poll tick.
func (f *Fake) NewPollTimer() Timer {
	f.mu.Lock()
	step := f.pollStep
	f.mu.Unlock()
	if step <= 0 {
		step = time.Millisecond
	}
	f.mu.Lock()
	t := &fakeTimer{fake: f, due: f.now.Add(step), ch: make(chan time.Time, 1), poll: true, period: step}
	f.pending[t] = struct{}{}
	f.mu.Unlock()
	return t
}

// Advance moves fake time by d and fires every timer that becomes due. Poll
// timers are re-armed automatically for their next period; regular timers are
// removed after firing. Firing happens in due-time order.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)

	var due []*fakeTimer
	for t := range f.pending {
		t.mu.Lock()
		if !t.fired && !t.due.After(f.now) {
			due = append(due, t)
		}
		t.mu.Unlock()
	}
	for i := 1; i < len(due); i++ {
		for j := i; j > 0 && due[j-1].due.After(due[j].due); j-- {
			due[j-1], due[j] = due[j], due[j-1]
		}
	}
	f.mu.Unlock()

	for _, t := range due {
		t.mu.Lock()
		if t.fired {
			t.mu.Unlock()
			continue
		}
		t.fired = true
		// Every fired timer leaves the pending set; the controller resets
		// poll ticks explicitly after consuming them.
		f.mu.Lock()
		delete(f.pending, t)
		f.mu.Unlock()
		// Buffered channel: non-blocking signal.
		select {
		case t.ch <- f.Now():
		default:
		}
		t.mu.Unlock()
	}
}

// Pending reports how many timers are still armed (test observability).
func (f *Fake) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pending)
}

type fakeTimer struct {
	fake   *Fake
	due    time.Time
	period time.Duration
	ch     chan time.Time
	mu     sync.Mutex
	fired  bool
	poll   bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasActive := !t.fired
	t.fired = true
	t.fake.mu.Lock()
	delete(t.fake.pending, t)
	t.fake.mu.Unlock()
	// Drain a previously delivered but unconsumed signal so a subsequent
	// Reset/select sees a clean channel.
	select {
	case <-t.ch:
	default:
	}
	return wasActive
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasActive := !t.fired
	select {
	case <-t.ch:
	default:
	}
	t.fake.mu.Lock()
	t.due = t.fake.now.Add(d)
	t.fired = false
	if t.poll {
		t.period = d
	}
	t.fake.pending[t] = struct{}{}
	t.fake.mu.Unlock()
	return wasActive
}
