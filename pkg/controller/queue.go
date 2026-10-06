package controller

import (
	"sync"
	"time"

	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/clock"
)

// queueItem is a name waiting for its backoff window to elapse.
type queueItem struct {
	name    string
	readyAt time.Time
	timer   clock.Timer
}

// WorkQueue is a dedup-per-key queue. A key is either immediately ready or
// parked on a timer until its retry time. Duplicate adds are coalesced, and
// keys whose backoff scheduling is in flight are temporarily protected from
// watch echoes that would otherwise defeat the wait.
type WorkQueue struct {
	clk clock.Clock

	mu       sync.Mutex
	ready    []string
	readySet map[string]struct{}
	parked   map[string]*queueItem
	// parking marks keys between deciding to park and the timer being
	// installed, closing the race where a Failed-status watch echo is
	// enqueued before AddAfter parks the key.
	parking map[string]struct{}
	notify  chan struct{}
	closed  bool
}

// NewWorkQueue builds a queue driven by the given clock.
func NewWorkQueue(clk clock.Clock) *WorkQueue {
	return &WorkQueue{
		clk:      clk,
		readySet: make(map[string]struct{}),
		parked:   make(map[string]*queueItem),
		parking:  make(map[string]struct{}),
		notify:   make(chan struct{}, 1),
	}
}

// Add enqueues name for immediate processing unless it is parked or a park
// is being scheduled (backoff must be respected).
func (q *WorkQueue) Add(name string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	if _, ok := q.readySet[name]; ok {
		return
	}
	if _, ok := q.parked[name]; ok {
		return
	}
	if _, ok := q.parking[name]; ok {
		return
	}
	q.ready = append(q.ready, name)
	q.readySet[name] = struct{}{}
	q.signal()
}

// AllowEarly forces a parked or about-to-be-parked key ready immediately.
// Used when a same-name resource is recreated: the new incarnation must not
// wait for the old UID's backoff.
func (q *WorkQueue) AllowEarly(name string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	if _, ok := q.readySet[name]; ok {
		return
	}
	delete(q.parking, name)
	if item, ok := q.parked[name]; ok {
		item.timer.Stop()
		delete(q.parked, name)
	} else {
		return
	}
	q.ready = append(q.ready, name)
	q.readySet[name] = struct{}{}
	q.signal()
}

// AddAfter parks name until at least t. Already-ready keys stay ready; an
// existing park is only shortened, never extended.
func (q *WorkQueue) AddAfter(name string, at time.Time) {
	d := at.Sub(q.clk.Now())
	if d <= 0 {
		q.Add(name)
		return
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	if _, ok := q.readySet[name]; ok {
		q.mu.Unlock()
		return
	}
	if existing, ok := q.parked[name]; ok {
		if !at.Before(existing.readyAt) {
			q.mu.Unlock()
			return
		}
		existing.timer.Stop()
		delete(q.parked, name)
	}
	// Reserve the parking slot before unlocking so a concurrent watch echo
	// cannot make the key ready in between.
	q.parking[name] = struct{}{}
	item := &queueItem{name: name, readyAt: at}
	item.timer = q.clk.NewTimer(d)
	q.parked[name] = item
	delete(q.parking, name)
	q.mu.Unlock()

	go func() {
		<-item.timer.C()
		q.promote(item)
	}()
}

func (q *WorkQueue) promote(item *queueItem) {
	q.mu.Lock()
	defer q.mu.Unlock()
	cur, ok := q.parked[item.name]
	if !ok || cur != item {
		return
	}
	delete(q.parked, item.name)
	q.ready = append(q.ready, item.name)
	q.readySet[item.name] = struct{}{}
	q.signal()
}

// Get blocks until an item is ready and returns it.
func (q *WorkQueue) Get() (string, bool) {
	for {
		q.mu.Lock()
		if len(q.ready) > 0 {
			name := q.ready[0]
			q.ready = q.ready[1:]
			delete(q.readySet, name)
			q.mu.Unlock()
			return name, true
		}
		if q.closed {
			q.mu.Unlock()
			return "", false
		}
		ch := q.notify
		q.mu.Unlock()
		<-ch
	}
}

// Close stops parked timers and unblocks Get.
func (q *WorkQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	q.closed = true
	for _, item := range q.parked {
		item.timer.Stop()
	}
	q.parked = map[string]*queueItem{}
	q.parking = map[string]struct{}{}
	q.signal()
}

// Len reports ready and parked key counts (for tests).
func (q *WorkQueue) Len() (ready, parked int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.ready), len(q.parked)
}

func (q *WorkQueue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
