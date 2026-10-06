// Package controller implements the declare->reconcile loop over the
// in-process store:
//
//   - at most one reconcile per resource at any instant (per-key singleflight),
//   - exponential backoff on failure (driven by an injectable clock),
//   - idempotent side effects keyed by resource UID and generation,
//   - soft delete with finalizer-based cleanup,
//   - resumption from persisted status after a restart.
package controller

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/lechandonga/declarative-reconcile-controller-go/api"
	"github.com/lechandonga/declarative-reconcile-controller-go/clock"
	"github.com/lechandonga/declarative-reconcile-controller-go/store"
)

// Reconciler drives the external world for one resource.
type Reconciler interface {
	// Apply creates or updates the side effect. Must be idempotent.
	Apply(r *api.Resource) error
	// Cleanup tears the side effect down. Must be idempotent.
	Cleanup(r *api.Resource) error
}

// Config holds controller tuning.
type Config struct {
	Store       *store.Store
	Reconciler  Reconciler
	Clock       clock.Clock
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// PollInterval is how often the scheduler re-evaluates backoff
	// deadlines. Defaults to 1ms; fake-clock tests set it equal to the base
	// backoff unit for deterministic stepping.
	PollInterval time.Duration
	Logger       *log.Logger
	// BeforeApply, when set, runs immediately before each external Apply.
	// Tests use it to force a concurrent mutation while an attempt is in
	// flight; production wiring leaves it nil.
	BeforeApply func(r *api.Resource)
	// SyncPoll, when true together with a clock.PollScheduler, runs due
	// reconciles synchronously inside the fake clock's Advance call instead
	// of on worker goroutines. This makes test scheduling deterministic.
	SyncPoll bool
}

// Controller watches the store and reconciles resources.
type Controller struct {
	st          *store.Store
	rec         Reconciler
	clk         clock.Clock
	base        time.Duration
	max         time.Duration
	tick        time.Duration
	log         *log.Logger
	beforeApply func(r *api.Resource)

	events   chan api.Event
	trigger  chan string
	results  chan result
	pumpReq  chan struct{}
	schedMu  sync.Mutex // serializes pump in sync mode
	stop     chan struct{}
	done     chan struct{}
	started  bool
	syncPoll bool

	running map[string]bool
	queued  map[string]bool
	retryAt map[string]time.Time // next eligible instant per failing key
}

type result struct {
	key      string
	failed   bool
	deadline time.Time
	failures int
	wait     time.Duration
}

// New builds a controller.
func New(c Config) *Controller {
	clk := c.Clock
	if clk == nil {
		clk = clock.RealClock{}
	}
	logger := c.Logger
	if logger == nil {
		logger = log.Default()
	}
	base := c.BaseBackoff
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	maxB := c.MaxBackoff
	if maxB <= 0 {
		maxB = 5 * time.Second
	}
	tick := time.Millisecond
	if c.PollInterval > 0 {
		tick = c.PollInterval
	}
	ctrl := &Controller{
		st:          c.Store,
		rec:         c.Reconciler,
		clk:         clk,
		base:        base,
		max:         maxB,
		tick:        tick,
		log:         logger,
		beforeApply: c.BeforeApply,
		events:      make(chan api.Event, 256),
		trigger:     make(chan string, 256),
		results:     make(chan result, 256),
		pumpReq:     make(chan struct{}, 1),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		syncPoll:    c.SyncPoll,
		running:     map[string]bool{},
		queued:      map[string]bool{},
		retryAt:     map[string]time.Time{},
	}
	return ctrl
}

// Start subscribes to the store, performs a startup relist (restart
// recovery) and runs the scheduler until Stop.
func (c *Controller) Start() {
	c.started = true
	if c.syncPoll {
		// Deterministic mode: store mutations enqueue synchronously; the
		// pump is driven by fake-clock Advance and an explicit Idle.
		c.st.AddHook(func(e api.Event) {
			if e.Type == api.EventDeleted {
				return
			}
			// In sync mode the pump re-reads state directly, so only spec
			// mutations/deletions need enqueueing; status writes are the
			// controller's own and must not perturb backoff bookkeeping.
			if e.Type != api.EventStatusUpdated {
				c.trigger <- e.Resource.Key()
			}
		})
	} else {
		c.st.Watch(c.events)
		go c.forwardEvents()
	}

	// Restart recovery: no in-memory reconcile state survives a restart, so
	// the persisted status block is the source of truth. Every resource that
	// is not fully converged (including terminating ones) is re-enqueued.
	for _, r := range c.st.List() {
		if needsWork(r) {
			c.log.Printf("startup recovery: requeueing %s (phase=%s gen=%d observed=%d deleting=%t)",
				r.Key(), r.Status.Phase, r.Generation, r.Status.ObservedGeneration, r.DeletionTimestamp != nil)
			c.enqueue(r.Key())
		}
	}

	go c.forwardEvents()
	go c.schedule()
}

// Stop halts the scheduler. In-flight reconciles finish on their own.
func (c *Controller) Stop() {
	if !c.started {
		return
	}
	close(c.stop)
	<-c.done
}

// Enqueue requests a reconcile for kind/name. It is the same path watch
// events take and is safe to call for an already-converged resource: the
// reconcile then performs no external call and no write.
func (c *Controller) Enqueue(kind, name string) {
	c.enqueue(kind + "/" + name)
}

// Pump performs one deterministic scheduler step on the scheduler
// goroutine. Intended for fake-clock/sync-poll tests.
func (c *Controller) Pump() {
	if c.syncPoll {
		c.schedMu.Lock()
		defer c.schedMu.Unlock()
		c.pump()
		return
	}
	select {
	case c.pumpReq <- struct{}{}:
	case <-c.stop:
	}
}

func needsWork(r *api.Resource) bool {
	if r.DeletionTimestamp != nil {
		return true
	}
	return r.Status.Phase != api.PhaseSucceeded ||
		r.Status.ObservedGeneration != r.Generation ||
		r.Status.AppliedToken != sideEffectToken(r)
}

func (c *Controller) forwardEvents() {
	for {
		select {
		case <-c.stop:
			return
		case e := <-c.events:
			if e.Type == api.EventDeleted {
				// Physically removed: nothing to reconcile.
				continue
			}
			// Only resource-related mutations can trigger reconcile.
			// Status-only writes that already mean "converged" must not
			// retrigger a loop (avoids self-generated hot loops).
			if e.Type != api.EventStatusUpdated || needsWork(e.Resource) {
				c.enqueue(e.Resource.Key())
			}
		}
	}
}

func (c *Controller) enqueue(key string) {
	select {
	case c.trigger <- key:
	case <-c.stop:
	}
}

// schedule is the single goroutine allowed to mutate running/queued/retryAt.
// Reconciles themselves run on worker goroutines; singleflight per key is
// enforced here.
//
// Backoff uses one ticking timer (a 1ms real tick, or any interval chosen
// by the caller via NewTimer). On every tick, trigger and result the
// scheduler starts every key whose backoff deadline has elapsed. This is a
// deliberately simple polling model: correctness never depends on the exact
// delivery of a one-shot timer signal, which keeps the fake-clock tests
// deterministic.
func (c *Controller) schedule() {
	if c.syncPoll {
		// Sync mode is driven entirely by Pump (tests). Just wait for stop;
		// pump() runs under schedMu so it can never overlap itself.
		<-c.stop
		close(c.done)
		return
	}
	tick := c.newPollTimer()
	tickStep := c.tick
	resetTick := func() { tick.Reset(tickStep) }

	for {
		select {
		case <-c.stop:
			tick.Stop()
			close(c.done)
			return
		case key := <-c.trigger:
			c.pumpTrigger(key)
		case res := <-c.results:
			c.pumpResult(res)
		case <-tick.C():
			c.pump()
			resetTick()
		case <-c.pumpReq:
			c.pump()
		}
	}
}

// drainTriggers processes every currently-queued trigger key. Caller runs on
// the scheduler goroutine (or synchronously via poll callback).
func (c *Controller) drainTriggers() {
	for {
		select {
		case key := <-c.trigger:
			c.handleTrigger(key)
		default:
			return
		}
	}
}

// startDueLocked launches every key whose backoff deadline has elapsed.
func (c *Controller) startDue() {
	now := c.clk.Now()
	for key, at := range c.retryAt {
		if !at.After(now) && !c.running[key] {
			delete(c.retryAt, key)
			c.queued[key] = false
			c.runKey(key)
		}
	}
}

func (c *Controller) pumpTrigger(key string) {
	c.handleTrigger(key)
	c.afterStep()
}

func (c *Controller) pumpResult(res result) {
	c.handleResult(res)
	c.afterStep()
}

// pump is the deterministic scheduler step: consume triggers, process any
// already-finished results, then start due backoff work. In sync-poll mode
// runKey executes inline and feeds its result back into the same call, so a
// single Advance fully settles the system.
func (c *Controller) pump() {
	c.drainTriggers()
	c.drainResults()
	c.startDue()
	if c.syncPoll {
		c.drainTriggers()
		c.drainResults()
		c.startDue()
	}
}

func (c *Controller) afterStep() {
	c.drainResults()
	c.startDue()
}

func (c *Controller) drainResults() {
	for {
		select {
		case res := <-c.results:
			c.handleResult(res)
		default:
			return
		}
	}
}

// runKey starts one reconcile for key, honoring singleflight. In sync-poll
// mode it runs inline; otherwise it spawns a worker goroutine.
func (c *Controller) runKey(key string) {
	c.running[key] = true
	c.queued[key] = false
	if !c.syncPoll {
		go func() {
			res := result{key: key}
			res.failed = c.reconcileOneWithResult(&res)
			select {
			case c.results <- res:
			case <-c.stop:
			}
		}()
		return
	}
	res := result{key: key}
	res.failed = c.reconcileOneWithResult(&res)
	c.handleResult(res)
}

// newPollTimer returns the scheduler's periodic tick: a 1ms Fake poll tick
// under the fake clock, or a normal 1ms timer against wall time.
func (c *Controller) newPollTimer() clock.Timer {
	if p, ok := c.clk.(clock.PollScheduler); ok {
		return p.NewPollTimer()
	}
	return c.clk.NewTimer(c.tick)
}

// handleTrigger processes a watch/API trigger. Scheduler-only.
func (c *Controller) handleTrigger(key string) {
	if c.running[key] {
		// Coalesce: one rerun after the current attempt catches the newest
		// state. Guarantees at most one concurrent reconcile per resource
		// while never dropping a change.
		c.queued[key] = true
		return
	}
	if at, waiting := c.retryAt[key]; waiting && c.clk.Now().Before(at) {
		// Failure backoff cannot be bypassed by a busy event stream.
		c.queued[key] = true
		return
	}
	delete(c.retryAt, key)
	c.queued[key] = false
	c.runKey(key)
}

// handleResult applies a finished attempt's bookkeeping. Scheduler-only.
func (c *Controller) handleResult(res result) {
	key := res.key
	c.running[key] = false
	if res.failed {
		// Backoff info travels on the same message as the result, so it can
		// never be ordered against a timer re-arm.
		if res.wait > 0 {
			c.retryAt[key] = res.deadline
			c.log.Printf("backoff: %s attempt #%d scheduled in %s", key, res.failures+1, res.wait)
		}
		// The backoff timer is the single retry trigger; changes that
		// arrived mid-attempt are picked up by re-reading state at retry.
		c.queued[key] = false
		return
	}
	delete(c.retryAt, key)
	// A coalesced change during a successful attempt is handled
	// immediately; a failed attempt waits for its backoff timer.
	if c.queued[key] {
		c.queued[key] = false
		c.runKey(key)
	}
}

// requeueAfter fills the backoff fields of a worker's result. Called from a
// worker goroutine before it sends its result.
func (c *Controller) fillBackoff(res *result, failures int) {
	wait := c.base
	for i := 1; i < failures; i++ {
		wait *= 2
		if wait >= c.max {
			wait = c.max
			break
		}
	}
	res.failures = failures
	res.wait = wait
	res.deadline = c.clk.Now().Add(wait)
}

// reconcileOne performs exactly one reconcile attempt. Returns true when the
// attempt failed and backoff is required.
func (c *Controller) reconcileOneWithResult(res *result) bool {
	key := res.key
	kind, name := splitKey(key)
	r, err := c.st.Get(kind, name)
	if err != nil {
		return false // physically gone, nothing to do
	}
	if r.DeletionTimestamp != nil {
		return c.reconcileDelete(res, r)
	}
	return c.reconcileApply(res, r)
}

func (c *Controller) reconcileApply(res *result, r *api.Resource) bool {
	key := r.Key()
	token := sideEffectToken(r)

	// Converged for this generation: no external call and no write.
	if r.Status.Phase == api.PhaseSucceeded &&
		r.Status.ObservedGeneration == r.Generation &&
		r.Status.AppliedToken == token {
		c.log.Printf("reconcile: %s already converged at generation %d, no-op", key, r.Generation)
		return false
	}

	// Mark "in progress", but only on a phase transition so retries do not
	// produce repeated meaningless writes.
	if r.Status.Phase != api.PhaseReconciling {
		next := r.Status
		next.Phase = api.PhaseReconciling
		next.Message = "applying desired state for generation " + fmt.Sprint(r.Generation)
		updated, changed, err := c.st.PatchStatus(r.Kind, r.Name, r.ResourceVersion, next)
		if err != nil {
			if stale(c, key, err) {
				return false
			}
			c.log.Printf("reconcile: %s status write failed: %v", key, err)
		} else if changed {
			r = updated
		}
	}

	// Apply unless a prior attempt already applied exactly this side effect
	// (apply may have succeeded externally while its status write raced with
	// a spec update). This prevents duplicate side effects on retries.
	if r.Status.AppliedToken != token {
		c.log.Printf("reconcile: %s applying side effect %q (generation %d)", key, token, r.Generation)
		if c.beforeApply != nil {
			c.beforeApply(r)
			// A mutation may have happened; refresh and verify liveness.
			fresh, err := c.st.Get(r.Kind, r.Name)
			if err != nil {
				return false
			}
			if fresh.ResourceVersion != r.ResourceVersion {
				c.log.Printf("reconcile: %s resource changed mid-attempt; discarding result for generation %d",
					key, r.Generation)
				c.enqueue(key)
				return false
			}
		}
		if err := c.rec.Apply(r); err != nil {
			return c.writeApplyFailure(res, r, err)
		}
	} else {
		c.log.Printf("reconcile: %s side effect %q already applied, skipping duplicate call", key, token)
	}

	// Success. CAS on the resource version captured before the side effect:
	// a conflict means a newer state exists and this success is obsolete.
	now := c.clk.Now()
	next := api.Status{
		Phase:              api.PhaseSucceeded,
		Message:            "converged at generation " + fmt.Sprint(r.Generation),
		ObservedGeneration: r.Generation,
		AppliedToken:       token,
		LastAttemptAt:      &now,
	}
	if _, _, err := c.st.PatchStatus(r.Kind, r.Name, r.ResourceVersion, next); err != nil {
		if stale(c, key, err) {
			// A newer generation exists. It already generated a watch event,
			// but enqueue defensively in case that event was coalesced while
			// this attempt was running. No backoff: the apply itself worked.
			c.enqueue(key)
			return false
		}
		c.log.Printf("reconcile: %s success status write failed: %v", key, err)
		c.fillBackoff(res, 1)
		return true
	}
	c.log.Printf("reconcile: %s converged at generation %d", key, r.Generation)
	return false
}

func (c *Controller) writeApplyFailure(res *result, r *api.Resource, applyErr error) bool {
	key := r.Key()
	failures := r.Status.ReconcileFailures + 1
	now := c.clk.Now()
	next := r.Status
	next.Phase = api.PhaseFailed
	next.Message = applyErr.Error()
	next.ReconcileFailures = failures
	next.LastAttemptAt = &now
	if _, _, err := c.st.PatchStatus(r.Kind, r.Name, r.ResourceVersion, next); err != nil {
		if stale(c, key, err) {
			// Newer state exists; retry it directly under backoff rather
			// than silently dropping the failure accounting.
			c.fillBackoff(res, failures)
			return true
		}
		c.log.Printf("reconcile: %s failure status write failed: %v", key, err)
	}
	c.log.Printf("reconcile: %s apply failed: %v", key, applyErr)
	c.fillBackoff(res, failures)
	return true
}

func (c *Controller) reconcileDelete(res *result, r *api.Resource) bool {
	key := r.Key()

	if !hasFinalizer(r, api.FinalizerManaged) {
		return false // nothing left to clean
	}

	if r.Status.Phase != api.PhaseTerminating {
		next := r.Status
		next.Phase = api.PhaseTerminating
		next.Message = "cleaning up before deletion"
		updated, _, err := c.st.PatchStatus(r.Kind, r.Name, r.ResourceVersion, next)
		if err != nil {
			if stale(c, key, err) {
				return false
			}
		} else {
			r = updated
		}
	}

	// Cleanup targets the token that was actually applied for this object.
	// Because the token embeds the UID, a same-named recreated resource
	// (new UID) can never be touched by an old resource's cleanup. A
	// repeated cleanup after external success is a safe no-op in the
	// external system.
	token := r.Status.AppliedToken
	if token == "" {
		token = sideEffectToken(r)
	}
	c.log.Printf("reconcile-delete: %s cleaning up side effect %q", key, token)
	if err := c.rec.Cleanup(r); err != nil {
		failures := r.Status.CleanupFailures + 1
		now := c.clk.Now()
		next := r.Status
		next.Phase = api.PhaseCleanupFailed
		next.Message = err.Error()
		next.CleanupFailures = failures
		next.LastAttemptAt = &now
		if _, _, perr := c.st.PatchStatus(r.Kind, r.Name, r.ResourceVersion, next); perr != nil {
			if stale(c, key, perr) {
				return false
			}
		}
		c.log.Printf("reconcile-delete: %s cleanup failed: %v", key, err)
		c.fillBackoff(res, failures)
		return true
	}

	// Cleanup succeeded: remove the finalizer. With the deletion timestamp
	// set and no finalizers left, the store physically deletes the object in
	// this same step. Until that instant the resource is still listed and
	// reads as Terminating - never "deleted".
	if _, _, err := c.st.RemoveFinalizer(r.Kind, r.Name, api.FinalizerManaged, r.ResourceVersion); err != nil {
		if stale(c, key, err) {
			return false
		}
		c.log.Printf("reconcile-delete: %s finalizer removal failed: %v", key, err)
		c.fillBackoff(res, 1)
		return true
	}
	c.log.Printf("reconcile-delete: %s fully cleaned up and removed", key)
	return false
}

// stale reports whether err is a version conflict: the attempt result is then
// obsolete, because a watch event already enqueued the newer state.
func stale(c *Controller, key string, err error) bool {
	var cf *store.ConflictError
	if errors.As(err, &cf) {
		c.log.Printf("reconcile: %s stale result discarded (%s)", key, cf.Error())
		return true
	}
	return false
}

// sideEffectToken identifies the desired external side effect of a resource.
// UID scopes it to the exact object; generation distinguishes spec versions
// of the same object.
func sideEffectToken(r *api.Resource) string {
	return r.UID + "#g" + fmt.Sprint(r.Generation)
}

func splitKey(key string) (string, string) {
	kind, name, ok := strings.Cut(key, "/")
	if !ok {
		return key, ""
	}
	return kind, name
}

func hasFinalizer(r *api.Resource, f string) bool {
	for _, x := range r.Finalizers {
		if x == f {
			return true
		}
	}
	return false
}
