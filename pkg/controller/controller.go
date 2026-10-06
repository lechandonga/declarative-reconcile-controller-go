// Package controller wires the store watch, work queue and reconciler into a
// restartable control loop. It triggers reconciliation only for resource
// lifecycle changes, applies exponential backoff on failure, and recovers
// unfinished resources by relisting on startup (and periodically).
package controller

import (
	"context"
	"log"
	"time"

	v1 "github.com/lechandonga/declarative-reconcile-controller-go/pkg/apis/v1"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/clock"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/reconcile"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/store"
)

// Controller runs the control loop.
type Controller struct {
	store       *store.Store
	rec         *reconcile.Reconciler
	clk         clock.Clock
	queue       *WorkQueue
	log         *log.Logger
	resyncEvery time.Duration

	cancelWatch context.CancelFunc
	workerDone  chan struct{}
}

// Config configures a Controller.
type Config struct {
	Store      *store.Store
	Reconciler *reconcile.Reconciler
	Clock      clock.Clock
	Logger     *log.Logger
	// ResyncEvery, when > 0, periodically re-enqueues all resources as the
	// safety net for dropped watch events.
	ResyncEvery time.Duration
}

// New constructs a Controller (not yet running).
func New(cfg Config) *Controller {
	clk := cfg.Clock
	if clk == nil {
		clk = clock.RealClock{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}
	resync := cfg.ResyncEvery
	if resync <= 0 {
		resync = 10 * time.Second
	}
	return &Controller{
		store:       cfg.Store,
		rec:         cfg.Reconciler,
		clk:         clk,
		queue:       NewWorkQueue(clk),
		log:         logger,
		resyncEvery: resync,
	}
}

// Start performs the startup relist (restart recovery) and launches the
// worker and watcher goroutines.
func (c *Controller) Start(ctx context.Context) {
	// Subscribe synchronously before returning so no event published after
	// Start can be missed; the relist below covers the list/watch gap.
	events, cancelWatch := c.store.Watch(256)
	c.relistForRecovery("startup")

	watchCtx, cancel := context.WithCancel(ctx)
	c.cancelWatch = func() {
		cancel()
		cancelWatch()
	}
	c.workerDone = make(chan struct{})

	go c.runWatch(watchCtx, events)
	go c.runResync(watchCtx)
	go c.runWorker(watchCtx)
}

// Stop terminates the loop. It is safe to Start again afterwards.
func (c *Controller) Stop() {
	if c.cancelWatch != nil {
		c.cancelWatch()
	}
	c.queue.Close()
	if c.workerDone != nil {
		<-c.workerDone
	}
}

// relistForRecovery enqueues every resource that is not already converged:
// this is what lets a freshly started (or restarted) controller pick up
// unfinished work purely from persisted state.
func (c *Controller) relistForRecovery(reason string) {
	for _, r := range c.store.List() {
		if needsReconcile(r) {
			c.log.Printf("[recovery:%s] enqueue %q (phase=%s, deletion=%v, ready=%d/%d)",
				reason, r.Name, r.Status.Phase, r.DeletionRequested,
				r.Status.ReadyInstances, r.Spec.InstanceCount)
			c.enqueue(r, reason)
		}
	}
}

func needsReconcile(r *v1.Resource) bool {
	if r.DeletionRequested {
		return true
	}
	if r.Status.Phase != v1.PhaseReady {
		return true
	}
	if r.Status.ObservedGeneration != r.Generation {
		return true
	}
	return r.Status.ReadyInstances != r.Spec.InstanceCount
}

func (c *Controller) runWatch(ctx context.Context, events <-chan store.Event) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			switch ev.Type {
			case store.EventCreated:
				c.log.Printf("[watch] created %q (rv=%d)", ev.Name, ev.Resource.ResourceVersion)
				// A fresh incarnation must never wait for a previous
				// incarnation's parked backoff.
				c.queue.AllowEarly(ev.Name)
				c.enqueue(ev.Resource, "watch:create")
			case store.EventUpdated:
				// Only resource-relevant changes trigger a pass. Status-only
				// writes from our own reconcile converge or record failure;
				// requeuing them would cause pointless loops.
				if c.isResourceChange(ev) {
					c.log.Printf("[watch] resource change %q (rv=%d, deletion=%v)",
						ev.Name, ev.Resource.ResourceVersion, ev.Resource.DeletionRequested)
					c.enqueue(ev.Resource, "watch:update")
				}
			case store.EventDeleted:
				c.log.Printf("[watch] deleted %q", ev.Name)
			}
		}
	}
}

// isResourceChange distinguishes user/deletion/finalizer changes from pure
// status heartbeats. We compare against nothing historical here: the event
// carries the post-state, so a resource change is a deletion request, a
// non-ready/failed state needing progression, or a generation ahead of the
// observed one. Pure successful status writes (Ready, observed generation
// current, no deletion) are ignored.
func (c *Controller) isResourceChange(ev store.Event) bool {
	r := ev.Resource
	if r.DeletionRequested {
		return true
	}
	if r.Generation != r.Status.ObservedGeneration {
		return true
	}
	if r.Status.Phase == v1.PhaseFailed {
		return true
	}
	if r.Status.Phase == v1.PhaseTerminating {
		return true
	}
	return false
}

func (c *Controller) runResync(ctx context.Context) {
	ticker := c.clk.NewTimer(c.resyncEvery)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C():
			c.relistForRecovery("resync")
			ticker.Reset(c.resyncEvery)
		}
	}
}

func (c *Controller) runWorker(ctx context.Context) {
	defer close(c.workerDone)
	for {
		name, ok := c.queue.Get()
		if !ok {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		c.process(ctx, name)
	}
}

func (c *Controller) process(ctx context.Context, name string) {
	res := c.rec.Reconcile(name)
	switch res.Outcome {
	case reconcile.OutcomeStable, reconcile.OutcomeGone, reconcile.OutcomeSkipped:
		c.log.Printf("[reconcile] %s", res.Summary)
	case reconcile.OutcomeProgressing:
		c.log.Printf("[reconcile] %s; requeue now", res.Summary)
		c.queue.Add(name)
	case reconcile.OutcomeFailed:
		c.log.Printf("[reconcile] %s", res.Summary)
		// Bind the delayed requeue to the current UID so a same-name
		// recreated object gets immediate watch-triggered treatment. The
		// key is parked exactly once: watch echoes of our own Failed status
		// write must not create an immediate duplicate reconcile.
		c.queue.AddAfter(name, res.NextRetry)
	}
}

// enqueue decides immediate vs delayed: resources waiting inside their
// backoff window stay parked; everything else runs at once.
func (c *Controller) enqueue(r *v1.Resource, reason string) {
	if !r.Status.NextRetryAt.IsZero() && r.Status.NextRetryAt.After(c.clk.Now()) {
		c.log.Printf("[schedule] %q parked until %s (%s)", r.Name,
			r.Status.NextRetryAt.Format("15:04:05.000"), reason)
		c.queue.AddAfter(r.Name, r.Status.NextRetryAt)
		return
	}
	c.queue.Add(r.Name)
}

// TriggerOnce enqueues a name directly (used by tests/tools).
func (c *Controller) TriggerOnce(name string) { c.queue.Add(name) }

// QueueLen exposes queue sizes for test assertions.
func (c *Controller) QueueLen() (ready, parked int) { return c.queue.Len() }
