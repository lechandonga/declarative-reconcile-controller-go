// Package reconcile contains the convergence logic: given a resource from
// the store, it drives an external Executor toward desired state and writes
// explainable status back. The Controller (controller.go) owns scheduling,
// retries and restart recovery; this package owns one reconcile pass.
package reconcile

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	v1 "github.com/lechandonga/declarative-reconcile-controller-go/pkg/apis/v1"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/external"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/store"
)

// Outcome tells the controller how to (re)schedule the resource.
type Outcome int

const (
	// OutcomeSkipped means another pass for the name was already running.
	OutcomeSkipped Outcome = iota
	// OutcomeStable means desired and observed state match; no retry.
	OutcomeStable
	// OutcomeProgressing means the result was discarded as stale or more
	// passes are needed immediately.
	OutcomeProgressing
	// OutcomeFailed means a transient failure occurred; wait for NextRetry.
	OutcomeFailed
	// OutcomeGone means the resource was physically deleted.
	OutcomeGone
)

// Result is what one Reconcile call produced.
type Result struct {
	Outcome   Outcome
	Version   int64
	NextRetry time.Time
	Summary   string
}

// Reconciler serializes concurrent calls for the same resource name while
// allowing different names to proceed in parallel.
type Reconciler struct {
	store    *store.Store
	executor external.Executor
	now      func() time.Time
	backoff  Backoff
	// beforePass, when set, runs after the single-flight slot is acquired
	// and before any work. Tests use it to force concurrent calls to
	// overlap. Its returned function (if any) runs on the way out.
	beforePass func(name string) func()

	mu      sync.Mutex
	running map[string]struct{}
}

// New builds a Reconciler.
func New(s *store.Store, exec external.Executor, now func() time.Time, b Backoff) *Reconciler {
	if now == nil {
		now = time.Now
	}
	return &Reconciler{store: s, executor: exec, now: now, backoff: b, running: map[string]struct{}{}}
}

// NewWithHook is New plus a test hook invoked inside the single-flight
// section (skipped passes never invoke it).
func NewWithHook(s *store.Store, exec external.Executor, now func() time.Time, b Backoff, hook func(name string) func()) *Reconciler {
	r := New(s, exec, now, b)
	r.beforePass = hook
	return r
}

// IsRunning reports whether a pass for name is in flight.
func (r *Reconciler) IsRunning(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.running[name]
	return ok
}

func (r *Reconciler) acquire(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.running[name]; ok {
		return false
	}
	r.running[name] = struct{}{}
	return true
}

func (r *Reconciler) release(name string) {
	r.mu.Lock()
	delete(r.running, name)
	r.mu.Unlock()
}

// Reconcile performs exactly one pass for the named resource.
func (r *Reconciler) Reconcile(name string) Result {
	if !r.acquire(name) {
		return Result{Outcome: OutcomeSkipped,
			Summary: fmt.Sprintf("%s: skipped, reconcile already in flight", name)}
	}
	defer r.release(name)
	if r.beforePass != nil {
		if after := r.beforePass(name); after != nil {
			defer after()
		}
	}

	res, err := r.store.Get(name)
	if err != nil {
		return Result{Outcome: OutcomeGone, Summary: fmt.Sprintf("%s: gone from store", name)}
	}
	if res.DeletionRequested {
		return r.reconcileDelete(res)
	}
	return r.reconcileNormal(res)
}

func sideEffectID(uid string, index int) string {
	return fmt.Sprintf("%s/instance-%d", uid, index)
}

// reconcileNormal applies missing instances and writes status back through a
// compare-and-swap. A pass is bound to (UID, generation, attempt count), so
// a recreated same-name resource or a newer attempt never receives stale
// writes.
func (r *Reconciler) reconcileNormal(initial *v1.Resource) Result {
	name := initial.Name
	now := r.now()
	snapUID := initial.UID
	snapGen := initial.Generation
	snapAttempts := initial.Status.ReconcileAttempts

	// Recorded IDs are the source of truth. If Apply succeeded but the
	// status write was lost, the idempotent executor just re-applies; the
	// external object is never duplicated.
	recorded := make(map[string]struct{}, len(initial.Status.SideEffectIDs))
	for _, id := range initial.Status.SideEffectIDs {
		recorded[id] = struct{}{}
	}
	applied := append([]string(nil), initial.Status.SideEffectIDs...)

	var failedID string
	var failedErr error
	for i := 0; i < initial.Spec.InstanceCount; i++ {
		id := sideEffectID(snapUID, i)
		if _, ok := recorded[id]; ok {
			continue
		}
		if err := r.executor.Apply(id); err != nil {
			failedID, failedErr = id, err
			break
		}
		applied = append(applied, id)
	}

	committed, err := r.store.Mutate(name, func(live *v1.Resource) (store.MutateResult, error) {
		if live.UID != snapUID || live.Generation != snapGen || live.DeletionRequested {
			return store.MutateResult{Stale: true}, nil
		}
		if live.Status.ReconcileAttempts > snapAttempts {
			return store.MutateResult{Stale: true}, nil
		}
		next := live.Clone()
		next.Status.SideEffectIDs = applied

		if failedErr != nil {
			next.Status.ReconcileAttempts = snapAttempts + 1
			next.Status.Phase = v1.PhaseFailed
			next.Status.ReadyInstances = len(applied)
			next.Status.NextRetryAt = now.Add(r.backoff.Delay(snapAttempts + 1))
			next.Status.SetCondition(v1.Condition{
				Type:               v1.ConditionReconciled,
				Status:             v1.ConditionFalse,
				Reason:             "ApplyError",
				Message:            fmt.Sprintf("apply %s: %v", failedID, failedErr),
				ObservedGeneration: snapGen,
			}, now)
			return store.MutateResult{Resource: next, Changed: true}, nil
		}

		next.Status.Phase = v1.PhaseReady
		next.Status.ReadyInstances = len(applied)
		next.Status.ObservedGeneration = snapGen
		next.Status.NextRetryAt = time.Time{}
		next.Status.SetCondition(v1.Condition{
			Type:               v1.ConditionReconciled,
			Status:             v1.ConditionTrue,
			Reason:             "InSync",
			Message:            fmt.Sprintf("%d instance(s) applied", len(applied)),
			ObservedGeneration: snapGen,
		}, now)
		// Suppress meaningless writes: identical status => no new version.
		return store.MutateResult{Resource: next, Changed: statusChanged(live, next)}, nil
	})
	if err != nil {
		return Result{Outcome: OutcomeProgressing,
			Summary: fmt.Sprintf("%s: stale result discarded (%v)", name, reasonOf(err, failedErr))}
	}

	if failedErr != nil {
		return Result{
			Outcome:   OutcomeFailed,
			Version:   committed.ResourceVersion,
			NextRetry: committed.Status.NextRetryAt,
			Summary: fmt.Sprintf("%s: reconcile failed on %s: %v; backoff retry scheduled (attempt %d)",
				name, failedID, failedErr, committed.Status.ReconcileAttempts),
		}
	}

	if committed.ResourceVersion == initial.ResourceVersion {
		return Result{Outcome: OutcomeStable, Version: committed.ResourceVersion,
			Summary: fmt.Sprintf("%s: already in sync, no write", name)}
	}
	return Result{Outcome: OutcomeStable, Version: committed.ResourceVersion,
		Summary: fmt.Sprintf("%s: reconciled, %d instance(s) ready",
			name, committed.Status.ReadyInstances)}
}

// reconcileDelete removes external effects in reverse order, persisting
// every success before continuing. Hence a crashed/retried pass never
// deletes an effect twice, and the resource cannot disappear until every
// effect is gone and the finalizer is released.
func (r *Reconciler) reconcileDelete(initial *v1.Resource) Result {
	name := initial.Name
	now := r.now()
	snapUID := initial.UID
	snapAttempts := initial.Status.CleanupAttempts

	remaining := append([]string(nil), initial.Status.SideEffectIDs...)
	var failedID string
	var failedErr error
	for len(remaining) > 0 {
		id := remaining[len(remaining)-1]
		if err := r.executor.Delete(id); err != nil {
			failedID, failedErr = id, err
			break
		}
		remaining = remaining[:len(remaining)-1]
	}

	committed, err := r.store.Mutate(name, func(live *v1.Resource) (store.MutateResult, error) {
		if live.UID != snapUID || !live.DeletionRequested {
			return store.MutateResult{Stale: true}, nil
		}
		if live.Status.CleanupAttempts > snapAttempts {
			return store.MutateResult{Stale: true}, nil
		}
		next := live.Clone()
		next.Status.SideEffectIDs = append([]string(nil), remaining...)
		next.Status.Phase = v1.PhaseTerminating

		if failedErr != nil {
			next.Status.CleanupAttempts = snapAttempts + 1
			next.Status.NextRetryAt = now.Add(r.backoff.Delay(snapAttempts + 1))
			next.Status.SetCondition(v1.Condition{
				Type:               v1.ConditionCleanedUp,
				Status:             v1.ConditionFalse,
				Reason:             "DeleteError",
				Message:            fmt.Sprintf("delete %s: %v", failedID, failedErr),
				ObservedGeneration: live.Generation,
			}, now)
			return store.MutateResult{Resource: next, Changed: true}, nil
		}

		next.Status.NextRetryAt = time.Time{}
		next.Status.ReadyInstances = 0
		next.Status.SetCondition(v1.Condition{
			Type:               v1.ConditionCleanedUp,
			Status:             v1.ConditionTrue,
			Reason:             "Cleaned",
			Message:            "all external effects removed",
			ObservedGeneration: live.Generation,
		}, now)
		// All effects gone: release our finalizer in the same write.
		finalizers := make([]string, 0, len(next.Finalizers))
		for _, f := range next.Finalizers {
			if f != v1.FinalizerController {
				finalizers = append(finalizers, f)
			}
		}
		next.Finalizers = finalizers
		changed := statusChanged(live, next) || len(finalizers) != len(live.Finalizers)
		return store.MutateResult{Resource: next, Changed: changed}, nil
	})
	if err != nil {
		return Result{Outcome: OutcomeProgressing,
			Summary: fmt.Sprintf("%s: stale cleanup result discarded", name)}
	}

	if failedErr != nil {
		return Result{
			Outcome:   OutcomeFailed,
			Version:   committed.ResourceVersion,
			NextRetry: committed.Status.NextRetryAt,
			Summary: fmt.Sprintf("%s: cleanup failed on %s: %v; backoff retry scheduled (attempt %d, %d effect(s) left)",
				name, failedID, failedErr, committed.Status.CleanupAttempts, len(remaining)),
		}
	}

	if err := r.store.Remove(name); err != nil {
		return Result{Outcome: OutcomeProgressing,
			Summary: fmt.Sprintf("%s: physical remove pending: %v", name, err)}
	}
	return Result{Outcome: OutcomeGone,
		Summary: fmt.Sprintf("%s: cleanup complete, physically deleted", name)}
}

// statusChanged reports whether controller-owned status differs between two
// snapshots. It is the gate that prevents meaningless repeated writes.
func statusChanged(live, next *v1.Resource) bool {
	a, b := live.Status, next.Status
	if a.Phase != b.Phase ||
		a.ReadyInstances != b.ReadyInstances ||
		a.ObservedGeneration != b.ObservedGeneration ||
		a.ReconcileAttempts != b.ReconcileAttempts ||
		a.CleanupAttempts != b.CleanupAttempts ||
		!a.NextRetryAt.Equal(b.NextRetryAt) ||
		!reflect.DeepEqual(a.SideEffectIDs, b.SideEffectIDs) ||
		!reflect.DeepEqual(live.Finalizers, next.Finalizers) {
		return true
	}
	return conditionsChanged(a.Conditions, b.Conditions)
}

func conditionsChanged(a, b []v1.Condition) bool {
	if len(a) != len(b) {
		return true
	}
	for i := range a {
		ca, cb := a[i], b[i]
		if ca.Type != cb.Type || ca.Status != cb.Status || ca.Reason != cb.Reason ||
			ca.Message != cb.Message || ca.ObservedGeneration != cb.ObservedGeneration {
			return true
		}
	}
	return false
}

func reasonOf(writeErr, execErr error) error {
	if execErr != nil {
		return execErr
	}
	return writeErr
}
