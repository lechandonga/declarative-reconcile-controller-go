// Package external simulates the world outside the controller. Calls stand
// in for cloud API requests: they can be scripted to fail transiently and
// they record every invocation so idempotency is observable in tests.
package external

import (
	"fmt"
	"sync"
)

// Executor is the side-effecting interface driven by the reconciler.
// Implementations must tolerate the same id being applied or deleted
// repeatedly without creating duplicate effects.
type Executor interface {
	// Apply creates the external object identified by id. Applying an
	// existing id is a successful no-op.
	Apply(id string) error
	// Delete removes the external object identified by id. Deleting a
	// missing id is a successful no-op.
	Delete(id string) error
}

// FakeExecutor is an in-memory Executor with scripted failures.
type FakeExecutor struct {
	mu sync.Mutex
	// Live is the set of external objects currently considered present.
	Live map[string]struct{}
	// ApplyFailUntil[id] is a failure count: the first N Apply(id) calls
	// fail, later calls succeed.
	ApplyFailUntil map[string]int
	// DeleteFailUntil[id] fails the first N Delete(id) calls.
	DeleteFailUntil map[string]int
	// Calls records every invocation in order (for test assertions).
	Calls       []Call
	applyTried  map[string]int
	deleteTried map[string]int
}

// Call is one observed executor invocation.
type Call struct {
	Op string // "apply" or "delete"
	ID string
}

// NewFakeExecutor builds an empty executor.
func NewFakeExecutor() *FakeExecutor {
	return &FakeExecutor{
		Live:            map[string]struct{}{},
		ApplyFailUntil:  map[string]int{},
		DeleteFailUntil: map[string]int{},
		applyTried:      map[string]int{},
		deleteTried:     map[string]int{},
	}
}

// FailApplies makes the first n Apply(id) calls fail transiently.
func (f *FakeExecutor) FailApplies(id string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ApplyFailUntil[id] = n
}

// FailDeletes makes the first n Delete(id) calls fail transiently.
func (f *FakeExecutor) FailDeletes(id string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.DeleteFailUntil[id] = n
}

// Apply creates id if absent. Idempotent.
func (f *FakeExecutor) Apply(id string) error {
	f.mu.Lock()
	f.Calls = append(f.Calls, Call{Op: "apply", ID: id})
	f.applyTried[id]++
	if f.applyTried[id] <= f.ApplyFailUntil[id] {
		f.mu.Unlock()
		return fmt.Errorf("simulated transient failure applying %q", id)
	}
	f.Live[id] = struct{}{}
	f.mu.Unlock()
	return nil
}

// Delete removes id if present. Idempotent.
func (f *FakeExecutor) Delete(id string) error {
	f.mu.Lock()
	f.Calls = append(f.Calls, Call{Op: "delete", ID: id})
	f.deleteTried[id]++
	if f.deleteTried[id] <= f.DeleteFailUntil[id] {
		f.mu.Unlock()
		return fmt.Errorf("simulated transient failure deleting %q", id)
	}
	delete(f.Live, id)
	f.mu.Unlock()
	return nil
}

// Snapshot returns a copy of the currently live external IDs.
func (f *FakeExecutor) Snapshot() map[string]struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]struct{}, len(f.Live))
	for id := range f.Live {
		out[id] = struct{}{}
	}
	return out
}

// CallCount returns how many times an op/id pair was invoked.
func (f *FakeExecutor) CallCount(op, id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.Calls {
		if c.Op == op && c.ID == id {
			n++
		}
	}
	return n
}
