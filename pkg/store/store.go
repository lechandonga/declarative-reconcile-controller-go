// Package store provides an in-process, thread-safe resource store with
// optimistic concurrency control, versioning, finalizers and watch events.
//
// Every successful mutation bumps ResourceVersion and emits an Event. The
// store deliberately has no external dependencies: it is the "cluster" that
// the controller drives toward desired state.
package store

import (
	"fmt"
	"sync"

	v1 "github.com/lechandonga/declarative-reconcile-controller-go/pkg/apis/v1"
)

// EventType identifies what happened to a resource.
type EventType string

const (
	EventCreated EventType = "Created"
	EventUpdated EventType = "Updated"
	EventDeleted EventType = "Deleted"
)

// Event is delivered to watchers after a committed mutation.
type Event struct {
	Type     EventType
	Name     string
	Resource *v1.Resource
	// OldVersion is the resourceVersion before this mutation (0 on create).
	OldVersion int64
}

type watcher struct {
	ch chan Event
}

// Store is safe for concurrent use.
type Store struct {
	mu       sync.RWMutex
	data     map[string]*v1.Resource
	version  int64
	uidSeq   int64
	watchers map[*watcher]struct{}
}

// New creates an empty store.
func New() *Store {
	return &Store{
		data:     make(map[string]*v1.Resource),
		watchers: make(map[*watcher]struct{}),
	}
}

// Create inserts a new resource, attaching the controller finalizer and
// assigning generation/version 1 and a unique UID.
func (s *Store) Create(name string, spec v1.Spec) (*v1.Resource, error) {
	if err := v1.ValidateCreate(name, spec); err != nil {
		return nil, mapValidation(name, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[name]; ok {
		return nil, &Error{Reason: ReasonAlreadyExists, Name: name}
	}
	s.version++
	s.uidSeq++
	r := &v1.Resource{
		Name:            name,
		Generation:      1,
		ResourceVersion: s.version,
		UID:             fmt.Sprintf("uid-%d", s.uidSeq),
		Spec:            spec,
		Finalizers:      []string{v1.FinalizerController},
		Status: v1.Status{
			Phase: v1.PhasePending,
		},
	}
	s.data[name] = r
	s.emitLocked(Event{Type: EventCreated, Name: name, Resource: r.Clone()})
	return r.Clone(), nil
}

// Get returns a deep copy of a resource.
func (s *Store) Get(name string) (*v1.Resource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.data[name]
	if !ok {
		return nil, &Error{Reason: ReasonNotFound, Name: name}
	}
	return r.Clone(), nil
}

// List returns deep copies of all live resources in name order.
func (s *Store) List() []*v1.Resource {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*v1.Resource, 0, len(s.data))
	for _, r := range s.data {
		out = append(out, r.Clone())
	}
	return out
}

// UpdateSpec applies a user spec update. The caller must present the
// resourceVersion it last saw; a mismatch is a Conflict. Immutable fields
// may not change and a deleting resource accepts no user updates.
func (s *Store) UpdateSpec(name string, expectedVersion int64, spec v1.Spec) (*v1.Resource, error) {
	if err := v1.ValidateSpec(spec); err != nil {
		return nil, mapValidation(name, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.data[name]
	if !ok {
		return nil, &Error{Reason: ReasonNotFound, Name: name}
	}
	if r.DeletionRequested {
		return nil, &Error{Reason: ReasonDeleting, Name: name, Detail: "spec updates are not allowed"}
	}
	if r.ResourceVersion != expectedVersion {
		return nil, &Error{
			Reason:         ReasonConflict,
			Name:           name,
			CurrentVersion: r.ResourceVersion,
		}
	}
	if spec.DisplayName != r.Spec.DisplayName {
		return nil, &Error{Reason: ReasonImmutable, Name: name,
			Field: "spec.displayName", Detail: "field cannot be changed after creation"}
	}
	if spec.InstanceCount != r.Spec.InstanceCount {
		return nil, &Error{Reason: ReasonImmutable, Name: name,
			Field: "spec.instanceCount", Detail: "field cannot be changed after creation"}
	}
	// Spec is identical: no mutation, no new version, no event.
	return r.Clone(), nil
}

// Mutate performs a controller-side compare-and-swap. fn receives the live
// resource and returns the desired mutation result:
//
//   - ok=true, err=nil  -> persist the returned resource as the new version;
//   - changed=false     -> suppress the write (still return current copy);
//   - stale=true        -> reject as Conflict (live UID/version moved on);
//   - err != nil        -> reject without writing.
//
// fn runs under the store lock, so it must be small and never block on
// controller I/O.
func (s *Store) Mutate(name string, fn func(live *v1.Resource) (MutateResult, error)) (*v1.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.data[name]
	if !ok {
		return nil, &Error{Reason: ReasonNotFound, Name: name}
	}
	res, err := fn(r.Clone())
	if err != nil {
		return nil, err
	}
	if res.Stale {
		return nil, &Error{Reason: ReasonConflict, Name: name, CurrentVersion: r.ResourceVersion}
	}
	if !res.Changed {
		return r.Clone(), nil
	}
	next := res.Resource
	if next == nil {
		return nil, fmt.Errorf("store: Mutate returned changed=true with nil resource")
	}
	oldVersion := r.ResourceVersion
	s.version++
	next.Name = name
	next.ResourceVersion = s.version
	s.data[name] = next
	s.emitLocked(Event{Type: EventUpdated, Name: name, Resource: next.Clone(), OldVersion: oldVersion})
	return next.Clone(), nil
}

// MutateResult instructs the store how to finish a compare-and-swap.
type MutateResult struct {
	// Resource is the desired new object when Changed is true.
	Resource *v1.Resource
	// Changed should be false when nothing needs persisting (prevents
	// meaningless writes).
	Changed bool
	// Stale should be true when the live object no longer matches the
	// generation/UID the worker operated on.
	Stale bool
}

// Delete marks a resource for deletion. The object stays in the store until
// the controller removes its finalizers. Deleting an already deleting
// resource is a no-op and returns the current copy.
func (s *Store) Delete(name string) (*v1.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.data[name]
	if !ok {
		return nil, &Error{Reason: ReasonNotFound, Name: name}
	}
	if r.DeletionRequested {
		return r.Clone(), nil
	}
	oldVersion := r.ResourceVersion
	r.DeletionRequested = true
	s.version++
	r.ResourceVersion = s.version
	s.emitLocked(Event{Type: EventUpdated, Name: name, Resource: r.Clone(), OldVersion: oldVersion})
	return r.Clone(), nil
}

// Remove physically deletes a resource. It is only callable by the
// controller after cleanup, and only when no finalizers remain.
func (s *Store) Remove(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.data[name]
	if !ok {
		return &Error{Reason: ReasonNotFound, Name: name}
	}
	if len(r.Finalizers) > 0 {
		return &Error{Reason: ReasonImmutable, Name: name,
			Field: "finalizers", Detail: "cannot remove resource while finalizers remain"}
	}
	delete(s.data, name)
	s.emitLocked(Event{Type: EventDeleted, Name: name, OldVersion: r.ResourceVersion})
	return nil
}

// Watch subscribes to store events. The channel is buffered and a slow
// watcher drops events rather than blocking writers; controllers use
// periodic relisting as the safety net.
func (s *Store) Watch(buffer int) (<-chan Event, func()) {
	ch := make(chan Event, buffer)
	w := &watcher{ch: ch}
	s.mu.Lock()
	s.watchers[w] = struct{}{}
	s.mu.Unlock()
	cancel := func() {
		s.mu.Lock()
		delete(s.watchers, w)
		s.mu.Unlock()
	}
	return ch, cancel
}

func (s *Store) emitLocked(ev Event) {
	for w := range s.watchers {
		select {
		case w.ch <- ev:
		default:
			// Drop on overflow; the consumer's relist/resync covers it.
		}
	}
}

func mapValidation(name string, err error) error {
	if ve, ok := err.(*v1.ValidationError); ok {
		return &Error{Reason: ReasonValidation, Name: name, Field: ve.Field, Detail: ve.Reason}
	}
	return err
}
