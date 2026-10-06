// Package store provides a concurrency-safe, versioned, in-process resource
// store. It simulates the storage semantics of a real API server:
//
//   - every successful mutation yields a new ResourceVersion,
//   - updates are conditional on the last seen ResourceVersion,
//   - deletions are soft deletes gated by a finalizer.
package store

import (
	"fmt"
	"sort"
	"sync"

	"github.com/lechandonga/declarative-reconcile-controller-go/api"
	"time"
)

// Conflicting error kinds, distinguishable by type so callers (and tests)
// can react differently to each failure mode.

// NotFoundError means no resource exists with the given key.
type NotFoundError struct{ Key string }

func (e *NotFoundError) Error() string { return "resource not found: " + e.Key }

// ConflictError means the expected resource version does not match the
// current one: another writer committed in between.
type ConflictError struct {
	Key      string
	Expected int64
	Actual   int64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("version conflict on %s: expected resourceVersion %d, got %d", e.Key, e.Expected, e.Actual)
}

// ValidationError means the payload itself is malformed (missing or illegal
// fields), independent of any concurrency condition.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return "validation error: " + e.Msg }

// ImmutableFieldError means an update tried to change a field that is fixed
// at creation time (name/kind/uid or the immutable spec field "region").
type ImmutableFieldError struct {
	Field  string
	OldVal any
	NewVal any
}

func (e *ImmutableFieldError) Error() string {
	return fmt.Sprintf("immutable field %q changed: %v -> %v", e.Field, e.OldVal, e.NewVal)
}

// DeletingError means a mutation was attempted against a resource that is
// already marked for deletion.
type DeletingError struct{ Key string }

func (e *DeletingError) Error() string {
	return "resource is being deleted: " + e.Key
}

// AlreadyExistsError means Create targeted a name that is in use, including a
// resource still in the Terminating phase (the new object can only be created
// after the old one is physically gone).
type AlreadyExistsError struct{ Key string }

func (e *AlreadyExistsError) Error() string { return "resource already exists: " + e.Key }

// Store is the versioned in-process resource store.
type Store struct {
	mu      sync.Mutex
	nextRV  int64
	items   map[string]*api.Resource
	watches []watch
	hooks   []func(api.Event)
}

type watch struct {
	ch   chan api.Event
	drop bool // when true, drop on overflow instead of blocking
}

// New creates an empty store.
func New() *Store {
	return &Store{nextRV: 0, items: map[string]*api.Resource{}}
}

// Create inserts a new resource. The caller supplies uid/timestamps through
// the resource object; the store assigns the first resource version.
func (s *Store) Create(r *api.Resource) (*api.Resource, error) {
	if r == nil {
		return nil, &ValidationError{Msg: "nil resource"}
	}
	if err := validateSpec(r.Spec); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.items[r.Key()]; ok {
		return nil, &AlreadyExistsError{Key: r.Key()}
	}
	s.nextRV++
	r.ResourceVersion = s.nextRV
	stored := r.Clone()
	s.items[stored.Key()] = stored
	s.notify(api.Event{Type: api.EventCreated, Resource: stored.Clone()})
	return stored.Clone(), nil
}

// Get returns a copy of the resource.
func (s *Store) Get(kind, name string) (*api.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.items[kind+"/"+name]
	if !ok {
		return nil, &NotFoundError{Key: kind + "/" + name}
	}
	return r.Clone(), nil
}

// List returns copies of all resources, ordered by key for determinism.
func (s *Store) List() []*api.Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*api.Resource, 0, len(s.items))
	for _, r := range s.items {
		out = append(out, r.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out
}

// UpdateSpec applies a spec mutation. expectedVersion must equal the resource
// version last observed by the caller; a concurrent writer bumps the version
// and turns a stale write into a ConflictError.
//
// Validation errors and immutable-field errors are reported independently of
// the version check semantics: they are payload problems, not conflicts.
func (s *Store) UpdateSpec(kind, name string, expectedVersion int64, newSpec map[string]any) (*api.Resource, error) {
	if err := validateSpec(newSpec); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.items[kind+"/"+name]
	if !ok {
		return nil, &NotFoundError{Key: kind + "/" + name}
	}
	if r.DeletionTimestamp != nil {
		return nil, &DeletingError{Key: r.Key()}
	}
	if r.ResourceVersion != expectedVersion {
		return nil, &ConflictError{Key: r.Key(), Expected: expectedVersion, Actual: r.ResourceVersion}
	}
	if newSpec["region"] != r.Spec["region"] {
		return nil, &ImmutableFieldError{Field: "spec.region", OldVal: r.Spec["region"], NewVal: newSpec["region"]}
	}
	r.Spec = cloneSpec(newSpec)
	r.Generation++
	return s.bumpLocked(r, api.EventUpdated)
}

// PatchStatus replaces the status block. Same conditional-update rules as
// UpdateSpec; the controller uses it so that stale reconcile results can
// never overwrite a newer state.
func (s *Store) PatchStatus(kind, name string, expectedVersion int64, st api.Status) (*api.Resource, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.items[kind+"/"+name]
	if !ok {
		return nil, false, &NotFoundError{Key: kind + "/" + name}
	}
	if r.ResourceVersion != expectedVersion {
		return nil, false, &ConflictError{Key: r.Key(), Expected: expectedVersion, Actual: r.ResourceVersion}
	}
	// No-op writes are suppressed: successful convergence must not produce
	// repeated, meaningless writes or repeated watch events.
	if statusEqual(r.Status, st) {
		return r.Clone(), false, nil
	}
	r.Status = st
	updated, err := s.bumpLocked(r, api.EventStatusUpdated)
	return updated, true, err
}

// MarkDeleting soft-deletes a resource: it stamps a deletion timestamp and
// adds the managed finalizer if missing. The resource keeps being listed and
// read until cleanup removes the finalizer.
func (s *Store) MarkDeleting(kind, name string, expectedVersion int64) (*api.Resource, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.items[kind+"/"+name]
	if !ok {
		return nil, false, &NotFoundError{Key: kind + "/" + name}
	}
	if r.DeletionTimestamp != nil {
		return r.Clone(), false, nil
	}
	if r.ResourceVersion != expectedVersion {
		return nil, false, &ConflictError{Key: r.Key(), Expected: expectedVersion, Actual: r.ResourceVersion}
	}
	t := now()
	t = api.Now()
	r.DeletionTimestamp = &t
	if !hasFinalizer(r, api.FinalizerManaged) {
		r.Finalizers = append(r.Finalizers, api.FinalizerManaged)
	}
	updated, err := s.bumpLocked(r, api.EventUpdated)
	return updated, true, err
}

// RemoveFinalizer removes a finalizer. When the resource is deleting and no
// finalizers remain, the object physically disappears atomically in the same
// version step, so a same-named Create can never observe a half-cleaned state.
func (s *Store) RemoveFinalizer(kind, name, finalizer string, expectedVersion int64) (*api.Resource, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := kind + "/" + name
	r, ok := s.items[key]
	if !ok {
		return nil, false, &NotFoundError{Key: key}
	}
	if r.ResourceVersion != expectedVersion {
		return nil, false, &ConflictError{Key: key, Expected: expectedVersion, Actual: r.ResourceVersion}
	}
	if !hasFinalizer(r, finalizer) {
		return r.Clone(), false, nil
	}
	filtered := r.Finalizers[:0]
	for _, f := range r.Finalizers {
		if f != finalizer {
			filtered = append(filtered, f)
		}
	}
	r.Finalizers = filtered
	if r.DeletionTimestamp != nil && len(r.Finalizers) == 0 {
		deleted := r.Clone()
		delete(s.items, key)
		s.notify(api.Event{Type: api.EventDeleted, Resource: deleted})
		return deleted, true, nil
	}
	updated, err := s.bumpLocked(r, api.EventUpdated)
	return updated, true, err
}

// Watch subscribes a buffered channel to all store mutations. Events hold
// copies. With a zero-sized/overflowing channel the store blocks briefly to
// deliver, so a sufficiently buffered controller channel does not lose
// events; callers that want drop semantics use WatchDropping.
func (s *Store) Watch(ch chan api.Event) {
	s.mu.Lock()
	s.watches = append(s.watches, watch{ch: ch})
	s.mu.Unlock()
}

// WatchDropping subscribes with drop-on-overflow semantics (callers must
// relist on startup, which the controller does).
func (s *Store) WatchDropping(ch chan api.Event) {
	s.mu.Lock()
	s.watches = append(s.watches, watch{ch: ch, drop: true})
	s.mu.Unlock()
}

// AddHook registers a synchronous callback invoked after every mutation.
func (s *Store) AddHook(fn func(api.Event)) {
	s.mu.Lock()
	s.hooks = append(s.hooks, fn)
	s.mu.Unlock()
}

func (s *Store) bumpLocked(r *api.Resource, evtType api.EventType) (*api.Resource, error) {
	s.nextRV++
	r.ResourceVersion = s.nextRV
	r.UpdatedAt = now()
	out := r.Clone()
	s.notify(api.Event{Type: evtType, Resource: out})
	return out, nil
}

func (s *Store) notify(e api.Event) {
	watchers := append([]watch(nil), s.watches...)
	hooks := append([]func(api.Event){}, s.hooks...)
	for _, w := range watchers {
		if w.drop {
			select {
			case w.ch <- e:
			default:
			}
			continue
		}
		w.ch <- e
	}
	for _, fn := range hooks {
		fn(e)
	}
}

func validateSpec(spec map[string]any) error {
	if spec == nil {
		return &ValidationError{Msg: "spec is required"}
	}
	region, ok := spec["region"].(string)
	if !ok || region == "" {
		return &ValidationError{Msg: "spec.region must be a non-empty string"}
	}
	image, ok := spec["image"].(string)
	if !ok || image == "" {
		return &ValidationError{Msg: "spec.image must be a non-empty string"}
	}
	replicas, ok := spec["replicas"].(int)
	if !ok {
		return &ValidationError{Msg: "spec.replicas must be an int"}
	}
	if replicas < 0 {
		return &ValidationError{Msg: "spec.replicas must be >= 0"}
	}
	return nil
}

func hasFinalizer(r *api.Resource, f string) bool {
	for _, x := range r.Finalizers {
		if x == f {
			return true
		}
	}
	return false
}

func statusEqual(a, b api.Status) bool {
	if a.Phase != b.Phase || a.Message != b.Message || a.AppliedToken != b.AppliedToken {
		return false
	}
	if a.ObservedGeneration != b.ObservedGeneration || a.ReconcileFailures != b.ReconcileFailures || a.CleanupFailures != b.CleanupFailures {
		return false
	}
	at := func(p *time.Time) int64 {
		if p == nil {
			return 0
		}
		return p.UnixNano()
	}
	return at(a.LastAttemptAt) == at(b.LastAttemptAt)
}

func cloneSpec(in map[string]any) map[string]any { return api.CloneSpec(in) }

func now() time.Time { return api.Now() }
