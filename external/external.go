// Package external simulates a non-transactional external system (cloud API,
// DNS provider, ...) that the controller has to drive toward the desired
// state. Calls may fail transiently, so the controller retries them; the
// external system records idempotency keys so retries never create
// duplicate side effects.
package external

import (
	"fmt"
	"sync"
)

// System is the fake external side-effect target.
type System struct {
	mu sync.Mutex
	// active holds apply tokens whose resource currently exists.
	active map[string]string
	// applied counts how many times each token was *actually* applied
	// (deduped retries do not increment it).
	applied map[string]int
	// cleaned counts actual cleanup executions per token.
	cleaned map[string]int

	// FailApplyN/failCleanupN make the first N calls for a given token fail.
	failApplyN   map[string]int
	failCleanupN map[string]int
}

// NewSystem creates an empty external system.
func NewSystem() *System {
	return &System{
		active:       map[string]string{},
		applied:      map[string]int{},
		cleaned:      map[string]int{},
		failApplyN:   map[string]int{},
		failCleanupN: map[string]int{},
	}
}

// FailApplyNext makes the next n Apply calls for token fail.
func (s *System) FailApplyNext(token string, n int) {
	s.mu.Lock()
	s.failApplyN[token] += n
	s.mu.Unlock()
}

// FailCleanupNext makes the next n Cleanup calls for token fail.
func (s *System) FailCleanupNext(token string, n int) {
	s.mu.Lock()
	s.failCleanupN[token] += n
	s.mu.Unlock()
}

// Apply creates/updates the side effect named by token. Repeated calls with
// the same token are acknowledged as success without duplicate work.
func (s *System) Apply(token, detail string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.failApplyN[token]; n > 0 {
		s.failApplyN[token] = n - 1
		return fmt.Errorf("external apply transient failure for %s (%d left)", token, n-1)
	}
	if _, ok := s.active[token]; !ok {
		s.applied[token]++
	}
	s.active[token] = detail
	return nil
}

// Cleanup removes the side effect named by token. Once removed, a later
// cleanup of the same token is a no-op returning success, so retries after a
// "request succeeded but response lost" scenario are safe.
func (s *System) Cleanup(token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.failCleanupN[token]; n > 0 {
		s.failCleanupN[token] = n - 1
		return fmt.Errorf("external cleanup transient failure for %s (%d left)", token, n-1)
	}
	if _, ok := s.active[token]; ok {
		delete(s.active, token)
		s.cleaned[token]++
	}
	return nil
}

// Has reports whether a side effect currently exists for token.
func (s *System) Has(token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.active[token]
	return ok
}

// AppliedCount returns how many times token was really applied (always 1 for
// a correctly idempotent controller).
func (s *System) AppliedCount(token string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied[token]
}

// CleanupCount returns how many times cleanup really ran for token.
func (s *System) CleanupCount(token string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cleaned[token]
}
