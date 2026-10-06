// Package v1 defines the in-memory resource model reconciled by the
// declarative controller.
//
// A Resource is the unit of desired state. It is stored with a monotonic,
// optimistic-concurrency ResourceVersion. Spec holds user intent, Status
// holds observed state written exclusively by the controller.
package v1

import (
	"fmt"
	"time"
)

// FinalizerController is attached to every resource on creation and blocks
// physical deletion until cleanup has completed.
const FinalizerController = "controller.demo/cleanup"

// Phase describes where a resource is in its lifecycle.
type Phase string

const (
	// PhasePending means the resource exists but has never been fully
	// reconciled, or is queued for another attempt.
	PhasePending Phase = "Pending"
	// PhaseReconciling means a reconcile attempt is in progress or the
	// last attempt has not converged yet.
	PhaseReconciling Phase = "Reconciling"
	// PhaseReady means observed state matches desired state.
	PhaseReady Phase = "Ready"
	// PhaseFailed means the last reconcile attempt failed and the resource
	// is waiting for the backoff window to elapse.
	PhaseFailed Phase = "Failed"
	// PhaseTerminating means the resource has been deleted by the user and
	// the controller is running (or retrying) cleanup.
	PhaseTerminating Phase = "Terminating"
)

// ConditionType enumerates the explainable conditions recorded on a resource.
type ConditionType string

const (
	// ConditionReconciled reports the outcome of the latest completed
	// reconcile attempt.
	ConditionReconciled ConditionType = "Reconciled"
	// ConditionCleanedUp reports the outcome of the latest cleanup attempt.
	ConditionCleanedUp ConditionType = "CleanedUp"
)

// ConditionStatus is a tri-state value.
type ConditionStatus string

const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// Condition is a point-in-time observation about a resource.
type Condition struct {
	Type               ConditionType   `json:"type"`
	Status             ConditionStatus `json:"status"`
	Reason             string          `json:"reason,omitempty"`
	Message            string          `json:"message,omitempty"`
	ObservedGeneration int64           `json:"observedGeneration,omitempty"`
	LastTransition     time.Time       `json:"lastTransition,omitempty"`
}

// Spec is the desired state. Both fields are immutable after creation in
// the demo model, which keeps update validation small and explicit.
type Spec struct {
	// DisplayName is a human readable name. Required, 1..64 chars, immutable.
	DisplayName string `json:"displayName"`
	// InstanceCount is the desired number of external instances. Must be
	// >= 1 and immutable.
	InstanceCount int `json:"instanceCount"`
}

// Status is controller-owned observed state.
type Status struct {
	Phase Phase `json:"phase,omitempty"`
	// ReadyInstances counts side effects still considered active.
	ReadyInstances     int   `json:"readyInstances,omitempty"`
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	ReconcileAttempts  int   `json:"reconcileAttempts,omitempty"`
	CleanupAttempts    int   `json:"cleanupAttempts,omitempty"`
	// SideEffectIDs are the idempotency tokens of side effects already
	// applied to the "outside world". Cleanup removes them in reverse order.
	SideEffectIDs []string    `json:"sideEffectIDs,omitempty"`
	Conditions    []Condition `json:"conditions,omitempty"`
	NextRetryAt   time.Time   `json:"nextRetryAt,omitempty"`
}

// Resource is the versioned object stored in the resource store.
type Resource struct {
	// Name is the primary key. Immutable.
	Name string `json:"name"`
	// Generation is bumped on every spec change (creation included).
	Generation int64 `json:"generation"`
	// ResourceVersion is bumped on every persisted change, including status
	// writes and finalizer changes. It is the optimistic concurrency token.
	ResourceVersion int64 `json:"resourceVersion"`
	// UID uniquely identifies a particular incarnation. A resource created
	// after an older one was physically deleted gets a fresh UID, so stale
	// workers can never confuse the two.
	UID        string   `json:"uid"`
	Spec       Spec     `json:"spec"`
	Status     Status   `json:"status,omitempty"`
	Finalizers []string `json:"finalizers,omitempty"`
	// DeletionRequested is set when the user deletes the resource. The
	// object remains in the store until finalizers are gone.
	DeletionRequested bool      `json:"deletionRequested,omitempty"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

// Clone returns a deep copy so callers can never mutate stored state through
// a pointer they received from the store.
func (r *Resource) Clone() *Resource {
	if r == nil {
		return nil
	}
	out := *r
	out.Finalizers = append([]string(nil), r.Finalizers...)
	out.Status.SideEffectIDs = append([]string(nil), r.Status.SideEffectIDs...)
	if r.Status.Conditions != nil {
		out.Status.Conditions = append([]Condition(nil), r.Status.Conditions...)
	}
	return &out
}

// HasFinalizer reports whether the resource carries the given finalizer.
func (r *Resource) HasFinalizer(name string) bool {
	for _, f := range r.Finalizers {
		if f == name {
			return true
		}
	}
	return false
}

// GetCondition returns a copy of the condition and its index (-1 absent).
func (s *Status) GetCondition(t ConditionType) (Condition, int) {
	for i := range s.Conditions {
		if s.Conditions[i].Type == t {
			return s.Conditions[i], i
		}
	}
	return Condition{}, -1
}

// SetCondition inserts or replaces a condition, stamping LastTransition only
// when status/reason/message actually changes. It reports whether status data
// changed, which lets callers suppress meaningless status writes.
func (s *Status) SetCondition(c Condition, now time.Time) bool {
	_, idx := s.GetCondition(c.Type)
	if idx >= 0 {
		old := s.Conditions[idx]
		c.LastTransition = old.LastTransition
		if old.Status == c.Status && old.Reason == c.Reason && old.Message == c.Message {
			s.Conditions[idx].ObservedGeneration = c.ObservedGeneration
			return c.ObservedGeneration != old.ObservedGeneration
		}
		c.LastTransition = now
		s.Conditions[idx] = c
		return true
	}
	c.LastTransition = now
	s.Conditions = append(s.Conditions, c)
	return true
}

// ValidateCreate validates the name and spec for creation.
func ValidateCreate(name string, spec Spec) error {
	if name == "" {
		return NewValidationError("name", "must not be empty")
	}
	return ValidateSpec(spec)
}

// ValidateSpec validates spec field values.
func ValidateSpec(spec Spec) error {
	if spec.DisplayName == "" {
		return NewValidationError("spec.displayName", "must not be empty")
	}
	if len(spec.DisplayName) > 64 {
		return NewValidationError("spec.displayName", "must be at most 64 characters")
	}
	if spec.InstanceCount < 1 {
		return NewValidationError("spec.instanceCount",
			fmt.Sprintf("must be >= 1, got %d", spec.InstanceCount))
	}
	return nil
}

// ValidationError describes a rejected request caused by malformed input.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error: %s: %s", e.Field, e.Reason)
}

// NewValidationError constructs a ValidationError.
func NewValidationError(field, reason string) *ValidationError {
	return &ValidationError{Field: field, Reason: reason}
}
