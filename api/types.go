package api

import "time"

// FinalizerManaged is attached to every resource managed by the controller.
// A resource can only physically disappear after this finalizer is removed,
// and the finalizer is removed only after cleanup succeeds.
const FinalizerManaged = "reconcile.example.com/cleanup"

// Status phases reported by the controller.
const (
	PhasePending       = "Pending"
	PhaseReconciling   = "Reconciling"
	PhaseSucceeded     = "Succeeded"
	PhaseFailed        = "Failed"
	PhaseTerminating   = "Terminating"
	PhaseCleanupFailed = "CleanupFailed"
)

// Resource is the in-process equivalent of a cluster resource object.
//
// Spec is a small declarative model:
//   - region  (string, required, immutable after creation)
//   - image   (string, required)
//   - replicas (int,    required, >= 0)
type Resource struct {
	// UID is assigned at creation and never reused. Recreating a resource
	// with the same name produces a new UID, so old reconcile/cleanup
	// results can never target the new object.
	UID               string
	Kind              string
	Name              string
	Generation        int64
	ResourceVersion   int64
	Spec              map[string]any
	Status            Status
	Finalizers        []string
	DeletionTimestamp *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Status is persisted inside the resource, so a restarted controller can
// resume from it instead of starting from scratch.
type Status struct {
	Phase              string
	Message            string
	ObservedGeneration int64
	// Failures counts consecutive failed reconcile attempts for the current
	// generation; it drives exponential backoff and is reset on success.
	ReconcileFailures int
	// CleanupFailures counts consecutive failed cleanup attempts.
	CleanupFailures int
	LastAttemptAt   *time.Time
	// AppliedToken records the idempotency token of the side effect that has
	// already been applied ("<uid>#<generation>"). It lets the controller and
	// the external system agree that a reconcile must not be applied twice.
	AppliedToken string
}

// Key returns the storage key of a resource.
func (r *Resource) Key() string { return r.Kind + "/" + r.Name }

// Clone returns a deep-enough copy safe to hand to callers.
func (r *Resource) Clone() *Resource {
	out := *r
	out.Spec = CloneSpec(r.Spec)
	out.Finalizers = append([]string(nil), r.Finalizers...)
	if r.DeletionTimestamp != nil {
		t := *r.DeletionTimestamp
		out.DeletionTimestamp = &t
	}
	if r.Status.LastAttemptAt != nil {
		t := *r.Status.LastAttemptAt
		out.Status.LastAttemptAt = &t
	}
	return &out
}

// CloneSpec returns a shallow copy of the spec map.
func CloneSpec(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
