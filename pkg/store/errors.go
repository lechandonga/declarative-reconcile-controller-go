package store

import "fmt"

// Reason classifies why a store request was rejected, so callers (and
// tests) can branch on the cause without parsing error strings.
type Reason string

const (
	// ReasonNotFound means no resource with that name exists (or it has
	// already been physically deleted).
	ReasonNotFound Reason = "NotFound"
	// ReasonAlreadyExists means a resource with that name already exists.
	ReasonAlreadyExists Reason = "AlreadyExists"
	// ReasonConflict means the supplied resourceVersion does not match the
	// current one: the object was concurrently modified.
	ReasonConflict Reason = "Conflict"
	// ReasonValidation means malformed input (missing/illegal fields).
	ReasonValidation Reason = "Validation"
	// ReasonImmutable means an update attempted to change a field that must
	// not change after creation.
	ReasonImmutable Reason = "Immutable"
	// ReasonDeleting means the resource is marked for deletion and accepts
	// no further user mutations.
	ReasonDeleting Reason = "Deleting"
)

// Error is the single structured error type returned by the store.
type Error struct {
	Reason Reason
	Name   string
	// Field is set for validation / immutable-field rejections.
	Field  string
	Detail string
	// CurrentVersion is set on Conflict so callers can re-read and retry.
	CurrentVersion int64
}

func (e *Error) Error() string {
	switch e.Reason {
	case ReasonNotFound:
		return fmt.Sprintf("%s: resource %q not found", e.Reason, e.Name)
	case ReasonAlreadyExists:
		return fmt.Sprintf("%s: resource %q already exists", e.Reason, e.Name)
	case ReasonConflict:
		return fmt.Sprintf("%s: resource %q version mismatch (supplied, current=%d)",
			e.Reason, e.Name, e.CurrentVersion)
	case ReasonValidation:
		return fmt.Sprintf("%s: resource %q field %q: %s", e.Reason, e.Name, e.Field, e.Detail)
	case ReasonImmutable:
		return fmt.Sprintf("%s: resource %q field %q is immutable: %s",
			e.Reason, e.Name, e.Field, e.Detail)
	case ReasonDeleting:
		return fmt.Sprintf("%s: resource %q is being deleted: %s", e.Reason, e.Name, e.Detail)
	default:
		return fmt.Sprintf("%s: resource %q: %s", e.Reason, e.Name, e.Detail)
	}
}

// AsError extracts a *store.Error from an error.
func AsError(err error) (*Error, bool) {
	if err == nil {
		return nil, false
	}
	if se, ok := err.(*Error); ok {
		return se, true
	}
	return nil, false
}
