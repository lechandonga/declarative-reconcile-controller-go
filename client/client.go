// Package client is a tiny typed convenience layer over the store.
package client

import (
	"fmt"
	"sync/atomic"

	"github.com/lechandonga/declarative-reconcile-controller-go/api"
	"github.com/lechandonga/declarative-reconcile-controller-go/store"
)

// DefaultKind is the only resource kind in this simulation.
const DefaultKind = "Widget"

// Client creates and mutates resources through the versioned store.
type Client struct {
	st  *store.Store
	uid atomic.Uint64
}

// New returns a client.
func New(st *store.Store) *Client {
	return &Client{st: st}
}

// Spec is the declarative payload.
type Spec struct {
	Region   string
	Image    string
	Replicas int
}

func (s Spec) toMap() map[string]any {
	return map[string]any{"region": s.Region, "image": s.Image, "replicas": s.Replicas}
}

// Create inserts a new resource and returns its first revision.
func (c *Client) Create(name string, spec Spec) (*api.Resource, error) {
	now := api.Now()
	r := &api.Resource{
		UID:        fmt.Sprintf("uid-%d", c.uid.Add(1)),
		Kind:       DefaultKind,
		Name:       name,
		Generation: 1,
		Spec:       spec.toMap(),
		Status:     api.Status{Phase: api.PhasePending},
		Finalizers: []string{api.FinalizerManaged},
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	return c.st.Create(r)
}

// Update changes the spec, guarded by the resource version in got.
func (c *Client) Update(got *api.Resource, spec Spec) (*api.Resource, error) {
	return c.st.UpdateSpec(got.Kind, got.Name, got.ResourceVersion, spec.toMap())
}

// Delete marks the resource for deletion. Callers poll Get until NotFound to
// observe physical removal.
func (c *Client) Delete(got *api.Resource) (*api.Resource, bool, error) {
	return c.st.MarkDeleting(got.Kind, got.Name, got.ResourceVersion)
}
