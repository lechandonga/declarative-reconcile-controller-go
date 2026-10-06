// Command demo runs the declarative controller entirely in one process:
// an in-memory versioned store, a flaky external side-effect simulator and
// the reconcile/delete loop. No cluster or network is involved.
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lechandonga/declarative-reconcile-controller-go/api"
	"github.com/lechandonga/declarative-reconcile-controller-go/client"
	"github.com/lechandonga/declarative-reconcile-controller-go/clock"
	"github.com/lechandonga/declarative-reconcile-controller-go/controller"
	"github.com/lechandonga/declarative-reconcile-controller-go/external"
	"github.com/lechandonga/declarative-reconcile-controller-go/store"
)

type adapter struct{ sys *external.System }

func (a adapter) Apply(r *api.Resource) error {
	detail, _ := r.Spec["image"].(string)
	return a.sys.Apply(r.UID+"#g"+fmt.Sprint(r.Generation), detail)
}

func (a adapter) Cleanup(r *api.Resource) error {
	token := r.Status.AppliedToken
	if token == "" {
		token = r.UID + "#g" + fmt.Sprint(r.Generation)
	}
	return a.sys.Cleanup(token)
}

func main() {
	logger := log.New(os.Stdout, "[demo] ", log.LstdFlags|log.Lmicroseconds)

	st := store.New()
	sys := external.NewSystem()
	cl := client.New(st)

	ctrl := controller.New(controller.Config{
		Store:        st,
		Reconciler:   adapter{sys: sys},
		Clock:        clock.RealClock{},
		BaseBackoff:  200 * time.Millisecond,
		MaxBackoff:   2 * time.Second,
		PollInterval: 50 * time.Millisecond,
		Logger:       logger,
	})
	ctrl.Start()
	defer ctrl.Stop()

	// 1) Declare desired state; first two applies fail transiently so the
	//    exponential-backoff retry path is visible.
	web, err := cl.Create("web", client.Spec{Region: "cn-east", Image: "app:1", Replicas: 2})
	must(err)
	sys.FailApplyNext(web.UID+"#g1", 2)
	waitPhase(st, "web", api.PhaseSucceeded)

	// 2) Change the spec once converged: generation 2 is reconciled once.
	cur := mustGet(st, "web")
	_, err = cl.Update(cur, client.Spec{Region: "cn-east", Image: "app:2", Replicas: 3})
	must(err)
	waitGeneration(st, "web", 2)

	// 3) Delete; make cleanup fail once to show retry-before-removal.
	sys.FailCleanupNext(web.UID+"#g2", 1)
	cur = mustGet(st, "web")
	_, _, err = cl.Delete(cur)
	must(err)
	waitGone(st, "web")

	logger.Printf("demo complete: g1 applied=%d | g2 applied=%d cleaned=%d",
		sys.AppliedCount(web.UID+"#g1"),
		sys.AppliedCount(web.UID+"#g2"),
		sys.CleanupCount(web.UID+"#g2"))
}

func mustGet(st *store.Store, name string) *api.Resource {
	r, err := st.Get(client.DefaultKind, name)
	must(err)
	return r
}

func waitPhase(st *store.Store, name, phase string) {
	for {
		r, err := st.Get(client.DefaultKind, name)
		if err == nil && r.Status.Phase == phase {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitGeneration(st *store.Store, name string, gen int64) {
	for {
		r, err := st.Get(client.DefaultKind, name)
		if err == nil && r.Status.Phase == api.PhaseSucceeded &&
			r.Status.ObservedGeneration == gen {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitGone(st *store.Store, name string) {
	for {
		if _, err := st.Get(client.DefaultKind, name); err != nil {
			if _, ok := err.(*store.NotFoundError); ok {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
