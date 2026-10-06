// Command demo runs the declarative controller entirely in-process: it
// creates resources, scripts a transient failure so backoff/retry is
// observable, then deletes a resource so cleanup/finalizer behavior is
// observable. No cluster, no network. Run with: go run ./cmd/demo
package main

import (
	"context"
	"log"
	"os"
	"time"

	v1 "github.com/lechandonga/declarative-reconcile-controller-go/pkg/apis/v1"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/clock"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/controller"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/external"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/reconcile"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/store"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ltime|log.Lmsgprefix)
	s := store.New()
	exec := external.NewFakeExecutor()
	rec := reconcile.New(s, exec, time.Now,
		reconcile.Backoff{Base: 200 * time.Millisecond, Factor: 2, Max: 2 * time.Second})
	ctl := controller.New(controller.Config{
		Store:       s,
		Reconciler:  rec,
		Clock:       clock.RealClock{},
		Logger:      logger,
		ResyncEvery: 30 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctl.Start(ctx)

	logger.Printf("--- create healthy resource 'orders' (2 instances) ---")
	if _, err := s.Create("orders", v1.Spec{DisplayName: "orders", InstanceCount: 2}); err != nil {
		log.Fatal(err)
	}

	logger.Printf("--- create 'payments' whose first external call fails once ---")
	r, err := s.Create("payments", v1.Spec{DisplayName: "payments", InstanceCount: 1})
	if err != nil {
		log.Fatal(err)
	}
	exec.FailApplies(r.UID+"/instance-0", 1)

	waitFor(func() bool {
		got, err := s.Get("payments")
		return err == nil && got.Status.Phase == v1.PhaseReady
	}, 3*time.Second, "payments never converged")

	logger.Printf("--- delete 'orders': cleanup runs before physical removal ---")
	if _, err := s.Delete("orders"); err != nil {
		log.Fatal(err)
	}
	waitFor(func() bool {
		_, err := s.Get("orders")
		se, ok := store.AsError(err)
		return ok && se.Reason == store.ReasonNotFound
	}, 3*time.Second, "orders never cleaned up")

	logger.Printf("--- final state: %d external objects live (expect 1) ---", len(exec.Snapshot()))
	ctl.Stop()
}

func waitFor(fn func() bool, timeout time.Duration, msg string) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	log.Fatal("demo timeout: " + msg)
}
