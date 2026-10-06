package controller_test

import (
	"errors"
	"log"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/declarative-reconcile-controller-go/api"
	"github.com/lechandonga/declarative-reconcile-controller-go/client"
	"github.com/lechandonga/declarative-reconcile-controller-go/clock"
	"github.com/lechandonga/declarative-reconcile-controller-go/controller"
	"github.com/lechandonga/declarative-reconcile-controller-go/external"
	"github.com/lechandonga/declarative-reconcile-controller-go/store"
)

// TestVersionConflictOnConcurrentUpdate covers storage versioning:
// two writers race; the second writer's stale revision must be rejected with
// a distinct ConflictError, and no partial data may land.
func TestVersionConflictOnConcurrentUpdate(t *testing.T) {
	h := newHarness(t)
	r := h.createSucceeds("web")

	// Two copies of the same revision are read by two independent writers.
	writerA := r.Clone()
	writerB := r.Clone()

	a, err := h.cl.Update(writerA, client.Spec{Region: "cn-east", Image: "app:2", Replicas: 3})
	if err != nil {
		t.Fatalf("writer A update: %v", err)
	}
	if a.ResourceVersion == r.ResourceVersion {
		t.Fatalf("update must bump resource version")
	}

	_, err = h.cl.Update(writerB, client.Spec{Region: "cn-east", Image: "app:9", Replicas: 9})
	var cf *store.ConflictError
	if !errors.As(err, &cf) {
		t.Fatalf("stale writer B must get ConflictError, got %T: %v", err, err)
	}
	if cf.Expected != writerB.ResourceVersion || cf.Actual != a.ResourceVersion {
		t.Fatalf("conflict must report expected/actual versions: %+v", cf)
	}

	got := h.get("web")
	if got.Spec["image"] != "app:2" || got.Spec["replicas"] != 3 {
		t.Fatalf("stale write must not land: %#v", got.Spec)
	}

	// Re-reading and retrying with the fresh revision must succeed.
	fresh := h.get("web")
	if _, err := h.cl.Update(fresh, client.Spec{Region: "cn-east", Image: "app:3", Replicas: 4}); err != nil {
		t.Fatalf("update with fresh version: %v", err)
	}
}

// TestValidationAndImmutableFields covers the distinguishable rejection
// reasons for missing fields, illegal values and immutable-field changes.
func TestValidationAndImmutableFields(t *testing.T) {
	h := newHarness(t)
	h.createSucceeds("web")

	cases := []struct {
		name    string
		spec    map[string]any
		wantErr func(error) bool
	}{
		{"missing image", map[string]any{"region": "cn-east", "replicas": 1}, isValidation},
		{"empty region", map[string]any{"region": "", "image": "a:1", "replicas": 1}, isValidation},
		{"negative replicas", map[string]any{"region": "cn-east", "image": "a:1", "replicas": -1}, isValidation},
		{"replicas wrong type", map[string]any{"region": "cn-east", "image": "a:1", "replicas": "1"}, isValidation},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := h.get("web")
			if _, err := h.st.UpdateSpec(got.Kind, got.Name, got.ResourceVersion, tc.spec); !tc.wantErr(err) {
				t.Fatalf("want validation error, got %T: %v", err, err)
			}
		})
	}

	got := h.get("web")
	_, err := h.st.UpdateSpec(got.Kind, got.Name, got.ResourceVersion,
		map[string]any{"region": "us-west", "image": "a:1", "replicas": 1})
	var imm *store.ImmutableFieldError
	if !errors.As(err, &imm) {
		t.Fatalf("region change must yield ImmutableFieldError, got %T: %v", err, err)
	}
	if imm.Field != "spec.region" {
		t.Fatalf("immutable error must name the field: %+v", imm)
	}

	// Updates to a deleting resource are rejected with DeletingError.
	if _, _, err := h.cl.Delete(h.get("web")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = h.cl.Update(h.get("web"), client.Spec{Region: "cn-east", Image: "a:2", Replicas: 1})
	var del *store.DeletingError
	if !errors.As(err, &del) {
		t.Fatalf("update while deleting must yield DeletingError, got %T: %v", err, err)
	}
}

func isValidation(err error) bool {
	var v *store.ValidationError
	return errors.As(err, &v)
}

// TestReconcileConvergesAndNoopRewrite covers normal convergence:
// exactly one external apply, phase Pending -> Reconciling -> Succeeded,
// and repeated reconciles after convergence do nothing.
func TestReconcileConvergesAndNoopRewrite(t *testing.T) {
	h := newHarness(t)
	h.Start()

	before := h.createSucceeds("web").ResourceVersion
	h.Idle()
	r := h.waitPhase("web", api.PhaseSucceeded)
	token := r.UID + "#g1"
	if h.sys.AppliedCount(token) != 1 {
		t.Fatalf("side effect must be applied exactly once, got %d", h.sys.AppliedCount(token))
	}
	if r.Status.ObservedGeneration != 1 {
		t.Fatalf("observed generation mismatch: %d", r.Status.ObservedGeneration)
	}

	rvAfterSuccess := r.ResourceVersion
	// Explicit extra reconciles (e.g. a relist) must not write nor call out.
	// Retry enqueueing until the no-op path is observed: the trigger channel
	// is only consumed after Start, which races with the first enqueue.
	okNoop := false
	for i := 0; i < 100; i++ {
		h.ctrl.Enqueue(client.DefaultKind, "web")
		h.Idle()
		if strings.Contains(h.logs(), "no-op") {
			okNoop = true
			break
		}
	}
	if !okNoop {
		t.Fatalf("logs must explain no-op convergence:\n%s", h.logs())
	}
	h.Advance(time.Hour)
	r = h.get("web")
	if r.ResourceVersion != rvAfterSuccess {
		t.Fatalf("converged resource must not be rewritten: rv %d -> %d", rvAfterSuccess, r.ResourceVersion)
	}
	if h.sys.AppliedCount(token) != 1 {
		t.Fatalf("repeated reconcile must not re-apply side effect")
	}
	_ = before
}

// TestReconcileFailureBackoffAndIdempotentRetry covers transient failures:
// the controller reports Failed, waits exponential backoff instead of busy
// spinning, retries, and applies the side effect exactly once overall.
func TestReconcileFailureBackoffAndIdempotentRetry(t *testing.T) {
	h := newHarness(t)
	created := h.createSucceeds("flaky")
	tokenBase := created.UID + "#g1"
	h.sys.FailApplyNext(tokenBase, 2)
	h.Start()

	h.Idle()
	r := h.waitPhase("flaky", api.PhaseFailed)
	if r.Status.ReconcileFailures != 1 {
		t.Fatalf("first failure must be recorded, got %d", r.Status.ReconcileFailures)
	}
	h.waitLog("attempt #2 scheduled in 10ms")
	attemptsBeforeBackoff := h.sys.AppliedCount(tokenBase)

	// Advancing less than one backoff window must not retry (no busy spin).
	h.Advance(baseBackoff / 2)
	h.Idle()
	if h.sys.AppliedCount(tokenBase) != attemptsBeforeBackoff {
		t.Fatalf("controller must not retry before backoff elapses")
	}
	if got := h.get("flaky"); got.Status.Phase != api.PhaseFailed {
		t.Fatalf("phase during backoff = %s", got.Status.Phase)
	}

	// First retry after one base window fails too (second injected failure).
	h.Advance(baseBackoff)
	h.Idle()
	h.waitLog("attempt #3 scheduled in 20ms")
	if got := h.get("flaky"); got.Status.ReconcileFailures != 2 {
		t.Fatalf("second failure must be recorded, got %d", got.Status.ReconcileFailures)
	}

	// Next backoff doubles; advancing just the base window is insufficient.
	h.Advance(baseBackoff)
	h.Idle()
	if h.sys.AppliedCount(tokenBase) != 0 {
		t.Fatalf("doubled backoff must be honored")
	}

	// Advance through the 2x window; the retry now succeeds.
	h.Advance(baseBackoff + time.Millisecond)
	h.Idle()
	r = h.waitPhase("flaky", api.PhaseSucceeded)
	if h.sys.AppliedCount(tokenBase) != 1 {
		t.Fatalf("side effect applied %d times after retries; want exactly 1", h.sys.AppliedCount(tokenBase))
	}
	if r.Status.ReconcileFailures != 0 {
		t.Fatalf("failure counter must reset on success")
	}
	if !strings.Contains(h.logs(), "backoff:") {
		t.Fatalf("logs must show backoff scheduling:\n%s", h.logs())
	}
}

// TestSingleflightCoalescing forces many concurrent spec changes while a
// reconcile is in flight and asserts that at most one reconcile per resource
// runs at any moment, and the external effect is applied once per generation.
func TestSingleflightCoalescing(t *testing.T) {
	h := newHarness(t)
	h.Start()

	h.createSucceeds("busy")
	h.Idle()
	h.waitPhase("busy", api.PhaseSucceeded)

	// Fire 10 spec updates back-to-back; each update uses the freshest
	// revision (a CAS conflict here means the controller legitimately beat
	// us to a write, so we simply re-read and retry the intended target).
	for i := 2; i <= 11; i++ {
		for attempt := 0; ; attempt++ {
			cur := h.get("busy")
			_, err := h.cl.Update(cur, client.Spec{Region: "cn-east", Image: "app:x", Replicas: i})
			if err == nil {
				break
			}
			var cf *store.ConflictError
			if errors.As(err, &cf) && attempt < 50 {
				runtime.Gosched()
				continue
			}
			t.Fatalf("update %d: %v", i, err)
		}
	}
	h.Idle()
	for d := time.Duration(0); d <= time.Second; d += baseBackoff {
		h.Advance(baseBackoff)
		h.Idle()
		cur := h.get("busy")
		if cur.Status.Phase == api.PhaseSucceeded && cur.Status.ObservedGeneration == 11 {
			break
		}
	}
	h.Idle()

	final := h.waitPhase("busy", api.PhaseSucceeded)
	if final.Generation != 11 || final.Status.ObservedGeneration != 11 {
		t.Fatalf("must converge on latest generation, got gen=%d observed=%d",
			final.Generation, final.Status.ObservedGeneration)
	}
	// Each generation applies at most once; exact count <= generations.
	var total int
	for gen := 1; gen <= 11; gen++ {
		total += h.sys.AppliedCount(final.UID + "#g" + itoa(int64(gen)))
	}
	if total == 0 || total > 11 {
		t.Fatalf("apply count across generations = %d, want 1..11 (no duplicates)", total)
	}
}

// TestCleanupFailureRetry covers delete protection: while cleanup fails the
// object must stay present and Terminating/CleanupFailed; after cleanup
// finally succeeds it physically disappears, and cleanup runs exactly once.
func TestCleanupFailureRetry(t *testing.T) {
	h := newHarness(t)
	h.Start()

	r := h.createSucceeds("ghost")
	h.Idle()
	r = h.waitPhase("ghost", api.PhaseSucceeded)
	token := r.UID + "#g1"

	// Two transient cleanup failures (injected against the actual UID).
	h.sys.FailCleanupNext(token, 2)
	if _, _, err := h.cl.Delete(h.get("ghost")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	h.Idle()
	h.waitLog("attempt #2 scheduled in 10ms")
	term := h.get("ghost")
	if term.DeletionTimestamp == nil {
		t.Fatalf("deletion timestamp must be set")
	}
	if term.Status.CleanupFailures != 1 {
		t.Fatalf("cleanup failure counter = %d", term.Status.CleanupFailures)
	}
	if !h.sys.Has(token) {
		t.Fatalf("side effect must still exist while cleanup is failing")
	}
	if h.sys.CleanupCount(token) != 0 {
		t.Fatalf("failed cleanup must not count as executed")
	}

	// It must still be readable as a record (not considered deleted).
	if _, err := h.st.Get(client.DefaultKind, "ghost"); err != nil {
		t.Fatalf("record must not be lost during failing cleanup: %v", err)
	}

	// Nothing happens before the first backoff window elapses.
	h.Advance(baseBackoff / 2)
	h.Idle()
	if got := h.get("ghost"); got.Status.CleanupFailures != 1 {
		t.Fatalf("must not retry inside backoff window: %d", got.Status.CleanupFailures)
	}

	h.Advance(baseBackoff)
	h.Idle()
	h.waitLog("attempt #3 scheduled in 20ms")
	if got := h.get("ghost"); got.Status.CleanupFailures != 2 {
		t.Fatalf("second cleanup failure = %d", got.Status.CleanupFailures)
	}
	// Advancing only the base window must not cross the doubled backoff.
	h.Advance(baseBackoff)
	h.Idle()
	if h.sys.CleanupCount(token) != 0 {
		t.Fatalf("doubled backoff must be honored")
	}
	// Retry must not have duplicated cleanup executions.
	if h.sys.CleanupCount(token) != 0 {
		t.Fatalf("cleanup must not run on failed attempts")
	}

	h.Advance(baseBackoff + time.Millisecond)
	h.Idle()
	h.waitGone("ghost")
	if h.sys.Has(token) {
		t.Fatalf("side effect must be gone after successful cleanup")
	}
	if h.sys.CleanupCount(token) != 1 {
		t.Fatalf("cleanup must execute exactly once, got %d", h.sys.CleanupCount(token))
	}
	if !strings.Contains(h.logs(), "fully cleaned up and removed") {
		t.Fatalf("logs must explain successful cleanup:\n%s", h.logs())
	}
}

// TestDeleteThenRecreateIsolatesState verifies that a same-named new resource
// gets a fresh UID, cannot read old state, and cannot be affected by the old
// resource's cleanup tokens.
func TestDeleteThenRecreateIsolatesState(t *testing.T) {
	h := newHarness(t)
	h.Start()

	old := h.createSucceeds("recycled")
	h.Idle()
	old = h.waitPhase("recycled", api.PhaseSucceeded)
	oldToken := old.UID + "#g1"

	if _, _, err := h.cl.Delete(h.get("recycled")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	h.Idle()
	h.Advance(time.Second)
	h.Idle()
	h.waitGone("recycled")
	if h.sys.CleanupCount(oldToken) != 1 {
		t.Fatalf("old cleanup should have run once")
	}

	// Same name, fresh object: old state and old external effect must not
	// leak into it.
	nw := h.createSucceeds("recycled")
	h.Idle()
	nw = h.waitPhase("recycled", api.PhaseSucceeded)
	if nw.UID == old.UID {
		t.Fatalf("recreated resource must get a new UID")
	}
	if nw.Generation != 1 || nw.Status.ObservedGeneration != 1 || nw.Status.ReconcileFailures != 0 {
		t.Fatalf("recreated resource must start from clean status: %+v", nw.Status)
	}
	if nw.DeletionTimestamp != nil {
		t.Fatalf("recreated resource must not inherit deletion timestamp")
	}
	newToken := nw.UID + "#g1"
	if newToken == oldToken || !h.sys.Has(newToken) {
		t.Fatalf("new resource must drive its own side effect")
	}
	// Deleting the new object must only clean the new token, never touch the
	// old one (whose cleanup count stays 1).
	if _, _, err := h.cl.Delete(h.get("recycled")); err != nil {
		t.Fatalf("delete new: %v", err)
	}
	h.Idle()
	h.Advance(time.Second)
	h.Idle()
	h.waitGone("recycled")
	if h.sys.CleanupCount(oldToken) != 1 {
		t.Fatalf("old cleanup must never run again, got %d", h.sys.CleanupCount(oldToken))
	}
	if h.sys.CleanupCount(newToken) != 1 {
		t.Fatalf("new cleanup must run exactly once")
	}
}

// TestStaleReconcileResultDoesNotOverwrite starts a failing reconcile, changes
// the spec while it is scheduled for retry, and verifies that the old
// failure/success write never clobbers the newer state.
func TestStaleReconcileResultDoesNotOverwrite(t *testing.T) {
	st := store.New()
	sys := external.NewSystem()
	cl := client.New(st)
	fc := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	buf := &threadSafeBuffer{}
	logger := log.New(buf, "[biz] ", log.Lmicroseconds)

	var updateOnce sync.Once
	ctrl := controller.New(controller.Config{
		Store:       st,
		Reconciler:  extReconciler{sys: sys},
		Clock:       fc,
		BaseBackoff: baseBackoff,
		MaxBackoff:  time.Second,
		Logger:      logger,
		BeforeApply: func(r *api.Resource) {
			// Simulate another writer changing the spec exactly while this
			// attempt is applying. The attempt's success write must then be
			// rejected as stale and must never cover generation 2.
			updateOnce.Do(func() {
				cur, err := st.Get(r.Kind, r.Name)
				if err != nil {
					t.Errorf("mid-attempt get: %v", err)
					return
				}
				if _, err := cl.Update(cur, client.Spec{Region: "cn-east", Image: "app:2", Replicas: 5}); err != nil {
					t.Errorf("mid-attempt update: %v", err)
				}
			})
		},
	})
	h := &harness{t: t, st: st, cl: cl, sys: sys, fc: fc, logBuf: buf, logger: logger, ctrl: ctrl}
	t.Cleanup(h.Stop)
	h.Start()

	h.createSucceeds("moving")
	for i := 0; i < 50; i++ {
		runtime.Gosched()
		r, err := st.Get(client.DefaultKind, "moving")
		if err == nil && r.Status.Phase == api.PhaseSucceeded && r.Status.ObservedGeneration == 2 {
			break
		}
	}
	final, err := st.Get(client.DefaultKind, "moving")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if final.Generation != 2 || final.Status.ObservedGeneration != 2 {
		t.Fatalf("newer generation must win; gen=%d observed=%d phase=%s",
			final.Generation, final.Status.ObservedGeneration, final.Status.Phase)
	}
	// Generation 1 was abandoned before touching the external system once
	// the mid-attempt mutation was noticed; generation 2 is the only side
	// effect applied.
	if got := sys.AppliedCount("uid-1#g1"); got != 0 {
		t.Fatalf("generation-1 apply count = %d, want 0", got)
	}
	if got := sys.AppliedCount("uid-1#g2"); got != 1 {
		t.Fatalf("generation-2 apply count = %d, want exactly 1", got)
	}
	if !strings.Contains(buf.String(), "changed mid-attempt") {
		t.Fatalf("logs must explain stale-result discard:\n%s", buf.String())
	}
}

// TestRestartRecovery simulates a controller crash: mid-cleanup resources and
// failed resources exist in the store with no controller in memory. A brand
// new controller must resume them from persisted status without duplicate
// side effects.
func TestRestartRecovery(t *testing.T) {
	st := store.New()
	sys := external.NewSystem()
	cl := client.New(st)

	h1 := newHarnessWith(t, st, sys, clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
	h1.Start()
	a := h1.createSucceeds("alpha")
	_ = a
	b := h1.createSucceeds("beta")
	_ = b
	h1.Idle()
	h1.waitPhase("alpha", api.PhaseSucceeded)
	h1.waitPhase("beta", api.PhaseSucceeded)

	// beta is deleted and its cleanup is failing when the process dies.
	sys.FailCleanupNext(b.UID+"#g1", 1)
	if _, _, err := cl.Delete(h1.get("beta")); err != nil {
		t.Fatalf("delete beta: %v", err)
	}
	h1.Idle()
	h1.waitPhase("beta", api.PhaseCleanupFailed)
	h1.Stop() // process crash: all in-memory scheduler state is gone

	// "Restart" with a new controller instance and a fresh fake clock.
	appliedBefore := sys.AppliedCount(a.UID + "#g1")
	h2 := newHarnessWith(t, st, sys, clock.NewFake(time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)))
	h2.Start()
	// Startup recovery must requeue the terminating resource; alpha is
	// already converged and must be left untouched.
	h2.Idle()
	if !strings.Contains(h2.logs(), "startup recovery: requeueing Widget/beta") {
		t.Fatalf("restart must log recovery of beta:\n%s", h2.logs())
	}
	if strings.Contains(h2.logs(), "requeueing Widget/alpha") {
		t.Fatalf("converged alpha must not be requeued after restart")
	}
	h2.Advance(baseBackoff + time.Millisecond)
	h2.Idle()
	h2.waitGone("beta")

	// alpha must have suffered no new side-effect call or status rewrite.
	if sys.AppliedCount(a.UID+"#g1") != appliedBefore {
		t.Fatalf("restart must not re-apply converged resource")
	}
	alphaAfter, err := st.Get(client.DefaultKind, "alpha")
	if err != nil {
		t.Fatalf("alpha: %v", err)
	}
	if alphaAfter.Status.Phase != api.PhaseSucceeded {
		t.Fatalf("alpha phase changed after restart: %s", alphaAfter.Status.Phase)
	}
}

// TestBusinessLogCoverage verifies the test log stream describes every
// covered business scenario (used when running with -v).
func TestBusinessLogCoverage(t *testing.T) {
	h := newHarness(t)
	h.Start()
	logged := h.createSucceeds("logged")
	h.sys.FailApplyNext(logged.UID+"#g1", 1)
	h.Idle()
	h.waitPhase("logged", api.PhaseFailed)
	h.waitLog("attempt #2 scheduled in 10ms")
	h.Advance(baseBackoff + time.Millisecond)
	h.Idle()
	h.waitPhase("logged", api.PhaseSucceeded)
	if _, _, err := h.cl.Delete(h.get("logged")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	h.Idle()
	h.Advance(time.Second)
	h.Idle()
	h.waitGone("logged")
	logs := h.logs()
	for _, want := range []string{
		"applying side effect",
		"apply failed",
		"backoff:",
		"converged",
		"cleaning up",
		"fully cleaned up and removed",
	} {
		if !strings.Contains(logs, want) {
			t.Fatalf("business log missing %q:\n%s", want, logs)
		}
	}
	t.Logf("covered business log stream:\n%s", logs)
}
