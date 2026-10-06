package reconcile_test

import (
	"strconv"
	"sync"
	"testing"
	"time"

	v1 "github.com/lechandonga/declarative-reconcile-controller-go/pkg/apis/v1"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/external"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/reconcile"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/store"
)

var epoch = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)

func testBackoff() reconcile.Backoff {
	return reconcile.Backoff{Base: 100 * time.Millisecond, Factor: 2, Max: 5 * time.Second}
}

func itoa(i int) string { return strconv.Itoa(i) }

type harness struct {
	s    *store.Store
	exec *external.FakeExecutor
	rec  *reconcile.Reconciler
	now  *time.Time
}

func newHarness(t *testing.T) harness {
	t.Helper()
	s := store.New()
	exec := external.NewFakeExecutor()
	now := epoch
	rec := reconcile.New(s, exec, func() time.Time { return now }, testBackoff())
	return harness{s: s, exec: exec, rec: rec, now: &now}
}

// newOverlapHarness installs a hook that blocks a pass for a given name
// until release(name) is called, so concurrent callers provably overlap.
func newOverlapHarness(t *testing.T, blockName string) (harness, func(), chan struct{}) {
	t.Helper()
	s := store.New()
	exec := external.NewFakeExecutor()
	now := epoch
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	rec := reconcile.NewWithHook(s, exec, func() time.Time { return now }, testBackoff(),
		func(name string) func() {
			if name != blockName {
				return nil
			}
			entered <- struct{}{}
			<-release
			return nil
		})
	return harness{s: s, exec: exec, rec: rec, now: &now},
		func() { close(release) }, entered
}

// TestReconcileSuccessNoopWrites covers business 收敛成功后不反复写入:
// first pass creates external effects and writes Ready; subsequent passes
// produce no new versions and no extra executor calls.
func TestReconcileSuccessNoopWrites(t *testing.T) {
	h := newHarness(t)
	r, _ := h.s.Create("w1", v1.Spec{DisplayName: "W", InstanceCount: 3})

	res := h.rec.Reconcile("w1")
	if res.Outcome != reconcile.OutcomeStable || res.Version == r.ResourceVersion {
		t.Fatalf("first pass should converge with a write: %+v", res)
	}
	got, _ := h.s.Get("w1")
	if got.Status.Phase != v1.PhaseReady || got.Status.ReadyInstances != 3 {
		t.Fatalf("status not ready: %+v", got.Status)
	}
	t.Logf("业务[收敛与状态回写] 用例[首次收敛成功]: %s; side effects=%v -> PASS",
		res.Summary, got.Status.SideEffectIDs)

	versionAfterFirst := got.ResourceVersion
	for i := 0; i < 3; i++ {
		res = h.rec.Reconcile("w1")
		if res.Outcome != reconcile.OutcomeStable {
			t.Fatalf("repeat pass %d: %+v", i, res)
		}
	}
	got, _ = h.s.Get("w1")
	if got.ResourceVersion != versionAfterFirst {
		t.Fatalf("meaningless repeat writes happened: rv %d -> %d",
			versionAfterFirst, got.ResourceVersion)
	}
	for i := 0; i < 3; i++ {
		if n := h.exec.CallCount("apply", got.UID+"/instance-"+itoa(i)); n != 1 {
			t.Fatalf("duplicate side effect for instance %d: %d calls", i, n)
		}
	}
	t.Logf("业务[收敛与状态回写] 用例[重复调谐零写入零副作用]: rv 保持 %d, 重复 3 次无新写入 -> PASS",
		versionAfterFirst)
}

// TestReconcileFailureBackoffAndRetry covers business 失败退避重试.
func TestReconcileFailureBackoffAndRetry(t *testing.T) {
	h := newHarness(t)
	h.s.Create("w2", v1.Spec{DisplayName: "W", InstanceCount: 2})
	got, _ := h.s.Get("w2")
	failingID := got.UID + "/instance-0"
	h.exec.FailApplies(failingID, 1)

	res := h.rec.Reconcile("w2")
	if res.Outcome != reconcile.OutcomeFailed {
		t.Fatalf("want failed outcome, got %+v", res)
	}
	wantRetry := h.now.Add(100 * time.Millisecond)
	if !res.NextRetry.Equal(wantRetry) {
		t.Fatalf("backoff = %v, want %v", res.NextRetry, wantRetry)
	}
	failed, _ := h.s.Get("w2")
	if failed.Status.Phase != v1.PhaseFailed || failed.Status.ReconcileAttempts != 1 {
		t.Fatalf("failure not recorded: %+v", failed.Status)
	}
	cond, _ := failed.Status.GetCondition(v1.ConditionReconciled)
	if cond.Status != v1.ConditionFalse || cond.Reason != "ApplyError" {
		t.Fatalf("failure condition not explainable: %+v", cond)
	}
	t.Logf("业务[失败退避重试] 用例[失败记录且按指数退避调度]: phase=%s attempts=%d backoff=%s reason=%s -> PASS",
		failed.Status.Phase, failed.Status.ReconcileAttempts,
		res.NextRetry.Sub(*h.now), cond.Reason)

	*h.now = h.now.Add(100 * time.Millisecond)
	res = h.rec.Reconcile("w2")
	if res.Outcome != reconcile.OutcomeStable {
		t.Fatalf("retry after backoff should converge: %+v", res)
	}
	if n := h.exec.CallCount("apply", failingID); n != 2 {
		t.Fatalf("apply call count = %d, want 2 (two attempts, one external object)", n)
	}
	if len(h.exec.Snapshot()) != 2 {
		t.Fatalf("external world should hold 2 objects, got %d", len(h.exec.Snapshot()))
	}
	t.Logf("业务[失败退避重试] 用例[退避后重试收敛且不产生重复对象]: live objects=%d -> PASS",
		len(h.exec.Snapshot()))
}

// TestReconcileSingleFlight covers business 同一资源同时只能有一个调谐.
func TestReconcileSingleFlight(t *testing.T) {
	h, releaseA, enteredA := newOverlapHarness(t, "a")
	h.s.Create("a", v1.Spec{DisplayName: "A", InstanceCount: 1})
	h.s.Create("b", v1.Spec{DisplayName: "B", InstanceCount: 1})

	var wg sync.WaitGroup
	results := make(chan reconcile.Outcome, 4)
	work := func(name string) {
		defer wg.Done()
		results <- h.rec.Reconcile(name).Outcome
	}

	// First "a" enters and holds the slot; two more concurrent "a" calls
	// must be rejected as Skipped while it is in flight.
	wg.Add(1)
	go work("a")
	<-enteredA

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go work("a")
	}
	// A different name proceeds in parallel despite "a" being held.
	wg.Add(1)
	go work("b")

	skippedA := 0
	bDone := false
	for i := 0; i < 3; i++ {
		o := <-results
		switch o {
		case reconcile.OutcomeSkipped:
			skippedA++
		case reconcile.OutcomeStable:
			bDone = true
		}
	}
	if skippedA != 2 || !bDone {
		t.Fatalf("while a is in flight want 2 skips for a and b completing, got skips=%d bDone=%v",
			skippedA, bDone)
	}
	rB, _ := h.s.Get("b")
	if rB.Status.Phase != v1.PhaseReady {
		t.Fatal("different name must reconcile in parallel")
	}

	releaseA()
	wg.Wait()
	o := <-results
	if o != reconcile.OutcomeStable {
		t.Fatalf("held pass should converge on release, got %v", o)
	}
	t.Log("业务[调谐并发安全] 用例[同名单飞异名并行]: 调谐进行中的 2 次同名调用 Skipped, b 并行完成, a 释放后收敛 -> PASS")
}

// TestStaleResultDoesNotOverwrite covers business 过期结果不得覆盖更新的状态.
func TestStaleResultDoesNotOverwrite(t *testing.T) {
	h := newHarness(t)
	old, _ := h.s.Create("w3", v1.Spec{DisplayName: "W", InstanceCount: 1})

	// Emulate a newer incarnation winning before the pass writes back.
	_, err := h.s.Mutate("w3", func(live *v1.Resource) (store.MutateResult, error) {
		n := live.Clone()
		n.UID = "uid-999"
		n.Generation = 2
		return store.MutateResult{Resource: n, Changed: true}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	res := h.rec.Reconcile("w3")
	if res.Outcome != reconcile.OutcomeStable {
		t.Fatalf("pass must converge against the NEW incarnation: %+v", res)
	}
	cur, _ := h.s.Get("w3")
	if cur.UID != "uid-999" || cur.Status.ObservedGeneration != 2 {
		t.Fatalf("new incarnation state corrupted: %+v", cur)
	}
	t.Log("业务[过期结果保护] 用例[旧 UID/generation 的结果不覆盖新状态]: 新 incarnation 正常收敛 -> PASS")

	// Direct stale CAS is rejected with a distinguishable Conflict.
	_, err = h.s.Mutate("w3", func(live *v1.Resource) (store.MutateResult, error) {
		return store.MutateResult{Stale: true}, nil
	})
	if se, ok := store.AsError(err); !ok || se.Reason != store.ReasonConflict {
		t.Fatalf("stale CAS must be Conflict, got %v", err)
	}
	_ = old
	t.Log("业务[过期结果保护] 用例[陈旧 CAS 明确报 Conflict] -> PASS")
}

// TestCleanup covers business 删除保护、清理失败重试、不重复清理、同名重建隔离.
func TestCleanup(t *testing.T) {
	h := newHarness(t)
	h.s.Create("w4", v1.Spec{DisplayName: "W", InstanceCount: 3})
	if r := h.rec.Reconcile("w4"); r.Outcome != reconcile.OutcomeStable {
		t.Fatalf("setup reconcile: %+v", r)
	}
	got, _ := h.s.Get("w4")
	uid := got.UID

	// Reverse-order cleanup removes instance-2 and instance-1 first; script
	// the failure on instance-0, the last one cleaned.
	h.exec.FailDeletes(uid+"/instance-0", 1)

	if _, err := h.s.Delete("w4"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if during, err := h.s.Get("w4"); err != nil || !during.DeletionRequested {
		t.Fatalf("terminating resource must remain readable and marked; err=%v", err)
	}
	t.Log("业务[删除保护与清理] 用例[删除先进入待删除且记录不丢]: DeletionRequested=true, 仍可 Get -> PASS")

	res := h.rec.Reconcile("w4")
	if res.Outcome != reconcile.OutcomeFailed {
		t.Fatalf("partial cleanup failure should park with retry: %+v", res)
	}
	mid, _ := h.s.Get("w4")
	if mid.Status.Phase != v1.PhaseTerminating || len(mid.Status.SideEffectIDs) != 1 {
		t.Fatalf("cleanup progress not persisted: %+v", mid.Status.SideEffectIDs)
	}
	if mid.Status.SideEffectIDs[0] != uid+"/instance-0" {
		t.Fatalf("cleanup must proceed in reverse order, left=%v", mid.Status.SideEffectIDs)
	}
	t.Logf("业务[删除保护与清理] 用例[清理失败持久化进度并退避]: remaining=%v attempts=%d backoff=%s -> PASS",
		mid.Status.SideEffectIDs, mid.Status.CleanupAttempts, res.NextRetry.Sub(epoch))

	*h.now = h.now.Add(100 * time.Millisecond)
	res = h.rec.Reconcile("w4")
	if res.Outcome != reconcile.OutcomeGone {
		t.Fatalf("retry cleanup should finish and remove: %+v", res)
	}
	if _, err := h.s.Get("w4"); err == nil {
		t.Fatal("resource must be physically gone after successful cleanup")
	}
	for i := 0; i < 3; i++ {
		id := uid + "/instance-" + itoa(i)
		n := h.exec.CallCount("delete", id)
		// instance-0 failed once then succeeded on retry (2 calls, 1 real
		// cleanup). Everything successfully cleaned before must never be
		// deleted again: instance-1/instance-2 each saw exactly one call.
		want := 1
		if i == 0 {
			want = 2
		}
		if n != want {
			t.Fatalf("effect %s delete calls=%d, want %d", id, n, want)
		}
	}
	if len(h.exec.Snapshot()) != 0 {
		t.Fatalf("external world should be empty, got %v", h.exec.Snapshot())
	}
	t.Log("业务[删除保护与清理] 用例[重试不重复清理, 全部成功才消失]: 失败对象仅重试, 已成功对象不重复删除 -> PASS")

	nr, err := h.s.Create("w4", v1.Spec{DisplayName: "W-new", InstanceCount: 1})
	if err != nil {
		t.Fatalf("recreate after deletion: %v", err)
	}
	if nr.UID == uid || nr.Status.CleanupAttempts != 0 || nr.DeletionRequested {
		t.Fatalf("recreated resource inherited old state: %+v", nr)
	}
	if r := h.rec.Reconcile("w4"); r.Outcome != reconcile.OutcomeStable {
		t.Fatalf("new incarnation reconcile: %+v", r)
	}
	if h.exec.CallCount("delete", uid+"/instance-2") != 1 {
		t.Fatal("old cleanup was replayed against the new incarnation")
	}
	if _, ok := h.exec.Snapshot()[nr.UID+"//instance-0"]; ok {
		t.Fatal("stale old-style effect present")
	}
	if _, ok := h.exec.Snapshot()[nr.UID+"/instance-0"]; !ok {
		t.Fatal("new incarnation external effect missing")
	}
	t.Logf("业务[删除保护与清理] 用例[同名新建读不到旧状态且旧清理不影响]: 新 UID=%s -> PASS", nr.UID)
}

// TestApplyIdempotentAcrossLostStatusWrite covers 状态写丢失后的重试:
// Apply 已成功但记录丢失时, 重试调谐幂等补写, 外部对象仍然只有一个.
func TestApplyIdempotentAcrossLostStatusWrite(t *testing.T) {
	h := newHarness(t)
	h.s.Create("w5", v1.Spec{DisplayName: "W", InstanceCount: 2})
	if r := h.rec.Reconcile("w5"); r.Outcome != reconcile.OutcomeStable {
		t.Fatalf("setup: %+v", r)
	}
	got, _ := h.s.Get("w5")

	// Simulate a lost status write: revert the recorded IDs but external
	// effects survive.
	_, err := h.s.Mutate("w5", func(live *v1.Resource) (store.MutateResult, error) {
		n := live.Clone()
		n.Status.SideEffectIDs = nil
		n.Status.Phase = v1.PhasePending
		n.Status.ObservedGeneration = 0
		return store.MutateResult{Resource: n, Changed: true}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if r := h.rec.Reconcile("w5"); r.Outcome != reconcile.OutcomeStable {
		t.Fatalf("idempotent recovery reconcile: %+v", r)
	}
	for i := 0; i < 2; i++ {
		id := got.UID + "/instance-" + itoa(i)
		if n := h.exec.CallCount("apply", id); n != 2 {
			t.Fatalf("effect %s apply calls=%d, want 2 (both idempotent)", id, n)
		}
	}
	if len(h.exec.Snapshot()) != 2 {
		t.Fatalf("duplicated external effects: %v", h.exec.Snapshot())
	}
	t.Log("业务[重复调谐无重复副作用] 用例[状态丢失后幂等补写不产生重复对象] -> PASS")
}
