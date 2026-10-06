package controller_test

import (
	"context"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/lechandonga/declarative-reconcile-controller-go/pkg/apis/v1"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/clock"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/controller"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/external"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/reconcile"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/store"
)

var testEpoch = time.Date(2026, 10, 6, 10, 0, 0, 0, time.UTC)

type safeBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type env struct {
	s    *store.Store
	exec *external.FakeExecutor
	clk  *clock.FakeClock
	logs *safeBuffer
}

func newEnv() env {
	return env{
		s:    store.New(),
		exec: external.NewFakeExecutor(),
		clk:  clock.NewFakeClock(testEpoch),
		logs: &safeBuffer{},
	}
}

func (e env) start(t *testing.T, resync time.Duration) (*controller.Controller, context.CancelFunc) {
	t.Helper()
	rec := reconcile.New(e.s, e.exec, e.clk.Now, reconcile.DefaultBackoff)
	ctl := controller.New(controller.Config{
		Store:       e.s,
		Reconciler:  rec,
		Clock:       e.clk,
		Logger:      log.New(e.logs, "", 0),
		ResyncEvery: resync,
	})
	ctx, cancel := context.WithCancel(context.Background())
	ctl.Start(ctx)
	return ctl, cancel
}

// waitForCondition bridges goroutine scheduling with bounded polling. It
// never waits on real backoff (the fake clock is advanced explicitly) and
// performs no network I/O.
func waitForCondition(t *testing.T, fn func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met: %s\nlogs:\n%s", msg, "")
}

// TestWatchTriggersReconcile covers business 仅资源相关变更触发调谐,
// and the full delete -> cleanup -> physical removal chain end to end.
func TestWatchTriggersReconcile(t *testing.T) {
	e := newEnv()
	ctl, cancel := e.start(t, time.Hour)
	defer func() { cancel(); ctl.Stop() }()

	_, err := e.s.Create("w1", v1.Spec{DisplayName: "W", InstanceCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, func() bool {
		got, err := e.s.Get("w1")
		return err == nil && got.Status.Phase == v1.PhaseReady
	}, "resource converges via watch:create")
	ready, _ := e.s.Get("w1")
	if ready.Status.ReadyInstances != 2 {
		t.Fatalf("expected 2 ready instances, got %d", ready.Status.ReadyInstances)
	}
	if rd, pk := ctl.QueueLen(); rd != 0 || pk != 0 {
		t.Fatalf("queue must drain after convergence, ready=%d parked=%d", rd, pk)
	}
	t.Log("业务[资源变更触发调谐] 用例[创建事件自动收敛并清空队列] -> PASS")

	// A user deletion is a resource change: watch triggers cleanup.
	if _, err := e.s.Delete("w1"); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, func() bool {
		_, err := e.s.Get("w1")
		se, ok := store.AsError(err)
		return ok && se.Reason == store.ReasonNotFound
	}, "deleted resource is cleaned up and physically removed")
	if len(e.exec.Snapshot()) != 0 {
		t.Fatalf("external effects leaked: %v", e.exec.Snapshot())
	}
	t.Log("业务[删除清理链路] 用例[删除事件触发清理并物理移除, 外部对象清空] -> PASS")
}

// TestFailureBackoffViaFakeClock covers business 退避重试而非原地空转.
func TestFailureBackoffViaFakeClock(t *testing.T) {
	e := newEnv()
	ctl, cancel := e.start(t, time.Hour)
	defer func() { cancel(); ctl.Stop() }()

	r, _ := e.s.Create("w2", v1.Spec{DisplayName: "W", InstanceCount: 1})
	failingID := r.UID + "/instance-0"
	e.exec.FailApplies(failingID, 1)

	waitForCondition(t, func() bool {
		got, err := e.s.Get("w2")
		return err == nil && got.Status.Phase == v1.PhaseFailed
	}, "first attempt fails")
	failed, _ := e.s.Get("w2")
	if !failed.Status.NextRetryAt.Equal(testEpoch.Add(100 * time.Millisecond)) {
		t.Fatalf("unexpected retry time %v", failed.Status.NextRetryAt)
	}
	if _, parked := ctl.QueueLen(); parked != 1 {
		t.Fatalf("failed key must be parked on backoff")
	}
	t.Log("业务[退避重试] 用例[失败后键被挂起到 nextRetryAt, 不空转]: backoff=100ms parked=1 -> PASS")

	e.clk.Step(50 * time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	got, _ := e.s.Get("w2")
	if got.Status.ReconcileAttempts != 1 {
		t.Fatalf("retry fired before backoff elapsed: attempts=%d", got.Status.ReconcileAttempts)
	}
	t.Log("业务[退避重试] 用例[时间未到不重试]: +50ms attempts 仍为 1 -> PASS")

	e.clk.Step(60 * time.Millisecond)
	waitForCondition(t, func() bool {
		cur, err := e.s.Get("w2")
		return err == nil && cur.Status.Phase == v1.PhaseReady
	}, "retry after backoff converges")
	t.Log("业务[退避重试] 用例[假时钟跨过退避点后重试收敛]: +110ms -> Ready -> PASS")
}

// TestRestartRecovery covers business 控制器重启后从已有状态接着处理:
// an unfinished Failed resource and an interrupted Terminating cleanup are
// both resumed purely from the startup relist, with no watch history.
func TestRestartRecovery(t *testing.T) {
	e := newEnv()

	// First controller lifetime: converge partially, then die.
	{
		rec := reconcile.New(e.s, e.exec, e.clk.Now, reconcile.DefaultBackoff)
		ctl := controller.New(controller.Config{
			Store: e.s, Reconciler: rec, Clock: e.clk,
			Logger: log.New(e.logs, "", 0), ResyncEvery: time.Hour,
		})
		ctx, cancel := context.WithCancel(context.Background())
		r, _ := e.s.Create("w3", v1.Spec{DisplayName: "W", InstanceCount: 3})
		e.exec.FailApplies(r.UID+"/instance-2", 1)
		ctl.Start(ctx)
		waitForCondition(t, func() bool {
			got, _ := e.s.Get("w3")
			return got.Status.Phase == v1.PhaseFailed
		}, "first controller records failure")
		cancel()
		ctl.Stop()
	}
	left, _ := e.s.Get("w3")
	if left.Status.Phase != v1.PhaseFailed || len(left.Status.SideEffectIDs) != 2 {
		t.Fatalf("unexpected persisted state: phase=%s effects=%v",
			left.Status.Phase, left.Status.SideEffectIDs)
	}
	t.Log("业务[重启恢复] 用例[控制器崩溃后进度持久化]: phase=Failed, 已完成 2/3 -> PASS")

	// A brand new controller with no watch history resumes via relist.
	e.clk.Step(time.Second)
	ctl, cancel := e.start(t, time.Hour)
	defer func() { cancel(); ctl.Stop() }()
	waitForCondition(t, func() bool {
		got, err := e.s.Get("w3")
		return err == nil && got.Status.Phase == v1.PhaseReady && got.Status.ReadyInstances == 3
	}, "restarted controller finishes unfinished resource")
	if !strings.Contains(e.logs.String(), `[recovery:startup] enqueue "w3"`) {
		t.Fatalf("startup relist missing from logs:\n%s", e.logs.String())
	}
	t.Log("业务[重启恢复] 用例[新控制器启动 relist 接续 Failed 资源]: recovery:startup -> Ready 3/3 -> PASS")
}

// TestRestartRecoveryTerminating resumes an interrupted delete cleanup.
func TestRestartRecoveryTerminating(t *testing.T) {
	e := newEnv()
	r, _ := e.s.Create("w4", v1.Spec{DisplayName: "W", InstanceCount: 2})
	rec := reconcile.New(e.s, e.exec, e.clk.Now, reconcile.DefaultBackoff)
	if res := rec.Reconcile("w4"); res.Outcome != reconcile.OutcomeStable {
		t.Fatalf("setup: %+v", res)
	}
	if _, err := e.s.Delete("w4"); err != nil {
		t.Fatal(err)
	}
	e.exec.FailDeletes(r.UID+"/instance-0", 1)
	if res := rec.Reconcile("w4"); res.Outcome != reconcile.OutcomeFailed {
		t.Fatalf("partial cleanup: %+v", res)
	}

	e.clk.Step(time.Second)
	ctl, cancel := e.start(t, time.Hour)
	defer func() { cancel(); ctl.Stop() }()
	waitForCondition(t, func() bool {
		_, err := e.s.Get("w4")
		se, ok := store.AsError(err)
		return ok && se.Reason == store.ReasonNotFound
	}, "restarted controller finishes cleanup")
	if len(e.exec.Snapshot()) != 0 {
		t.Fatalf("external effects leaked: %v", e.exec.Snapshot())
	}
	t.Log("业务[重启恢复] 用例[重启后续做 Terminating 资源清理直到物理删除] -> PASS")
}

// TestWorkQueueBackoffAndRecreate proves:
//   - a failed key parked on backoff is not promoted by watch-echo Adds;
//   - it is promoted only when the fake clock crosses the retry time;
//   - a same-name recreate (AllowEarly) releases it immediately.
func TestWorkQueueBackoffAndRecreate(t *testing.T) {
	e := newEnv()
	q := controller.NewWorkQueue(e.clk)

	q.AddAfter("k", testEpoch.Add(100*time.Millisecond))
	// Watch echo of the Failed status write must be ignored while parked.
	q.Add("k")
	ready, parked := q.Len()
	if ready != 0 || parked != 1 {
		t.Fatalf("echo Add defeated backoff: ready=%d parked=%d", ready, parked)
	}
	t.Log("业务[退避不被回声打破] 用例[挂起期间即时 Add 被忽略]: parked=1 -> PASS")

	e.clk.Step(50 * time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if got, _ := q.Len(); got != 0 {
		t.Fatal("key became ready before backoff elapsed")
	}
	e.clk.Step(60 * time.Millisecond)
	waitForCondition(t, func() bool {
		ready, _ = q.Len()
		return ready == 1
	}, "key promoted after crossing retry time")
	name, ok := q.Get()
	if !ok || name != "k" {
		t.Fatalf("got %q ok=%v", name, ok)
	}
	t.Log("业务[退避调度] 用例[时钟跨过退避点才出队] -> PASS")

	// Same-name recreate releases a fresh park immediately.
	q.AddAfter("k2", testEpoch.Add(10*time.Second))
	if _, parked := q.Len(); parked != 1 {
		t.Fatal("expected parked key")
	}
	q.AllowEarly("k2")
	ready, _ = q.Len()
	if ready != 1 {
		t.Fatalf("recreate must bypass old backoff, ready=%d", ready)
	}
	t.Log("业务[同名重建不受旧退避影响] 用例[AllowEarly 立即释放挂起键] -> PASS")
}
