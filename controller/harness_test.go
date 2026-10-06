package controller_test

import (
	"bytes"
	"errors"
	"io"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/declarative-reconcile-controller-go/api"
	"github.com/lechandonga/declarative-reconcile-controller-go/client"
	"github.com/lechandonga/declarative-reconcile-controller-go/clock"
	"github.com/lechandonga/declarative-reconcile-controller-go/controller"
	"github.com/lechandonga/declarative-reconcile-controller-go/external"
	"github.com/lechandonga/declarative-reconcile-controller-go/store"
)

const baseBackoff = 10 * time.Millisecond

// extReconciler adapts the fake external system to the Reconciler interface.
type extReconciler struct{ sys *external.System }

func (e extReconciler) Apply(r *api.Resource) error {
	detail, _ := r.Spec["image"].(string)
	return e.sys.Apply(r.UID+"#g"+itoa(r.Generation), detail)
}

func (e extReconciler) Cleanup(r *api.Resource) error {
	token := r.Status.AppliedToken
	if token == "" {
		token = r.UID + "#g" + itoa(r.Generation)
	}
	return e.sys.Cleanup(token)
}

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}

// harness wires every package together with a fake clock and a buffer that
// captures the business-level log stream.
type harness struct {
	t       *testing.T
	st      *store.Store
	cl      *client.Client
	sys     *external.System
	fc      *clock.Fake
	logBuf  *threadSafeBuffer
	logger  *log.Logger
	ctrl    *controller.Controller
	stopped atomic.Bool
}

type threadSafeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	fc := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	fc.SetPollStep(baseBackoff)
	buf := &threadSafeBuffer{}
	l := log.New(buf, "[biz] ", log.Lmicroseconds)
	st := store.New()
	sys := external.NewSystem()
	ctrl := controller.New(controller.Config{
		Store:        st,
		Reconciler:   extReconciler{sys: sys},
		Clock:        fc,
		BaseBackoff:  baseBackoff,
		MaxBackoff:   time.Second,
		PollInterval: baseBackoff,
		Logger:       l,
	})
	h := &harness{t: t, st: st, cl: client.New(st), sys: sys, fc: fc, logBuf: buf, logger: l, ctrl: ctrl}
	t.Cleanup(h.Stop)
	return h
}

func (h *harness) Start() { h.ctrl.Start() }

func (h *harness) Stop() {
	if h.stopped.CompareAndSwap(false, true) {
		h.ctrl.Stop()
	}
}

// Advance moves the fake clock and lets all work that becomes due complete.
// It never performs a real sleep: the fake clock fires timers synchronously,
// and Gosched merely lets already-runnable goroutines execute.
func (h *harness) Advance(d time.Duration) {
	h.fc.Advance(d)
	h.pumps(10)
}

// Idle runs runnable goroutines without moving the clock.
func (h *harness) Idle() {
	h.ctrl.Pump()
	h.yield()
	h.ctrl.Pump()
}

func (h *harness) pumps(n int) {
	for i := 0; i < n; i++ {
		h.ctrl.Pump()
		h.yield()
	}
}

// yield gives scheduler, forwarder and workers a bounded chance to run,
// without any real sleep.
func (h *harness) yield() {
	for i := 0; i < 30; i++ {
		runtime.Gosched()
	}
}

func (h *harness) get(name string) *api.Resource {
	h.t.Helper()
	r, err := h.st.Get(client.DefaultKind, name)
	if err != nil {
		h.t.Fatalf("get %s: %v", name, err)
	}
	return r
}

func (h *harness) createSucceeds(name string) *api.Resource {
	h.t.Helper()
	r, err := h.cl.Create(name, client.Spec{Region: "cn-east", Image: "app:1", Replicas: 2})
	if err != nil {
		h.t.Fatalf("create %s: %v", name, err)
	}
	return r
}

func (h *harness) waitPhase(name, phase string) *api.Resource {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		r, err := h.st.Get(client.DefaultKind, name)
		if errors.As(err, new(*store.NotFoundError)) && phase == "" {
			return nil
		}
		if err == nil && r.Status.Phase == phase {
			return r
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("resource %s never reached phase %q (last err=%v)", name, phase, err)
		}
		runtime.Gosched()
	}
}

func (h *harness) waitGone(name string) {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := h.st.Get(client.DefaultKind, name)
		if errors.As(err, new(*store.NotFoundError)) {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("resource %s was not physically removed", name)
		}
		runtime.Gosched()
	}
}

func (h *harness) logs() string { return h.logBuf.String() }

func (b *threadSafeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}
func (b *threadSafeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// waitLog waits (without sleeping) until a log line containing sub appears.
func (h *harness) waitLog(sub string) {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if strings.Contains(h.logs(), sub) {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for log %q\n%s", sub, h.logs())
		}
		runtime.Gosched()
	}
}

var _ io.Writer = (*threadSafeBuffer)(nil)

// newHarnessWith builds a controller over an existing store/external system
// (used to simulate process restart with shared durable state).
func newHarnessWith(t *testing.T, st *store.Store, sys *external.System, fc *clock.Fake) *harness {
	t.Helper()
	buf := &threadSafeBuffer{}
	l := log.New(buf, "[biz] ", log.Lmicroseconds)
	ctrl := controller.New(controller.Config{
		Store:        st,
		Reconciler:   extReconciler{sys: sys},
		Clock:        fc,
		BaseBackoff:  baseBackoff,
		MaxBackoff:   time.Second,
		PollInterval: baseBackoff,
		Logger:       l,
	})
	h := &harness{t: t, st: st, cl: client.New(st), sys: sys, fc: fc, logBuf: buf, logger: l, ctrl: ctrl}
	t.Cleanup(h.Stop)
	return h
}
