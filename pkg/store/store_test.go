package store_test

import (
	"errors"
	"testing"

	v1 "github.com/lechandonga/declarative-reconcile-controller-go/pkg/apis/v1"
	"github.com/lechandonga/declarative-reconcile-controller-go/pkg/store"
)

func validSpec() v1.Spec {
	return v1.Spec{DisplayName: "widget-a", InstanceCount: 2}
}

// TestCreateVersioningAndValidation covers business:
//  1. 存储与版本化 — create assigns version 1 and the controller finalizer.
//  2. 非法/缺失字段 are rejected with ReasonValidation naming the field.
func TestCreateVersioningAndValidation(t *testing.T) {
	s := store.New()

	r, err := s.Create("w1", validSpec())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if r.ResourceVersion != 1 || r.Generation != 1 || r.UID == "" {
		t.Fatalf("unexpected initial versioning: %+v", r)
	}
	if !r.HasFinalizer(v1.FinalizerController) {
		t.Fatalf("controller finalizer missing: %v", r.Finalizers)
	}
	t.Logf("用例[创建即版本化]: rv=%d gen=%d uid=%s finalizers=%v -> PASS",
		r.ResourceVersion, r.Generation, r.UID, r.Finalizers)

	if _, err := s.Create("w1", validSpec()); err == nil {
		t.Fatal("duplicate create must fail")
	} else if se, _ := store.AsError(err); se.Reason != store.ReasonAlreadyExists {
		t.Fatalf("want AlreadyExists, got %v", err)
	}
	t.Logf("用例[同名创建拒绝]: reason=%s -> PASS", store.ReasonAlreadyExists)

	bad := []struct {
		name string
		spec v1.Spec
		want string
	}{
		{"missing displayName", v1.Spec{InstanceCount: 1}, "spec.displayName"},
		{"instanceCount too small", v1.Spec{DisplayName: "x", InstanceCount: 0}, "spec.instanceCount"},
		{"displayName too long", v1.Spec{DisplayName: string(make([]byte, 65)), InstanceCount: 1}, "spec.displayName"},
	}
	for _, tc := range bad {
		_, err := s.Create("bad-"+tc.name, tc.spec)
		se, ok := store.AsError(err)
		if !ok || se.Reason != store.ReasonValidation || se.Field != tc.want {
			t.Fatalf("%s: want validation error on %q, got %v", tc.name, tc.want, err)
		}
		t.Logf("用例[非法字段:%s]: reason=%s field=%s -> PASS", tc.name, se.Reason, se.Field)
	}

	// Stored objects must be safe from caller mutation.
	r.Spec.DisplayName = "mutated"
	again, _ := s.Get("w1")
	if again.Spec.DisplayName != "widget-a" {
		t.Fatal("store handed out a mutable pointer")
	}
	t.Log("用例[存储对象深拷贝隔离] -> PASS")
}

// TestOptimisticConcurrencyConflict covers business 版本冲突:
// an update carrying a stale resourceVersion is rejected with ReasonConflict
// and reports the current version; the fresh writer succeeds.
func TestOptimisticConcurrencyConflict(t *testing.T) {
	s := store.New()
	r, _ := s.Create("w1", validSpec())

	// Simulate a concurrent writer committing first (controller status write).
	newer, err := s.Mutate("w1", func(live *v1.Resource) (store.MutateResult, error) {
		n := live.Clone()
		n.Status.Phase = v1.PhaseReady
		return store.MutateResult{Resource: n, Changed: true}, nil
	})
	if err != nil {
		t.Fatalf("concurrent mutate: %v", err)
	}

	_, err = s.UpdateSpec("w1", r.ResourceVersion, validSpec())
	se, ok := store.AsError(err)
	if !ok || se.Reason != store.ReasonConflict {
		t.Fatalf("stale update must be Conflict, got %v", err)
	}
	if se.CurrentVersion != newer.ResourceVersion {
		t.Fatalf("conflict must report current version %d, got %d",
			newer.ResourceVersion, se.CurrentVersion)
	}
	t.Logf("用例[并发版本冲突]: supplied=rv%d current=rv%d reason=%s -> PASS",
		r.ResourceVersion, se.CurrentVersion, se.Reason)

	// Re-reading and presenting the fresh version succeeds (no-op identical
	// spec -> same version, no event).
	fresh, _ := s.Get("w1")
	updated, err := s.UpdateSpec("w1", fresh.ResourceVersion, validSpec())
	if err != nil {
		t.Fatalf("fresh update: %v", err)
	}
	if updated.ResourceVersion != newer.ResourceVersion {
		t.Fatalf("identical spec update must not bump version")
	}
	t.Logf("用例[冲突后重读重试成功]: rv=%d -> PASS", updated.ResourceVersion)

	// Updating a missing resource is NotFound, so callers can distinguish.
	_, err = s.UpdateSpec("ghost", 1, validSpec())
	if se, _ := store.AsError(err); !ok || se.Reason != store.ReasonNotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
	t.Log("用例[更新不存在资源]: reason=NotFound -> PASS")
}

// TestImmutableFieldsAndDeleting covers business 不允许修改的字段:
// spec fields are immutable after creation, and deleting resources reject
// user writes with a distinct reason.
func TestImmutableFieldsAndDeleting(t *testing.T) {
	s := store.New()
	r, _ := s.Create("w1", validSpec())

	changed := validSpec()
	changed.InstanceCount = 3
	_, err := s.UpdateSpec("w1", r.ResourceVersion, changed)
	if se, ok := store.AsError(err); !ok || se.Reason != store.ReasonImmutable || se.Field != "spec.instanceCount" {
		t.Fatalf("want Immutable on spec.instanceCount, got %v", err)
	}
	t.Logf("用例[修改不可变字段 instanceCount]: reason=%s field=%s -> PASS",
		store.ReasonImmutable, "spec.instanceCount")

	changed2 := validSpec()
	changed2.DisplayName = "other"
	_, err = s.UpdateSpec("w1", r.ResourceVersion, changed2)
	if se, _ := store.AsError(err); se == nil || se.Reason != store.ReasonImmutable {
		t.Fatalf("want Immutable on displayName, got %v", err)
	}
	t.Log("用例[修改不可变字段 displayName] -> PASS")

	if _, err := s.Delete("w1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = s.UpdateSpec("w1", r.ResourceVersion+2, validSpec())
	se, ok := store.AsError(err)
	if !ok || se.Reason != store.ReasonDeleting {
		t.Fatalf("want Deleting, got %v", err)
	}
	t.Log("用例[待删除资源拒绝用户更新]: reason=Deleting -> PASS")

	// Sanity: typed errors are detectable via errors.As-style assertion.
	if !errors.As(err, &se) {
		t.Fatal("store errors must support errors.As")
	}
}

// TestWatchEvents verifies only meaningful mutations emit events.
func TestWatchEvents(t *testing.T) {
	s := store.New()
	ch, cancel := s.Watch(8)
	defer cancel()

	r, _ := s.Create("w1", validSpec())
	ev := <-ch
	if ev.Type != store.EventCreated || ev.OldVersion != 0 {
		t.Fatalf("unexpected event %+v", ev)
	}

	_, _ = s.Mutate("w1", func(live *v1.Resource) (store.MutateResult, error) {
		n := live.Clone()
		n.Status.Phase = v1.PhaseReady
		return store.MutateResult{Resource: n, Changed: true}, nil
	})
	if ev := <-ch; ev.Type != store.EventUpdated {
		t.Fatalf("want updated event, got %+v", ev)
	}

	// No-op mutation emits nothing.
	_, _ = s.Mutate("w1", func(live *v1.Resource) (store.MutateResult, error) {
		return store.MutateResult{Changed: false}, nil
	})
	_, _ = s.UpdateSpec("w1", r.ResourceVersion+1, validSpec())
	select {
	case ev := <-ch:
		t.Fatalf("no-op writes must not emit events, got %+v", ev)
	default:
	}
	t.Log("用例[仅有效变更产生事件] -> PASS")
}
