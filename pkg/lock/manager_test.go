package lock

import (
	"testing"
	"time"
)

func TestLockManager_Acquire(t *testing.T) {
	tmpDir := t.TempDir()
	lm := NewLockManager(tmpDir)

	acquired, err := lm.Acquire("task-1", "worker-1", RoleWorker)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	if !acquired {
		t.Error("Expected lock to be acquired")
	}

	if !lm.IsLocked("task-1") {
		t.Error("Expected task-1 to be locked")
	}
}

func TestLockManager_AcquireAlreadyLocked(t *testing.T) {
	tmpDir := t.TempDir()
	lm := NewLockManager(tmpDir)

	lm.Acquire("task-1", "worker-1", RoleWorker)

	acquired, _ := lm.Acquire("task-1", "worker-2", RoleWorker)
	if acquired {
		t.Error("Expected acquire to fail for already locked task")
	}
}

func TestLockManager_Release(t *testing.T) {
	tmpDir := t.TempDir()
	lm := NewLockManager(tmpDir)

	lm.Acquire("task-1", "worker-1", RoleWorker)

	err := lm.Release("task-1", "worker-1")
	if err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	if lm.IsLocked("task-1") {
		t.Error("Expected task-1 to be unlocked after release")
	}
}

func TestLockManager_ReleaseWrongOwner(t *testing.T) {
	tmpDir := t.TempDir()
	lm := NewLockManager(tmpDir)

	lm.Acquire("task-1", "worker-1", RoleWorker)

	err := lm.Release("task-1", "worker-2")
	if err == nil {
		t.Error("Expected error when releasing with wrong owner")
	}
}

func TestLockManager_ForceRelease(t *testing.T) {
	tmpDir := t.TempDir()
	lm := NewLockManager(tmpDir)

	lm.Acquire("task-1", "worker-1", RoleWorker)

	err := lm.ForceRelease("task-1", "admin intervention")
	if err != nil {
		t.Fatalf("ForceRelease failed: %v", err)
	}

	if lm.IsLocked("task-1") {
		t.Error("Expected task-1 to be unlocked after force release")
	}
}

func TestLockManager_GetOwner(t *testing.T) {
	tmpDir := t.TempDir()
	lm := NewLockManager(tmpDir)

	lm.Acquire("task-1", "worker-1", RoleWorker)

	ownerID, role, ok := lm.GetOwner("task-1")
	if !ok {
		t.Fatal("Expected to find lock owner")
	}

	if ownerID != "worker-1" {
		t.Errorf("Expected owner 'worker-1', got '%s'", ownerID)
	}

	if role != RoleWorker {
		t.Errorf("Expected role 'worker', got '%s'", role)
	}
}

func TestLockManager_Extend(t *testing.T) {
	tmpDir := t.TempDir()
	lm := NewLockManager(tmpDir)

	lm.Acquire("task-1", "worker-1", RoleWorker)

	entry, _ := lm.GetLock("task-1")
	originalExpiry := entry.ExpiresAt

	err := lm.Extend("task-1", "worker-1", 5*time.Minute)
	if err != nil {
		t.Fatalf("Extend failed: %v", err)
	}

	entry, _ = lm.GetLock("task-1")
	if !entry.ExpiresAt.After(originalExpiry) {
		t.Error("Expected expiry time to be extended")
	}
}

func TestLockManager_Cleanup(t *testing.T) {
	tmpDir := t.TempDir()
	lm := NewLockManager(tmpDir)

	lm.AcquireWithTTL("task-1", "worker-1", RoleWorker, 10*time.Millisecond)
	lm.AcquireWithTTL("task-2", "worker-2", RoleWorker, 10*time.Millisecond)
	lm.AcquireWithTTL("task-3", "worker-3", RoleWorker, 1*time.Hour)

	time.Sleep(20 * time.Millisecond)

	expired := lm.Cleanup()

	if len(expired) != 2 {
		t.Errorf("Expected 2 expired locks, got %d", len(expired))
	}

	if lm.IsLocked("task-1") {
		t.Error("Expected task-1 to be cleaned up")
	}

	if !lm.IsLocked("task-3") {
		t.Error("Expected task-3 to still be locked")
	}
}

func TestLockManager_TTLByRole(t *testing.T) {
	tmpDir := t.TempDir()
	lm := NewLockManager(tmpDir)

	expectedTTLs := map[Role]time.Duration{
		RoleOrchestrator: 30 * time.Minute,
		RoleMonitor:      20 * time.Minute,
		RoleWorker:       15 * time.Minute,
	}

	for role, expected := range expectedTTLs {
		actual := lm.GetDefaultTTL(role)
		if actual != expected {
			t.Errorf("Expected %s TTL to be %v, got %v", role, expected, actual)
		}
	}
}

func TestLockManager_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	lm1 := NewLockManager(tmpDir)
	lm1.Acquire("task-1", "worker-1", RoleWorker)

	lm2 := NewLockManager(tmpDir)

	if !lm2.IsLocked("task-1") {
		t.Error("Expected lock to persist after reload")
	}

	ownerID, _, ok := lm2.GetOwner("task-1")
	if !ok || ownerID != "worker-1" {
		t.Errorf("Expected owner to persist, got '%s'", ownerID)
	}
}

func TestLockManager_GetByOwner(t *testing.T) {
	tmpDir := t.TempDir()
	lm := NewLockManager(tmpDir)

	lm.Acquire("task-1", "worker-1", RoleWorker)
	lm.Acquire("task-2", "worker-1", RoleWorker)
	lm.Acquire("task-3", "worker-2", RoleWorker)

	entries := lm.GetByOwner("worker-1")
	if len(entries) != 2 {
		t.Errorf("Expected 2 locks for worker-1, got %d", len(entries))
	}
}

func TestLockManager_GetByRole(t *testing.T) {
	tmpDir := t.TempDir()
	lm := NewLockManager(tmpDir)

	lm.Acquire("task-1", "worker-1", RoleWorker)
	lm.Acquire("task-2", "worker-2", RoleWorker)
	lm.Acquire("task-3", "monitor-1", RoleMonitor)

	workerLocks := lm.GetByRole(RoleWorker)
	if len(workerLocks) != 2 {
		t.Errorf("Expected 2 worker locks, got %d", len(workerLocks))
	}

	monitorLocks := lm.GetByRole(RoleMonitor)
	if len(monitorLocks) != 1 {
		t.Errorf("Expected 1 monitor lock, got %d", len(monitorLocks))
	}
}

func TestLockManager_GetStats(t *testing.T) {
	tmpDir := t.TempDir()
	lm := NewLockManager(tmpDir)

	lm.Acquire("task-1", "worker-1", RoleWorker)
	lm.Acquire("task-2", "monitor-1", RoleMonitor)

	stats := lm.GetStats()

	if stats.Total != 2 {
		t.Errorf("Expected 2 total locks, got %d", stats.Total)
	}

	if stats.ByRole[RoleWorker] != 1 {
		t.Errorf("Expected 1 worker lock, got %d", stats.ByRole[RoleWorker])
	}

	if stats.ByRole[RoleMonitor] != 1 {
		t.Errorf("Expected 1 monitor lock, got %d", stats.ByRole[RoleMonitor])
	}
}
