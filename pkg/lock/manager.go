package lock

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Role string

const (
	RoleOrchestrator Role = "orchestrator"
	RoleMonitor      Role = "monitor"
	RoleWorker       Role = "worker"
)

type LockEntry struct {
	TaskID     string    `json:"task_id"`
	OwnerID    string    `json:"owner_id"`
	OwnerRole  Role      `json:"owner_role"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	Reason     string    `json:"reason,omitempty"`
}

func (e *LockEntry) IsExpired() bool {
	return time.Now().After(e.ExpiresAt)
}

func (e *LockEntry) TimeRemaining() time.Duration {
	remaining := time.Until(e.ExpiresAt)
	if remaining < 0 {
		return 0
	}
	return remaining
}

type LockManager struct {
	path     string
	locks    map[string]LockEntry
	defaults map[Role]time.Duration
	mu       sync.RWMutex
}

func NewLockManager(workspace string) *LockManager {
	memoryDir := filepath.Join(workspace, "memory")
	os.MkdirAll(memoryDir, 0o755)

	defaults := map[Role]time.Duration{
		RoleOrchestrator: 30 * time.Minute,
		RoleMonitor:      20 * time.Minute,
		RoleWorker:       15 * time.Minute,
	}

	lm := &LockManager{
		path:     filepath.Join(memoryDir, "locks.json"),
		locks:    make(map[string]LockEntry),
		defaults: defaults,
	}
	lm.load()
	return lm
}

func (lm *LockManager) load() error {
	data, err := os.ReadFile(lm.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var entries []LockEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsExpired() {
			lm.locks[entry.TaskID] = entry
		}
	}
	return nil
}

func (lm *LockManager) save() error {
	entries := make([]LockEntry, 0, len(lm.locks))
	for _, entry := range lm.locks {
		entries = append(entries, entry)
	}

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(lm.path, data, 0o644)
}

func (lm *LockManager) Acquire(taskID, ownerID string, role Role) (bool, error) {
	return lm.AcquireWithTTL(taskID, ownerID, role, 0)
}

func (lm *LockManager) AcquireWithTTL(taskID, ownerID string, role Role, ttl time.Duration) (bool, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if entry, exists := lm.locks[taskID]; exists {
		if entry.IsExpired() {
			delete(lm.locks, taskID)
		} else {
			return false, nil
		}
	}

	if ttl == 0 {
		ttl = lm.defaults[role]
	}

	now := time.Now()
	entry := LockEntry{
		TaskID:     taskID,
		OwnerID:    ownerID,
		OwnerRole:  role,
		AcquiredAt: now,
		ExpiresAt:  now.Add(ttl),
	}

	lm.locks[taskID] = entry
	if err := lm.save(); err != nil {
		delete(lm.locks, taskID)
		return false, err
	}

	return true, nil
}

func (lm *LockManager) Release(taskID, ownerID string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[taskID]
	if !exists {
		return nil
	}

	if entry.OwnerID != ownerID {
		return fmt.Errorf("lock owned by %s, not %s", entry.OwnerID, ownerID)
	}

	delete(lm.locks, taskID)
	return lm.save()
}

func (lm *LockManager) ForceRelease(taskID, reason string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if _, exists := lm.locks[taskID]; !exists {
		return fmt.Errorf("lock not found: %s", taskID)
	}

	delete(lm.locks, taskID)
	return lm.save()
}

func (lm *LockManager) IsLocked(taskID string) bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	entry, exists := lm.locks[taskID]
	if !exists {
		return false
	}
	return !entry.IsExpired()
}

func (lm *LockManager) GetOwner(taskID string) (ownerID string, role Role, ok bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	entry, exists := lm.locks[taskID]
	if !exists || entry.IsExpired() {
		return "", "", false
	}
	return entry.OwnerID, entry.OwnerRole, true
}

func (lm *LockManager) GetLock(taskID string) (LockEntry, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	entry, exists := lm.locks[taskID]
	if !exists || entry.IsExpired() {
		return LockEntry{}, false
	}
	return entry, true
}

func (lm *LockManager) Extend(taskID, ownerID string, extra time.Duration) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, exists := lm.locks[taskID]
	if !exists {
		return fmt.Errorf("lock not found: %s", taskID)
	}

	if entry.IsExpired() {
		delete(lm.locks, taskID)
		return fmt.Errorf("lock expired: %s", taskID)
	}

	if entry.OwnerID != ownerID {
		return fmt.Errorf("lock owned by %s, not %s", entry.OwnerID, ownerID)
	}

	entry.ExpiresAt = entry.ExpiresAt.Add(extra)
	lm.locks[taskID] = entry
	return lm.save()
}

func (lm *LockManager) Cleanup() []LockEntry {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	var expired []LockEntry
	for taskID, entry := range lm.locks {
		if entry.IsExpired() {
			expired = append(expired, entry)
			delete(lm.locks, taskID)
		}
	}

	if len(expired) > 0 {
		lm.save()
	}
	return expired
}

func (lm *LockManager) GetAll() []LockEntry {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	var entries []LockEntry
	for _, entry := range lm.locks {
		if !entry.IsExpired() {
			entries = append(entries, entry)
		}
	}
	return entries
}

func (lm *LockManager) GetByOwner(ownerID string) []LockEntry {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	var entries []LockEntry
	for _, entry := range lm.locks {
		if entry.OwnerID == ownerID && !entry.IsExpired() {
			entries = append(entries, entry)
		}
	}
	return entries
}

func (lm *LockManager) GetByRole(role Role) []LockEntry {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	var entries []LockEntry
	for _, entry := range lm.locks {
		if entry.OwnerRole == role && !entry.IsExpired() {
			entries = append(entries, entry)
		}
	}
	return entries
}

func (lm *LockManager) Count() int {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	count := 0
	for _, entry := range lm.locks {
		if !entry.IsExpired() {
			count++
		}
	}
	return count
}

func (lm *LockManager) SetDefaultTTL(role Role, ttl time.Duration) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.defaults[role] = ttl
}

func (lm *LockManager) GetDefaultTTL(role Role) time.Duration {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.defaults[role]
}

func (lm *LockManager) Clear() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.locks = make(map[string]LockEntry)
	return lm.save()
}

type LockStats struct {
	Total        int          `json:"total"`
	ByRole       map[Role]int `json:"by_role"`
	Expired      int          `json:"expired"`
	ExpiringSoon int          `json:"expiring_soon"`
}

func (lm *LockManager) GetStats() LockStats {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	stats := LockStats{
		ByRole: make(map[Role]int),
	}

	now := time.Now()
	soonThreshold := now.Add(5 * time.Minute)

	for _, entry := range lm.locks {
		if entry.IsExpired() {
			stats.Expired++
			continue
		}
		stats.Total++
		stats.ByRole[entry.OwnerRole]++
		if entry.ExpiresAt.Before(soonThreshold) {
			stats.ExpiringSoon++
		}
	}

	return stats
}
