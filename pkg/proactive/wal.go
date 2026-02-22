package proactive

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type EntryType string

const (
	EntryTypeMonitor     EntryType = "monitor"
	EntryTypeHeal        EntryType = "heal"
	EntryTypeSchedule    EntryType = "schedule"
	EntryTypeInvestigate EntryType = "investigate"
	EntryTypeTask        EntryType = "task"
	EntryTypePlan        EntryType = "plan"
	EntryTypeUserRequest EntryType = "user_request"
)

type EntryStatus string

const (
	EntryStatusPending   EntryStatus = "pending"
	EntryStatusCompleted EntryStatus = "completed"
	EntryStatusFailed    EntryStatus = "failed"
)

type WALEntry struct {
	ID        string      `json:"id"`
	Timestamp time.Time   `json:"timestamp"`
	Type      EntryType   `json:"type"`
	Status    EntryStatus `json:"status"`
	Command   string      `json:"command,omitempty"`
	Output    string      `json:"output,omitempty"`
	Recovery  string      `json:"recovery,omitempty"`
	Message   string      `json:"message,omitempty"`
}

type WAL struct {
	path    string
	entries []WALEntry
	mu      sync.RWMutex
}

func NewWAL(workspace string) *WAL {
	memoryDir := filepath.Join(workspace, "memory")
	os.MkdirAll(memoryDir, 0o755)

	wal := &WAL{
		path:    filepath.Join(memoryDir, "wal.json"),
		entries: make([]WALEntry, 0),
	}
	wal.load()
	return wal
}

func (w *WAL) load() error {
	data, err := os.ReadFile(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(data, &w.entries)
}

func (w *WAL) save() error {
	data, err := json.MarshalIndent(w.entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(w.path, data, 0o644)
}

func (w *WAL) Write(entryType EntryType, command, message string) (*WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := WALEntry{
		ID:        generateID(),
		Timestamp: time.Now(),
		Type:      entryType,
		Status:    EntryStatusPending,
		Command:   command,
		Message:   message,
	}

	w.entries = append(w.entries, entry)
	if err := w.save(); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (w *WAL) Complete(id, output string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := range w.entries {
		if w.entries[i].ID == id {
			w.entries[i].Status = EntryStatusCompleted
			w.entries[i].Output = output
			return w.save()
		}
	}
	return fmt.Errorf("entry not found: %s", id)
}

func (w *WAL) Fail(id, output, recovery string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := range w.entries {
		if w.entries[i].ID == id {
			w.entries[i].Status = EntryStatusFailed
			w.entries[i].Output = output
			w.entries[i].Recovery = recovery
			return w.save()
		}
	}
	return fmt.Errorf("entry not found: %s", id)
}

func (w *WAL) Recover() []WALEntry {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var pending []WALEntry
	for _, entry := range w.entries {
		if entry.Status == EntryStatusPending {
			pending = append(pending, entry)
		}
	}
	return pending
}

func (w *WAL) Read(filterStatus ...EntryStatus) []WALEntry {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if len(filterStatus) == 0 {
		result := make([]WALEntry, len(w.entries))
		copy(result, w.entries)
		return result
	}

	var filtered []WALEntry
	for _, entry := range w.entries {
		for _, status := range filterStatus {
			if entry.Status == status {
				filtered = append(filtered, entry)
				break
			}
		}
	}
	return filtered
}

func (w *WAL) ReadByType(entryType EntryType) []WALEntry {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var filtered []WALEntry
	for _, entry := range w.entries {
		if entry.Type == entryType {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func (w *WAL) Clear(completedOnly bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if completedOnly {
		var remaining []WALEntry
		for _, entry := range w.entries {
			if entry.Status != EntryStatusCompleted {
				remaining = append(remaining, entry)
			}
		}
		w.entries = remaining
	} else {
		w.entries = make([]WALEntry, 0)
	}
	return w.save()
}

func (w *WAL) Get(id string) (*WALEntry, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	for i := range w.entries {
		if w.entries[i].ID == id {
			return &w.entries[i], nil
		}
	}
	return nil, fmt.Errorf("entry not found: %s", id)
}

func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
