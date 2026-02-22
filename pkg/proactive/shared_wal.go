package proactive

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type CoordinationType string

const (
	CoordinationTaskAssigned CoordinationType = "task_assigned"
	CoordinationTaskClaimed  CoordinationType = "task_claimed"
	CoordinationTaskComplete CoordinationType = "task_complete"
	CoordinationTaskFailed   CoordinationType = "task_failed"
	CoordinationBroadcast    CoordinationType = "broadcast"
	CoordinationHeartbeat    CoordinationType = "heartbeat"
)

type CoordinationEntry struct {
	ID          string           `json:"id"`
	Timestamp   time.Time        `json:"timestamp"`
	AgentID     string           `json:"agent_id"`
	Type        CoordinationType `json:"type"`
	TargetAgent string           `json:"target_agent,omitempty"`
	TaskID      string           `json:"task_id,omitempty"`
	Action      string           `json:"action,omitempty"`
	Status      string           `json:"status,omitempty"`
	Payload     map[string]any   `json:"payload,omitempty"`
	ClaimedBy   string           `json:"claimed_by,omitempty"`
	ClaimedAt   *time.Time       `json:"claimed_at,omitempty"`
	ExpiresAt   *time.Time       `json:"expires_at,omitempty"`
	CompletedAt *time.Time       `json:"completed_at,omitempty"`
	Result      string           `json:"result,omitempty"`
}

type SharedWAL struct {
	path    string
	entries []CoordinationEntry
	mu      sync.RWMutex
	lock    *FileLock
	agentID string
}

func NewSharedWAL(workspace, agentID string) *SharedWAL {
	memoryDir := filepath.Join(workspace, "memory")
	os.MkdirAll(memoryDir, 0o755)

	wal := &SharedWAL{
		path:    filepath.Join(memoryDir, "shared_wal.json"),
		entries: make([]CoordinationEntry, 0),
		lock:    NewFileLock(filepath.Join(memoryDir, "shared_wal.json")),
		agentID: agentID,
	}
	wal.load()
	return wal
}

func (w *SharedWAL) load() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := os.ReadFile(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(data, &w.entries)
}

func (w *SharedWAL) save() error {
	data, err := json.MarshalIndent(w.entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(w.path, data, 0o644)
}

func (w *SharedWAL) AssignTask(targetAgent, action string, payload map[string]any, expiresIn time.Duration) (*CoordinationEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	entry := CoordinationEntry{
		ID:          generateID(),
		Timestamp:   now,
		AgentID:     w.agentID,
		Type:        CoordinationTaskAssigned,
		TargetAgent: targetAgent,
		TaskID:      generateTaskID(),
		Action:      action,
		Status:      "pending",
		Payload:     payload,
	}

	if expiresIn > 0 {
		expires := now.Add(expiresIn)
		entry.ExpiresAt = &expires
	}

	w.entries = append(w.entries, entry)
	if err := w.save(); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (w *SharedWAL) ClaimTask(taskID string) (*CoordinationEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := range w.entries {
		if w.entries[i].TaskID == taskID && w.entries[i].Status == "pending" {
			if w.entries[i].ExpiresAt != nil && time.Now().After(*w.entries[i].ExpiresAt) {
				continue
			}

			now := time.Now()
			w.entries[i].Status = "claimed"
			w.entries[i].ClaimedBy = w.agentID
			w.entries[i].ClaimedAt = &now
			w.entries[i].Type = CoordinationTaskClaimed

			if err := w.save(); err != nil {
				return nil, err
			}
			return &w.entries[i], nil
		}
	}
	return nil, fmt.Errorf("task not found or already claimed: %s", taskID)
}

func (w *SharedWAL) CompleteTask(taskID, result string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := range w.entries {
		if w.entries[i].TaskID == taskID {
			now := time.Now()
			w.entries[i].Status = "completed"
			w.entries[i].Type = CoordinationTaskComplete
			w.entries[i].CompletedAt = &now
			w.entries[i].Result = result

			return w.save()
		}
	}
	return fmt.Errorf("task not found: %s", taskID)
}

func (w *SharedWAL) FailTask(taskID, reason string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := range w.entries {
		if w.entries[i].TaskID == taskID {
			now := time.Now()
			w.entries[i].Status = "failed"
			w.entries[i].Type = CoordinationTaskFailed
			w.entries[i].CompletedAt = &now
			w.entries[i].Result = reason

			return w.save()
		}
	}
	return fmt.Errorf("task not found: %s", taskID)
}

func (w *SharedWAL) Broadcast(message string, targetAgents ...string) (*CoordinationEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := CoordinationEntry{
		ID:        generateID(),
		Timestamp: time.Now(),
		AgentID:   w.agentID,
		Type:      CoordinationBroadcast,
		Action:    message,
		Status:    "broadcast",
		Payload:   map[string]any{"targets": targetAgents},
	}

	w.entries = append(w.entries, entry)
	if err := w.save(); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (w *SharedWAL) Heartbeat() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := CoordinationEntry{
		ID:        generateID(),
		Timestamp: time.Now(),
		AgentID:   w.agentID,
		Type:      CoordinationHeartbeat,
		Status:    "active",
	}

	w.entries = append(w.entries, entry)

	cleaned := make([]CoordinationEntry, 0)
	cutoff := time.Now().Add(-5 * time.Minute)
	for _, e := range w.entries {
		if e.Type != CoordinationHeartbeat || e.Timestamp.After(cutoff) {
			cleaned = append(cleaned, e)
		}
	}
	w.entries = cleaned

	return w.save()
}

func (w *SharedWAL) Read(filterType ...CoordinationType) []CoordinationEntry {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if len(filterType) == 0 {
		result := make([]CoordinationEntry, len(w.entries))
		copy(result, w.entries)
		return result
	}

	filterSet := make(map[CoordinationType]bool)
	for _, t := range filterType {
		filterSet[t] = true
	}

	var filtered []CoordinationEntry
	for _, entry := range w.entries {
		if filterSet[entry.Type] {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func (w *SharedWAL) ReadForAgent(agentID string, filterStatus ...string) []CoordinationEntry {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var filtered []CoordinationEntry
	for _, entry := range w.entries {
		if entry.TargetAgent == agentID || entry.AgentID == agentID {
			if len(filterStatus) == 0 {
				filtered = append(filtered, entry)
				continue
			}
			for _, status := range filterStatus {
				if entry.Status == status {
					filtered = append(filtered, entry)
					break
				}
			}
		}
	}
	return filtered
}

func (w *SharedWAL) GetPendingTasks(agentID string) []CoordinationEntry {
	return w.ReadForAgent(agentID, "pending")
}

func (w *SharedWAL) GetMyTasks() []CoordinationEntry {
	return w.ReadForAgent(w.agentID, "pending", "claimed")
}

func (w *SharedWAL) Clear(completedOnly bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if completedOnly {
		var remaining []CoordinationEntry
		for _, entry := range w.entries {
			if entry.Status != "completed" && entry.Status != "failed" {
				remaining = append(remaining, entry)
			}
		}
		w.entries = remaining
	} else {
		w.entries = make([]CoordinationEntry, 0)
	}
	return w.save()
}

func (w *SharedWAL) GetActiveAgents() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	agents := make(map[string]bool)
	cutoff := time.Now().Add(-5 * time.Minute)

	for _, entry := range w.entries {
		if entry.Type == CoordinationHeartbeat && entry.Timestamp.After(cutoff) {
			agents[entry.AgentID] = true
		}
	}

	var result []string
	for agent := range agents {
		result = append(result, agent)
	}
	return result
}

func (w *SharedWAL) FormatEntries(entries []CoordinationEntry) string {
	if len(entries) == 0 {
		return "No entries found"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Shared WAL entries (%d total):\n\n", len(entries)))

	for _, e := range entries {
		sb.WriteString(fmt.Sprintf("- [%s] %s | Agent: %s | Status: %s\n",
			e.ID, e.Timestamp.Format("2006-01-02 15:04"), e.AgentID, e.Status))
		if e.TargetAgent != "" {
			sb.WriteString(fmt.Sprintf("  Target: %s\n", e.TargetAgent))
		}
		if e.Action != "" {
			sb.WriteString(fmt.Sprintf("  Action: %s\n", e.Action))
		}
		if e.TaskID != "" {
			sb.WriteString(fmt.Sprintf("  TaskID: %s\n", e.TaskID))
		}
		if e.Result != "" {
			sb.WriteString(fmt.Sprintf("  Result: %s\n", e.Result))
		}
	}

	return sb.String()
}

func generateTaskID() string {
	return fmt.Sprintf("task-%d", time.Now().UnixNano())
}
