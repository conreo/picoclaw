package proactive

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Priority string

const (
	PriorityCritical Priority = "critical"
	PriorityHigh     Priority = "high"
	PriorityNormal   Priority = "normal"
	PriorityLow      Priority = "low"
)

type QueueSource string

const (
	SourceUser         QueueSource = "user"
	SourceOrchestrator QueueSource = "orchestrator"
	SourceMonitor      QueueSource = "monitor"
)

type QueueStatus string

const (
	QueueStatusPending   QueueStatus = "pending"
	QueueStatusClaimed   QueueStatus = "claimed"
	QueueStatusCompleted QueueStatus = "completed"
	QueueStatusFailed    QueueStatus = "failed"
	QueueStatusCancelled QueueStatus = "cancelled"
)

type QueuedTask struct {
	ID           string         `json:"id"`
	PlanID       string         `json:"plan_id,omitempty"`
	TaskID       string         `json:"task_id"`
	Description  string         `json:"description"`
	Model        string         `json:"model"`
	Priority     Priority       `json:"priority"`
	Source       QueueSource    `json:"source"`
	Status       QueueStatus    `json:"status"`
	Dependencies []string       `json:"dependencies,omitempty"`
	EnqueuedAt   time.Time      `json:"enqueued_at"`
	ClaimedBy    string         `json:"claimed_by,omitempty"`
	ClaimedAt    *time.Time     `json:"claimed_at,omitempty"`
	CompletedAt  *time.Time     `json:"completed_at,omitempty"`
	Result       string         `json:"result,omitempty"`
	Error        string         `json:"error,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

func (t *QueuedTask) IsClaimed() bool {
	return t.Status == QueueStatusClaimed && t.ClaimedBy != ""
}

func (t *QueuedTask) IsReady() bool {
	return t.Status == QueueStatusPending && !t.IsClaimed()
}

func (t *QueuedTask) CanClaim(availableSlots int, reservedSlots int) bool {
	if !t.IsReady() {
		return false
	}
	if t.Priority == PriorityCritical || t.Priority == PriorityHigh {
		return true
	}
	return availableSlots > reservedSlots
}

type QueueStats struct {
	Total      int                 `json:"total"`
	Pending    int                 `json:"pending"`
	Claimed    int                 `json:"claimed"`
	Completed  int                 `json:"completed"`
	Failed     int                 `json:"failed"`
	ByPriority map[Priority]int    `json:"by_priority"`
	ByModel    map[string]int      `json:"by_model"`
	BySource   map[QueueSource]int `json:"by_source"`
}

type TaskQueue struct {
	path          string
	tasks         []*QueuedTask
	reservedSlots int
	mu            sync.RWMutex
	nextID        int64
}

func NewTaskQueue(workspace string, reservedSlots int) *TaskQueue {
	memoryDir := filepath.Join(workspace, "memory")
	os.MkdirAll(memoryDir, 0o755)

	q := &TaskQueue{
		path:          filepath.Join(memoryDir, "queue.json"),
		tasks:         make([]*QueuedTask, 0),
		reservedSlots: reservedSlots,
		nextID:        1,
	}
	q.load()
	return q
}

func (q *TaskQueue) load() error {
	data, err := os.ReadFile(q.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var tasks []*QueuedTask
	if err := json.Unmarshal(data, &tasks); err != nil {
		return err
	}

	q.tasks = tasks
	for _, t := range q.tasks {
		if id := parseQueueID(t.ID); id >= q.nextID {
			q.nextID = id + 1
		}
	}
	return nil
}

func (q *TaskQueue) save() error {
	data, err := json.MarshalIndent(q.tasks, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(q.path, data, 0o644)
}

func (q *TaskQueue) generateID() string {
	id := fmt.Sprintf("tq-%d", q.nextID)
	q.nextID++
	return id
}

func parseQueueID(id string) int64 {
	var n int64
	fmt.Sscanf(id, "tq-%d", &n)
	return n
}

func (q *TaskQueue) Enqueue(task *QueuedTask) (*QueuedTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if task.ID == "" {
		task.ID = q.generateID()
	}
	if task.EnqueuedAt.IsZero() {
		task.EnqueuedAt = time.Now()
	}
	if task.Status == "" {
		task.Status = QueueStatusPending
	}

	q.tasks = append(q.tasks, task)
	if err := q.save(); err != nil {
		q.tasks = q.tasks[:len(q.tasks)-1]
		return nil, err
	}

	return task, nil
}

func (q *TaskQueue) EnqueueBatch(tasks []*QueuedTask) ([]*QueuedTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	enqueued := make([]*QueuedTask, 0, len(tasks))

	for _, task := range tasks {
		if task.ID == "" {
			task.ID = q.generateID()
		}
		if task.EnqueuedAt.IsZero() {
			task.EnqueuedAt = now
		}
		if task.Status == "" {
			task.Status = QueueStatusPending
		}
		q.tasks = append(q.tasks, task)
		enqueued = append(enqueued, task)
	}

	if err := q.save(); err != nil {
		q.tasks = q.tasks[:len(q.tasks)-len(tasks)]
		return nil, err
	}

	return enqueued, nil
}

func (q *TaskQueue) Dequeue(availableSlots int, model string) *QueuedTask {
	q.mu.Lock()
	defer q.mu.Unlock()

	ready := q.getReadyTasks()

	sort.Slice(ready, func(i, j int) bool {
		pi := priorityValue(ready[i].Priority)
		pj := priorityValue(ready[j].Priority)
		if pi != pj {
			return pi < pj
		}
		return ready[i].EnqueuedAt.Before(ready[j].EnqueuedAt)
	})

	for _, task := range ready {
		if !task.CanClaim(availableSlots, q.reservedSlots) {
			continue
		}
		if model != "" && task.Model != "" && task.Model != model && task.Model != "any" {
			continue
		}
		if !q.areDependenciesMet(task) {
			continue
		}

		task.Status = QueueStatusClaimed
		task.ClaimedBy = ""
		now := time.Now()
		task.ClaimedAt = &now
		q.save()
		return task
	}

	return nil
}

func (q *TaskQueue) getReadyTasks() []*QueuedTask {
	var ready []*QueuedTask
	for _, t := range q.tasks {
		if t.IsReady() {
			ready = append(ready, t)
		}
	}
	return ready
}

func (q *TaskQueue) areDependenciesMet(task *QueuedTask) bool {
	if len(task.Dependencies) == 0 {
		return true
	}

	completed := make(map[string]bool)
	for _, t := range q.tasks {
		if t.Status == QueueStatusCompleted {
			completed[t.TaskID] = true
		}
	}

	for _, dep := range task.Dependencies {
		if !completed[dep] {
			return false
		}
	}
	return true
}

func priorityValue(p Priority) int {
	switch p {
	case PriorityCritical:
		return 0
	case PriorityHigh:
		return 1
	case PriorityNormal:
		return 2
	case PriorityLow:
		return 3
	default:
		return 4
	}
}

func (q *TaskQueue) Claim(taskID, ownerID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, t := range q.tasks {
		if t.ID == taskID {
			if t.Status != QueueStatusClaimed {
				return fmt.Errorf("task %s is not in claimed state", taskID)
			}
			t.ClaimedBy = ownerID
			return q.save()
		}
	}
	return fmt.Errorf("task not found: %s", taskID)
}

func (q *TaskQueue) Release(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, t := range q.tasks {
		if t.ID == taskID {
			t.Status = QueueStatusPending
			t.ClaimedBy = ""
			t.ClaimedAt = nil
			return q.save()
		}
	}
	return fmt.Errorf("task not found: %s", taskID)
}

func (q *TaskQueue) Complete(taskID, result string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, t := range q.tasks {
		if t.ID == taskID {
			t.Status = QueueStatusCompleted
			t.Result = result
			now := time.Now()
			t.CompletedAt = &now
			return q.save()
		}
	}
	return fmt.Errorf("task not found: %s", taskID)
}

func (q *TaskQueue) Fail(taskID, errMsg string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, t := range q.tasks {
		if t.ID == taskID {
			t.Status = QueueStatusFailed
			t.Error = errMsg
			now := time.Now()
			t.CompletedAt = &now
			return q.save()
		}
	}
	return fmt.Errorf("task not found: %s", taskID)
}

func (q *TaskQueue) Remove(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i, t := range q.tasks {
		if t.ID == taskID {
			q.tasks = append(q.tasks[:i], q.tasks[i+1:]...)
			return q.save()
		}
	}
	return fmt.Errorf("task not found: %s", taskID)
}

func (q *TaskQueue) Cancel(taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, t := range q.tasks {
		if t.ID == taskID {
			t.Status = QueueStatusCancelled
			return q.save()
		}
	}
	return fmt.Errorf("task not found: %s", taskID)
}

func (q *TaskQueue) Get(taskID string) (*QueuedTask, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	for _, t := range q.tasks {
		if t.ID == taskID {
			return t, nil
		}
	}
	return nil, fmt.Errorf("task not found: %s", taskID)
}

func (q *TaskQueue) GetByPlan(planID string) []*QueuedTask {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var result []*QueuedTask
	for _, t := range q.tasks {
		if t.PlanID == planID {
			result = append(result, t)
		}
	}
	return result
}

func (q *TaskQueue) GetByTaskID(taskID string) *QueuedTask {
	q.mu.RLock()
	defer q.mu.RUnlock()

	for _, t := range q.tasks {
		if t.TaskID == taskID {
			return t
		}
	}
	return nil
}

func (q *TaskQueue) Peek(limit int) []*QueuedTask {
	q.mu.RLock()
	defer q.mu.RUnlock()

	ready := q.getReadyTasks()

	sort.Slice(ready, func(i, j int) bool {
		pi := priorityValue(ready[i].Priority)
		pj := priorityValue(ready[j].Priority)
		if pi != pj {
			return pi < pj
		}
		return ready[i].EnqueuedAt.Before(ready[j].EnqueuedAt)
	})

	if limit > 0 && len(ready) > limit {
		ready = ready[:limit]
	}
	return ready
}

func (q *TaskQueue) GetPending(model string) []*QueuedTask {
	q.mu.RLock()
	defer q.mu.RUnlock()

	var result []*QueuedTask
	for _, t := range q.tasks {
		if t.IsReady() {
			if model == "" || t.Model == "" || t.Model == model || t.Model == "any" {
				result = append(result, t)
			}
		}
	}
	return result
}

func (q *TaskQueue) GetStats() QueueStats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	stats := QueueStats{
		ByPriority: make(map[Priority]int),
		ByModel:    make(map[string]int),
		BySource:   make(map[QueueSource]int),
	}

	for _, t := range q.tasks {
		stats.Total++
		switch t.Status {
		case QueueStatusPending:
			if !t.IsClaimed() {
				stats.Pending++
			} else {
				stats.Claimed++
			}
		case QueueStatusClaimed:
			stats.Claimed++
		case QueueStatusCompleted:
			stats.Completed++
		case QueueStatusFailed:
			stats.Failed++
		}
		stats.ByPriority[t.Priority]++
		if t.Model != "" {
			stats.ByModel[t.Model]++
		}
		stats.BySource[t.Source]++
	}

	return stats
}

func (q *TaskQueue) Clear(completedOnly bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if completedOnly {
		var remaining []*QueuedTask
		for _, t := range q.tasks {
			if t.Status != QueueStatusCompleted && t.Status != QueueStatusFailed && t.Status != QueueStatusCancelled {
				remaining = append(remaining, t)
			}
		}
		q.tasks = remaining
	} else {
		q.tasks = make([]*QueuedTask, 0)
	}
	return q.save()
}

func (q *TaskQueue) RequeueStuck(olderThan time.Duration) []string {
	q.mu.Lock()
	defer q.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	var requeued []string

	for _, t := range q.tasks {
		if t.Status == QueueStatusClaimed && t.ClaimedAt != nil && t.ClaimedAt.Before(cutoff) {
			t.Status = QueueStatusPending
			t.ClaimedBy = ""
			t.ClaimedAt = nil
			requeued = append(requeued, t.ID)
		}
	}

	if len(requeued) > 0 {
		q.save()
	}
	return requeued
}

func (q *TaskQueue) Size() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.tasks)
}

func (q *TaskQueue) PendingCount() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	count := 0
	for _, t := range q.tasks {
		if t.IsReady() {
			count++
		}
	}
	return count
}
