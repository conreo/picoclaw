package proactive

import (
	"testing"
	"time"
)

func TestTaskQueue_Enqueue(t *testing.T) {
	tmpDir := t.TempDir()
	q := NewTaskQueue(tmpDir, 1)

	task := &QueuedTask{
		Description: "Test task",
		Model:       "balanced",
		Priority:    PriorityNormal,
		Source:      SourceUser,
	}

	enqueued, err := q.Enqueue(task)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	if enqueued.ID == "" {
		t.Error("Expected task ID to be set")
	}

	if enqueued.Status != QueueStatusPending {
		t.Errorf("Expected status pending, got %s", enqueued.Status)
	}

	if q.PendingCount() != 1 {
		t.Errorf("Expected 1 pending task, got %d", q.PendingCount())
	}
}

func TestTaskQueue_Dequeue(t *testing.T) {
	tmpDir := t.TempDir()
	q := NewTaskQueue(tmpDir, 1)

	highTask := &QueuedTask{
		Description: "High priority task",
		Model:       "balanced",
		Priority:    PriorityHigh,
		Source:      SourceUser,
	}

	normalTask := &QueuedTask{
		Description: "Normal priority task",
		Model:       "balanced",
		Priority:    PriorityNormal,
		Source:      SourceOrchestrator,
	}

	q.Enqueue(normalTask)
	q.Enqueue(highTask)

	dequeued := q.Dequeue(2, "balanced")
	if dequeued == nil {
		t.Fatal("Expected to dequeue a task")
	}

	if dequeued.Priority != PriorityHigh {
		t.Errorf("Expected high priority task, got %s", dequeued.Priority)
	}

	if dequeued.Status != QueueStatusClaimed {
		t.Errorf("Expected status claimed, got %s", dequeued.Status)
	}
}

func TestTaskQueue_SlotReservation(t *testing.T) {
	tmpDir := t.TempDir()
	q := NewTaskQueue(tmpDir, 1)

	normalTask := &QueuedTask{
		Description: "Normal task",
		Model:       "balanced",
		Priority:    PriorityNormal,
		Source:      SourceOrchestrator,
	}

	q.Enqueue(normalTask)

	dequeued := q.Dequeue(1, "balanced")
	if dequeued != nil {
		t.Error("Expected no dequeue when only reserved slot available and task is normal priority")
	}

	dequeued = q.Dequeue(2, "balanced")
	if dequeued == nil {
		t.Error("Expected dequeue when slots > reserved")
	}
}

func TestTaskQueue_HighPriorityIgnoresReservation(t *testing.T) {
	tmpDir := t.TempDir()
	q := NewTaskQueue(tmpDir, 1)

	highTask := &QueuedTask{
		Description: "High task",
		Model:       "balanced",
		Priority:    PriorityHigh,
		Source:      SourceUser,
	}

	q.Enqueue(highTask)

	dequeued := q.Dequeue(1, "balanced")
	if dequeued == nil {
		t.Error("Expected high priority task to bypass reservation")
	}
}

func TestTaskQueue_Dependencies(t *testing.T) {
	tmpDir := t.TempDir()
	q := NewTaskQueue(tmpDir, 1)

	task1 := &QueuedTask{
		TaskID:      "task-1",
		Description: "Task 1",
		Model:       "balanced",
		Priority:    PriorityNormal,
		Source:      SourceOrchestrator,
	}

	task2 := &QueuedTask{
		TaskID:       "task-2",
		Description:  "Task 2",
		Model:        "balanced",
		Priority:     PriorityNormal,
		Source:       SourceOrchestrator,
		Dependencies: []string{"task-1"},
	}

	q.Enqueue(task1)
	q.Enqueue(task2)

	dequeued := q.Dequeue(2, "balanced")
	if dequeued == nil || dequeued.TaskID != "task-1" {
		t.Error("Expected task-1 to be dequeued first (no dependencies)")
	}

	q.Complete(dequeued.ID, "done")

	dequeued = q.Dequeue(2, "balanced")
	if dequeued == nil || dequeued.TaskID != "task-2" {
		t.Error("Expected task-2 to be dequeued after task-1 completed")
	}
}

func TestTaskQueue_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	q := NewTaskQueue(tmpDir, 1)

	task := &QueuedTask{
		Description: "Persistent task",
		Model:       "balanced",
		Priority:    PriorityNormal,
		Source:      SourceUser,
	}

	enqueued, _ := q.Enqueue(task)

	q2 := NewTaskQueue(tmpDir, 1)

	loaded, err := q2.Get(enqueued.ID)
	if err != nil {
		t.Fatalf("Failed to load persisted task: %v", err)
	}

	if loaded.Description != "Persistent task" {
		t.Errorf("Expected 'Persistent task', got '%s'", loaded.Description)
	}
}

func TestTaskQueue_Complete(t *testing.T) {
	tmpDir := t.TempDir()
	q := NewTaskQueue(tmpDir, 1)

	task := &QueuedTask{
		Description: "Task to complete",
		Model:       "balanced",
		Priority:    PriorityNormal,
		Source:      SourceUser,
	}

	enqueued, _ := q.Enqueue(task)

	err := q.Complete(enqueued.ID, "success")
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	loaded, _ := q.Get(enqueued.ID)
	if loaded.Status != QueueStatusCompleted {
		t.Errorf("Expected status completed, got %s", loaded.Status)
	}

	if loaded.Result != "success" {
		t.Errorf("Expected result 'success', got '%s'", loaded.Result)
	}
}

func TestTaskQueue_Fail(t *testing.T) {
	tmpDir := t.TempDir()
	q := NewTaskQueue(tmpDir, 1)

	task := &QueuedTask{
		Description: "Task to fail",
		Model:       "balanced",
		Priority:    PriorityNormal,
		Source:      SourceUser,
	}

	enqueued, _ := q.Enqueue(task)

	err := q.Fail(enqueued.ID, "something went wrong")
	if err != nil {
		t.Fatalf("Fail failed: %v", err)
	}

	loaded, _ := q.Get(enqueued.ID)
	if loaded.Status != QueueStatusFailed {
		t.Errorf("Expected status failed, got %s", loaded.Status)
	}

	if loaded.Error != "something went wrong" {
		t.Errorf("Expected error 'something went wrong', got '%s'", loaded.Error)
	}
}

func TestTaskQueue_RequeueStuck(t *testing.T) {
	tmpDir := t.TempDir()
	q := NewTaskQueue(tmpDir, 1)

	task := &QueuedTask{
		Description: "Stuck task",
		Model:       "balanced",
		Priority:    PriorityNormal,
		Source:      SourceUser,
	}

	enqueued, _ := q.Enqueue(task)

	claimed := q.Dequeue(2, "balanced")
	if claimed == nil {
		t.Fatal("Expected to dequeue task")
	}

	time.Sleep(10 * time.Millisecond)

	requeued := q.RequeueStuck(5 * time.Millisecond)
	if len(requeued) != 1 {
		t.Errorf("Expected 1 requeued task, got %d", len(requeued))
	}

	loaded, _ := q.Get(enqueued.ID)
	if loaded.Status != QueueStatusPending {
		t.Errorf("Expected status pending after requeue, got %s", loaded.Status)
	}
}

func TestTaskQueue_GetStats(t *testing.T) {
	tmpDir := t.TempDir()
	q := NewTaskQueue(tmpDir, 1)

	q.Enqueue(&QueuedTask{Description: "High", Model: "balanced", Priority: PriorityHigh, Source: SourceUser})
	q.Enqueue(&QueuedTask{Description: "Normal", Model: "fast", Priority: PriorityNormal, Source: SourceOrchestrator})
	q.Enqueue(&QueuedTask{Description: "Low", Model: "balanced", Priority: PriorityLow, Source: SourceOrchestrator})

	stats := q.GetStats()

	if stats.Total != 3 {
		t.Errorf("Expected 3 total, got %d", stats.Total)
	}

	if stats.Pending != 3 {
		t.Errorf("Expected 3 pending, got %d", stats.Pending)
	}

	if stats.ByPriority[PriorityHigh] != 1 {
		t.Errorf("Expected 1 high priority, got %d", stats.ByPriority[PriorityHigh])
	}

	if stats.ByModel["balanced"] != 2 {
		t.Errorf("Expected 2 balanced model, got %d", stats.ByModel["balanced"])
	}
}
