package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/llm"
	"github.com/sipeed/picoclaw/pkg/plan"
	"github.com/sipeed/picoclaw/pkg/proactive"
)

type WorkerState string

const (
	WorkerStateIdle    WorkerState = "idle"
	WorkerStateBusy    WorkerState = "busy"
	WorkerStateStopped WorkerState = "stopped"
	WorkerStateError   WorkerState = "error"
)

type WorkerTask struct {
	PlanID     string
	TaskID     string
	Task       plan.Task
	ResultChan chan TaskResult
}

type TaskResult struct {
	Success bool
	Result  string
	Error   error
}

type Worker struct {
	ID            string
	Model         string
	State         WorkerState
	CurrentTask   *WorkerTask
	wal           *proactive.WAL
	llmPool       *llm.Pool
	heartbeatChan chan time.Time
	stopChan      chan struct{}
	mu            sync.RWMutex
	startedAt     time.Time
	lastActivity  time.Time
	tasksComplete int64
	tasksFailed   int64
}

func NewWorker(id, model string, workspace string, llmPool *llm.Pool) *Worker {
	return &Worker{
		ID:            id,
		Model:         model,
		State:         WorkerStateIdle,
		wal:           proactive.NewWAL(workspace),
		llmPool:       llmPool,
		heartbeatChan: make(chan time.Time, 10),
		stopChan:      make(chan struct{}),
		startedAt:     time.Now(),
		lastActivity:  time.Now(),
	}
}

func (w *Worker) Start(ctx context.Context, taskChan <-chan WorkerTask) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				w.setState(WorkerStateStopped)
				return
			case <-w.stopChan:
				w.setState(WorkerStateStopped)
				return
			case task := <-taskChan:
				w.executeTask(ctx, task)
			case w.heartbeatChan <- time.Now():
			}
		}
	}()
}

func (w *Worker) Stop() {
	close(w.stopChan)
}

func (w *Worker) executeTask(ctx context.Context, task WorkerTask) {
	w.mu.Lock()
	w.CurrentTask = &task
	w.State = WorkerStateBusy
	w.lastActivity = time.Now()
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.CurrentTask = nil
		w.State = WorkerStateIdle
		w.lastActivity = time.Now()
		w.mu.Unlock()
	}()

	entry, err := w.wal.Write(proactive.EntryTypeSchedule, task.Task.Description, fmt.Sprintf("Plan: %s, Task: %s", task.PlanID, task.TaskID))
	if err != nil {
		task.ResultChan <- TaskResult{Success: false, Error: err}
		w.tasksFailed++
		return
	}

	timeout := 30 * time.Second
	if err := w.llmPool.Acquire(w.Model, timeout); err != nil {
		w.wal.Fail(entry.ID, err.Error(), "Retry with different model or increase timeout")
		task.ResultChan <- TaskResult{Success: false, Error: err}
		w.tasksFailed++
		return
	}
	defer w.llmPool.Release(w.Model)

	result, err := w.processTask(ctx, task.Task)
	if err != nil {
		w.wal.Fail(entry.ID, err.Error(), "Manual intervention required")
		task.ResultChan <- TaskResult{Success: false, Error: err}
		w.tasksFailed++
		return
	}

	w.wal.Complete(entry.ID, result)
	task.ResultChan <- TaskResult{Success: true, Result: result}
	w.tasksComplete++
}

func (w *Worker) processTask(ctx context.Context, task plan.Task) (string, error) {
	time.Sleep(100 * time.Millisecond)
	return fmt.Sprintf("Task '%s' completed", task.Description), nil
}

func (w *Worker) setState(state WorkerState) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.State = state
	w.lastActivity = time.Now()
}

func (w *Worker) GetStatus() WorkerStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()

	var currentTaskID string
	if w.CurrentTask != nil {
		currentTaskID = w.CurrentTask.TaskID
	}

	return WorkerStatus{
		ID:            w.ID,
		Model:         w.Model,
		State:         string(w.State),
		CurrentTask:   currentTaskID,
		LastActivity:  w.lastActivity,
		TasksComplete: w.tasksComplete,
		TasksFailed:   w.tasksFailed,
		Uptime:        time.Since(w.startedAt),
	}
}

func (w *Worker) Heartbeat() <-chan time.Time {
	return w.heartbeatChan
}

type WorkerStatus struct {
	ID            string        `json:"id"`
	Model         string        `json:"model"`
	State         string        `json:"state"`
	CurrentTask   string        `json:"current_task,omitempty"`
	LastActivity  time.Time     `json:"last_activity"`
	TasksComplete int64         `json:"tasks_complete"`
	TasksFailed   int64         `json:"tasks_failed"`
	Uptime        time.Duration `json:"uptime"`
}
