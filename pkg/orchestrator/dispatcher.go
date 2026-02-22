package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/llm"
	"github.com/sipeed/picoclaw/pkg/lock"
	"github.com/sipeed/picoclaw/pkg/proactive"
)

type Dispatcher struct {
	queue            *proactive.TaskQueue
	lockManager      *lock.LockManager
	llmPool          *llm.Pool
	workers          map[string]*Worker
	taskChans        map[string]chan WorkerTask
	stopChan         chan struct{}
	dispatchInterval time.Duration
	mu               sync.RWMutex
}

type DispatcherConfig struct {
	Queue            *proactive.TaskQueue
	LockManager      *lock.LockManager
	LLMPool          *llm.Pool
	DispatchInterval time.Duration
}

func NewDispatcher(cfg DispatcherConfig) *Dispatcher {
	if cfg.DispatchInterval == 0 {
		cfg.DispatchInterval = 500 * time.Millisecond
	}

	return &Dispatcher{
		queue:            cfg.Queue,
		lockManager:      cfg.LockManager,
		llmPool:          cfg.LLMPool,
		workers:          make(map[string]*Worker),
		taskChans:        make(map[string]chan WorkerTask),
		stopChan:         make(chan struct{}),
		dispatchInterval: cfg.DispatchInterval,
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	go d.dispatchLoop(ctx)
}

func (d *Dispatcher) Stop() {
	close(d.stopChan)
}

func (d *Dispatcher) RegisterWorker(worker *Worker, taskChan chan WorkerTask) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.workers[worker.ID] = worker
	d.taskChans[worker.ID] = taskChan
}

func (d *Dispatcher) UnregisterWorker(workerID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.workers, workerID)
	delete(d.taskChans, workerID)
}

func (d *Dispatcher) GetWorker(workerID string) (*Worker, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	w, ok := d.workers[workerID]
	return w, ok
}

func (d *Dispatcher) GetIdleWorkers() []*Worker {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var idle []*Worker
	for _, w := range d.workers {
		if w.GetStatus().State == string(WorkerStateIdle) {
			idle = append(idle, w)
		}
	}
	return idle
}

func (d *Dispatcher) dispatchLoop(ctx context.Context) {
	ticker := time.NewTicker(d.dispatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopChan:
			return
		case <-ticker.C:
			d.tryDispatch()
		}
	}
}

func (d *Dispatcher) tryDispatch() {
	d.mu.Lock()
	defer d.mu.Unlock()

	idleWorkers := d.getIdleWorkersLocked()
	if len(idleWorkers) == 0 {
		return
	}

	for _, worker := range idleWorkers {
		availableSlots := d.getAvailableSlots(worker.Model)
		task := d.queue.Dequeue(availableSlots, worker.Model)
		if task == nil {
			continue
		}

		acquired, err := d.lockManager.Acquire(task.ID, worker.ID, lock.RoleWorker)
		if err != nil || !acquired {
			d.queue.Release(task.ID)
			continue
		}

		if err := d.queue.Claim(task.ID, worker.ID); err != nil {
			d.lockManager.Release(task.ID, worker.ID)
			d.queue.Release(task.ID)
			continue
		}

		workerTask := WorkerTask{
			QueueTaskID: task.ID,
			PlanID:      task.PlanID,
			TaskID:      task.TaskID,
			Description: task.Description,
			Model:       task.Model,
			Priority:    string(task.Priority),
			ResultChan:  make(chan TaskResult, 1),
		}

		taskChan, ok := d.taskChans[worker.ID]
		if !ok {
			d.lockManager.Release(task.ID, worker.ID)
			d.queue.Release(task.ID)
			continue
		}

		select {
		case taskChan <- workerTask:
			worker.SetCurrentTask(&workerTask)
		default:
			d.lockManager.Release(task.ID, worker.ID)
			d.queue.Release(task.ID)
		}
	}
}

func (d *Dispatcher) getIdleWorkersLocked() []*Worker {
	var idle []*Worker
	for _, w := range d.workers {
		if w.GetStatus().State == string(WorkerStateIdle) {
			idle = append(idle, w)
		}
	}
	return idle
}

func (d *Dispatcher) getAvailableSlots(model string) int {
	totalSlots := d.llmPool.TotalConcurrent()
	reservedSlots := 1

	poolAvailable := d.llmPool.Available(model)
	if poolAvailable == 0 {
		return 0
	}

	activeWorkers := 0
	for _, w := range d.workers {
		if w.GetStatus().State == string(WorkerStateBusy) {
			activeWorkers++
		}
	}

	availableSlots := totalSlots - activeWorkers - reservedSlots
	if availableSlots < 0 {
		availableSlots = 0
	}

	if availableSlots > poolAvailable {
		availableSlots = poolAvailable
	}

	return availableSlots
}

func (d *Dispatcher) HandleTaskResult(taskID string, result TaskResult) {
	if result.Success {
		d.queue.Complete(taskID, result.Result)
	} else {
		errMsg := ""
		if result.Error != nil {
			errMsg = result.Error.Error()
		}
		d.queue.Fail(taskID, errMsg)
	}

	if ownerID, _, ok := d.lockManager.GetOwner(taskID); ok {
		d.lockManager.Release(taskID, ownerID)
	}
}

func (d *Dispatcher) GetStats() DispatcherStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	idle := 0
	busy := 0
	for _, w := range d.workers {
		if w.GetStatus().State == string(WorkerStateIdle) {
			idle++
		} else if w.GetStatus().State == string(WorkerStateBusy) {
			busy++
		}
	}

	queueStats := d.queue.GetStats()

	return DispatcherStats{
		TotalWorkers:    len(d.workers),
		IdleWorkers:     idle,
		BusyWorkers:     busy,
		QueuePending:    queueStats.Pending,
		QueueClaimed:    queueStats.Claimed,
		QueueByPriority: queueStats.ByPriority,
	}
}

type DispatcherStats struct {
	TotalWorkers    int                        `json:"total_workers"`
	IdleWorkers     int                        `json:"idle_workers"`
	BusyWorkers     int                        `json:"busy_workers"`
	QueuePending    int                        `json:"queue_pending"`
	QueueClaimed    int                        `json:"queue_claimed"`
	QueueByPriority map[proactive.Priority]int `json:"queue_by_priority"`
}

func (d *Dispatcher) ForceDispatch(taskID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	task, err := d.queue.Get(taskID)
	if err != nil {
		return fmt.Errorf("task not found: %s", taskID)
	}

	if !task.IsReady() {
		return fmt.Errorf("task is not ready: %s", taskID)
	}

	for _, worker := range d.workers {
		if worker.GetStatus().State != string(WorkerStateIdle) {
			continue
		}

		if task.Model != "" && worker.Model != task.Model {
			continue
		}

		acquired, err := d.lockManager.Acquire(task.ID, worker.ID, lock.RoleWorker)
		if err != nil || !acquired {
			continue
		}

		if err := d.queue.Claim(task.ID, worker.ID); err != nil {
			d.lockManager.Release(task.ID, worker.ID)
			continue
		}

		workerTask := WorkerTask{
			QueueTaskID: task.ID,
			PlanID:      task.PlanID,
			TaskID:      task.TaskID,
			Description: task.Description,
			Model:       task.Model,
			Priority:    string(task.Priority),
			ResultChan:  make(chan TaskResult, 1),
		}

		taskChan, ok := d.taskChans[worker.ID]
		if !ok {
			d.lockManager.Release(task.ID, worker.ID)
			d.queue.Release(task.ID)
			continue
		}

		select {
		case taskChan <- workerTask:
			worker.SetCurrentTask(&workerTask)
			return nil
		default:
			d.lockManager.Release(task.ID, worker.ID)
			d.queue.Release(task.ID)
		}
	}

	return fmt.Errorf("no available worker for task %s", taskID)
}

func (d *Dispatcher) ReassignTask(taskID string, newModel string) error {
	if ownerID, _, ok := d.lockManager.GetOwner(taskID); ok {
		d.lockManager.ForceRelease(taskID, "reassignment")
		if worker, ok := d.workers[ownerID]; ok {
			worker.StopCurrentTask()
		}
	}

	if err := d.queue.Release(taskID); err != nil {
		return err
	}

	return nil
}
