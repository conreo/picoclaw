package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/llm"
	"github.com/sipeed/picoclaw/pkg/lock"
	"github.com/sipeed/picoclaw/pkg/messaging"
	"github.com/sipeed/picoclaw/pkg/plan"
	"github.com/sipeed/picoclaw/pkg/proactive"
)

type Executor struct {
	planner        *plan.Planner
	llmPool        *llm.Pool
	sharedWAL      *proactive.SharedWAL
	monitor        *Monitor
	dispatcher     *Dispatcher
	queue          *proactive.TaskQueue
	lockManager    *lock.LockManager
	eventBus       *messaging.EventBus
	workers        map[string]*Worker
	taskChans      map[string]chan WorkerTask
	approvalConfig plan.ApprovalConfig
	workspace      string
	mu             sync.RWMutex
	nextWorkerID   int64
}

type ExecutorConfig struct {
	Workspace      string
	LLMPool        *llm.Pool
	ApprovalConfig plan.ApprovalConfig
	MonitorConfig  MonitorConfig
}

func NewExecutor(cfg ExecutorConfig) *Executor {
	planner := plan.NewPlanner(cfg.Workspace)
	sharedWAL := proactive.NewSharedWAL(cfg.Workspace, "executor")
	queue := proactive.NewTaskQueue(cfg.Workspace, 1)
	lockManager := lock.NewLockManager(cfg.Workspace)
	eventBus := messaging.NewEventBus(cfg.Workspace)

	monitorCfg := cfg.MonitorConfig
	if monitorCfg.LockManager == nil {
		monitorCfg.LockManager = lockManager
	}
	if monitorCfg.EventQueue == nil {
		monitorCfg.EventQueue = queue
	}
	monitor := NewMonitor(monitorCfg, cfg.LLMPool, eventBus)

	dispatcher := NewDispatcher(DispatcherConfig{
		Queue:            queue,
		LockManager:      lockManager,
		LLMPool:          cfg.LLMPool,
		DispatchInterval: 500 * time.Millisecond,
	})

	return &Executor{
		planner:        planner,
		llmPool:        cfg.LLMPool,
		sharedWAL:      sharedWAL,
		monitor:        monitor,
		dispatcher:     dispatcher,
		queue:          queue,
		lockManager:    lockManager,
		eventBus:       eventBus,
		workers:        make(map[string]*Worker),
		taskChans:      make(map[string]chan WorkerTask),
		approvalConfig: cfg.ApprovalConfig,
		workspace:      cfg.Workspace,
		nextWorkerID:   1,
	}
}

func (e *Executor) Start(ctx context.Context) {
	e.dispatcher.Start(ctx)
	e.monitor.Start(ctx)
	go e.handleEscalations(ctx)
	go e.handleTaskResults(ctx)
}

func (e *Executor) CreatePlan(request string, complexity plan.Complexity, tasks []plan.Task) (*plan.Plan, error) {
	approvalRequired := e.approvalConfig.RequiresApproval(&plan.Plan{
		Request:    request,
		Complexity: complexity,
		Tasks:      tasks,
	})

	p := e.planner.Create(request, complexity, tasks, approvalRequired)
	return p, nil
}

func (e *Executor) GetPlan(id string) (*plan.Plan, error) {
	return e.planner.Get(id)
}

func (e *Executor) ApprovePlan(id, approvedBy string) error {
	if err := e.planner.Approve(id, approvedBy); err != nil {
		return err
	}

	p, err := e.planner.Get(id)
	if err != nil {
		return err
	}

	e.sharedWAL.Broadcast(fmt.Sprintf("Plan approved by %s with %d tasks", approvedBy, len(p.Tasks)))

	priority := proactive.PriorityNormal
	if p.Complexity == plan.ComplexityHigh {
		priority = proactive.PriorityHigh
	}

	for _, task := range p.Tasks {
		queuedTask := &proactive.QueuedTask{
			PlanID:       id,
			TaskID:       task.ID,
			Description:  task.Description,
			Model:        task.Model,
			Priority:     priority,
			Source:       proactive.SourceOrchestrator,
			Dependencies: task.Dependencies,
			Metadata: map[string]any{
				"worker_type":      task.WorkerType,
				"estimated_tokens": task.EstimatedTokens,
			},
		}
		if _, err := e.queue.Enqueue(queuedTask); err != nil {
			return fmt.Errorf("failed to enqueue task %s: %w", task.ID, err)
		}
	}

	return nil
}

func (e *Executor) RejectPlan(id string) error {
	return e.planner.Reject(id)
}

func (e *Executor) EditPlanTask(planID string, response plan.UserResponse) error {
	p, err := e.planner.Get(planID)
	if err != nil {
		return err
	}
	return plan.ApplyEdit(p, response)
}

func (e *Executor) HandleUserRequest(request string, model string) (string, error) {
	queuedTask := &proactive.QueuedTask{
		Description: request,
		Model:       model,
		Priority:    proactive.PriorityHigh,
		Source:      proactive.SourceUser,
	}

	task, err := e.queue.Enqueue(queuedTask)
	if err != nil {
		return "", fmt.Errorf("failed to enqueue user request: %w", err)
	}

	return task.ID, nil
}

func (e *Executor) ExecutePlan(ctx context.Context, planID string) error {
	p, err := e.planner.Get(planID)
	if err != nil {
		return err
	}

	if p.Status != plan.TaskStatusApproved {
		return fmt.Errorf("plan not approved: %s", p.Status)
	}

	p.Status = plan.TaskStatusRunning
	return nil
}

func (e *Executor) handleTaskResults(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			e.processCompletedTasks(ctx)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (e *Executor) processCompletedTasks(ctx context.Context) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for workerID, worker := range e.workers {
		status := worker.GetStatus()
		_ = workerID
		_ = status
	}
}

func (e *Executor) getOrCreateWorker(model string) *Worker {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, w := range e.workers {
		if w.Model == model && w.GetStatus().State == string(WorkerStateIdle) {
			return w
		}
	}

	workerID := fmt.Sprintf("worker-%d", e.nextWorkerID)
	e.nextWorkerID++

	worker := NewWorker(workerID, model, e.workspace, e.llmPool)
	taskChan := make(chan WorkerTask, 10)

	e.workers[workerID] = worker
	e.taskChans[workerID] = taskChan
	e.monitor.RegisterWorker(worker)
	e.dispatcher.RegisterWorker(worker, taskChan)

	worker.Start(context.Background(), taskChan)

	return worker
}

func (e *Executor) handleEscalations(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case escalation := <-e.monitor.Escalations():
			e.handleEscalation(escalation)
		}
	}
}

func (e *Executor) handleEscalation(escalation Escalation) {
	switch escalation.Type {
	case EscalationWorkerDead:
		if workerID := escalation.WorkerID; workerID != "" {
			e.reassignWorkerTasks(workerID)
			e.monitor.RestartWorker(workerID)
			if worker, ok := e.workers[workerID]; ok {
				worker.Start(context.Background(), e.taskChans[workerID])
			}
		}
	case EscalationWorkerStuck:
		if workerID := escalation.WorkerID; workerID != "" {
			e.reassignWorkerTasks(workerID)
			e.dispatcher.ReassignTask(escalation.Details["task_id"].(string), "")
			e.monitor.RestartWorker(workerID)
		}
	case EscalationAPILimit:
		e.sharedWAL.Broadcast(fmt.Sprintf("API limit reached: %s", escalation.Message))
	case EscalationTaskFailed:
		e.sharedWAL.Broadcast(fmt.Sprintf("Task failed: %s", escalation.Message))
	case EscalationAllWorkersBusy:
		e.scaleUpIfNeeded()
	}
}

func (e *Executor) reassignWorkerTasks(workerID string) {
	locks := e.lockManager.GetByOwner(workerID)
	for _, lockEntry := range locks {
		e.lockManager.ForceRelease(lockEntry.TaskID, "worker_reassignment")
		e.queue.Release(lockEntry.TaskID)
	}
}

func (e *Executor) scaleUpIfNeeded() {
	e.mu.RLock()
	busyCount := 0
	for _, w := range e.workers {
		if w.GetStatus().State == string(WorkerStateBusy) {
			busyCount++
		}
	}
	e.mu.RUnlock()

	if busyCount >= len(e.workers) && len(e.workers) < 8 {
		e.getOrCreateWorker("balanced")
	}
}

func (e *Executor) GetProgress() string {
	return e.monitor.FormatProgress()
}

func (e *Executor) GetStatus() ExecutorStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()

	workers := make([]WorkerStatus, 0, len(e.workers))
	for _, w := range e.workers {
		workers = append(workers, w.GetStatus())
	}

	return ExecutorStatus{
		Workers:    len(e.workers),
		Plans:      len(e.planner.GetApprovedPlans()),
		LLMStats:   e.llmPool.GetStats(),
		Monitor:    e.monitor.GetStatus(),
		QueueStats: e.queue.GetStats(),
		LockStats:  e.lockManager.GetStats(),
		Dispatcher: e.dispatcher.GetStats(),
	}
}

func (e *Executor) GetQueue() *proactive.TaskQueue {
	return e.queue
}

func (e *Executor) GetLockManager() *lock.LockManager {
	return e.lockManager
}

func (e *Executor) GetEventBus() *messaging.EventBus {
	return e.eventBus
}

func (e *Executor) GetDispatcher() *Dispatcher {
	return e.dispatcher
}

type ExecutorStatus struct {
	Workers    int                  `json:"workers"`
	Plans      int                  `json:"plans"`
	LLMStats   []llm.Stats          `json:"llm_stats"`
	Monitor    MonitorStatus        `json:"monitor"`
	QueueStats proactive.QueueStats `json:"queue_stats"`
	LockStats  lock.LockStats       `json:"lock_stats"`
	Dispatcher DispatcherStats      `json:"dispatcher"`
}
