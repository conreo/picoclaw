package orchestrator

import (
	"context"
	"fmt"
	"sync"

	"github.com/sipeed/picoclaw/pkg/llm"
	"github.com/sipeed/picoclaw/pkg/plan"
	"github.com/sipeed/picoclaw/pkg/proactive"
)

type Executor struct {
	planner        *plan.Planner
	llmPool        *llm.Pool
	sharedWAL      *proactive.SharedWAL
	monitor        *Monitor
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
	monitor := NewMonitor(cfg.MonitorConfig, cfg.LLMPool)

	return &Executor{
		planner:        planner,
		llmPool:        cfg.LLMPool,
		sharedWAL:      sharedWAL,
		monitor:        monitor,
		workers:        make(map[string]*Worker),
		taskChans:      make(map[string]chan WorkerTask),
		approvalConfig: cfg.ApprovalConfig,
		workspace:      cfg.Workspace,
		nextWorkerID:   1,
	}
}

func (e *Executor) Start(ctx context.Context) {
	go e.monitor.Start(ctx)
	go e.handleEscalations(ctx)
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
	return e.planner.Approve(id, approvedBy)
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

func (e *Executor) ExecutePlan(ctx context.Context, planID string) error {
	p, err := e.planner.Get(planID)
	if err != nil {
		return err
	}

	if p.Status != plan.TaskStatusApproved {
		return fmt.Errorf("plan not approved: %s", p.Status)
	}

	p.Status = plan.TaskStatusRunning

	parallelTasks, sequentialTasks := e.categorizeTasks(p.Tasks)

	var wg sync.WaitGroup

	for _, task := range parallelTasks {
		wg.Add(1)
		go func(t plan.Task) {
			defer wg.Done()
			e.executeTask(ctx, planID, t)
		}(task)
	}

	wg.Wait()

	for _, task := range sequentialTasks {
		if err := e.executeTask(ctx, planID, task); err != nil {
			return err
		}
	}

	return nil
}

func (e *Executor) executeTask(ctx context.Context, planID string, task plan.Task) error {
	worker := e.getOrCreateWorker(task.Model)
	if worker == nil {
		return fmt.Errorf("failed to get worker for model: %s", task.Model)
	}

	e.planner.StartTask(planID, task.ID, worker.ID)

	resultChan := make(chan TaskResult, 1)
	workerTask := WorkerTask{
		PlanID:     planID,
		TaskID:     task.ID,
		Task:       task,
		ResultChan: resultChan,
	}

	e.mu.RLock()
	taskChan := e.taskChans[worker.ID]
	e.mu.RUnlock()

	taskChan <- workerTask

	select {
	case result := <-resultChan:
		if result.Success {
			e.planner.CompleteTask(planID, task.ID, result.Result)
			return nil
		}
		e.planner.FailTask(planID, task.ID, result.Error.Error())
		return result.Error
	case <-ctx.Done():
		e.planner.FailTask(planID, task.ID, "context cancelled")
		return ctx.Err()
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

	worker.Start(context.Background(), taskChan)

	return worker
}

func (e *Executor) categorizeTasks(tasks []plan.Task) (parallel, sequential []plan.Task) {
	for _, task := range tasks {
		if len(task.Dependencies) == 0 {
			parallel = append(parallel, task)
		} else {
			sequential = append(sequential, task)
		}
	}
	return
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
		if worker, ok := e.workers[escalation.WorkerID]; ok {
			e.monitor.RestartWorker(escalation.WorkerID)
			worker.Start(context.Background(), e.taskChans[escalation.WorkerID])
		}
	case EscalationWorkerStuck:
		e.monitor.RestartWorker(escalation.WorkerID)
	case EscalationAPILimit:
		e.sharedWAL.Broadcast(fmt.Sprintf("API limit reached: %s", escalation.Message))
	case EscalationTaskFailed:
		e.sharedWAL.Broadcast(fmt.Sprintf("Task failed: %s", escalation.Message))
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
		Workers:  len(e.workers),
		Plans:    len(e.planner.GetApprovedPlans()),
		LLMStats: e.llmPool.GetStats(),
		Monitor:  e.monitor.GetStatus(),
	}
}

type ExecutorStatus struct {
	Workers  int                 `json:"workers"`
	Plans    int                 `json:"plans"`
	LLMStats []llm.Stats         `json:"llm_stats"`
	Monitor  MonitorStatus `json:"monitor"`
}
