package orchestrator

import (
	"fmt"

	"github.com/sipeed/picoclaw/pkg/lock"
	"github.com/sipeed/picoclaw/pkg/plan"
	"github.com/sipeed/picoclaw/pkg/proactive"
)

type RecoveryManager struct {
	wal         *proactive.WAL
	queue       *proactive.TaskQueue
	lockManager *lock.LockManager
	planner     *plan.Planner
	workspace   string
}

func NewRecoveryManager(workspace string, wal *proactive.WAL, queue *proactive.TaskQueue, lm *lock.LockManager, planner *plan.Planner) *RecoveryManager {
	return &RecoveryManager{
		wal:         wal,
		queue:       queue,
		lockManager: lm,
		planner:     planner,
		workspace:   workspace,
	}
}

type RecoveryResult struct {
	TasksRecovered []string `json:"tasks_recovered"`
	TasksRequeued  []string `json:"tasks_requeued"`
	LocksReleased  []string `json:"locks_released"`
	PlansResumed   []string `json:"plans_resumed"`
	Errors         []string `json:"errors,omitempty"`
}

func (r *RecoveryManager) Recover() (*RecoveryResult, error) {
	result := &RecoveryResult{}

	expiredLocks := r.lockManager.Cleanup()
	for _, entry := range expiredLocks {
		result.LocksReleased = append(result.LocksReleased, entry.TaskID)
	}

	requeued := r.queue.RequeueStuck(0)
	result.TasksRequeued = append(result.TasksRequeued, requeued...)

	pending := r.wal.Recover()
	for _, entry := range pending {
		switch entry.Type {
		case proactive.EntryTypeTask:
			r.recoverTask(entry.ID, entry.Command, result)
		case proactive.EntryTypePlan:
			r.recoverPlan(entry.ID, result)
		}
	}

	return result, nil
}

func (r *RecoveryManager) recoverTask(entryID, description string, result *RecoveryResult) {
	if r.lockManager.IsLocked(entryID) {
		return
	}

	queueTask := &proactive.QueuedTask{
		Description: description,
		Priority:    proactive.PriorityNormal,
		Source:      proactive.SourceMonitor,
		Metadata: map[string]any{
			"recovered_from": entryID,
		},
	}

	task, err := r.queue.Enqueue(queueTask)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to requeue task %s: %v", entryID, err))
		return
	}

	result.TasksRecovered = append(result.TasksRecovered, task.ID)
}

func (r *RecoveryManager) recoverPlan(planID string, result *RecoveryResult) {
	if r.planner == nil {
		return
	}

	p, err := r.planner.Get(planID)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("plan not found: %s", planID))
		return
	}

	incompleteTasks := []string{}
	for _, task := range p.Tasks {
		if task.Status != plan.TaskStatusCompleted {
			incompleteTasks = append(incompleteTasks, task.ID)
		}
	}

	if len(incompleteTasks) == 0 {
		return
	}

	err = r.RecoverPlan(planID)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to recover plan %s: %v", planID, err))
		return
	}

	result.PlansResumed = append(result.PlansResumed, planID)
}

func (r *RecoveryManager) RecoverPlan(planID string) error {
	if r.planner == nil {
		return fmt.Errorf("planner not available")
	}

	p, err := r.planner.Get(planID)
	if err != nil {
		return fmt.Errorf("plan not found: %s", planID)
	}

	for _, task := range p.Tasks {
		if task.Status == plan.TaskStatusCompleted || task.Status == plan.TaskStatusCancelled {
			continue
		}

		if r.queue.GetByTaskID(task.ID) != nil {
			continue
		}

		if r.lockManager.IsLocked(task.ID) {
			continue
		}

		priority := proactive.PriorityNormal
		if p.Complexity == plan.ComplexityHigh {
			priority = proactive.PriorityHigh
		}

		queuedTask := &proactive.QueuedTask{
			PlanID:       planID,
			TaskID:       task.ID,
			Description:  task.Description,
			Model:        task.Model,
			Priority:     priority,
			Source:       proactive.SourceMonitor,
			Dependencies: task.Dependencies,
			Metadata: map[string]any{
				"recovered":        true,
				"original_status":  task.Status,
				"worker_type":      task.WorkerType,
				"estimated_tokens": task.EstimatedTokens,
			},
		}

		if _, err := r.queue.Enqueue(queuedTask); err != nil {
			return fmt.Errorf("failed to requeue task %s: %w", task.ID, err)
		}
	}

	return nil
}

func (r *RecoveryManager) RecoverWorker(workerID string) ([]string, error) {
	var recoveredTasks []string

	locks := r.lockManager.GetByOwner(workerID)
	for _, entry := range locks {
		r.lockManager.ForceRelease(entry.TaskID, "worker_recovery")

		if err := r.queue.Release(entry.TaskID); err == nil {
			recoveredTasks = append(recoveredTasks, entry.TaskID)
		}
	}

	return recoveredTasks, nil
}

func (r *RecoveryManager) GetRecoveryStatus() map[string]any {
	queueStats := r.queue.GetStats()
	lockStats := r.lockManager.GetStats()

	pendingPlans := 0
	if r.planner != nil {
		plans := r.planner.GetApprovedPlans()
		for _, p := range plans {
			for _, t := range p.Tasks {
				if t.Status != plan.TaskStatusCompleted {
					pendingPlans++
					break
				}
			}
		}
	}

	pendingWAL := len(r.wal.Recover())

	return map[string]any{
		"queue_pending":    queueStats.Pending,
		"queue_claimed":    queueStats.Claimed,
		"locks_active":     lockStats.Total,
		"locks_expiring":   lockStats.ExpiringSoon,
		"plans_incomplete": pendingPlans,
		"wal_pending":      pendingWAL,
	}
}
