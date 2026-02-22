package plan

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Complexity string

const (
	ComplexityLow    Complexity = "low"
	ComplexityMedium Complexity = "medium"
	ComplexityHigh   Complexity = "high"
)

type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusApproved  TaskStatus = "approved"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

type Task struct {
	ID              string         `json:"id"`
	Description     string         `json:"description"`
	Model           string         `json:"model"`
	WorkerType      string         `json:"worker_type"`
	EstimatedTokens int            `json:"estimated_tokens"`
	Dependencies    []string       `json:"dependencies,omitempty"`
	Status          TaskStatus     `json:"status"`
	AssignedWorker  string         `json:"assigned_worker,omitempty"`
	Result          string         `json:"result,omitempty"`
	Error           string         `json:"error,omitempty"`
	StartedAt       *time.Time     `json:"started_at,omitempty"`
	CompletedAt     *time.Time     `json:"completed_at,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

type Plan struct {
	ID                string     `json:"id"`
	CreatedAt         time.Time  `json:"created_at"`
	Request           string     `json:"request"`
	Complexity        Complexity `json:"complexity"`
	Status            TaskStatus `json:"status"`
	Tasks             []Task     `json:"tasks"`
	WorkersNeeded     int        `json:"workers_needed"`
	EstimatedDuration string     `json:"estimated_duration"`
	EstimatedCost     string     `json:"estimated_cost"`
	ApprovalRequired  bool       `json:"approval_required"`
	ApprovedBy        string     `json:"approved_by,omitempty"`
	ApprovedAt        *time.Time `json:"approved_at,omitempty"`
}

type Planner struct {
	path   string
	plans  map[string]*Plan
	mu     sync.RWMutex
	nextID int64
}

func NewPlanner(workspace string) *Planner {
	plansDir := filepath.Join(workspace, "memory")
	os.MkdirAll(plansDir, 0o755)

	p := &Planner{
		path:   filepath.Join(plansDir, "plans.json"),
		plans:  make(map[string]*Plan),
		nextID: 1,
	}
	p.load()
	return p
}

func (p *Planner) load() error {
	data, err := os.ReadFile(p.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var plans []*Plan
	if err := json.Unmarshal(data, &plans); err != nil {
		return err
	}

	for _, plan := range plans {
		p.plans[plan.ID] = plan
		if id := parseID(plan.ID); id >= p.nextID {
			p.nextID = id + 1
		}
	}
	return nil
}

func (p *Planner) save() error {
	plans := make([]*Plan, 0, len(p.plans))
	for _, plan := range p.plans {
		plans = append(plans, plan)
	}

	data, err := json.MarshalIndent(plans, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p.path, data, 0o644)
}

func (p *Planner) Create(request string, complexity Complexity, tasks []Task, approvalRequired bool) *Plan {
	p.mu.Lock()
	defer p.mu.Unlock()

	plan := &Plan{
		ID:                fmt.Sprintf("plan-%d", p.nextID),
		CreatedAt:         time.Now(),
		Request:           request,
		Complexity:        complexity,
		Status:            TaskStatusPending,
		Tasks:             tasks,
		WorkersNeeded:     calculateWorkersNeeded(tasks),
		EstimatedDuration: estimateDuration(tasks),
		EstimatedCost:     estimateCost(tasks),
		ApprovalRequired:  approvalRequired,
	}

	p.nextID++
	p.plans[plan.ID] = plan
	p.save()

	return plan
}

func (p *Planner) Get(id string) (*Plan, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	plan, ok := p.plans[id]
	if !ok {
		return nil, fmt.Errorf("plan not found: %s", id)
	}
	return plan, nil
}

func (p *Planner) Approve(id, approvedBy string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	plan, ok := p.plans[id]
	if !ok {
		return fmt.Errorf("plan not found: %s", id)
	}

	if plan.Status != TaskStatusPending {
		return fmt.Errorf("plan is not pending: %s", plan.Status)
	}

	now := time.Now()
	plan.Status = TaskStatusApproved
	plan.ApprovedBy = approvedBy
	plan.ApprovedAt = &now

	for i := range plan.Tasks {
		plan.Tasks[i].Status = TaskStatusApproved
	}

	return p.save()
}

func (p *Planner) Reject(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	plan, ok := p.plans[id]
	if !ok {
		return fmt.Errorf("plan not found: %s", id)
	}

	plan.Status = TaskStatusCancelled
	return p.save()
}

func (p *Planner) UpdateTask(planID, taskID string, updates map[string]any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	plan, ok := p.plans[planID]
	if !ok {
		return fmt.Errorf("plan not found: %s", planID)
	}

	for i := range plan.Tasks {
		if plan.Tasks[i].ID == taskID {
			if desc, ok := updates["description"].(string); ok {
				plan.Tasks[i].Description = desc
			}
			if model, ok := updates["model"].(string); ok {
				plan.Tasks[i].Model = model
			}
			if status, ok := updates["status"].(TaskStatus); ok {
				plan.Tasks[i].Status = status
			}
			if worker, ok := updates["assigned_worker"].(string); ok {
				plan.Tasks[i].AssignedWorker = worker
			}
			if result, ok := updates["result"].(string); ok {
				plan.Tasks[i].Result = result
			}
			if errStr, ok := updates["error"].(string); ok {
				plan.Tasks[i].Error = errStr
			}
			if t, ok := updates["started_at"].(time.Time); ok {
				plan.Tasks[i].StartedAt = &t
			}
			if t, ok := updates["completed_at"].(time.Time); ok {
				plan.Tasks[i].CompletedAt = &t
			}
			return p.save()
		}
	}

	return fmt.Errorf("task not found: %s", taskID)
}

func (p *Planner) StartTask(planID, taskID, workerID string) error {
	now := time.Now()
	return p.UpdateTask(planID, taskID, map[string]any{
		"status":          TaskStatusRunning,
		"assigned_worker": workerID,
		"started_at":      now,
	})
}

func (p *Planner) CompleteTask(planID, taskID, result string) error {
	now := time.Now()
	return p.UpdateTask(planID, taskID, map[string]any{
		"status":       TaskStatusCompleted,
		"result":       result,
		"completed_at": now,
	})
}

func (p *Planner) FailTask(planID, taskID, errMsg string) error {
	now := time.Now()
	return p.UpdateTask(planID, taskID, map[string]any{
		"status":       TaskStatusFailed,
		"error":        errMsg,
		"completed_at": now,
	})
}

func (p *Planner) GetPendingPlans() []*Plan {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var result []*Plan
	for _, plan := range p.plans {
		if plan.Status == TaskStatusPending {
			result = append(result, plan)
		}
	}
	return result
}

func (p *Planner) GetApprovedPlans() []*Plan {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var result []*Plan
	for _, plan := range p.plans {
		if plan.Status == TaskStatusApproved || plan.Status == TaskStatusRunning {
			result = append(result, plan)
		}
	}
	return result
}

func (p *Planner) FormatPlan(plan *Plan) string {
	var result string
	result += fmt.Sprintf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	result += fmt.Sprintf("📋 PLAN: %s\n", plan.Request)
	result += fmt.Sprintf("ID: %s\n", plan.ID)
	result += fmt.Sprintf("Complexity: %s\n", plan.Complexity)
	result += fmt.Sprintf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	result += "Tasks:\n"
	for i, task := range plan.Tasks {
		status := ""
		switch task.Status {
		case TaskStatusCompleted:
			status = " ✓"
		case TaskStatusRunning:
			status = " ⏳"
		case TaskStatusFailed:
			status = " ✗"
		}
		result += fmt.Sprintf("  %d. %s (%s)%s\n", i+1, task.Description, task.Model, status)
	}

	result += fmt.Sprintf("\nResources:\n")
	result += fmt.Sprintf("  • Workers: %d\n", plan.WorkersNeeded)
	result += fmt.Sprintf("  • Estimated time: %s\n", plan.EstimatedDuration)
	result += fmt.Sprintf("  • Estimated cost: %s\n", plan.EstimatedCost)

	if plan.ApprovalRequired && plan.Status == TaskStatusPending {
		result += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
		result += "Reply ✓ to approve, ✗ to cancel, or 'edit N description/model' to modify.\n"
	}

	return result
}

func calculateWorkersNeeded(tasks []Task) int {
	parallel := 0
	sequential := 0

	for _, task := range tasks {
		if len(task.Dependencies) == 0 {
			parallel++
		} else {
			sequential++
		}
	}

	if parallel == 0 {
		return 1
	}
	if parallel > 5 {
		return 5
	}
	return parallel
}

func estimateDuration(tasks []Task) string {
	total := len(tasks)
	if total <= 3 {
		return "~2 min"
	} else if total <= 6 {
		return "~5 min"
	} else if total <= 10 {
		return "~10 min"
	}
	return "~15 min"
}

func estimateCost(tasks []Task) string {
	total := 0
	for _, t := range tasks {
		total += t.EstimatedTokens
	}
	if total < 1000 {
		return "~$0.01"
	} else if total < 5000 {
		return "~$0.05"
	} else if total < 10000 {
		return "~$0.10"
	}
	return "~$0.20"
}

func parseID(id string) int64 {
	var n int64
	fmt.Sscanf(id, "plan-%d", &n)
	return n
}
