package orchestrator

import (
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/messaging"
	"github.com/sipeed/picoclaw/pkg/proactive"
)

type Metrics struct {
	TaskQueueDepth     int           `json:"task_queue_depth"`
	ActiveAgents       int           `json:"active_agents"`
	IdleAgents         int           `json:"idle_agents"`
	TotalAgents        int           `json:"total_agents"`
	UtilizationPercent float64       `json:"utilization_percent"`
	AvgTaskDuration    time.Duration `json:"avg_task_duration"`
	TaskCompletionRate float64       `json:"task_completion_rate"`
	CollectedAt        time.Time     `json:"collected_at"`
}

type AgentState string

const (
	AgentStateIdle  AgentState = "idle"
	AgentStateBusy  AgentState = "busy"
	AgentStateStart AgentState = "starting"
	AgentStateStop  AgentState = "stopping"
)

type AgentStatus struct {
	ID            string     `json:"id"`
	Template      string     `json:"template"`
	State         AgentState `json:"state"`
	Capabilities  []string   `json:"capabilities"`
	CurrentTasks  int        `json:"current_tasks"`
	MaxTasks      int        `json:"max_tasks"`
	StartedAt     time.Time  `json:"started_at"`
	LastActivity  time.Time  `json:"last_activity"`
	TasksComplete int64      `json:"tasks_complete"`
	TasksFailed   int64      `json:"tasks_failed"`
}

type MetricsCollector struct {
	sharedWAL   *proactive.SharedWAL
	eventBus    *messaging.EventBus
	agentPool   *AgentPool
	taskHistory []taskRecord
	mu          sync.RWMutex
}

type taskRecord struct {
	startedAt   time.Time
	completedAt time.Time
	success     bool
}

func NewMetricsCollector(sharedWAL *proactive.SharedWAL, eventBus *messaging.EventBus, pool *AgentPool) *MetricsCollector {
	return &MetricsCollector{
		sharedWAL:   sharedWAL,
		eventBus:    eventBus,
		agentPool:   pool,
		taskHistory: make([]taskRecord, 0),
	}
}

func (m *MetricsCollector) Collect() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	agents := m.agentPool.GetAll()

	var activeCount, idleCount int
	for _, agent := range agents {
		switch agent.State {
		case AgentStateBusy:
			activeCount++
		case AgentStateIdle:
			idleCount++
		}
	}

	total := len(agents)
	utilization := 0.0
	if total > 0 {
		utilization = float64(activeCount) / float64(total) * 100
	}

	pendingTasks := m.sharedWAL.GetPendingTasks("all")

	return Metrics{
		TaskQueueDepth:     len(pendingTasks),
		ActiveAgents:       activeCount,
		IdleAgents:         idleCount,
		TotalAgents:        total,
		UtilizationPercent: utilization,
		AvgTaskDuration:    m.calculateAvgDuration(),
		TaskCompletionRate: m.calculateCompletionRate(),
		CollectedAt:        time.Now(),
	}
}

func (m *MetricsCollector) RecordTaskStart() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.taskHistory = append(m.taskHistory, taskRecord{
		startedAt: time.Now(),
	})

	if len(m.taskHistory) > 1000 {
		m.taskHistory = m.taskHistory[len(m.taskHistory)-1000:]
	}
}

func (m *MetricsCollector) RecordTaskComplete(success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := len(m.taskHistory) - 1; i >= 0; i-- {
		if m.taskHistory[i].completedAt.IsZero() {
			m.taskHistory[i].completedAt = time.Now()
			m.taskHistory[i].success = success
			break
		}
	}
}

func (m *MetricsCollector) calculateAvgDuration() time.Duration {
	if len(m.taskHistory) == 0 {
		return 0
	}

	var total time.Duration
	var count int

	for _, record := range m.taskHistory {
		if !record.completedAt.IsZero() && !record.startedAt.IsZero() {
			total += record.completedAt.Sub(record.startedAt)
			count++
		}
	}

	if count == 0 {
		return 0
	}
	return total / time.Duration(count)
}

func (m *MetricsCollector) calculateCompletionRate() float64 {
	if len(m.taskHistory) < 2 {
		return 0
	}

	now := time.Now()
	oneMinuteAgo := now.Add(-time.Minute)

	var completedLastMin int
	for _, record := range m.taskHistory {
		if !record.completedAt.IsZero() && record.completedAt.After(oneMinuteAgo) {
			completedLastMin++
		}
	}

	return float64(completedLastMin)
}

func (m *MetricsCollector) GetAgentStatus(agentID string) *AgentStatus {
	agent := m.agentPool.Get(agentID)
	if agent == nil {
		return nil
	}
	return agent
}

func (m *MetricsCollector) GetAllAgentStatus() []AgentStatus {
	agents := m.agentPool.GetAll()
	result := make([]AgentStatus, 0, len(agents))
	for _, agent := range agents {
		result = append(result, *agent)
	}
	return result
}
