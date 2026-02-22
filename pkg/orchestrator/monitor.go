package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/llm"
	"github.com/sipeed/picoclaw/pkg/lock"
	"github.com/sipeed/picoclaw/pkg/messaging"
	"github.com/sipeed/picoclaw/pkg/proactive"
)

type EscalationType string

const (
	EscalationWorkerDead     EscalationType = "worker_dead"
	EscalationWorkerStuck    EscalationType = "worker_stuck"
	EscalationAPILimit       EscalationType = "api_limit"
	EscalationTaskFailed     EscalationType = "task_failed"
	EscalationAllWorkersBusy EscalationType = "all_workers_busy"
)

type Escalation struct {
	Type      EscalationType `json:"type"`
	WorkerID  string         `json:"worker_id,omitempty"`
	Message   string         `json:"message"`
	Timestamp time.Time      `json:"timestamp"`
	Details   map[string]any `json:"details,omitempty"`
}

type Monitor struct {
	ID                 string
	Model              string
	workers            map[string]*Worker
	llmPool            *llm.Pool
	wal                *proactive.WAL
	lockManager        *lock.LockManager
	eventQueue         *proactive.TaskQueue
	eventBus           *messaging.EventBus
	escalationChan     chan Escalation
	checkInterval      time.Duration
	heartbeatTimeout   time.Duration
	llmActivityTimeout time.Duration
	restartAttempts    int
	stopChan           chan struct{}
	mu                 sync.RWMutex
}

type MonitorConfig struct {
	ID                 string
	Model              string
	Workspace          string
	CheckInterval      time.Duration
	HeartbeatTimeout   time.Duration
	LLMActivityTimeout time.Duration
	RestartAttempts    int
	LockManager        *lock.LockManager
	EventQueue         *proactive.TaskQueue
}

func NewMonitor(cfg MonitorConfig, llmPool *llm.Pool, eventBus *messaging.EventBus) *Monitor {
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 30 * time.Second
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 2 * time.Minute
	}
	if cfg.LLMActivityTimeout == 0 {
		cfg.LLMActivityTimeout = 5 * time.Minute
	}
	if cfg.RestartAttempts == 0 {
		cfg.RestartAttempts = 3
	}

	var wal *proactive.WAL
	if cfg.Workspace != "" {
		wal = proactive.NewWAL(cfg.Workspace)
	}

	return &Monitor{
		ID:                 cfg.ID,
		Model:              cfg.Model,
		workers:            make(map[string]*Worker),
		llmPool:            llmPool,
		wal:                wal,
		lockManager:        cfg.LockManager,
		eventQueue:         cfg.EventQueue,
		eventBus:           eventBus,
		escalationChan:     make(chan Escalation, 100),
		checkInterval:      cfg.CheckInterval,
		heartbeatTimeout:   cfg.HeartbeatTimeout,
		llmActivityTimeout: cfg.LLMActivityTimeout,
		restartAttempts:    cfg.RestartAttempts,
		stopChan:           make(chan struct{}),
	}
}

func (m *Monitor) RegisterWorker(worker *Worker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workers[worker.ID] = worker
}

func (m *Monitor) UnregisterWorker(workerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.workers, workerID)
}

func (m *Monitor) Start(ctx context.Context) {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopChan:
			return
		case <-ticker.C:
			m.checkWorkers()
			m.cleanupExpiredLocks()
		}
	}
}

func (m *Monitor) Stop() {
	close(m.stopChan)
}

func (m *Monitor) checkWorkers() {
	m.mu.RLock()
	workers := make([]*Worker, 0, len(m.workers))
	for _, w := range m.workers {
		workers = append(workers, w)
	}
	m.mu.RUnlock()

	busyCount := 0
	for _, worker := range workers {
		status := worker.GetStatus()

		if status.State == string(WorkerStateStopped) {
			m.escalate(EscalationWorkerDead, worker.ID, "Worker stopped unexpectedly", nil)
			continue
		}

		if status.State == string(WorkerStateBusy) {
			busyCount++

			if time.Since(status.LastLLMActivity) > m.llmActivityTimeout {
				taskID := status.CurrentTask
				m.escalate(EscalationWorkerStuck, worker.ID,
					fmt.Sprintf("Worker stuck (no LLM activity for %v)", time.Since(status.LastLLMActivity)),
					map[string]any{"task_id": taskID, "last_llm_activity": status.LastLLMActivity})

				if m.lockManager != nil && taskID != "" {
					m.lockManager.ForceRelease(taskID, "worker_stuck_no_llm_activity")
				}
				continue
			}

			if time.Since(status.LastActivity) > m.heartbeatTimeout {
				m.escalate(EscalationWorkerStuck, worker.ID,
					fmt.Sprintf("Worker stuck for %v", time.Since(status.LastActivity)),
					map[string]any{"last_activity": status.LastActivity})
			}
		}
	}

	if busyCount >= len(workers) && len(workers) > 0 {
		m.escalate(EscalationAllWorkersBusy, "",
			fmt.Sprintf("All %d workers are busy", busyCount), nil)
	}

	m.checkAPIHealth()
}

func (m *Monitor) checkAPIHealth() {
	stats := m.llmPool.GetStats()
	for _, s := range stats {
		if s.Available == 0 && s.Max > 0 {
			m.escalate(EscalationAPILimit, "",
				fmt.Sprintf("Model %s at capacity (%d/%d)", s.ModelName, s.InUse, s.Max),
				map[string]any{"model": s.ModelName, "in_use": s.InUse, "max": s.Max})
		}
	}
}

func (m *Monitor) cleanupExpiredLocks() {
	if m.lockManager == nil {
		return
	}

	expired := m.lockManager.Cleanup()
	for _, entry := range expired {
		if m.eventQueue != nil {
			m.eventQueue.Release(entry.TaskID)
		}

		m.escalate(EscalationTaskFailed, entry.OwnerID,
			fmt.Sprintf("Lock expired for task %s (held by %s)", entry.TaskID, entry.OwnerID),
			map[string]any{"task_id": entry.TaskID, "lock_owner": entry.OwnerID, "expired_at": entry.ExpiresAt})
	}
}

func (m *Monitor) RestartWorker(workerID string) error {
	m.mu.RLock()
	worker, ok := m.workers[workerID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("worker not found: %s", workerID)
	}

	worker.Stop()
	time.Sleep(1 * time.Second)

	return nil
}

func (m *Monitor) escalate(escalationType EscalationType, workerID, message string, details map[string]any) {
	escalation := Escalation{
		Type:      escalationType,
		WorkerID:  workerID,
		Message:   message,
		Timestamp: time.Now(),
		Details:   details,
	}

	if m.wal != nil {
		m.wal.Write(proactive.EntryTypeMonitor, fmt.Sprintf("Escalation: %s", escalationType), message)
	}

	select {
	case m.escalationChan <- escalation:
	default:
	}

	if m.eventBus != nil {
		priority := messaging.PriorityNormal
		if escalationType == EscalationWorkerDead || escalationType == EscalationWorkerStuck {
			priority = messaging.PriorityHigh
		}

		eventType := messaging.EventAlert
		switch escalationType {
		case EscalationWorkerDead:
			eventType = messaging.EventError
		case EscalationWorkerStuck:
			eventType = messaging.EventWarning
		case EscalationAPILimit:
			eventType = messaging.EventWarning
		}

		event := messaging.NewEvent(eventType, "monitor").
			WithTarget("orchestrator").
			WithMessage(fmt.Sprintf("[%s] %s", escalationType, message)).
			WithPriority(priority)

		for k, v := range details {
			event.WithMetadata(k, v)
		}

		m.eventBus.Publish(event)
	}
}

func (m *Monitor) Escalations() <-chan Escalation {
	return m.escalationChan
}

func (m *Monitor) GetStatus() MonitorStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	workers := make([]WorkerStatus, 0, len(m.workers))
	for _, w := range m.workers {
		workers = append(workers, w.GetStatus())
	}

	var lockStats lock.LockStats
	if m.lockManager != nil {
		lockStats = m.lockManager.GetStats()
	}

	var queueStats proactive.QueueStats
	if m.eventQueue != nil {
		queueStats = m.eventQueue.GetStats()
	}

	return MonitorStatus{
		ID:                 m.ID,
		Model:              m.Model,
		WorkersMonitored:   len(m.workers),
		CheckInterval:      m.checkInterval,
		LLMActivityTimeout: m.llmActivityTimeout,
		Workers:            workers,
		LockStats:          lockStats,
		QueueStats:         queueStats,
	}
}

type MonitorStatus struct {
	ID                 string               `json:"id"`
	Model              string               `json:"model"`
	WorkersMonitored   int                  `json:"workers_monitored"`
	CheckInterval      time.Duration        `json:"check_interval"`
	LLMActivityTimeout time.Duration        `json:"llm_activity_timeout"`
	Workers            []WorkerStatus       `json:"workers"`
	LockStats          lock.LockStats       `json:"lock_stats"`
	QueueStats         proactive.QueueStats `json:"queue_stats"`
}

func (s MonitorStatus) String() string {
	var result string
	result += fmt.Sprintf("Monitor: %s (model: %s)\n", s.ID, s.Model)
	result += fmt.Sprintf("   Workers: %d monitored\n", s.WorkersMonitored)
	result += fmt.Sprintf("   Check interval: %v\n", s.CheckInterval)
	result += fmt.Sprintf("   LLM activity timeout: %v\n\n", s.LLMActivityTimeout)

	for _, w := range s.Workers {
		status := w.State
		if w.CurrentTask != "" {
			status = fmt.Sprintf("%s (task: %s)", w.State, w.CurrentTask)
		}
		result += fmt.Sprintf("   [%s] %s: %s\n", w.ID, w.Model, status)
	}

	return result
}

func (m *Monitor) FormatProgress() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result string
	active := 0
	idle := 0

	for _, w := range m.workers {
		status := w.GetStatus()
		if status.State == string(WorkerStateBusy) {
			active++
			result += fmt.Sprintf("Worker %s: %s\n", w.ID, status.CurrentTask)
		} else if status.State == string(WorkerStateIdle) {
			idle++
		}
	}

	if active == 0 && idle == 0 {
		return "No active workers\n"
	}

	return fmt.Sprintf("Active: %d | Idle: %d\n%s", active, idle, result)
}
