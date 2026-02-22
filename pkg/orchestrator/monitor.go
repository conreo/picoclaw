package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/llm"
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
	ID               string
	Model            string
	workers          map[string]*Worker
	llmPool          *llm.Pool
	wal              *proactive.WAL
	escalationChan   chan Escalation
	checkInterval    time.Duration
	heartbeatTimeout time.Duration
	restartAttempts  int
	stopChan         chan struct{}
	mu               sync.RWMutex
}

type MonitorConfig struct {
	ID               string
	Model            string
	Workspace        string
	CheckInterval    time.Duration
	HeartbeatTimeout time.Duration
	RestartAttempts  int
}

func NewMonitor(cfg MonitorConfig, llmPool *llm.Pool) *Monitor {
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 30 * time.Second
	}
	if cfg.HeartbeatTimeout == 0 {
		cfg.HeartbeatTimeout = 2 * time.Minute
	}
	if cfg.RestartAttempts == 0 {
		cfg.RestartAttempts = 3
	}

	return &Monitor{
		ID:               cfg.ID,
		Model:            cfg.Model,
		workers:          make(map[string]*Worker),
		llmPool:          llmPool,
		wal:              proactive.NewWAL(cfg.Workspace),
		escalationChan:   make(chan Escalation, 100),
		checkInterval:    cfg.CheckInterval,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		restartAttempts:  cfg.RestartAttempts,
		stopChan:         make(chan struct{}),
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

	for _, worker := range workers {
		status := worker.GetStatus()

		if status.State == string(WorkerStateStopped) {
			m.escalate(EscalationWorkerDead, worker.ID, "Worker stopped unexpectedly")
			continue
		}

		if time.Since(status.LastActivity) > m.heartbeatTimeout {
			if status.State == string(WorkerStateBusy) {
				m.escalate(EscalationWorkerStuck, worker.ID,
					fmt.Sprintf("Worker stuck for %v", time.Since(status.LastActivity)))
			}
		}
	}

	m.checkAPIHealth()
}

func (m *Monitor) checkAPIHealth() {
	stats := m.llmPool.GetStats()
	for _, s := range stats {
		if s.Available == 0 && s.Max > 0 {
			m.escalate(EscalationAPILimit, "",
				fmt.Sprintf("Model %s at capacity (%d/%d)", s.ModelName, s.InUse, s.Max))
		}
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

func (m *Monitor) escalate(escalationType EscalationType, workerID, message string) {
	escalation := Escalation{
		Type:      escalationType,
		WorkerID:  workerID,
		Message:   message,
		Timestamp: time.Now(),
		Details: map[string]any{
			"monitor_id": m.ID,
		},
	}

	m.wal.Write(proactive.EntryTypeMonitor, fmt.Sprintf("Escalation: %s", escalationType), message)

	select {
	case m.escalationChan <- escalation:
	default:
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

	return MonitorStatus{
		ID:               m.ID,
		Model:            m.Model,
		WorkersMonitored: len(m.workers),
		CheckInterval:    m.checkInterval,
		Workers:          workers,
	}
}

type MonitorStatus struct {
	ID               string         `json:"id"`
	Model            string         `json:"model"`
	WorkersMonitored int            `json:"workers_monitored"`
	CheckInterval    time.Duration  `json:"check_interval"`
	Workers          []WorkerStatus `json:"workers"`
}

func (s MonitorStatus) String() string {
	var result string
	result += fmt.Sprintf("🔍 Monitor: %s (model: %s)\n", s.ID, s.Model)
	result += fmt.Sprintf("   Workers: %d monitored\n", s.WorkersMonitored)
	result += fmt.Sprintf("   Check interval: %v\n\n", s.CheckInterval)

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
			result += fmt.Sprintf("⏳ Worker %s: %s\n", w.ID, status.CurrentTask)
		} else if status.State == string(WorkerStateIdle) {
			idle++
		}
	}

	if active == 0 && idle == 0 {
		return "⏸️ No active workers\n"
	}

	return fmt.Sprintf("🔄 Active: %d | Idle: %d\n%s", active, idle, result)
}
