package orchestrator

import (
	"fmt"
	"sync"
	"time"
)

type ScalingDecision int

const (
	NoChange ScalingDecision = iota
	ScaleUp
	ScaleDown
)

func (d ScalingDecision) String() string {
	switch d {
	case ScaleUp:
		return "scale_up"
	case ScaleDown:
		return "scale_down"
	default:
		return "no_change"
	}
}

type AutoScaler struct {
	minAgents        int
	maxAgents        int
	scaleUpThreshold int
	scaleDownAfter   time.Duration
	utilizationHigh  float64
	utilizationLow   float64
	metrics          *MetricsCollector
	pool             *AgentPool
	templates        *TemplateRegistry
	defaultTemplate  string
	mu               sync.RWMutex
	lastScaleTime    time.Time
	scaleCooldown    time.Duration
}

type ScalerConfig struct {
	MinAgents        int
	MaxAgents        int
	ScaleUpThreshold int
	ScaleDownAfter   time.Duration
	UtilizationHigh  float64
	UtilizationLow   float64
	ScaleCooldown    time.Duration
	DefaultTemplate  string
}

func NewAutoScaler(cfg ScalerConfig, metrics *MetricsCollector, pool *AgentPool, templates *TemplateRegistry) *AutoScaler {
	if cfg.ScaleCooldown == 0 {
		cfg.ScaleCooldown = 30 * time.Second
	}
	if cfg.DefaultTemplate == "" {
		cfg.DefaultTemplate = "worker"
	}
	if cfg.UtilizationHigh == 0 {
		cfg.UtilizationHigh = 80.0
	}
	if cfg.UtilizationLow == 0 {
		cfg.UtilizationLow = 30.0
	}

	return &AutoScaler{
		minAgents:        cfg.MinAgents,
		maxAgents:        cfg.MaxAgents,
		scaleUpThreshold: cfg.ScaleUpThreshold,
		scaleDownAfter:   cfg.ScaleDownAfter,
		utilizationHigh:  cfg.UtilizationHigh,
		utilizationLow:   cfg.UtilizationLow,
		metrics:          metrics,
		pool:             pool,
		templates:        templates,
		defaultTemplate:  cfg.DefaultTemplate,
		scaleCooldown:    cfg.ScaleCooldown,
	}
}

func (s *AutoScaler) Evaluate() ScalingDecision {
	s.mu.Lock()
	defer s.mu.Unlock()

	if time.Since(s.lastScaleTime) < s.scaleCooldown {
		return NoChange
	}

	m := s.metrics.Collect()

	if s.shouldScaleUp(m) {
		return ScaleUp
	}

	if s.shouldScaleDown(m) {
		return ScaleDown
	}

	return NoChange
}

func (s *AutoScaler) shouldScaleUp(m Metrics) bool {
	if m.TotalAgents >= s.maxAgents {
		return false
	}

	if m.TaskQueueDepth > s.scaleUpThreshold {
		return true
	}

	if m.UtilizationPercent >= s.utilizationHigh && m.TaskQueueDepth > 0 {
		return true
	}

	if m.ActiveAgents == m.TotalAgents && m.TaskQueueDepth > 0 {
		return true
	}

	return false
}

func (s *AutoScaler) shouldScaleDown(m Metrics) bool {
	if m.TotalAgents <= s.minAgents {
		return false
	}

	if m.TaskQueueDepth > 0 {
		return false
	}

	if m.UtilizationPercent > s.utilizationLow {
		return false
	}

	idleAgents := s.pool.GetOverIdleAgents(s.scaleDownAfter)
	if len(idleAgents) > 0 && m.TotalAgents > s.minAgents {
		return true
	}

	return false
}

func (s *AutoScaler) Execute(decision ScalingDecision) (*ScalingResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := &ScalingResult{
		Decision:  decision,
		Timestamp: time.Now(),
		Before:    s.pool.Count(),
	}

	switch decision {
	case ScaleUp:
		agent, err := s.scaleUp()
		if err != nil {
			return nil, err
		}
		result.AgentID = agent.ID
		result.Message = fmt.Sprintf("Spawned agent %s", agent.ID)

	case ScaleDown:
		agentID, err := s.scaleDown()
		if err != nil {
			return nil, err
		}
		result.AgentID = agentID
		result.Message = fmt.Sprintf("Stopped agent %s", agentID)
	}

	result.After = s.pool.Count()
	s.lastScaleTime = time.Now()

	return result, nil
}

func (s *AutoScaler) scaleUp() (*AgentStatus, error) {
	agent, err := s.pool.Spawn(s.defaultTemplate)
	if err != nil {
		return nil, fmt.Errorf("failed to spawn agent: %w", err)
	}

	return agent.AgentStatus, nil
}

func (s *AutoScaler) scaleDown() (string, error) {
	idleAgents := s.pool.GetOverIdleAgents(s.scaleDownAfter)

	for _, agentID := range idleAgents {
		err := s.pool.Stop(agentID)
		if err == nil {
			return agentID, nil
		}
	}

	return "", fmt.Errorf("no suitable agent to stop")
}

func (s *AutoScaler) ForceScaleUp(template string) (*AgentStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pool.Count() >= s.maxAgents {
		return nil, fmt.Errorf("maximum agents (%d) reached", s.maxAgents)
	}

	if template == "" {
		template = s.defaultTemplate
	}

	agent, err := s.pool.Spawn(template)
	if err != nil {
		return nil, err
	}

	s.lastScaleTime = time.Now()
	return agent.AgentStatus, nil
}

func (s *AutoScaler) ForceScaleDown(agentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.pool.Stop(agentID)
	if err != nil {
		return err
	}

	s.lastScaleTime = time.Now()
	return nil
}

func (s *AutoScaler) SetDefaultTemplate(template string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultTemplate = template
}

func (s *AutoScaler) GetStatus() ScalerStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	m := s.metrics.Collect()

	return ScalerStatus{
		TotalAgents:      m.TotalAgents,
		ActiveAgents:     m.ActiveAgents,
		IdleAgents:       m.IdleAgents,
		TaskQueueDepth:   m.TaskQueueDepth,
		Utilization:      m.UtilizationPercent,
		LastScaleTime:    s.lastScaleTime,
		MinAgents:        s.minAgents,
		MaxAgents:        s.maxAgents,
		ScaleUpThreshold: s.scaleUpThreshold,
	}
}

type ScalingResult struct {
	Decision  ScalingDecision `json:"decision"`
	AgentID   string          `json:"agent_id,omitempty"`
	Message   string          `json:"message"`
	Before    int             `json:"before"`
	After     int             `json:"after"`
	Timestamp time.Time       `json:"timestamp"`
}

type ScalerStatus struct {
	TotalAgents      int       `json:"total_agents"`
	ActiveAgents     int       `json:"active_agents"`
	IdleAgents       int       `json:"idle_agents"`
	TaskQueueDepth   int       `json:"task_queue_depth"`
	Utilization      float64   `json:"utilization"`
	LastScaleTime    time.Time `json:"last_scale_time"`
	MinAgents        int       `json:"min_agents"`
	MaxAgents        int       `json:"max_agents"`
	ScaleUpThreshold int       `json:"scale_up_threshold"`
}
