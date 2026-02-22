package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/messaging"
	"github.com/sipeed/picoclaw/pkg/proactive"
)

type Controller struct {
	config    *OrchestratorConfig
	sharedWAL *proactive.SharedWAL
	eventBus  *messaging.EventBus
	pool      *AgentPool
	scaler    *AutoScaler
	metrics   *MetricsCollector
	templates *TemplateRegistry
	running   bool
	mu        sync.RWMutex
	stopChan  chan struct{}
	agentID   string
	workspace string
}

type OrchestratorConfig struct {
	Enabled          bool                     `json:"enabled"`
	AutoScale        bool                     `json:"auto_scale"`
	MinAgents        int                      `json:"min_agents"`
	MaxAgents        int                      `json:"max_agents"`
	ScaleUpThreshold int                      `json:"scale_up_threshold"`
	ScaleDownAfter   time.Duration            `json:"scale_down_after"`
	CheckInterval    time.Duration            `json:"check_interval"`
	UtilizationHigh  float64                  `json:"utilization_high"`
	UtilizationLow   float64                  `json:"utilization_low"`
	DefaultTemplate  string                   `json:"default_template"`
	Templates        map[string]AgentTemplate `json:"templates"`
}

func DefaultOrchestratorConfig() OrchestratorConfig {
	return OrchestratorConfig{
		Enabled:          true,
		AutoScale:        true,
		MinAgents:        1,
		MaxAgents:        10,
		ScaleUpThreshold: 5,
		ScaleDownAfter:   5 * time.Minute,
		CheckInterval:    30 * time.Second,
		UtilizationHigh:  80.0,
		UtilizationLow:   30.0,
		DefaultTemplate:  "worker",
		Templates:        make(map[string]AgentTemplate),
	}
}

func NewController(cfg OrchestratorConfig, workspace, agentID string) *Controller {
	sharedWAL := proactive.NewSharedWAL(workspace, agentID)
	eventBus := messaging.NewEventBus(workspace)
	templates := NewTemplateRegistry()

	if len(cfg.Templates) > 0 {
		templates.LoadFromConfig(cfg.Templates)
	}

	pool := NewAgentPool(templates, workspace, cfg.MinAgents, cfg.MaxAgents)
	metrics := NewMetricsCollector(sharedWAL, eventBus, pool)

	scalerCfg := ScalerConfig{
		MinAgents:        cfg.MinAgents,
		MaxAgents:        cfg.MaxAgents,
		ScaleUpThreshold: cfg.ScaleUpThreshold,
		ScaleDownAfter:   cfg.ScaleDownAfter,
		UtilizationHigh:  cfg.UtilizationHigh,
		UtilizationLow:   cfg.UtilizationLow,
		DefaultTemplate:  cfg.DefaultTemplate,
	}
	scaler := NewAutoScaler(scalerCfg, metrics, pool, templates)

	return &Controller{
		config:    &cfg,
		sharedWAL: sharedWAL,
		eventBus:  eventBus,
		pool:      pool,
		scaler:    scaler,
		metrics:   metrics,
		templates: templates,
		agentID:   agentID,
		workspace: workspace,
		stopChan:  make(chan struct{}),
	}
}

func (c *Controller) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("controller already running")
	}
	c.running = true
	c.mu.Unlock()

	logger.InfoCF("orchestrator", "Orchestrator started",
		map[string]any{
			"auto_scale": c.config.AutoScale,
			"min_agents": c.config.MinAgents,
			"max_agents": c.config.MaxAgents,
		})

	ticker := time.NewTicker(c.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.Stop()
			return ctx.Err()
		case <-c.stopChan:
			return nil
		case <-ticker.C:
			c.tick()
		}
	}
}

func (c *Controller) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return
	}

	c.running = false
	close(c.stopChan)

	logger.InfoCF("orchestrator", "Orchestrator stopped", nil)
}

func (c *Controller) tick() {
	if !c.config.AutoScale {
		return
	}

	decision := c.scaler.Evaluate()
	if decision == NoChange {
		return
	}

	result, err := c.scaler.Execute(decision)
	if err != nil {
		logger.ErrorCF("orchestrator", "Scaling failed",
			map[string]any{"decision": decision.String(), "error": err.Error()})
		return
	}

	logger.InfoCF("orchestrator", "Scaling executed",
		map[string]any{
			"decision": result.Decision.String(),
			"agent_id": result.AgentID,
			"before":   result.Before,
			"after":    result.After,
			"message":  result.Message,
		})

	c.eventBus.Broadcast(c.agentID, result.Message, messaging.PriorityNormal)
}

func (c *Controller) Status() ControllerStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return ControllerStatus{
		Running:       c.running,
		AutoScale:     c.config.AutoScale,
		Agents:        c.metrics.GetAllAgentStatus(),
		ScalerStatus:  c.scaler.GetStatus(),
		CurrentConfig: *c.config,
	}
}

func (c *Controller) GetMetrics() Metrics {
	return c.metrics.Collect()
}

func (c *Controller) ScaleUp(template string) (*AgentStatus, error) {
	agent, err := c.scaler.ForceScaleUp(template)
	if err != nil {
		return nil, err
	}

	logger.InfoCF("orchestrator", "Manual scale up",
		map[string]any{"agent_id": agent.ID, "template": template})

	return agent, nil
}

func (c *Controller) ScaleDown(agentID string) error {
	err := c.scaler.ForceScaleDown(agentID)
	if err != nil {
		return err
	}

	logger.InfoCF("orchestrator", "Manual scale down",
		map[string]any{"agent_id": agentID})

	return nil
}

func (c *Controller) AssignTask(agentID string, task TaskAssignment) error {
	return c.pool.AssignTask(agentID, task)
}

func (c *Controller) GetIdleAgent(capability string) *AgentStatus {
	agent := c.pool.GetIdleAgent(capability)
	if agent == nil {
		return nil
	}
	return agent.AgentStatus
}

func (c *Controller) DelegateTask(requiredCapabilities []string, task TaskAssignment) (*AgentStatus, error) {
	agent := c.pool.GetIdleAgent(requiredCapabilities[0])
	if agent == nil {
		tmpl := c.templates.BestForTask(requiredCapabilities)
		agentStatus, err := c.ScaleUp(tmpl.Name)
		if err != nil {
			return nil, fmt.Errorf("no available agent and failed to spawn: %w", err)
		}
		agent = c.pool.GetIdleAgent(requiredCapabilities[0])
		if agent == nil {
			return agentStatus, nil
		}
	}

	if err := c.AssignTask(agent.ID, task); err != nil {
		return nil, err
	}

	return agent.AgentStatus, nil
}

func (c *Controller) TaskComplete(agentID string, success bool) {
	c.pool.TaskComplete(agentID, success)
	c.metrics.RecordTaskComplete(success)
}

func (c *Controller) UpdateConfig(cfg OrchestratorConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.config = &cfg
	if len(cfg.Templates) > 0 {
		c.templates.LoadFromConfig(cfg.Templates)
	}
}

type ControllerStatus struct {
	Running       bool               `json:"running"`
	AutoScale     bool               `json:"auto_scale"`
	Agents        []AgentStatus      `json:"agents"`
	ScalerStatus  ScalerStatus       `json:"scaler_status"`
	CurrentConfig OrchestratorConfig `json:"current_config"`
}

func (s ControllerStatus) String() string {
	var result string
	result += fmt.Sprintf("Orchestrator Status:\n")
	result += fmt.Sprintf("  Running: %v\n", s.Running)
	result += fmt.Sprintf("  Auto-Scale: %v\n", s.AutoScale)
	result += fmt.Sprintf("  Agents: %d total, %d active, %d idle\n",
		s.ScalerStatus.TotalAgents, s.ScalerStatus.ActiveAgents, s.ScalerStatus.IdleAgents)
	result += fmt.Sprintf("  Task Queue: %d pending\n", s.ScalerStatus.TaskQueueDepth)
	result += fmt.Sprintf("  Utilization: %.1f%%\n", s.ScalerStatus.Utilization)
	return result
}
