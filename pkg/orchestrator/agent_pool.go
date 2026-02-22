package orchestrator

import (
	"fmt"
	"sync"
	"time"
)

type ManagedAgent struct {
	*AgentStatus
	template     AgentTemplate
	taskChan     chan TaskAssignment
	stopChan     chan struct{}
	onTaskStart  func()
	onTaskFinish func(bool)
}

type TaskAssignment struct {
	TaskID   string
	Action   string
	Payload  map[string]any
	Priority int
}

type AgentPool struct {
	agents    map[string]*ManagedAgent
	templates *TemplateRegistry
	mu        sync.RWMutex
	maxAgents int
	minAgents int
	nextID    int64
	workspace string
}

func NewAgentPool(templates *TemplateRegistry, workspace string, minAgents, maxAgents int) *AgentPool {
	return &AgentPool{
		agents:    make(map[string]*ManagedAgent),
		templates: templates,
		maxAgents: maxAgents,
		minAgents: minAgents,
		workspace: workspace,
		nextID:    1,
	}
}

func (p *AgentPool) Spawn(templateName string) (*ManagedAgent, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.agents) >= p.maxAgents {
		return nil, fmt.Errorf("maximum agents (%d) reached", p.maxAgents)
	}

	tmpl, ok := p.templates.Get(templateName)
	if !ok {
		return nil, fmt.Errorf("template not found: %s", templateName)
	}

	agentID := fmt.Sprintf("%s-%d", templateName, p.nextID)
	p.nextID++

	agent := &ManagedAgent{
		AgentStatus: &AgentStatus{
			ID:           agentID,
			Template:     templateName,
			State:        AgentStateStart,
			Capabilities: tmpl.Capabilities,
			CurrentTasks: 0,
			MaxTasks:     tmpl.MaxConcurrentTasks,
			StartedAt:    time.Now(),
			LastActivity: time.Now(),
		},
		template: tmpl,
		taskChan: make(chan TaskAssignment, tmpl.MaxConcurrentTasks*2),
		stopChan: make(chan struct{}),
	}

	p.agents[agentID] = agent

	agent.State = AgentStateIdle

	return agent, nil
}

func (p *AgentPool) Stop(agentID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	agent, ok := p.agents[agentID]
	if !ok {
		return fmt.Errorf("agent not found: %s", agentID)
	}

	if agent.State == AgentStateBusy {
		return fmt.Errorf("cannot stop busy agent: %s", agentID)
	}

	if len(p.agents) <= p.minAgents {
		return fmt.Errorf("cannot stop agent: minimum agents (%d) required", p.minAgents)
	}

	agent.State = AgentStateStop
	close(agent.stopChan)

	delete(p.agents, agentID)

	return nil
}

func (p *AgentPool) Get(agentID string) *AgentStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if agent, ok := p.agents[agentID]; ok {
		return agent.AgentStatus
	}
	return nil
}

func (p *AgentPool) GetAll() map[string]*AgentStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make(map[string]*AgentStatus)
	for id, agent := range p.agents {
		result[id] = agent.AgentStatus
	}
	return result
}

func (p *AgentPool) GetIdleAgent(capability string) *ManagedAgent {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, agent := range p.agents {
		if agent.State == AgentStateIdle && agent.CurrentTasks < agent.MaxTasks {
			for _, cap := range agent.Capabilities {
				if cap == capability {
					return agent
				}
			}
		}
	}
	return nil
}

func (p *AgentPool) AssignTask(agentID string, task TaskAssignment) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	agent, ok := p.agents[agentID]
	if !ok {
		return fmt.Errorf("agent not found: %s", agentID)
	}

	if agent.CurrentTasks >= agent.MaxTasks {
		return fmt.Errorf("agent at max capacity: %s", agentID)
	}

	select {
	case agent.taskChan <- task:
		agent.CurrentTasks++
		agent.LastActivity = time.Now()
		if agent.CurrentTasks > 0 {
			agent.State = AgentStateBusy
		}
		return nil
	default:
		return fmt.Errorf("agent task queue full: %s", agentID)
	}
}

func (p *AgentPool) TaskComplete(agentID string, success bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	agent, ok := p.agents[agentID]
	if !ok {
		return
	}

	agent.CurrentTasks--
	agent.LastActivity = time.Now()

	if success {
		agent.TasksComplete++
	} else {
		agent.TasksFailed++
	}

	if agent.CurrentTasks <= 0 {
		agent.CurrentTasks = 0
		agent.State = AgentStateIdle
	}
}

func (p *AgentPool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.agents)
}

func (p *AgentPool) CountByState(state AgentState) int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var count int
	for _, agent := range p.agents {
		if agent.State == state {
			count++
		}
	}
	return count
}

func (p *AgentPool) CountIdle() int {
	return p.CountByState(AgentStateIdle)
}

func (p *AgentPool) CountBusy() int {
	return p.CountByState(AgentStateBusy)
}

func (p *AgentPool) GetIdleDuration(agentID string) time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()

	agent, ok := p.agents[agentID]
	if !ok {
		return 0
	}

	if agent.State == AgentStateIdle {
		return time.Since(agent.LastActivity)
	}
	return 0
}

func (p *AgentPool) GetOverIdleAgents(threshold time.Duration) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var result []string
	for id, agent := range p.agents {
		if agent.State == AgentStateIdle {
			if time.Since(agent.LastActivity) > threshold {
				result = append(result, id)
			}
		}
	}
	return result
}

func (a *ManagedAgent) TaskChannel() <-chan TaskAssignment {
	return a.taskChan
}

func (a *ManagedAgent) StopChannel() <-chan struct{} {
	return a.stopChan
}

func (a *ManagedAgent) MarkBusy() {
	a.State = AgentStateBusy
	a.LastActivity = time.Now()
}

func (a *ManagedAgent) MarkIdle() {
	a.State = AgentStateIdle
	a.LastActivity = time.Now()
}
