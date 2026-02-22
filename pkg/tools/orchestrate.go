package tools

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/orchestrator"
)

type OrchestrateTool struct {
	controller *orchestrator.Controller
	workspace  string
	agentID    string
	channel    string
	chatID     string
	mu         sync.RWMutex
}

func NewOrchestrateTool(controller *orchestrator.Controller, workspace, agentID string) *OrchestrateTool {
	return &OrchestrateTool{
		controller: controller,
		workspace:  workspace,
		agentID:    agentID,
	}
}

func NewOrchestrateToolSimple(workspace, agentID string) *OrchestrateTool {
	cfg := orchestrator.DefaultOrchestratorConfig()
	controller := orchestrator.NewController(cfg, workspace, agentID)

	return &OrchestrateTool{
		controller: controller,
		workspace:  workspace,
		agentID:    agentID,
	}
}

func (t *OrchestrateTool) Name() string {
	return "orchestrate"
}

func (t *OrchestrateTool) Description() string {
	return `Orchestration and auto-scaling control for multi-agent coordination. Use this tool when:

1. **Check system status** - View agents, metrics, and scaling status
2. **Scale agents** - Manually spawn or stop agents
3. **Delegate tasks** - Assign tasks to available agents
4. **Configure scaling** - Adjust auto-scaling parameters

Actions: status, scale_up, scale_down, delegate, metrics, agents, config`
}

func (t *OrchestrateTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type":        "string",
				"enum":        []string{"status", "scale_up", "scale_down", "delegate", "metrics", "agents", "config"},
				"description": "Action to perform",
			},
			"template": map[string]any{
				"type":        "string",
				"description": "Agent template name (for scale_up)",
			},
			"agent_id": map[string]any{
				"type":        "string",
				"description": "Agent ID (for scale_down, delegate)",
			},
			"task_id": map[string]any{
				"type":        "string",
				"description": "Task ID (for delegate)",
			},
			"task_action": map[string]any{
				"type":        "string",
				"description": "Task action/description (for delegate)",
			},
			"capabilities": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "Required capabilities (for delegate)",
			},
			"priority": map[string]any{
				"type":        "integer",
				"description": "Task priority (for delegate)",
			},
			"payload": map[string]any{
				"type":        "object",
				"description": "Task payload (for delegate)",
			},
		},
		"required": []string{"action"},
	}
}

func (t *OrchestrateTool) SetContext(channel, chatID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.channel = channel
	t.chatID = chatID
}

func (t *OrchestrateTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	action, ok := args["action"].(string)
	if !ok {
		return ErrorResult("action is required")
	}

	switch action {
	case "status":
		return t.status()
	case "scale_up":
		return t.scaleUp(args)
	case "scale_down":
		return t.scaleDown(args)
	case "delegate":
		return t.delegate(args)
	case "metrics":
		return t.metrics()
	case "agents":
		return t.agents()
	case "config":
		return t.config()
	default:
		return ErrorResult(fmt.Sprintf("unknown action: %s", action))
	}
}

func (t *OrchestrateTool) status() *ToolResult {
	status := t.controller.Status()
	return SilentResult(status.String())
}

func (t *OrchestrateTool) scaleUp(args map[string]any) *ToolResult {
	template, _ := args["template"].(string)
	if template == "" {
		template = "worker"
	}

	agent, err := t.controller.ScaleUp(template)
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to scale up: %v", err))
	}

	return SilentResult(fmt.Sprintf("Agent spawned: id=%s template=%s capabilities=%v",
		agent.ID, agent.Template, agent.Capabilities))
}

func (t *OrchestrateTool) scaleDown(args map[string]any) *ToolResult {
	agentID, _ := args["agent_id"].(string)
	if agentID == "" {
		return ErrorResult("agent_id is required for scale_down")
	}

	if err := t.controller.ScaleDown(agentID); err != nil {
		return ErrorResult(fmt.Sprintf("failed to scale down: %v", err))
	}

	return SilentResult(fmt.Sprintf("Agent stopped: %s", agentID))
}

func (t *OrchestrateTool) delegate(args map[string]any) *ToolResult {
	capabilitiesRaw, _ := args["capabilities"].([]any)
	capabilities := make([]string, 0, len(capabilitiesRaw))
	for _, c := range capabilitiesRaw {
		if s, ok := c.(string); ok {
			capabilities = append(capabilities, s)
		}
	}

	if len(capabilities) == 0 {
		capabilities = []string{"execute"}
	}

	taskID, _ := args["task_id"].(string)
	if taskID == "" {
		taskID = fmt.Sprintf("task-%d", time.Now().UnixNano())
	}

	taskAction, _ := args["task_action"].(string)
	if taskAction == "" {
		taskAction, _ = args["task_id"].(string)
	}

	priority, _ := args["priority"].(int)
	if priority == 0 {
		priority = 10
	}

	payload, _ := args["payload"].(map[string]any)
	if payload == nil {
		payload = make(map[string]any)
	}

	task := orchestrator.TaskAssignment{
		TaskID:   taskID,
		Action:   taskAction,
		Priority: priority,
		Payload:  payload,
	}

	agent, err := t.controller.DelegateTask(capabilities, task)
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to delegate task: %v", err))
	}

	return SilentResult(fmt.Sprintf("Task delegated: task_id=%s agent=%s capabilities=%v",
		taskID, agent.ID, capabilities))
}

func (t *OrchestrateTool) metrics() *ToolResult {
	m := t.controller.GetMetrics()

	var sb strings.Builder
	sb.WriteString("Orchestrator Metrics:\n\n")
	sb.WriteString(fmt.Sprintf("- Task Queue Depth: %d\n", m.TaskQueueDepth))
	sb.WriteString(fmt.Sprintf("- Total Agents: %d\n", m.TotalAgents))
	sb.WriteString(fmt.Sprintf("- Active Agents: %d\n", m.ActiveAgents))
	sb.WriteString(fmt.Sprintf("- Idle Agents: %d\n", m.IdleAgents))
	sb.WriteString(fmt.Sprintf("- Utilization: %.1f%%\n", m.UtilizationPercent))
	sb.WriteString(fmt.Sprintf("- Avg Task Duration: %v\n", m.AvgTaskDuration))
	sb.WriteString(fmt.Sprintf("- Tasks/min: %.1f\n", m.TaskCompletionRate))

	return SilentResult(sb.String())
}

func (t *OrchestrateTool) agents() *ToolResult {
	status := t.controller.Status()

	if len(status.Agents) == 0 {
		return SilentResult("No agents in pool")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Agents (%d total):\n\n", len(status.Agents)))

	for _, agent := range status.Agents {
		sb.WriteString(fmt.Sprintf("- %s [%s]: %s\n", agent.ID, agent.Template, agent.State))
		sb.WriteString(fmt.Sprintf("  Capabilities: %v\n", agent.Capabilities))
		sb.WriteString(fmt.Sprintf("  Tasks: %d/%d\n", agent.CurrentTasks, agent.MaxTasks))
		sb.WriteString(fmt.Sprintf("  Completed: %d, Failed: %d\n", agent.TasksComplete, agent.TasksFailed))
		sb.WriteString(fmt.Sprintf("  Last Activity: %s\n\n", agent.LastActivity.Format("2006-01-02 15:04:05")))
	}

	return SilentResult(sb.String())
}

func (t *OrchestrateTool) config() *ToolResult {
	status := t.controller.Status()
	cfg := status.CurrentConfig

	var sb strings.Builder
	sb.WriteString("Orchestrator Configuration:\n\n")
	sb.WriteString(fmt.Sprintf("- Enabled: %v\n", cfg.Enabled))
	sb.WriteString(fmt.Sprintf("- Auto-Scale: %v\n", cfg.AutoScale))
	sb.WriteString(fmt.Sprintf("- Min Agents: %d\n", cfg.MinAgents))
	sb.WriteString(fmt.Sprintf("- Max Agents: %d\n", cfg.MaxAgents))
	sb.WriteString(fmt.Sprintf("- Scale Up Threshold: %d tasks\n", cfg.ScaleUpThreshold))
	sb.WriteString(fmt.Sprintf("- Scale Down After: %v idle\n", cfg.ScaleDownAfter))
	sb.WriteString(fmt.Sprintf("- Check Interval: %v\n", cfg.CheckInterval))
	sb.WriteString(fmt.Sprintf("- Utilization High: %.1f%%\n", cfg.UtilizationHigh))
	sb.WriteString(fmt.Sprintf("- Utilization Low: %.1f%%\n", cfg.UtilizationLow))
	sb.WriteString(fmt.Sprintf("- Default Template: %s\n", cfg.DefaultTemplate))

	return SilentResult(sb.String())
}
