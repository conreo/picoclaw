package tools

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/messaging"
)

type AgentMessageTool struct {
	eventBus  *messaging.EventBus
	workspace string
	agentID   string
	channel   string
	chatID    string
	mu        sync.RWMutex
}

func NewAgentMessageTool(workspace, agentID string) *AgentMessageTool {
	return &AgentMessageTool{
		workspace: workspace,
		agentID:   agentID,
		eventBus:  messaging.NewEventBus(workspace),
	}
}

func (t *AgentMessageTool) Name() string {
	return "agent_message"
}

func (t *AgentMessageTool) Description() string {
	return `Cross-agent messaging for multi-agent coordination. Use this tool when:

1. **Send messages to other agents** - Notify specific agents or broadcast to all
2. **Read messages** - Get pending messages for current agent
3. **Agent events** - Signal agent state (busy, idle, etc.)

Actions: send, broadcast, read, read_new, mark_read, stats, clear`
}

func (t *AgentMessageTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type":        "string",
				"enum":        []string{"send", "broadcast", "read", "read_new", "mark_read", "stats", "clear"},
				"description": "Action to perform",
			},
			"target_agent": map[string]any{
				"type":        "string",
				"description": "Target agent ID (for send)",
			},
			"event_type": map[string]any{
				"type":        "string",
				"enum":        []string{"agent:busy", "agent:idle", "task:assigned", "task:complete", "task:failed", "alert:broadcast"},
				"description": "Event type (for send/broadcast)",
			},
			"message": map[string]any{
				"type":        "string",
				"description": "Message content",
			},
			"priority": map[string]any{
				"type":        "string",
				"enum":        []string{"critical", "high", "normal", "low"},
				"description": "Message priority (default: normal)",
			},
			"event_id": map[string]any{
				"type":        "string",
				"description": "Event ID (for mark_read)",
			},
			"limit": map[string]any{
				"type":        "integer",
				"description": "Max events to read (default: 10)",
			},
			"older_than_hours": map[string]any{
				"type":        "integer",
				"description": "Clear events older than X hours (for clear)",
			},
		},
		"required": []string{"action"},
	}
}

func (t *AgentMessageTool) SetContext(channel, chatID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.channel = channel
	t.chatID = chatID
}

func (t *AgentMessageTool) SetAgentID(agentID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.agentID = agentID
}

func (t *AgentMessageTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	action, ok := args["action"].(string)
	if !ok {
		return ErrorResult("action is required")
	}

	switch action {
	case "send":
		return t.send(args)
	case "broadcast":
		return t.broadcast(args)
	case "read":
		return t.read(args)
	case "read_new":
		return t.readNew()
	case "mark_read":
		return t.markRead(args)
	case "stats":
		return t.stats()
	case "clear":
		return t.clear(args)
	default:
		return ErrorResult(fmt.Sprintf("unknown action: %s", action))
	}
}

func (t *AgentMessageTool) send(args map[string]any) *ToolResult {
	targetAgent, _ := args["target_agent"].(string)
	if targetAgent == "" {
		return ErrorResult("target_agent is required for send")
	}

	message, _ := args["message"].(string)
	if message == "" {
		return ErrorResult("message is required for send")
	}

	eventTypeStr, _ := args["event_type"].(string)
	eventType := messaging.EventType(eventTypeStr)
	if eventType == "" {
		eventType = messaging.EventTaskAssigned
	}

	priority := t.getPriority(args)

	event := messaging.NewEvent(eventType, t.agentID).
		WithTarget(targetAgent).
		WithMessage(message).
		WithPriority(priority)

	if err := t.eventBus.Publish(event); err != nil {
		return ErrorResult(fmt.Sprintf("failed to send message: %v", err))
	}

	return SilentResult(fmt.Sprintf("Message sent to agent '%s': %s", targetAgent, message))
}

func (t *AgentMessageTool) broadcast(args map[string]any) *ToolResult {
	message, _ := args["message"].(string)
	if message == "" {
		return ErrorResult("message is required for broadcast")
	}

	priority := t.getPriority(args)

	if err := t.eventBus.Broadcast(t.agentID, message, priority); err != nil {
		return ErrorResult(fmt.Sprintf("failed to broadcast: %v", err))
	}

	return SilentResult(fmt.Sprintf("Broadcast sent to all agents: %s", message))
}

func (t *AgentMessageTool) read(args map[string]any) *ToolResult {
	limit := 10
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	events := t.eventBus.Read(t.agentID, limit)

	if len(events) == 0 {
		return SilentResult("No messages found")
	}

	return SilentResult(t.eventBus.FormatEvents(events))
}

func (t *AgentMessageTool) readNew() *ToolResult {
	events := t.eventBus.ReadUnprocessed(t.agentID)

	if len(events) == 0 {
		return SilentResult("No new messages")
	}

	return SilentResult(t.eventBus.FormatEvents(events))
}

func (t *AgentMessageTool) markRead(args map[string]any) *ToolResult {
	eventID, _ := args["event_id"].(string)
	if eventID == "" {
		return ErrorResult("event_id is required for mark_read")
	}

	if err := t.eventBus.MarkProcessed(eventID); err != nil {
		return ErrorResult(fmt.Sprintf("failed to mark event as read: %v", err))
	}

	return SilentResult(fmt.Sprintf("Event marked as read: %s", eventID))
}

func (t *AgentMessageTool) stats() *ToolResult {
	stats := t.eventBus.GetStats()

	var sb strings.Builder
	sb.WriteString("Event Bus Statistics:\n\n")
	sb.WriteString(fmt.Sprintf("- Total events: %v\n", stats["total"]))
	sb.WriteString(fmt.Sprintf("- Unprocessed: %v\n", stats["unprocessed"]))
	sb.WriteString(fmt.Sprintf("- Max capacity: %v\n", stats["max_events"]))

	if byType, ok := stats["by_type"].(map[messaging.EventType]int); ok && len(byType) > 0 {
		sb.WriteString("\nBy type:\n")
		for t, c := range byType {
			sb.WriteString(fmt.Sprintf("  - %s: %d\n", t, c))
		}
	}

	if byPriority, ok := stats["by_priority"].(map[messaging.Priority]int); ok && len(byPriority) > 0 {
		sb.WriteString("\nBy priority:\n")
		for p, c := range byPriority {
			sb.WriteString(fmt.Sprintf("  - %s: %d\n", p, c))
		}
	}

	return SilentResult(sb.String())
}

func (t *AgentMessageTool) clear(args map[string]any) *ToolResult {
	var olderThan time.Duration
	if h, ok := args["older_than_hours"].(float64); ok && h > 0 {
		olderThan = time.Duration(h) * time.Hour
	}

	if err := t.eventBus.Clear(olderThan); err != nil {
		return ErrorResult(fmt.Sprintf("failed to clear events: %v", err))
	}

	if olderThan > 0 {
		return SilentResult(fmt.Sprintf("Cleared events older than %v", olderThan))
	}
	return SilentResult("Cleared all events")
}

func (t *AgentMessageTool) getPriority(args map[string]any) messaging.Priority {
	p, ok := args["priority"].(string)
	if !ok {
		return messaging.PriorityNormal
	}
	return messaging.Priority(p)
}
