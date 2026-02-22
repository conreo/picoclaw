package tools

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sipeed/picoclaw/pkg/proactive"
)

type ProactiveTool struct {
	wal       *proactive.WAL
	sharedWAL *proactive.SharedWAL
	buffer    *proactive.Buffer
	workspace string
	agentID   string
	channel   string
	chatID    string
	mu        sync.RWMutex
}

func NewProactiveTool(workspace string, restrict bool) *ProactiveTool {
	return &ProactiveTool{
		workspace: workspace,
		agentID:   "main",
		wal:       proactive.NewWAL(workspace),
		sharedWAL: proactive.NewSharedWAL(workspace, "main"),
		buffer:    proactive.NewBuffer(workspace),
	}
}

func NewProactiveToolWithAgent(workspace, agentID string, restrict bool) *ProactiveTool {
	return &ProactiveTool{
		workspace: workspace,
		agentID:   agentID,
		wal:       proactive.NewWAL(workspace),
		sharedWAL: proactive.NewSharedWAL(workspace, agentID),
		buffer:    proactive.NewBuffer(workspace),
	}
}

func (t *ProactiveTool) SetAgentID(agentID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.agentID = agentID
	t.sharedWAL = proactive.NewSharedWAL(t.workspace, agentID)
}

func (t *ProactiveTool) Name() string {
	return "proactive"
}

func (t *ProactiveTool) Description() string {
	return `Proactive agent capabilities for autonomous operations. Use this tool when:

1. **WAL Operations** - Before executing autonomous actions, write to WAL for crash recovery:
   - 'wal_write' to log an action before execution
   - 'wal_complete' after successful execution
   - 'wal_fail' if action fails (with recovery steps)
   - 'wal_recover' on startup to check pending actions

2. **Buffer Operations** - For multi-step tasks spanning interactions:
   - 'buffer_append' to store intermediate results
   - 'buffer_read' to retrieve stored content
   - 'buffer_clear' when task completes

3. **Shared WAL Operations** - Multi-agent coordination:
   - 'shared_assign' to assign a task to another agent
   - 'shared_claim' to claim an available task
   - 'shared_complete' to mark a claimed task complete
   - 'shared_fail' to mark a claimed task failed
   - 'shared_broadcast' to send a message to all agents
   - 'shared_read' to read coordination entries
   - 'shared_heartbeat' to signal agent is alive
   - 'shared_agents' to list active agents

4. **Autonomous Scheduling** - Combine with 'cron' tool for self-scheduling tasks

Actions: wal_write, wal_complete, wal_fail, wal_read, wal_recover, wal_clear, buffer_append, buffer_read, buffer_clear, buffer_sections, shared_assign, shared_claim, shared_complete, shared_fail, shared_broadcast, shared_read, shared_heartbeat, shared_agents, shared_clear`
}

func (t *ProactiveTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type":        "string",
				"enum":        []string{"wal_write", "wal_complete", "wal_fail", "wal_read", "wal_recover", "wal_clear", "buffer_append", "buffer_read", "buffer_clear", "buffer_sections", "shared_assign", "shared_claim", "shared_complete", "shared_fail", "shared_broadcast", "shared_read", "shared_heartbeat", "shared_agents", "shared_clear", "shared_my_tasks"},
				"description": "Action to perform",
			},
			"type": map[string]any{
				"type":        "string",
				"enum":        []string{"monitor", "heal", "schedule", "investigate"},
				"description": "Entry type for wal_write: monitor (system checks), heal (self-healing), schedule (scheduled tasks), investigate (multi-step investigations)",
			},
			"entry_id": map[string]any{
				"type":        "string",
				"description": "WAL entry ID (for wal_complete, wal_fail)",
			},
			"command": map[string]any{
				"type":        "string",
				"description": "Command or action being executed (for wal_write)",
			},
			"message": map[string]any{
				"type":        "string",
				"description": "Description or message (for wal_write, buffer_append, shared_broadcast)",
			},
			"output": map[string]any{
				"type":        "string",
				"description": "Result output (for wal_complete, wal_fail, shared_complete)",
			},
			"recovery": map[string]any{
				"type":        "string",
				"description": "Recovery steps if action failed (for wal_fail)",
			},
			"status": map[string]any{
				"type":        "string",
				"enum":        []string{"pending", "completed", "failed"},
				"description": "Filter by status (for wal_read)",
			},
			"section": map[string]any{
				"type":        "string",
				"description": "Buffer section name (for buffer_append, buffer_read, buffer_clear)",
			},
			"content": map[string]any{
				"type":        "string",
				"description": "Content to append (for buffer_append)",
			},
			"target_agent": map[string]any{
				"type":        "string",
				"description": "Target agent ID (for shared_assign)",
			},
			"task_action": map[string]any{
				"type":        "string",
				"description": "Action/task description (for shared_assign)",
			},
			"task_id": map[string]any{
				"type":        "string",
				"description": "Task ID (for shared_claim, shared_complete, shared_fail)",
			},
			"expires_seconds": map[string]any{
				"type":        "integer",
				"description": "Task expiration in seconds (for shared_assign)",
			},
			"payload": map[string]any{
				"type":        "object",
				"description": "Additional payload data (for shared_assign)",
			},
		},
		"required": []string{"action"},
	}
}

func (t *ProactiveTool) SetContext(channel, chatID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.channel = channel
	t.chatID = chatID
}

func (t *ProactiveTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	action, ok := args["action"].(string)
	if !ok {
		return ErrorResult("action is required")
	}

	switch action {
	case "wal_write":
		return t.walWrite(args)
	case "wal_complete":
		return t.walComplete(args)
	case "wal_fail":
		return t.walFail(args)
	case "wal_read":
		return t.walRead(args)
	case "wal_recover":
		return t.walRecover()
	case "wal_clear":
		return t.walClear(args)
	case "buffer_append":
		return t.bufferAppend(args)
	case "buffer_read":
		return t.bufferRead(args)
	case "buffer_clear":
		return t.bufferClear(args)
	case "buffer_sections":
		return t.bufferSections()
	case "shared_assign":
		return t.sharedAssign(args)
	case "shared_claim":
		return t.sharedClaim(args)
	case "shared_complete":
		return t.sharedComplete(args)
	case "shared_fail":
		return t.sharedFail(args)
	case "shared_broadcast":
		return t.sharedBroadcast(args)
	case "shared_read":
		return t.sharedRead(args)
	case "shared_heartbeat":
		return t.sharedHeartbeat()
	case "shared_agents":
		return t.sharedAgents()
	case "shared_clear":
		return t.sharedClear(args)
	case "shared_my_tasks":
		return t.sharedMyTasks()
	default:
		return ErrorResult(fmt.Sprintf("unknown action: %s", action))
	}
}

func (t *ProactiveTool) walWrite(args map[string]any) *ToolResult {
	entryTypeStr, _ := args["type"].(string)
	if entryTypeStr == "" {
		entryTypeStr = "monitor"
	}
	entryType := proactive.EntryType(entryTypeStr)

	command, _ := args["command"].(string)
	message, _ := args["message"].(string)

	if command == "" && message == "" {
		return ErrorResult("command or message is required for wal_write")
	}

	entry, err := t.wal.Write(entryType, command, message)
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to write WAL entry: %v", err))
	}

	return SilentResult(fmt.Sprintf("WAL entry created: id=%s type=%s status=pending", entry.ID, entry.Type))
}

func (t *ProactiveTool) walComplete(args map[string]any) *ToolResult {
	entryID, _ := args["entry_id"].(string)
	if entryID == "" {
		return ErrorResult("entry_id is required for wal_complete")
	}

	output, _ := args["output"].(string)

	if err := t.wal.Complete(entryID, output); err != nil {
		return ErrorResult(fmt.Sprintf("failed to complete WAL entry: %v", err))
	}

	return SilentResult(fmt.Sprintf("WAL entry completed: id=%s", entryID))
}

func (t *ProactiveTool) walFail(args map[string]any) *ToolResult {
	entryID, _ := args["entry_id"].(string)
	if entryID == "" {
		return ErrorResult("entry_id is required for wal_fail")
	}

	output, _ := args["output"].(string)
	recovery, _ := args["recovery"].(string)

	if err := t.wal.Fail(entryID, output, recovery); err != nil {
		return ErrorResult(fmt.Sprintf("failed to mark WAL entry as failed: %v", err))
	}

	return SilentResult(fmt.Sprintf("WAL entry marked failed: id=%s", entryID))
}

func (t *ProactiveTool) walRead(args map[string]any) *ToolResult {
	statusStr, _ := args["status"].(string)

	var entries []proactive.WALEntry
	if statusStr != "" {
		entries = t.wal.Read(proactive.EntryStatus(statusStr))
	} else {
		entries = t.wal.Read()
	}

	if len(entries) == 0 {
		return SilentResult("No WAL entries found")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("WAL entries (%d total):\n\n", len(entries)))
	for _, e := range entries {
		sb.WriteString(fmt.Sprintf("- [%s] %s | %s | %s\n", e.ID, e.Timestamp.Format("2006-01-02 15:04"), e.Type, e.Status))
		if e.Command != "" {
			sb.WriteString(fmt.Sprintf("  Command: %s\n", e.Command))
		}
		if e.Message != "" {
			sb.WriteString(fmt.Sprintf("  Message: %s\n", e.Message))
		}
	}

	return SilentResult(sb.String())
}

func (t *ProactiveTool) walRecover() *ToolResult {
	entries := t.wal.Recover()

	if len(entries) == 0 {
		return SilentResult("No pending WAL entries to recover")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Pending WAL entries for recovery (%d total):\n\n", len(entries)))
	for _, e := range entries {
		sb.WriteString(fmt.Sprintf("- [%s] %s | %s\n", e.ID, e.Type, e.Timestamp.Format("2006-01-02 15:04")))
		if e.Command != "" {
			sb.WriteString(fmt.Sprintf("  Command: %s\n", e.Command))
		}
		if e.Message != "" {
			sb.WriteString(fmt.Sprintf("  Message: %s\n", e.Message))
		}
		sb.WriteString("\n")
	}

	return SilentResult(sb.String())
}

func (t *ProactiveTool) walClear(args map[string]any) *ToolResult {
	completedOnly := true
	if v, ok := args["all"].(bool); ok && v {
		completedOnly = false
	}

	if err := t.wal.Clear(completedOnly); err != nil {
		return ErrorResult(fmt.Sprintf("failed to clear WAL: %v", err))
	}

	if completedOnly {
		return SilentResult("WAL cleared (completed entries only)")
	}
	return SilentResult("WAL cleared (all entries)")
}

func (t *ProactiveTool) bufferAppend(args map[string]any) *ToolResult {
	content, _ := args["content"].(string)
	if content == "" {
		content, _ = args["message"].(string)
	}
	if content == "" {
		return ErrorResult("content is required for buffer_append")
	}

	section, _ := args["section"].(string)

	if err := t.buffer.Append(section, content); err != nil {
		return ErrorResult(fmt.Sprintf("failed to append to buffer: %v", err))
	}

	if section != "" {
		return SilentResult(fmt.Sprintf("Content appended to buffer section: %s", section))
	}
	return SilentResult("Content appended to buffer")
}

func (t *ProactiveTool) bufferRead(args map[string]any) *ToolResult {
	section, _ := args["section"].(string)

	content, err := t.buffer.Read(section)
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to read buffer: %v", err))
	}

	if content == "" {
		if section != "" {
			return SilentResult(fmt.Sprintf("Buffer section '%s' is empty or not found", section))
		}
		return SilentResult("Buffer is empty")
	}

	return SilentResult(content)
}

func (t *ProactiveTool) bufferClear(args map[string]any) *ToolResult {
	section, _ := args["section"].(string)

	if err := t.buffer.Clear(section); err != nil {
		return ErrorResult(fmt.Sprintf("failed to clear buffer: %v", err))
	}

	if section != "" {
		return SilentResult(fmt.Sprintf("Buffer section cleared: %s", section))
	}
	return SilentResult("Buffer cleared")
}

func (t *ProactiveTool) bufferSections() *ToolResult {
	sections, err := t.buffer.ListSections()
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to list buffer sections: %v", err))
	}

	if len(sections) == 0 {
		return SilentResult("No buffer sections found")
	}

	return SilentResult(fmt.Sprintf("Buffer sections: %s", strings.Join(sections, ", ")))
}

func (t *ProactiveTool) sharedAssign(args map[string]any) *ToolResult {
	targetAgent, _ := args["target_agent"].(string)
	if targetAgent == "" {
		return ErrorResult("target_agent is required for shared_assign")
	}

	taskAction, _ := args["task_action"].(string)
	if taskAction == "" {
		taskAction, _ = args["action"].(string)
	}
	if taskAction == "" {
		return ErrorResult("task_action is required for shared_assign")
	}

	payload, _ := args["payload"].(map[string]any)
	if payload == nil {
		payload = make(map[string]any)
	}

	var expiresIn time.Duration
	if sec, ok := args["expires_seconds"].(float64); ok && sec > 0 {
		expiresIn = time.Duration(sec) * time.Second
	}

	entry, err := t.sharedWAL.AssignTask(targetAgent, taskAction, payload, expiresIn)
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to assign task: %v", err))
	}

	return SilentResult(fmt.Sprintf("Task assigned to agent '%s': task_id=%s action=%s",
		targetAgent, entry.TaskID, taskAction))
}

func (t *ProactiveTool) sharedClaim(args map[string]any) *ToolResult {
	taskID, _ := args["task_id"].(string)
	if taskID == "" {
		return ErrorResult("task_id is required for shared_claim")
	}

	entry, err := t.sharedWAL.ClaimTask(taskID)
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to claim task: %v", err))
	}

	return SilentResult(fmt.Sprintf("Task claimed: task_id=%s action=%s", entry.TaskID, entry.Action))
}

func (t *ProactiveTool) sharedComplete(args map[string]any) *ToolResult {
	taskID, _ := args["task_id"].(string)
	if taskID == "" {
		return ErrorResult("task_id is required for shared_complete")
	}

	output, _ := args["output"].(string)

	if err := t.sharedWAL.CompleteTask(taskID, output); err != nil {
		return ErrorResult(fmt.Sprintf("failed to complete task: %v", err))
	}

	return SilentResult(fmt.Sprintf("Task completed: task_id=%s", taskID))
}

func (t *ProactiveTool) sharedFail(args map[string]any) *ToolResult {
	taskID, _ := args["task_id"].(string)
	if taskID == "" {
		return ErrorResult("task_id is required for shared_fail")
	}

	reason, _ := args["output"].(string)
	if reason == "" {
		reason, _ = args["message"].(string)
	}

	if err := t.sharedWAL.FailTask(taskID, reason); err != nil {
		return ErrorResult(fmt.Sprintf("failed to mark task as failed: %v", err))
	}

	return SilentResult(fmt.Sprintf("Task failed: task_id=%s reason=%s", taskID, reason))
}

func (t *ProactiveTool) sharedBroadcast(args map[string]any) *ToolResult {
	message, _ := args["message"].(string)
	if message == "" {
		return ErrorResult("message is required for shared_broadcast")
	}

	entry, err := t.sharedWAL.Broadcast(message)
	if err != nil {
		return ErrorResult(fmt.Sprintf("failed to broadcast: %v", err))
	}

	return SilentResult(fmt.Sprintf("Broadcast sent: id=%s message=%s", entry.ID, message))
}

func (t *ProactiveTool) sharedRead(args map[string]any) *ToolResult {
	entries := t.sharedWAL.Read()

	if len(entries) == 0 {
		return SilentResult("No shared WAL entries found")
	}

	return SilentResult(t.sharedWAL.FormatEntries(entries))
}

func (t *ProactiveTool) sharedHeartbeat() *ToolResult {
	if err := t.sharedWAL.Heartbeat(); err != nil {
		return ErrorResult(fmt.Sprintf("failed to send heartbeat: %v", err))
	}

	return SilentResult(fmt.Sprintf("Heartbeat sent from agent: %s", t.agentID))
}

func (t *ProactiveTool) sharedAgents() *ToolResult {
	agents := t.sharedWAL.GetActiveAgents()

	if len(agents) == 0 {
		return SilentResult("No active agents found")
	}

	return SilentResult(fmt.Sprintf("Active agents: %s", strings.Join(agents, ", ")))
}

func (t *ProactiveTool) sharedClear(args map[string]any) *ToolResult {
	completedOnly := true
	if v, ok := args["all"].(bool); ok && v {
		completedOnly = false
	}

	if err := t.sharedWAL.Clear(completedOnly); err != nil {
		return ErrorResult(fmt.Sprintf("failed to clear shared WAL: %v", err))
	}

	if completedOnly {
		return SilentResult("Shared WAL cleared (completed entries only)")
	}
	return SilentResult("Shared WAL cleared (all entries)")
}

func (t *ProactiveTool) sharedMyTasks() *ToolResult {
	entries := t.sharedWAL.GetMyTasks()

	if len(entries) == 0 {
		return SilentResult(fmt.Sprintf("No pending tasks for agent: %s", t.agentID))
	}

	return SilentResult(t.sharedWAL.FormatEntries(entries))
}
