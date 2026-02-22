package tools

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/sipeed/picoclaw/pkg/proactive"
)

type ProactiveTool struct {
	wal       *proactive.WAL
	buffer    *proactive.Buffer
	workspace string
	channel   string
	chatID    string
	mu        sync.RWMutex
}

func NewProactiveTool(workspace string, restrict bool) *ProactiveTool {
	return &ProactiveTool{
		workspace: workspace,
		wal:       proactive.NewWAL(workspace),
		buffer:    proactive.NewBuffer(workspace),
	}
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

3. **Autonomous Scheduling** - Combine with 'cron' tool for self-scheduling tasks

Actions: wal_write, wal_complete, wal_fail, wal_read, wal_recover, wal_clear, buffer_append, buffer_read, buffer_clear, buffer_sections`
}

func (t *ProactiveTool) Parameters() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type":        "string",
				"enum":        []string{"wal_write", "wal_complete", "wal_fail", "wal_read", "wal_recover", "wal_clear", "buffer_append", "buffer_read", "buffer_clear", "buffer_sections"},
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
				"description": "Description or message (for wal_write, buffer_append)",
			},
			"output": map[string]any{
				"type":        "string",
				"description": "Result output (for wal_complete, wal_fail)",
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
