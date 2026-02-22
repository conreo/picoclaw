package tools

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sipeed/picoclaw/pkg/proactive"
)

func TestProactiveTool_NameAndDescription(t *testing.T) {
	tmpDir := t.TempDir()
	tool := NewProactiveTool(tmpDir, true)

	if tool.Name() != "proactive" {
		t.Errorf("expected name 'proactive', got '%s'", tool.Name())
	}

	if tool.Description() == "" {
		t.Error("description should not be empty")
	}
}

func TestProactiveTool_Parameters(t *testing.T) {
	tmpDir := t.TempDir()
	tool := NewProactiveTool(tmpDir, true)

	params := tool.Parameters()
	if params["type"] != "object" {
		t.Error("expected object type")
	}

	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatal("expected properties map")
	}

	if _, exists := props["action"]; !exists {
		t.Error("expected action property")
	}
}

func TestProactiveTool_WALWriteAndComplete(t *testing.T) {
	tmpDir := t.TempDir()
	tool := NewProactiveTool(tmpDir, true)
	ctx := context.Background()

	writeResult := tool.Execute(ctx, map[string]any{
		"action":  "wal_write",
		"type":    "monitor",
		"command": "df -h",
		"message": "Check disk space",
	})

	if writeResult.IsError {
		t.Errorf("wal_write failed: %s", writeResult.ForLLM)
	}

	entries := tool.wal.Read()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	entryID := entries[0].ID
	completeResult := tool.Execute(ctx, map[string]any{
		"action":   "wal_complete",
		"entry_id": entryID,
		"output":   "Disk usage: 50%",
	})

	if completeResult.IsError {
		t.Errorf("wal_complete failed: %s", completeResult.ForLLM)
	}

	updated, _ := tool.wal.Get(entryID)
	if updated.Status != proactive.EntryStatusCompleted {
		t.Errorf("expected status completed, got %s", updated.Status)
	}
}

func TestProactiveTool_WALFail(t *testing.T) {
	tmpDir := t.TempDir()
	tool := NewProactiveTool(tmpDir, true)
	ctx := context.Background()

	writeResult := tool.Execute(ctx, map[string]any{
		"action":  "wal_write",
		"type":    "heal",
		"command": "restart service",
	})

	if writeResult.IsError {
		t.Errorf("wal_write failed: %s", writeResult.ForLLM)
	}

	entries := tool.wal.Read()
	entryID := entries[0].ID

	failResult := tool.Execute(ctx, map[string]any{
		"action":   "wal_fail",
		"entry_id": entryID,
		"output":   "Service failed to restart",
		"recovery": "Manual intervention required",
	})

	if failResult.IsError {
		t.Errorf("wal_fail failed: %s", failResult.ForLLM)
	}

	updated, _ := tool.wal.Get(entryID)
	if updated.Status != proactive.EntryStatusFailed {
		t.Errorf("expected status failed, got %s", updated.Status)
	}
	if updated.Recovery != "Manual intervention required" {
		t.Errorf("expected recovery message, got %s", updated.Recovery)
	}
}

func TestProactiveTool_WALRecover(t *testing.T) {
	tmpDir := t.TempDir()
	tool := NewProactiveTool(tmpDir, true)
	ctx := context.Background()

	tool.Execute(ctx, map[string]any{
		"action":  "wal_write",
		"type":    "monitor",
		"command": "check1",
	})

	tool.Execute(ctx, map[string]any{
		"action":  "wal_write",
		"type":    "monitor",
		"command": "check2",
	})

	recoverResult := tool.Execute(ctx, map[string]any{
		"action": "wal_recover",
	})

	if recoverResult.IsError {
		t.Errorf("wal_recover failed: %s", recoverResult.ForLLM)
	}

	pending := tool.wal.Recover()
	if len(pending) != 2 {
		t.Errorf("expected 2 pending entries, got %d", len(pending))
	}
}

func TestProactiveTool_BufferAppendAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	tool := NewProactiveTool(tmpDir, true)
	ctx := context.Background()

	appendResult := tool.Execute(ctx, map[string]any{
		"action":  "buffer_append",
		"section": "investigation",
		"content": "Found error in log file",
	})

	if appendResult.IsError {
		t.Errorf("buffer_append failed: %s", appendResult.ForLLM)
	}

	readResult := tool.Execute(ctx, map[string]any{
		"action":  "buffer_read",
		"section": "investigation",
	})

	if readResult.IsError {
		t.Errorf("buffer_read failed: %s", readResult.ForLLM)
	}

	if readResult.ForLLM == "" {
		t.Error("expected buffer content")
	}
}

func TestProactiveTool_BufferClear(t *testing.T) {
	tmpDir := t.TempDir()
	tool := NewProactiveTool(tmpDir, true)
	ctx := context.Background()

	tool.Execute(ctx, map[string]any{
		"action":  "buffer_append",
		"section": "test",
		"content": "test content",
	})

	clearResult := tool.Execute(ctx, map[string]any{
		"action":  "buffer_clear",
		"section": "test",
	})

	if clearResult.IsError {
		t.Errorf("buffer_clear failed: %s", clearResult.ForLLM)
	}

	readResult := tool.Execute(ctx, map[string]any{
		"action":  "buffer_read",
		"section": "test",
	})

	if readResult.IsError {
		t.Errorf("buffer_read failed: %s", readResult.ForLLM)
	}
}

func TestProactiveTool_BufferSections(t *testing.T) {
	tmpDir := t.TempDir()
	tool := NewProactiveTool(tmpDir, true)
	ctx := context.Background()

	tool.Execute(ctx, map[string]any{
		"action":  "buffer_append",
		"section": "section1",
		"content": "content1",
	})

	tool.Execute(ctx, map[string]any{
		"action":  "buffer_append",
		"section": "section2",
		"content": "content2",
	})

	sectionsResult := tool.Execute(ctx, map[string]any{
		"action": "buffer_sections",
	})

	if sectionsResult.IsError {
		t.Errorf("buffer_sections failed: %s", sectionsResult.ForLLM)
	}

	sections, _ := tool.buffer.ListSections()
	if len(sections) != 2 {
		t.Errorf("expected 2 sections, got %d", len(sections))
	}
}

func TestProactiveTool_MissingAction(t *testing.T) {
	tmpDir := t.TempDir()
	tool := NewProactiveTool(tmpDir, true)
	ctx := context.Background()

	result := tool.Execute(ctx, map[string]any{})

	if !result.IsError {
		t.Error("expected error for missing action")
	}
}

func TestProactiveTool_InvalidAction(t *testing.T) {
	tmpDir := t.TempDir()
	tool := NewProactiveTool(tmpDir, true)
	ctx := context.Background()

	result := tool.Execute(ctx, map[string]any{
		"action": "invalid_action",
	})

	if !result.IsError {
		t.Error("expected error for invalid action")
	}
}

func TestWAL_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	memoryDir := filepath.Join(tmpDir, "memory")
	os.MkdirAll(memoryDir, 0o755)

	wal1 := proactive.NewWAL(tmpDir)
	wal1.Write(proactive.EntryTypeMonitor, "test command", "test message")

	wal2 := proactive.NewWAL(tmpDir)
	entries := wal2.Read()

	if len(entries) != 1 {
		t.Errorf("expected 1 entry after reload, got %d", len(entries))
	}

	if entries[0].Command != "test command" {
		t.Errorf("expected command 'test command', got '%s'", entries[0].Command)
	}
}

func TestBuffer_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	memoryDir := filepath.Join(tmpDir, "memory")
	os.MkdirAll(memoryDir, 0o755)

	buf1 := proactive.NewBuffer(tmpDir)
	buf1.Append("test", "test content")

	buf2 := proactive.NewBuffer(tmpDir)
	content, _ := buf2.Read("test")

	if content == "" {
		t.Error("expected content after reload")
	}
}
