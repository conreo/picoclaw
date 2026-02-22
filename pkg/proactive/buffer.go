package proactive

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Buffer struct {
	path string
	mu   sync.RWMutex
}

func NewBuffer(workspace string) *Buffer {
	memoryDir := filepath.Join(workspace, "memory")
	os.MkdirAll(memoryDir, 0o755)

	return &Buffer{
		path: filepath.Join(memoryDir, "buffer.md"),
	}
}

func (b *Buffer) Append(section, content string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	var entry string

	if section != "" {
		entry = fmt.Sprintf("\n### %s\n**[%s]**\n%s\n", section, timestamp, content)
	} else {
		entry = fmt.Sprintf("\n**[%s]**\n%s\n", timestamp, content)
	}

	f, err := os.OpenFile(b.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.WriteString(entry)
	return err
}

func (b *Buffer) Read(section ...string) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	data, err := os.ReadFile(b.path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}

	content := string(data)

	if len(section) == 0 || section[0] == "" {
		return content, nil
	}

	return b.extractSection(content, section[0]), nil
}

func (b *Buffer) extractSection(content, section string) string {
	sectionHeader := "### " + section
	lines := strings.Split(content, "\n")
	var result []string
	inSection := false

	for _, line := range lines {
		if strings.HasPrefix(line, "### ") {
			if line == sectionHeader {
				inSection = true
				result = append(result, line)
				continue
			} else if inSection {
				break
			}
		}
		if inSection {
			result = append(result, line)
		}
	}

	return strings.Join(result, "\n")
}

func (b *Buffer) Clear(section ...string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(section) == 0 || section[0] == "" || section[0] == "all" {
		return os.WriteFile(b.path, []byte("# Working Buffer\n\n"), 0o644)
	}

	data, err := os.ReadFile(b.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	content := string(data)
	sectionHeader := "### " + section[0]
	lines := strings.Split(content, "\n")
	var result []string
	skipSection := false

	for _, line := range lines {
		if strings.HasPrefix(line, "### ") {
			if line == sectionHeader {
				skipSection = true
				continue
			}
			skipSection = false
		}
		if !skipSection {
			result = append(result, line)
		}
	}

	return os.WriteFile(b.path, []byte(strings.Join(result, "\n")), 0o644)
}

func (b *Buffer) ListSections() ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	data, err := os.ReadFile(b.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var sections []string
	lines := strings.Split(string(data), "\n")

	for _, line := range lines {
		if strings.HasPrefix(line, "### ") {
			section := strings.TrimPrefix(line, "### ")
			sections = append(sections, strings.TrimSpace(section))
		}
	}

	return sections, nil
}

func (b *Buffer) Exists() bool {
	_, err := os.Stat(b.path)
	return err == nil
}
