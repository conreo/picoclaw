package providers

import "testing"

func TestGetContextWindow(t *testing.T) {
	tests := []struct {
		model    string
		expected int
	}{
		{"glm-5", 202800},
		{"zhipu/glm-5", 202800},
		{"GLM-5", 202800},
		{"glm-4.7", 200000},
		{"glm-4.7-flashx", 202800},
		{"gpt-4o", 128000},
		{"openai/gpt-4o", 128000},
		{"claude-3-5-sonnet", 200000},
		{"anthropic/claude-3-5-sonnet", 200000},
		{"gemini-1.5-pro", 1000000},
		{"deepseek-v3", 64000},
		{"unknown-model", defaultContextWindow},
		{"", defaultContextWindow},
	}

	for _, tt := range tests {
		t.Run(tt.model, func(t *testing.T) {
			got := GetContextWindow(tt.model)
			if got != tt.expected {
				t.Errorf("GetContextWindow(%q) = %d, want %d", tt.model, got, tt.expected)
			}
		})
	}
}

func TestGetContextWindow_PartialMatch(t *testing.T) {
	tests := []struct {
		model   string
		minimum int
	}{
		{"glm-4-plus-2024", 128000},
		{"gpt-4o-2024-11-20", 128000},
		{"claude-sonnet-4-20250514", 200000},
		{"gemini-2.0-flash-exp", 1000000},
	}

	for _, tt := range tests {
		t.Run(tt.model, func(t *testing.T) {
			got := GetContextWindow(tt.model)
			if got < tt.minimum {
				t.Errorf("GetContextWindow(%q) = %d, want at least %d", tt.model, got, tt.minimum)
			}
		})
	}
}

func TestGetContextWindow_GLMVariants(t *testing.T) {
	models := []string{
		"glm-4-plus",
		"glm-4",
		"glm-4-air",
		"glm-4-airx",
		"glm-4-flash",
		"some-glm-variant",
	}

	for _, model := range models {
		t.Run(model, func(t *testing.T) {
			got := GetContextWindow(model)
			if got < 128000 {
				t.Errorf("GetContextWindow(%q) = %d, want at least 128000 for GLM model", model, got)
			}
		})
	}
}
