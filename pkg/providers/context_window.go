package providers

import "strings"

var modelContextWindows = map[string]int{
	"glm-5":          202800,
	"glm-4.7":        200000,
	"glm-4.7-flashx": 202800,
	"glm-4-plus":     128000,
	"glm-4":          128000,
	"glm-4-air":      128000,
	"glm-4-airx":     128000,
	"glm-4-flash":    128000,

	"gpt-4o":            128000,
	"gpt-4o-mini":       128000,
	"gpt-4-turbo":       128000,
	"gpt-4":             8192,
	"gpt-4-32k":         32768,
	"gpt-3.5-turbo":     16385,
	"gpt-3.5-turbo-16k": 16385,
	"o1":                200000,
	"o1-mini":           128000,
	"o1-preview":        128000,
	"o3-mini":           200000,

	"claude-3-5-sonnet": 200000,
	"claude-3-5-haiku":  200000,
	"claude-3-opus":     200000,
	"claude-3-sonnet":   200000,
	"claude-3-haiku":    200000,
	"claude-sonnet-4":   200000,
	"claude-sonnet-4-5": 200000,
	"claude-4-sonnet":   200000,

	"gemini-1.5-pro":   1000000,
	"gemini-1.5-flash": 1000000,
	"gemini-2.0-flash": 1000000,
	"gemini-pro":       32760,

	"deepseek-chat":  64000,
	"deepseek-coder": 64000,
	"deepseek-v3":    64000,
	"deepseek-r1":    64000,

	"llama-3.1-405b": 128000,
	"llama-3.1-70b":  128000,
	"llama-3.1-8b":   128000,
	"llama-3-70b":    8192,
	"llama-3-8b":     8192,

	"qwen-2.5-72b": 131072,
	"qwen-2.5-32b": 131072,
	"qwen-2.5-14b": 131072,
	"qwen-2.5-7b":  131072,
	"qwen-max":     32768,
	"qwen-plus":    32768,
	"qwen-turbo":   8192,
}

const defaultContextWindow = 32000

func GetContextWindow(model string) int {
	model = strings.ToLower(strings.TrimSpace(model))

	if idx := strings.Index(model, "/"); idx != -1 {
		model = model[idx+1:]
	}

	for pattern, ctx := range modelContextWindows {
		if strings.Contains(model, pattern) || strings.Contains(pattern, model) {
			return ctx
		}
	}

	if strings.Contains(model, "glm") {
		return 128000
	}
	if strings.Contains(model, "gpt-4") || strings.Contains(model, "gpt-4o") {
		return 128000
	}
	if strings.Contains(model, "claude") {
		return 200000
	}
	if strings.Contains(model, "gemini") {
		return 1000000
	}
	if strings.Contains(model, "deepseek") {
		return 64000
	}
	if strings.Contains(model, "llama") || strings.Contains(model, "llm") {
		return 128000
	}
	if strings.Contains(model, "qwen") {
		return 131072
	}

	return defaultContextWindow
}
