package orchestrator

import (
	"sync"
	"time"
)

type AgentTemplate struct {
	Name               string        `json:"name"`
	Capabilities       []string      `json:"capabilities"`
	MaxConcurrentTasks int           `json:"max_concurrent_tasks"`
	IdleTimeout        time.Duration `json:"idle_timeout"`
	Workspace          string        `json:"workspace,omitempty"`
	Priority           int           `json:"priority"`
}

var DefaultTemplates = map[string]AgentTemplate{
	"worker": {
		Name:               "worker",
		Capabilities:       []string{"execute", "write", "read", "edit", "list_dir"},
		MaxConcurrentTasks: 3,
		IdleTimeout:        5 * time.Minute,
		Priority:           10,
	},
	"builder": {
		Name:               "builder",
		Capabilities:       []string{"execute", "build", "compile", "test"},
		MaxConcurrentTasks: 1,
		IdleTimeout:        10 * time.Minute,
		Priority:           20,
	},
	"monitor": {
		Name:               "monitor",
		Capabilities:       []string{"monitor", "alert", "health_check", "read"},
		MaxConcurrentTasks: 10,
		IdleTimeout:        30 * time.Minute,
		Priority:           5,
	},
	"healer": {
		Name:               "healer",
		Capabilities:       []string{"execute", "restart", "cleanup", "heal"},
		MaxConcurrentTasks: 2,
		IdleTimeout:        15 * time.Minute,
		Priority:           30,
	},
}

type TemplateRegistry struct {
	templates map[string]AgentTemplate
	mu        sync.RWMutex
}

func NewTemplateRegistry() *TemplateRegistry {
	reg := &TemplateRegistry{
		templates: make(map[string]AgentTemplate),
	}

	for name, tmpl := range DefaultTemplates {
		reg.templates[name] = tmpl
	}

	return reg
}

func (r *TemplateRegistry) Get(name string) (AgentTemplate, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tmpl, ok := r.templates[name]
	return tmpl, ok
}

func (r *TemplateRegistry) Register(tmpl AgentTemplate) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.templates[tmpl.Name] = tmpl
}

func (r *TemplateRegistry) List() []AgentTemplate {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]AgentTemplate, 0, len(r.templates))
	for _, tmpl := range r.templates {
		result = append(result, tmpl)
	}
	return result
}

func (r *TemplateRegistry) FindByCapability(capability string) []AgentTemplate {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []AgentTemplate
	for _, tmpl := range r.templates {
		for _, cap := range tmpl.Capabilities {
			if cap == capability {
				result = append(result, tmpl)
				break
			}
		}
	}
	return result
}

func (r *TemplateRegistry) BestForTask(requiredCapabilities []string) AgentTemplate {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var best AgentTemplate
	bestScore := -1

	for _, tmpl := range r.templates {
		score := r.scoreTemplate(tmpl, requiredCapabilities)
		if score > bestScore {
			bestScore = score
			best = tmpl
		}
	}

	return best
}

func (r *TemplateRegistry) scoreTemplate(tmpl AgentTemplate, requiredCapabilities []string) int {
	score := 0
	capSet := make(map[string]bool)
	for _, cap := range tmpl.Capabilities {
		capSet[cap] = true
	}

	for _, req := range requiredCapabilities {
		if capSet[req] {
			score += 10
		} else {
			score -= 5
		}
	}

	score += tmpl.Priority

	return score
}

func (r *TemplateRegistry) LoadFromConfig(templates map[string]AgentTemplate) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for name, tmpl := range templates {
		if tmpl.IdleTimeout == 0 {
			tmpl.IdleTimeout = 5 * time.Minute
		}
		if tmpl.MaxConcurrentTasks == 0 {
			tmpl.MaxConcurrentTasks = 1
		}
		r.templates[name] = tmpl
	}
}
