package llm

import (
	"fmt"
	"sync"
	"time"
)

type ModelConfig struct {
	Name             string `json:"name"`
	Model            string `json:"model"`
	MaxConcurrentAPI int    `json:"max_concurrent_api"`
	MaxTokensPerMin  int    `json:"max_tokens_per_min,omitempty"`
	RequestTimeout   string `json:"request_timeout,omitempty"`
}

type Pool struct {
	models    map[string]*ModelConfig
	semaphore map[string]chan struct{}
	mu        sync.RWMutex
}

func NewPool(models []ModelConfig) *Pool {
	p := &Pool{
		models:    make(map[string]*ModelConfig),
		semaphore: make(map[string]chan struct{}),
	}

	for i := range models {
		m := models[i]
		p.models[m.Name] = &m
		p.semaphore[m.Name] = make(chan struct{}, m.MaxConcurrentAPI)
	}

	return p
}

func (p *Pool) Get(name string) (*ModelConfig, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	m, ok := p.models[name]
	if !ok {
		return nil, fmt.Errorf("model not found: %s", name)
	}
	return m, nil
}

func (p *Pool) List() []ModelConfig {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]ModelConfig, 0, len(p.models))
	for _, m := range p.models {
		result = append(result, *m)
	}
	return result
}

func (p *Pool) Acquire(name string, timeout time.Duration) error {
	p.mu.RLock()
	sem, ok := p.semaphore[name]
	p.mu.RUnlock()

	if !ok {
		return fmt.Errorf("model not found: %s", name)
	}

	select {
	case sem <- struct{}{}:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout acquiring connection for model: %s", name)
	}
}

func (p *Pool) Release(name string) {
	p.mu.RLock()
	sem, ok := p.semaphore[name]
	p.mu.RUnlock()

	if !ok {
		return
	}

	select {
	case <-sem:
	default:
	}
}

func (p *Pool) Available(name string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	sem, ok := p.semaphore[name]
	if !ok {
		return 0
	}

	return cap(sem) - len(sem)
}

func (p *Pool) TotalConcurrent() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	total := 0
	for _, m := range p.models {
		total += m.MaxConcurrentAPI
	}
	return total
}

func (p *Pool) BestForComplexity(complexity string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	switch complexity {
	case "low", "simple":
		if m, ok := p.models["fast"]; ok {
			return m.Name
		}
	case "medium", "moderate":
		if m, ok := p.models["balanced"]; ok {
			return m.Name
		}
	case "high", "complex":
		if m, ok := p.models["smart"]; ok {
			return m.Name
		}
	}

	for name := range p.models {
		return name
	}
	return ""
}

type Stats struct {
	ModelName string `json:"model_name"`
	Max       int    `json:"max"`
	Available int    `json:"available"`
	InUse     int    `json:"in_use"`
}

func (p *Pool) GetStats() []Stats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var stats []Stats
	for name, m := range p.models {
		sem := p.semaphore[name]
		inUse := len(sem)
		stats = append(stats, Stats{
			ModelName: name,
			Max:       m.MaxConcurrentAPI,
			Available: m.MaxConcurrentAPI - inUse,
			InUse:     inUse,
		})
	}
	return stats
}
