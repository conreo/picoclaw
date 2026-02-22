package messaging

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type EventBus struct {
	path        string
	events      []AgentEvent
	subscribers map[string][]chan AgentEvent
	mu          sync.RWMutex
	maxEvents   int
}

func NewEventBus(workspace string) *EventBus {
	eventsDir := filepath.Join(workspace, "memory")
	os.MkdirAll(eventsDir, 0o755)

	bus := &EventBus{
		path:        filepath.Join(eventsDir, "events.json"),
		events:      make([]AgentEvent, 0),
		subscribers: make(map[string][]chan AgentEvent),
		maxEvents:   1000,
	}
	bus.load()
	return bus
}

func (b *EventBus) load() error {
	data, err := os.ReadFile(b.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(data, &b.events)
}

func (b *EventBus) save() error {
	data, err := json.MarshalIndent(b.events, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(b.path, data, 0o644)
}

func (b *EventBus) Publish(event *AgentEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.events = append(b.events, *event)

	if len(b.events) > b.maxEvents {
		b.events = b.events[len(b.events)-b.maxEvents:]
	}

	if err := b.save(); err != nil {
		return err
	}

	b.notifySubscribers(event)

	return nil
}

func (b *EventBus) Subscribe(agentID string) chan AgentEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan AgentEvent, 100)
	b.subscribers[agentID] = append(b.subscribers[agentID], ch)
	return ch
}

func (b *EventBus) Unsubscribe(agentID string, ch chan AgentEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	subs := b.subscribers[agentID]
	for i, sub := range subs {
		if sub == ch {
			b.subscribers[agentID] = append(subs[:i], subs[i+1:]...)
			close(ch)
			break
		}
	}
}

func (b *EventBus) notifySubscribers(event *AgentEvent) {
	for agentID, chans := range b.subscribers {
		if event.MatchesTarget(agentID) {
			for _, ch := range chans {
				select {
				case ch <- *event:
				default:
				}
			}
		}
	}
}

func (b *EventBus) Read(agentID string, limit int) []AgentEvent {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []AgentEvent
	count := 0

	for i := len(b.events) - 1; i >= 0 && count < limit; i-- {
		if b.events[i].MatchesTarget(agentID) {
			result = append(result, b.events[i])
			count++
		}
	}

	return result
}

func (b *EventBus) ReadUnprocessed(agentID string) []AgentEvent {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var result []AgentEvent
	for i := len(b.events) - 1; i >= 0; i-- {
		if b.events[i].MatchesTarget(agentID) && !b.events[i].Processed {
			result = append(result, b.events[i])
		}
	}
	return result
}

func (b *EventBus) MarkProcessed(eventID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i := range b.events {
		if b.events[i].ID == eventID {
			b.events[i].Processed = true
			return b.save()
		}
	}
	return fmt.Errorf("event not found: %s", eventID)
}

func (b *EventBus) Broadcast(sourceAgent, message string, priority Priority) error {
	event := NewEvent(EventAlert, sourceAgent).
		WithMessage(message).
		WithPriority(priority)
	return b.Publish(event)
}

func (b *EventBus) SendTo(sourceAgent, targetAgent string, eventType EventType, message string) error {
	event := NewEvent(eventType, sourceAgent).
		WithTarget(targetAgent).
		WithMessage(message)
	return b.Publish(event)
}

func (b *EventBus) Clear(olderThan time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if olderThan == 0 {
		b.events = make([]AgentEvent, 0)
		return b.save()
	}

	cutoff := time.Now().Add(-olderThan)
	var remaining []AgentEvent
	for _, e := range b.events {
		if e.Timestamp.After(cutoff) {
			remaining = append(remaining, e)
		}
	}
	b.events = remaining
	return b.save()
}

func (b *EventBus) GetStats() map[string]any {
	b.mu.RLock()
	defer b.mu.RUnlock()

	unprocessed := 0
	byType := make(map[EventType]int)
	byPriority := make(map[Priority]int)

	for _, e := range b.events {
		if !e.Processed {
			unprocessed++
		}
		byType[e.Type]++
		byPriority[e.Priority]++
	}

	return map[string]any{
		"total":       len(b.events),
		"unprocessed": unprocessed,
		"by_type":     byType,
		"by_priority": byPriority,
		"max_events":  b.maxEvents,
	}
}

func (b *EventBus) FormatEvents(events []AgentEvent) string {
	if len(events) == 0 {
		return "No events found"
	}

	var result string
	for _, e := range events {
		processed := ""
		if !e.Processed {
			processed = " [NEW]"
		}
		result += fmt.Sprintf("- [%s] %s | %s -> %s | %s%s\n",
			e.Timestamp.Format("2006-01-02 15:04:05"),
			e.Type,
			e.SourceAgent,
			e.TargetAgent,
			e.Message,
			processed,
		)
	}
	return result
}
