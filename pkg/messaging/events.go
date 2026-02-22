package messaging

import (
	"time"
)

type EventType string

const (
	EventAgentStart   EventType = "agent:start"
	EventAgentStop    EventType = "agent:stop"
	EventAgentBusy    EventType = "agent:busy"
	EventAgentIdle    EventType = "agent:idle"
	EventTaskAssigned EventType = "task:assigned"
	EventTaskClaimed  EventType = "task:claimed"
	EventTaskComplete EventType = "task:complete"
	EventTaskFailed   EventType = "task:failed"
	EventAlert        EventType = "alert:broadcast"
	EventHeartbeat    EventType = "agent:heartbeat"
	EventError        EventType = "error"
	EventWarning      EventType = "warning"
)

type Priority string

const (
	PriorityCritical Priority = "critical"
	PriorityHigh     Priority = "high"
	PriorityNormal   Priority = "normal"
	PriorityLow      Priority = "low"
)

type AgentEvent struct {
	ID          string         `json:"id"`
	Type        EventType      `json:"type"`
	Timestamp   time.Time      `json:"timestamp"`
	SourceAgent string         `json:"source_agent"`
	TargetAgent string         `json:"target_agent,omitempty"`
	Priority    Priority       `json:"priority"`
	Payload     map[string]any `json:"payload,omitempty"`
	Message     string         `json:"message,omitempty"`
	Processed   bool           `json:"processed,omitempty"`
}

func NewEvent(eventType EventType, sourceAgent string) *AgentEvent {
	return &AgentEvent{
		ID:          generateEventID(),
		Type:        eventType,
		Timestamp:   time.Now(),
		SourceAgent: sourceAgent,
		Priority:    PriorityNormal,
		Payload:     make(map[string]any),
	}
}

func (e *AgentEvent) WithTarget(target string) *AgentEvent {
	e.TargetAgent = target
	return e
}

func (e *AgentEvent) WithPriority(p Priority) *AgentEvent {
	e.Priority = p
	return e
}

func (e *AgentEvent) WithMessage(msg string) *AgentEvent {
	e.Message = msg
	return e
}

func (e *AgentEvent) WithPayload(key string, value any) *AgentEvent {
	if e.Payload == nil {
		e.Payload = make(map[string]any)
	}
	e.Payload[key] = value
	return e
}

func (e *AgentEvent) WithMetadata(key string, value any) *AgentEvent {
	return e.WithPayload(key, value)
}

func (e *AgentEvent) IsBroadcast() bool {
	return e.TargetAgent == "" || e.TargetAgent == "all" || e.Type == EventAlert
}

func (e *AgentEvent) MatchesTarget(agentID string) bool {
	if e.IsBroadcast() {
		return true
	}
	return e.TargetAgent == agentID
}

func generateEventID() string {
	return EventPrefix + time.Now().Format("20060102150405") + "-" + randomSuffix(6)
}

func randomSuffix(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
	}
	return string(b)
}

const EventPrefix = "evt-"
