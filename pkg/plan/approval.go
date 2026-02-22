package plan

import (
	"fmt"
	"regexp"
	"strings"
)

type ApprovalMode string

const (
	ApprovalModeAuto   ApprovalMode = "auto"
	ApprovalModeManual ApprovalMode = "manual"
	ApprovalModeSmart  ApprovalMode = "smart"
)

type ApprovalConfig struct {
	Mode              ApprovalMode `json:"mode"`
	AutoApproveSimple bool         `json:"auto_approve_simple"`
	SimpleThreshold   int          `json:"simple_threshold"`
}

func DefaultApprovalConfig() ApprovalConfig {
	return ApprovalConfig{
		Mode:              ApprovalModeSmart,
		AutoApproveSimple: true,
		SimpleThreshold:   1,
	}
}

func (c *ApprovalConfig) RequiresApproval(plan *Plan) bool {
	switch c.Mode {
	case ApprovalModeAuto:
		return false
	case ApprovalModeManual:
		return true
	case ApprovalModeSmart:
		if c.AutoApproveSimple && len(plan.Tasks) <= c.SimpleThreshold {
			return false
		}
		return true
	default:
		return true
	}
}

type UserResponse struct {
	Action    string `json:"action"`
	TaskIndex int    `json:"task_index,omitempty"`
	NewValue  string `json:"new_value,omitempty"`
}

func ParseUserResponse(input string) UserResponse {
	input = strings.TrimSpace(input)
	input = strings.ToLower(input)

	if input == "yes" || input == "y" || input == "✓" || input == "approve" || input == "go" {
		return UserResponse{Action: "approve"}
	}

	if input == "no" || input == "n" || input == "✗" || input == "cancel" || input == "reject" {
		return UserResponse{Action: "reject"}
	}

	editPattern := regexp.MustCompile(`^edit\s+(\d+)\s+(.+)$`)
	if matches := editPattern.FindStringSubmatch(input); len(matches) == 3 {
		idx := 0
		fmt.Sscanf(matches[1], "%d", &idx)
		return UserResponse{
			Action:    "edit",
			TaskIndex: idx - 1,
			NewValue:  matches[2],
		}
	}

	modelPattern := regexp.MustCompile(`^model\s+(\d+)\s+(.+)$`)
	if matches := modelPattern.FindStringSubmatch(input); len(matches) == 3 {
		idx := 0
		fmt.Sscanf(matches[1], "%d", &idx)
		return UserResponse{
			Action:    "model",
			TaskIndex: idx - 1,
			NewValue:  matches[2],
		}
	}

	return UserResponse{Action: "unknown"}
}

func ApplyEdit(plan *Plan, response UserResponse) error {
	if response.TaskIndex < 0 || response.TaskIndex >= len(plan.Tasks) {
		return fmt.Errorf("invalid task index: %d", response.TaskIndex+1)
	}

	switch response.Action {
	case "edit":
		plan.Tasks[response.TaskIndex].Description = response.NewValue
	case "model":
		plan.Tasks[response.TaskIndex].Model = response.NewValue
	default:
		return fmt.Errorf("unknown edit action: %s", response.Action)
	}

	return nil
}
