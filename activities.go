// Package engine implements a Temporal-based graph interpreter workflow engine.
package engine

import (
	"context"
	"log/slog"
	"strconv"
	"strings"

	"go.temporal.io/sdk/activity"
)

// Activities encapsulates the Temporal activity implementations utilized by the workflow engine.
// It maps the activity execution flow to custom callback handlers provided by the host application.
type Activities struct {
	// ExecuteTaskActivityHandler is invoked when the workflow engine encounters a task node.
	// - For synchronous execution, it should return a nil error with a map containing the results.
	// - For asynchronous execution, it should return a nil map and an ErrResultPending error,
	//   which pauses the workflow activity until an external handler triggers TaskDone.
	ExecuteTaskActivityHandler func(TaskPayload) (map[string]any, error)

	// WorkflowCompletedActivityHandler is invoked when the overall workflow execution succeeds and reaches
	// an End node. It receives the workflow ID and the final accumulated workflow variables, allowing the
	// host application to run any necessary completion triggers, notify listeners, or persist final state.
	WorkflowCompletedActivityHandler func(string, map[string]any) error
}

// ExecuteTaskActivity pushes the task to your application and sleeps waiting for it or completes synchronously
func (a *Activities) ExecuteTaskActivity(ctx context.Context, taskTemplateID string, inputs map[string]any) (map[string]any, error) {
	info := activity.GetInfo(ctx)
	templateNodeID, groupKey, groupItemIndex := parseActivityID(info.ActivityID)
	payload := TaskPayload{
		WorkflowID:     info.WorkflowExecution.ID,
		RunID:          info.WorkflowExecution.RunID,
		NodeID:         info.ActivityID, // Composite/Unique execution ID used for TaskDone completion
		TemplateNodeID: templateNodeID,  // Clean template node ID from graph definition
		TaskTemplateID: taskTemplateID,
		Inputs:         inputs,
		GroupKey:       groupKey,
		GroupItemIndex: groupItemIndex,
	}

	slog.Info("ExecuteTaskActivity", "payload", payload)

	// Trigger custom code block. ExecuteTaskActivityHandler can return error ErrResultPending to pause the workflow
	// or return a nil error with the outputs for the next step to consume (synchronous execution)
	res, err := a.ExecuteTaskActivityHandler(payload)
	if err != nil {
		return nil, err
	}

	// Return result immediately for synchronous steps
	return res, nil
}

// WorkflowCompletedActivity is a Temporal activity that executes when a workflow completes successfully.
func (a *Activities) WorkflowCompletedActivity(_ context.Context, workflowID string, finalContext map[string]any) error {
	return a.WorkflowCompletedActivityHandler(workflowID, finalContext)
}

func parseActivityID(activityID string) (templateNodeID, groupKey string, groupItemIndex int) {
	parts := strings.Split(activityID, ":")
	switch len(parts) {
	case 3: // fan-out instance: "<template>:<groupKey>:<index>"
		idx, err := strconv.Atoi(parts[2])
		if err != nil {
			slog.Error("invalid group index for activity: " + activityID + " error: " + err.Error())
			return activityID, "", 0
		}
		return parts[0], parts[1], idx
	case 2: // standard node: "<template>:<uuid>"
		return parts[0], "", 0
	default:
		return activityID, "", 0
	}
}
