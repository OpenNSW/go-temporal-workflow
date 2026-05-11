package engine

import (
	"context"
	"errors"
	"log/slog"

	"go.temporal.io/sdk/activity"
)

// EngineActivities encapsulates the Temporal activity implementations utilized by the workflow engine.
// It maps the activity execution flow to custom callback handlers provided by the host application.
type EngineActivities struct {
	// ExecuteTaskActivityHandler is invoked when the workflow engine reaches a task node.
	// Use it to notify the host application (persist, enqueue, send event, etc.).
	// The workflow always pauses after this call; completion requires Manager.TaskDone.
	// Return a non-nil error only to fail the task immediately (e.g. notification failed).
	ExecuteTaskActivityHandler       func(TaskPayload) (map[string]any, error)

	// WorkflowCompletedActivityHandler is invoked when the overall workflow execution succeeds and reaches
	// an End node. It receives the workflow ID and the final accumulated workflow variables, allowing the
	// host application to run any necessary completion triggers, notify listeners, or persist final state.
	WorkflowCompletedActivityHandler func(string, map[string]any) error
}

// ExecuteTaskActivity notifies the host application that a task node has been reached,
// then unconditionally pauses. The activity only completes when the host calls Manager.TaskDone,
// which delivers the result via Temporal's CompleteActivityByID.
func (a *EngineActivities) ExecuteTaskActivity(ctx context.Context, taskTemplateID string, inputs map[string]any) (map[string]any, error) {
	info := activity.GetInfo(ctx)
	payload := TaskPayload{
		WorkflowID:     info.WorkflowExecution.ID,
		RunID:          info.WorkflowExecution.RunID,
		NodeID:         info.ActivityID,
		TaskTemplateID: taskTemplateID,
		Inputs:         inputs,
	}

	slog.Info("ExecuteTaskActivity", "payload", payload)

	_, err := a.ExecuteTaskActivityHandler(payload)
	if err != nil && !errors.Is(err, activity.ErrResultPending) {
		return nil, err
	}

	// Always pause — TaskDone (CompleteActivityByID) is the only path to completion.
	return nil, activity.ErrResultPending
}

func (a *EngineActivities) WorkflowCompletedActivity(ctx context.Context, workflowID string, finalContext map[string]any) error {
	return a.WorkflowCompletedActivityHandler(workflowID, finalContext)
}
