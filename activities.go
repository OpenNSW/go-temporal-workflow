package engine

import (
	"context"

	"go.temporal.io/sdk/activity"
)

type EngineActivities struct {
	Manager Manager
}

// ExecuteTaskActivity pushes the task to your application and sleeps waiting for it
func (a *EngineActivities) ExecuteTaskActivity(ctx context.Context, taskID string, inputs map[string]any) (map[string]any, error) {
	info := activity.GetInfo(ctx)
	payload := TaskPayload{
		WorkflowID: info.WorkflowExecution.ID,
		RunID:      info.WorkflowExecution.RunID,
		NodeID:     info.ActivityID, // this is Node.ID which was passed in workflow.WithActivityOptions(ctx, nodeActOpts)
		TaskID:     taskID,
		Inputs:     inputs,
	}

	// Trigger custom code block
	err := a.Manager.GetTaskActivationHandler()(payload)
	if err != nil {
		return nil, err
	}

	// Return ErrResultPending to put Workflow Coroutine to sleep
	return nil, activity.ErrResultPending
}

func (a *EngineActivities) WorkflowCompletedActivity(ctx context.Context, workflowID string, finalContext map[string]any) error {
	return a.Manager.GetWorkflowCompletionHandler()(workflowID, finalContext)
}
