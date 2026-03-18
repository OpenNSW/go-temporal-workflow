package engine

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type TaskPayload struct {
	WorkflowID string
	RunID      string
	NodeID     string
	TaskID     string
	Inputs     map[string]any
}

type TaskActivationHandler func(payload TaskPayload) error
type WorkflowCompletionHandler func(workflowID string, finalContext map[string]any) error

type Manager interface {
	StartWorkflow(ctx context.Context, jsonDSL []byte, initialWorkflowVariables map[string]any) (string, error)
	TaskDone(ctx context.Context, workflowID, runID, activityID string, output map[string]any) error
	TaskUpdate(ctx context.Context, workflowID string, message string) error
	GetStatus(ctx context.Context, workflowID string) (*WorkflowInstance, error)

	GetTaskActivationHandler() TaskActivationHandler
	GetWorkflowCompletionHandler() WorkflowCompletionHandler

	StartWorker() error
	StopWorker()
}

type managerImpl struct {
	temporalClient            client.Client
	worker                    worker.Worker
	taskActivationHandler     TaskActivationHandler
	workflowCompletionHandler WorkflowCompletionHandler
}

func NewManager(c client.Client, taskQueue string, taskHandler TaskActivationHandler, completionHandler WorkflowCompletionHandler) Manager {
	m := &managerImpl{
		temporalClient:            c,
		taskActivationHandler:     taskHandler,
		workflowCompletionHandler: completionHandler,
	}

	w := worker.New(c, taskQueue, worker.Options{})

	w.RegisterWorkflowWithOptions(GraphInterpreterWorkflow, workflow.RegisterOptions{Name: "GraphInterpreterWorkflow"})

	acts := &EngineActivities{Manager: m}
	w.RegisterActivityWithOptions(acts.ExecuteTaskActivity, activity.RegisterOptions{Name: "ExecuteTaskActivity"})
	w.RegisterActivityWithOptions(acts.WorkflowCompletedActivity, activity.RegisterOptions{Name: "WorkflowCompletedActivity"})

	m.worker = w
	return m
}

func (m *managerImpl) GetTaskActivationHandler() TaskActivationHandler {
	return m.taskActivationHandler
}
func (m *managerImpl) GetWorkflowCompletionHandler() WorkflowCompletionHandler {
	return m.workflowCompletionHandler
}

func (m *managerImpl) StartWorkflow(ctx context.Context, jsonDSL []byte, initialWorkflowVariables map[string]any) (string, error) {
	var def WorkflowDefinition
	if err := json.Unmarshal(jsonDSL, &def); err != nil {
		return "", err
	}

	workflowID := "workflow-" + uuid.NewString()
	opts := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: "INTERPRETER_TASK_QUEUE",
	}

	we, err := m.temporalClient.ExecuteWorkflow(ctx, opts, "GraphInterpreterWorkflow", def, initialWorkflowVariables)
	if err != nil {
		return "", err
	}

	return we.GetID(), nil
}

// TaskDone is invoked by the external application to complete a dormant asynchronous Temporal Activity.
// WorkflowID is the ID of the workflow
// runID is the ID of the run
// nodeID is the ID of the node
// output is the key valye pairs that should be added to the global context
func (m *managerImpl) TaskDone(ctx context.Context, workflowID, runID, nodeID string, output map[string]any) error {
	return m.temporalClient.CompleteActivityByID(ctx, "default", workflowID, runID, nodeID, output, nil)
}

func (m *managerImpl) TaskUpdate(ctx context.Context, workflowID string, message string) error {
	return m.temporalClient.SignalWorkflow(ctx, workflowID, "", "TaskUpdateSignal", message)
}

func (m *managerImpl) GetStatus(ctx context.Context, workflowID string) (*WorkflowInstance, error) {
	val, err := m.temporalClient.QueryWorkflow(ctx, workflowID, "", "GetStatus")
	if err != nil {
		return nil, err
	}

	var instance WorkflowInstance
	if err := val.Get(&instance); err != nil {
		return nil, err
	}

	return &instance, nil
}

func (m *managerImpl) StartWorker() error {
	return m.worker.Start()
}

func (m *managerImpl) StopWorker() {
	m.worker.Stop()
}
