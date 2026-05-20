package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

const dynamicSplitWorkflowJSON = `
{
  "workflow_id": "dynamic-split-test",
  "name": "Dynamic Split Test",
  "version": 1,
  "edges":[
    { "id": "e1", "source_id": "start", "target_id": "split" },
    { "id": "e2", "source_id": "split", "target_id": "task_inside" },
    { "id": "e3", "source_id": "task_inside", "target_id": "join" },
    { "id": "e4", "source_id": "join", "target_id": "end" }
  ],
  "nodes":[
    { "id": "start", "type": "START" },
    {
      "id": "split",
      "type": "GATEWAY",
      "gateway_type": "DYNAMIC_SPLIT",
      "dynamic_split": {
        "paired_join_id": "join",
        "items_variable": "containers",
        "iteration_key": "_iter"
      }
    },
    {
      "id": "task_inside",
      "type": "TASK",
      "task_template_id": "PROCESS_CONTAINER",
      "input_mapping": {
        "_iter.item": "container_name",
        "_iter.index": "index"
      },
      "output_mapping": {
        "status": "_iter.local.status"
      }
    },
    {
      "id": "join",
      "type": "GATEWAY",
      "gateway_type": "DYNAMIC_JOIN",
      "dynamic_join": {
        "paired_split_id": "split",
        "results_variable": "aggregation_results",
        "failure_mode": "fail_fast"
      }
    },
    { "id": "end", "type": "END" }
  ]
}`

func TestDynamicSplitAndJoin(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var def WorkflowDefinition
	err := json.Unmarshal([]byte(dynamicSplitWorkflowJSON), &def)
	require.NoError(t, err)

	initialWorkflowVariables := map[string]any{
		"containers": []any{"container-A", "container-B", "container-C"},
	}

	acts := &EngineActivities{}
	env.RegisterActivityWithOptions(acts.ExecuteTaskActivity, activity.RegisterOptions{Name: "ExecuteTaskActivity"})
	env.RegisterActivityWithOptions(acts.WorkflowCompletedActivity, activity.RegisterOptions{Name: "WorkflowCompletedActivity"})

	env.OnActivity("ExecuteTaskActivity", mock.Anything, "PROCESS_CONTAINER", mock.Anything).
		Return(func(ctx context.Context, templateID string, inputs map[string]any) (map[string]any, error) {
			cName := inputs["container_name"].(string)
			return map[string]any{
				"status": "processed-" + cName,
			}, nil
		}).Times(3)

	env.OnActivity("WorkflowCompletedActivity", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, initialWorkflowVariables)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var instance WorkflowInstance
	err = env.GetWorkflowResult(&instance)
	require.NoError(t, err)

	require.Equal(t, StatusCompleted, instance.Status)

	// Verify Result aggregation
	aggRaw, ok := instance.WorkflowVariables["aggregation_results"]
	require.True(t, ok)
	agg, ok := aggRaw.([]any)
	require.True(t, ok)
	require.Len(t, agg, 3)

	require.Equal(t, "processed-container-A", agg[0].(map[string]any)["status"])
	require.Equal(t, "processed-container-B", agg[1].(map[string]any)["status"])
	require.Equal(t, "processed-container-C", agg[2].(map[string]any)["status"])

	// Verify NodeInfo tracking
	taskInsideNodes, exists := instance.NodeInfo["task_inside"]
	require.True(t, exists)
	require.Len(t, taskInsideNodes, 3)
	for i := 0; i < 3; i++ {
		require.Equal(t, NodeStatusCompleted, taskInsideNodes[i].Status)
		require.Equal(t, i, taskInsideNodes[i].GroupItemIndex)
	}

	env.AssertExpectations(t)
}

func TestDynamicSplitFailFast(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var def WorkflowDefinition
	err := json.Unmarshal([]byte(dynamicSplitWorkflowJSON), &def)
	require.NoError(t, err)

	initialWorkflowVariables := map[string]any{
		"containers": []any{"container-A", "container-B"},
	}

	acts := &EngineActivities{}
	env.RegisterActivityWithOptions(acts.ExecuteTaskActivity, activity.RegisterOptions{Name: "ExecuteTaskActivity"})
	env.RegisterActivityWithOptions(acts.WorkflowCompletedActivity, activity.RegisterOptions{Name: "WorkflowCompletedActivity"})

	// Make branch 0 fail immediately
	env.OnActivity("ExecuteTaskActivity", mock.Anything, "PROCESS_CONTAINER", mock.Anything).
		Return(func(ctx context.Context, templateID string, inputs map[string]any) (map[string]any, error) {
			cName := inputs["container_name"].(string)
			if cName == "container-A" {
				return nil, fmt.Errorf("failed branch A")
			}
			return map[string]any{"status": "ok"}, nil
		})

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, initialWorkflowVariables)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "failed branch A")
}

const dynamicSplitCollectAllWorkflowJSON = `
{
  "workflow_id": "dynamic-split-collect-all-test",
  "name": "Dynamic Split Collect All Test",
  "version": 1,
  "edges":[
    { "id": "e1", "source_id": "start", "target_id": "split" },
    { "id": "e2", "source_id": "split", "target_id": "task_inside" },
    { "id": "e3", "source_id": "task_inside", "target_id": "join" },
    { "id": "e4", "source_id": "join", "target_id": "end" }
  ],
  "nodes":[
    { "id": "start", "type": "START" },
    {
      "id": "split",
      "type": "GATEWAY",
      "gateway_type": "DYNAMIC_SPLIT",
      "dynamic_split": {
        "paired_join_id": "join",
        "items_variable": "containers",
        "iteration_key": "_iter"
      }
    },
    {
      "id": "task_inside",
      "type": "TASK",
      "task_template_id": "PROCESS_CONTAINER",
      "input_mapping": {
        "_iter.item": "container_name"
      },
      "output_mapping": {
        "status": "_iter.local.status"
      }
    },
    {
      "id": "join",
      "type": "GATEWAY",
      "gateway_type": "DYNAMIC_JOIN",
      "dynamic_join": {
        "paired_split_id": "split",
        "results_variable": "aggregation_results",
        "failure_mode": "COLLECT_ALL"
      }
    },
    { "id": "end", "type": "END" }
  ]
}`

func TestDynamicSplitCollectAll(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var def WorkflowDefinition
	err := json.Unmarshal([]byte(dynamicSplitCollectAllWorkflowJSON), &def)
	require.NoError(t, err)

	initialWorkflowVariables := map[string]any{
		"containers": []any{"container-A", "container-B"},
	}

	acts := &EngineActivities{}
	env.RegisterActivityWithOptions(acts.ExecuteTaskActivity, activity.RegisterOptions{Name: "ExecuteTaskActivity"})
	env.RegisterActivityWithOptions(acts.WorkflowCompletedActivity, activity.RegisterOptions{Name: "WorkflowCompletedActivity"})

	// Branch 0 fails, Branch 1 succeeds
	env.OnActivity("ExecuteTaskActivity", mock.Anything, "PROCESS_CONTAINER", mock.Anything).
		Return(func(ctx context.Context, templateID string, inputs map[string]any) (map[string]any, error) {
			cName := inputs["container_name"].(string)
			if cName == "container-A" {
				return nil, fmt.Errorf("failed branch A")
			}
			return map[string]any{"status": "processed-B"}, nil
		})

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, initialWorkflowVariables)

	require.True(t, env.IsWorkflowCompleted())
	// Should fail since one of the branches failed
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "failed branch A")

	// But it should have run both and populated results (successes and partial failures)
	queryResult, err := env.QueryWorkflow("GetStatus")
	require.NoError(t, err)
	var instance WorkflowInstance
	err = queryResult.Get(&instance)
	require.NoError(t, err)

	aggRaw, ok := instance.WorkflowVariables["aggregation_results"]
	require.True(t, ok)
	agg, ok := aggRaw.([]any)
	require.True(t, ok)
	require.Len(t, agg, 2)

	// agg[0] is empty or nil/empty map (since branch A failed, status output wasn't mapped)
	// agg[1] has the status for B
	require.Equal(t, "processed-B", agg[1].(map[string]any)["status"])
}

func TestDynamicValidationConstraints(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}

	t.Run("missing paired join ID", func(t *testing.T) {
		env := testSuite.NewTestWorkflowEnvironment()
		invalidJSON := `
		{
		  "workflow_id": "invalid-validation",
		  "name": "invalid-validation",
		  "version": 1,
		  "edges":[],
		  "nodes":[
			{
			  "id": "split",
			  "type": "GATEWAY",
			  "gateway_type": "DYNAMIC_SPLIT",
			  "dynamic_split": {
				"items_variable": "containers"
			  }
			}
		  ]
		}`
		var def WorkflowDefinition
		err := json.Unmarshal([]byte(invalidJSON), &def)
		require.NoError(t, err)

		env.ExecuteWorkflow(GraphInterpreterWorkflow, def, map[string]any{})
		require.True(t, env.IsWorkflowCompleted())
		require.Error(t, env.GetWorkflowError())
		require.Contains(t, env.GetWorkflowError().Error(), "missing paired_join_id")
	})

	t.Run("node ID contains colon", func(t *testing.T) {
		env := testSuite.NewTestWorkflowEnvironment()
		invalidJSON := `
		{
		  "workflow_id": "invalid-colon",
		  "name": "invalid-colon",
		  "version": 1,
		  "edges":[],
		  "nodes":[
			{
			  "id": "node:invalid",
			  "type": "START"
			}
		  ]
		}`
		var def WorkflowDefinition
		err := json.Unmarshal([]byte(invalidJSON), &def)
		require.NoError(t, err)

		env.ExecuteWorkflow(GraphInterpreterWorkflow, def, map[string]any{})
		require.True(t, env.IsWorkflowCompleted())
		require.Error(t, env.GetWorkflowError())
		require.Contains(t, env.GetWorkflowError().Error(), "cannot contain ':' character")
	})

	t.Run("task template ID contains colon", func(t *testing.T) {
		env := testSuite.NewTestWorkflowEnvironment()
		invalidJSON := `
		{
		  "workflow_id": "invalid-template-colon",
		  "name": "invalid-template-colon",
		  "version": 1,
		  "edges":[],
		  "nodes":[
			{
			  "id": "node1",
			  "type": "TASK",
			  "task_template_id": "task:invalid"
			}
		  ]
		}`
		var def WorkflowDefinition
		err := json.Unmarshal([]byte(invalidJSON), &def)
		require.NoError(t, err)

		env.ExecuteWorkflow(GraphInterpreterWorkflow, def, map[string]any{})
		require.True(t, env.IsWorkflowCompleted())
		require.Error(t, env.GetWorkflowError())
		require.Contains(t, env.GetWorkflowError().Error(), "cannot contain ':' character")
	})
}
