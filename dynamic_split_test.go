package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

// ---------------------------------------------------------------------------
// Shared workflow definitions
// ---------------------------------------------------------------------------

// Items-based fan-out: one branch per element of `containers`. The task receives
// the item (_iter.item) and its index (_iter.index), and writes a per-branch
// result into _iter.local.status which the join aggregates.
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
        "failure_mode": "FAIL_FAST"
      }
    },
    { "id": "end", "type": "END" }
  ]
}`

// Count-based fan-out: spawns `containerCount` branches. There is no item; each
// branch only differs by _iter.index.
const dynamicSplitCountWorkflowJSON = `
{
  "workflow_id": "dynamic-split-count-test",
  "name": "Dynamic Split Count Test",
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
        "count_variable": "containerCount",
        "iteration_key": "_iter"
      }
    },
    {
      "id": "task_inside",
      "type": "TASK",
      "task_template_id": "PROCESS_CONTAINER",
      "input_mapping": {
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
        "results_variable": "aggregation_results"
      }
    },
    { "id": "end", "type": "END" }
  ]
}`

// Collect-all fan-out: the join waits for every branch even if one fails, marks
// the workflow failed, but still exposes partial results.
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

// Writes a task output into a read-only iteration key (_iter.index), which the
// engine must reject during output mapping.
const dynamicSplitReadOnlyOutputWorkflowJSON = `
{
  "workflow_id": "dynamic-split-readonly-test",
  "name": "Dynamic Split Read Only Output Test",
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
      "dynamic_split": { "paired_join_id": "join", "items_variable": "containers" }
    },
    {
      "id": "task_inside",
      "type": "TASK",
      "task_template_id": "PROCESS_CONTAINER",
      "output_mapping": { "status": "_iter.index" }
    },
    {
      "id": "join",
      "type": "GATEWAY",
      "gateway_type": "DYNAMIC_JOIN",
      "dynamic_join": { "paired_split_id": "split" }
    },
    { "id": "end", "type": "END" }
  ]
}`

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newDynamicEnv builds a test environment with both engine activities registered.
func newDynamicEnv(t *testing.T, jsonDef string) (*testsuite.TestWorkflowEnvironment, WorkflowDefinition) {
	t.Helper()
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	var def WorkflowDefinition
	require.NoError(t, json.Unmarshal([]byte(jsonDef), &def))

	acts := &Activities{}
	env.RegisterActivityWithOptions(acts.ExecuteTaskActivity, activity.RegisterOptions{Name: "ExecuteTaskActivity"})
	env.RegisterActivityWithOptions(acts.WorkflowCompletedActivity, activity.RegisterOptions{Name: "WorkflowCompletedActivity"})
	return env, def
}

// expectValidationError runs a definition that should be rejected by
// validateDefinition (before any node executes) and returns the workflow error.
func expectValidationError(t *testing.T, jsonDef string) error {
	t.Helper()
	env, def := newDynamicEnv(t, jsonDef)
	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, map[string]any{})
	require.True(t, env.IsWorkflowCompleted())
	return env.GetWorkflowError()
}

// ---------------------------------------------------------------------------
// Happy path — items
// ---------------------------------------------------------------------------

func TestDynamicSplitAndJoin(t *testing.T) {
	env, def := newDynamicEnv(t, dynamicSplitWorkflowJSON)

	initialWorkflowVariables := map[string]any{
		"containers": []any{"container-A", "container-B", "container-C"},
	}

	env.OnActivity("ExecuteTaskActivity", mock.Anything, "PROCESS_CONTAINER", mock.Anything).
		Return(func(_ context.Context, _ string, inputs map[string]any) (map[string]any, error) {
			cName := inputs["container_name"].(string)
			return map[string]any{"status": "processed-" + cName}, nil
		}).Times(3)

	env.OnActivity("WorkflowCompletedActivity", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, initialWorkflowVariables)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var instance WorkflowInstance
	require.NoError(t, env.GetWorkflowResult(&instance))
	require.Equal(t, StatusCompleted, instance.Status)

	// Aggregated results are in branch-index order, not completion order.
	agg := requireResultSlice(t, instance, "aggregation_results", 3)
	require.Equal(t, "processed-container-A", agg[0].(map[string]any)["status"])
	require.Equal(t, "processed-container-B", agg[1].(map[string]any)["status"])
	require.Equal(t, "processed-container-C", agg[2].(map[string]any)["status"])

	// NodeInfo: one record per branch, with a shared group key and the right index.
	taskNodes, exists := instance.NodeInfo["task_inside"]
	require.True(t, exists)
	require.Len(t, taskNodes, 3)
	groupKey := taskNodes[0].GroupKey
	require.NotEmpty(t, groupKey, "fan-out nodes must carry a group key")
	for i := 0; i < 3; i++ {
		require.Equal(t, NodeStatusCompleted, taskNodes[i].Status)
		require.Equal(t, i, taskNodes[i].GroupItemIndex)
		require.Equal(t, groupKey, taskNodes[i].GroupKey, "all siblings share one group key")
	}

	env.AssertExpectations(t)
}

// ---------------------------------------------------------------------------
// Happy path — count
// ---------------------------------------------------------------------------

func TestDynamicSplitCountVariable(t *testing.T) {
	env, def := newDynamicEnv(t, dynamicSplitCountWorkflowJSON)

	initialWorkflowVariables := map[string]any{"containerCount": 3}

	// Each branch derives its result purely from _iter.index, which proves the
	// engine handed each branch a distinct, correct index.
	env.OnActivity("ExecuteTaskActivity", mock.Anything, "PROCESS_CONTAINER", mock.Anything).
		Return(func(_ context.Context, _ string, inputs map[string]any) (map[string]any, error) {
			idx, err := toInt(inputs["index"]) // serialized numbers arrive as float64
			if err != nil {
				return nil, fmt.Errorf("index not numeric: %w", err)
			}
			return map[string]any{"status": fmt.Sprintf("processed-%d", idx)}, nil
		}).Times(3)

	env.OnActivity("WorkflowCompletedActivity", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, initialWorkflowVariables)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var instance WorkflowInstance
	require.NoError(t, env.GetWorkflowResult(&instance))
	require.Equal(t, StatusCompleted, instance.Status)

	agg := requireResultSlice(t, instance, "aggregation_results", 3)
	for i := 0; i < 3; i++ {
		require.Equal(t, fmt.Sprintf("processed-%d", i), agg[i].(map[string]any)["status"])
	}

	env.AssertExpectations(t)
}

// ---------------------------------------------------------------------------
// Zero iterations
// ---------------------------------------------------------------------------

func TestDynamicSplitZeroIterations(t *testing.T) {
	t.Run("count is zero", func(t *testing.T) {
		env, def := newDynamicEnv(t, dynamicSplitCountWorkflowJSON)
		env.OnActivity("WorkflowCompletedActivity", mock.Anything, mock.Anything, mock.Anything).
			Return(nil).Once()

		env.ExecuteWorkflow(GraphInterpreterWorkflow, def, map[string]any{"containerCount": 0})

		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		// The branch task must never run.
		env.AssertNotCalled(t, "ExecuteTaskActivity", mock.Anything, mock.Anything, mock.Anything)

		var instance WorkflowInstance
		require.NoError(t, env.GetWorkflowResult(&instance))
		require.Equal(t, StatusCompleted, instance.Status)
		requireResultSlice(t, instance, "aggregation_results", 0)
	})

	t.Run("items list is empty", func(t *testing.T) {
		env, def := newDynamicEnv(t, dynamicSplitWorkflowJSON)
		env.OnActivity("WorkflowCompletedActivity", mock.Anything, mock.Anything, mock.Anything).
			Return(nil).Once()

		env.ExecuteWorkflow(GraphInterpreterWorkflow, def, map[string]any{"containers": []any{}})

		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())
		env.AssertNotCalled(t, "ExecuteTaskActivity", mock.Anything, mock.Anything, mock.Anything)

		var instance WorkflowInstance
		require.NoError(t, env.GetWorkflowResult(&instance))
		require.Equal(t, StatusCompleted, instance.Status)
		requireResultSlice(t, instance, "aggregation_results", 0)
	})
}

// ---------------------------------------------------------------------------
// Out-of-order branch completion
// ---------------------------------------------------------------------------

// Branches finish in reverse order (C, then B, then A) but aggregation must
// remain in branch-index order, and the join must wait for all of them.
func TestDynamicSplitBranchesCompleteOutOfOrder(t *testing.T) {
	env, def := newDynamicEnv(t, dynamicSplitWorkflowJSON)

	initialWorkflowVariables := map[string]any{
		"containers": []any{"container-A", "container-B", "container-C"},
	}

	delays := map[string]time.Duration{
		"container-A": 30 * time.Minute, // index 0 finishes last
		"container-B": 20 * time.Minute,
		"container-C": 10 * time.Minute, // index 2 finishes first
	}
	for name, d := range delays {
		cName := name
		env.OnActivity("ExecuteTaskActivity", mock.Anything, "PROCESS_CONTAINER",
			mock.MatchedBy(func(inputs map[string]any) bool { return inputs["container_name"] == cName })).
			Return(map[string]any{"status": "processed-" + cName}, nil).
			After(d).Once()
	}

	env.OnActivity("WorkflowCompletedActivity", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, initialWorkflowVariables)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var instance WorkflowInstance
	require.NoError(t, env.GetWorkflowResult(&instance))

	agg := requireResultSlice(t, instance, "aggregation_results", 3)
	require.Equal(t, "processed-container-A", agg[0].(map[string]any)["status"])
	require.Equal(t, "processed-container-B", agg[1].(map[string]any)["status"])
	require.Equal(t, "processed-container-C", agg[2].(map[string]any)["status"])

	env.AssertExpectations(t)
}

// ---------------------------------------------------------------------------
// FAIL_FAST
// ---------------------------------------------------------------------------

func TestDynamicSplitFailFast(t *testing.T) {
	env, def := newDynamicEnv(t, dynamicSplitWorkflowJSON)

	initialWorkflowVariables := map[string]any{
		"containers": []any{"container-A", "container-B"},
	}

	// A later branch (index 1) fails; the workflow must still fail.
	env.OnActivity("ExecuteTaskActivity", mock.Anything, "PROCESS_CONTAINER", mock.Anything).
		Return(func(_ context.Context, _ string, inputs map[string]any) (map[string]any, error) {
			if inputs["container_name"] == "container-B" {
				return nil, fmt.Errorf("failed branch B")
			}
			return map[string]any{"status": "ok"}, nil
		})

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, initialWorkflowVariables)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "failed branch B")
}

// Regression for the Selector-based fail-fast fix: a fast-failing branch must
// fail the workflow without blocking on a slow lower-indexed branch. The slow
// branch is delayed an hour; with the old index-ordered f.Get loop the workflow
// would not fail until that hour elapsed (virtual clock).
func TestDynamicSplitFailFastDoesNotWaitForSlowBranch(t *testing.T) {
	env, def := newDynamicEnv(t, dynamicSplitWorkflowJSON)

	startTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	env.SetStartTime(startTime)

	initialWorkflowVariables := map[string]any{
		"containers": []any{"container-A", "container-B"},
	}

	// Branch 0 (A) would succeed, but only after a long delay.
	env.OnActivity("ExecuteTaskActivity", mock.Anything, "PROCESS_CONTAINER",
		mock.MatchedBy(func(inputs map[string]any) bool { return inputs["container_name"] == "container-A" })).
		Return(map[string]any{"status": "ok"}, nil).
		After(time.Hour)
	// Branch 1 (B) fails immediately.
	env.OnActivity("ExecuteTaskActivity", mock.Anything, "PROCESS_CONTAINER",
		mock.MatchedBy(func(inputs map[string]any) bool { return inputs["container_name"] == "container-B" })).
		Return(nil, fmt.Errorf("failed branch B"))

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, initialWorkflowVariables)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "failed branch B")
	// Fast: failed well before the slow branch's 1h delay.
	require.Less(t, env.Now().Sub(startTime), time.Hour,
		"FAIL_FAST should not wait for the slow branch")
}

// ---------------------------------------------------------------------------
// COLLECT_ALL
// ---------------------------------------------------------------------------

func TestDynamicSplitCollectAll(t *testing.T) {
	env, def := newDynamicEnv(t, dynamicSplitCollectAllWorkflowJSON)

	initialWorkflowVariables := map[string]any{
		"containers": []any{"container-A", "container-B"},
	}

	// Branch 0 fails, branch 1 succeeds.
	env.OnActivity("ExecuteTaskActivity", mock.Anything, "PROCESS_CONTAINER", mock.Anything).
		Return(func(_ context.Context, _ string, inputs map[string]any) (map[string]any, error) {
			if inputs["container_name"] == "container-A" {
				return nil, fmt.Errorf("failed branch A")
			}
			return map[string]any{"status": "processed-B"}, nil
		})

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, initialWorkflowVariables)

	require.True(t, env.IsWorkflowCompleted())
	// COLLECT_ALL still fails the workflow when any branch fails.
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "failed branch A")

	// ...but partial results are populated. Query the final in-memory state
	// (GetWorkflowResult would surface the error, not the instance).
	queryResult, err := env.QueryWorkflow("GetStatus")
	require.NoError(t, err)
	var instance WorkflowInstance
	require.NoError(t, queryResult.Get(&instance))

	agg := requireResultSlice(t, instance, "aggregation_results", 2)
	// agg[0] is the failed branch's empty local map; agg[1] holds B's result.
	require.Equal(t, "processed-B", agg[1].(map[string]any)["status"])
}

func TestDynamicSplitCollectAllAllSucceed(t *testing.T) {
	env, def := newDynamicEnv(t, dynamicSplitCollectAllWorkflowJSON)

	initialWorkflowVariables := map[string]any{
		"containers": []any{"container-A", "container-B"},
	}

	env.OnActivity("ExecuteTaskActivity", mock.Anything, "PROCESS_CONTAINER", mock.Anything).
		Return(func(_ context.Context, _ string, inputs map[string]any) (map[string]any, error) {
			return map[string]any{"status": "processed-" + inputs["container_name"].(string)}, nil
		}).Times(2)

	env.OnActivity("WorkflowCompletedActivity", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, initialWorkflowVariables)

	require.True(t, env.IsWorkflowCompleted())
	// With no failures, COLLECT_ALL transitions past the join and completes.
	require.NoError(t, env.GetWorkflowError())

	var instance WorkflowInstance
	require.NoError(t, env.GetWorkflowResult(&instance))
	require.Equal(t, StatusCompleted, instance.Status)

	agg := requireResultSlice(t, instance, "aggregation_results", 2)
	require.Equal(t, "processed-container-A", agg[0].(map[string]any)["status"])
	require.Equal(t, "processed-container-B", agg[1].(map[string]any)["status"])

	env.AssertExpectations(t)
}

// ---------------------------------------------------------------------------
// Read-only iteration key
// ---------------------------------------------------------------------------

func TestDynamicSplitRejectsWriteToReadOnlyIterationKey(t *testing.T) {
	env, def := newDynamicEnv(t, dynamicSplitReadOnlyOutputWorkflowJSON)

	initialWorkflowVariables := map[string]any{"containers": []any{"container-A"}}

	env.OnActivity("ExecuteTaskActivity", mock.Anything, "PROCESS_CONTAINER", mock.Anything).
		Return(map[string]any{"status": "value"}, nil)

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, initialWorkflowVariables)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
	require.Contains(t, env.GetWorkflowError().Error(), "read-only iteration key")
}

// ---------------------------------------------------------------------------
// Activity ID encoding (round-trip used by async TaskDone)
// ---------------------------------------------------------------------------

// parseActivityID must split the composite activity ID into its parts. The
// opaque NodeID (the full string) is what the caller echoes back to TaskDone;
// these parsed parts are the convenience fields for identifying parallel work.
//
// NOTE: the 2-part (standard node) case assumes parseActivityID returns the
// bare template ID. If this case fails, parseActivityID still needs the
// explicit 2-part handling discussed (return parts[0] for "<template>:<uuid>").
func TestParseActivityID(t *testing.T) {
	cases := []struct {
		name      string
		in        string
		template  string
		groupKey  string
		itemIndex int
	}{
		{"fan-out instance", "task_1:group-abc:2", "task_1", "group-abc", 2},
		{"standard node with uuid", "task_1:3f2a-uuid", "task_1", "", 0},
		{"bare node id", "task_1", "task_1", "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmpl, gk, idx := parseActivityID(tc.in)
			require.Equal(t, tc.template, tmpl)
			require.Equal(t, tc.groupKey, gk)
			require.Equal(t, tc.itemIndex, idx)
		})
	}
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

func TestDynamicValidationConstraints(t *testing.T) {
	t.Run("split missing paired_join_id", func(t *testing.T) {
		err := expectValidationError(t, `
		{
		  "workflow_id":"v","name":"v","version":1,
		  "edges":[],
		  "nodes":[
			{ "id":"split","type":"GATEWAY","gateway_type":"DYNAMIC_SPLIT",
			  "dynamic_split":{ "items_variable":"containers" } }
		  ]
		}`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing paired_join_id")
	})

	t.Run("join missing paired_split_id", func(t *testing.T) {
		err := expectValidationError(t, `
		{
		  "workflow_id":"v","name":"v","version":1,
		  "edges":[
			{ "id":"e1","source_id":"start","target_id":"split" },
			{ "id":"e2","source_id":"split","target_id":"t" },
			{ "id":"e3","source_id":"t","target_id":"join" },
			{ "id":"e4","source_id":"join","target_id":"end" }
		  ],
		  "nodes":[
			{ "id":"start","type":"START" },
			{ "id":"split","type":"GATEWAY","gateway_type":"DYNAMIC_SPLIT",
			  "dynamic_split":{ "paired_join_id":"join","count_variable":"c" } },
			{ "id":"t","type":"TASK","task_template_id":"X" },
			{ "id":"join","type":"GATEWAY","gateway_type":"DYNAMIC_JOIN","dynamic_join":{} },
			{ "id":"end","type":"END" }
		  ]
		}`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing paired_split_id")
	})

	t.Run("both count and items set", func(t *testing.T) {
		err := expectValidationError(t, `
		{
		  "workflow_id":"v","name":"v","version":1,
		  "edges":[
			{ "id":"e1","source_id":"start","target_id":"split" },
			{ "id":"e2","source_id":"split","target_id":"t" },
			{ "id":"e3","source_id":"t","target_id":"join" },
			{ "id":"e4","source_id":"join","target_id":"end" }
		  ],
		  "nodes":[
			{ "id":"start","type":"START" },
			{ "id":"split","type":"GATEWAY","gateway_type":"DYNAMIC_SPLIT",
			  "dynamic_split":{ "paired_join_id":"join","count_variable":"c","items_variable":"i" } },
			{ "id":"t","type":"TASK","task_template_id":"X" },
			{ "id":"join","type":"GATEWAY","gateway_type":"DYNAMIC_JOIN","dynamic_join":{ "paired_split_id":"split" } },
			{ "id":"end","type":"END" }
		  ]
		}`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exactly one of count_variable or items_variable")
	})

	t.Run("neither count nor items set", func(t *testing.T) {
		err := expectValidationError(t, `
		{
		  "workflow_id":"v","name":"v","version":1,
		  "edges":[
			{ "id":"e1","source_id":"start","target_id":"split" },
			{ "id":"e2","source_id":"split","target_id":"t" },
			{ "id":"e3","source_id":"t","target_id":"join" },
			{ "id":"e4","source_id":"join","target_id":"end" }
		  ],
		  "nodes":[
			{ "id":"start","type":"START" },
			{ "id":"split","type":"GATEWAY","gateway_type":"DYNAMIC_SPLIT",
			  "dynamic_split":{ "paired_join_id":"join" } },
			{ "id":"t","type":"TASK","task_template_id":"X" },
			{ "id":"join","type":"GATEWAY","gateway_type":"DYNAMIC_JOIN","dynamic_join":{ "paired_split_id":"split" } },
			{ "id":"end","type":"END" }
		  ]
		}`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exactly one of count_variable or items_variable")
	})

	t.Run("pairing mismatch", func(t *testing.T) {
		err := expectValidationError(t, `
		{
		  "workflow_id":"v","name":"v","version":1,
		  "edges":[],
		  "nodes":[
			{ "id":"split","type":"GATEWAY","gateway_type":"DYNAMIC_SPLIT",
			  "dynamic_split":{ "paired_join_id":"join","count_variable":"c" } },
			{ "id":"join","type":"GATEWAY","gateway_type":"DYNAMIC_JOIN",
			  "dynamic_join":{ "paired_split_id":"some_other_split" } }
		  ]
		}`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "but join is paired with")
	})

	t.Run("split paired to a non-join node", func(t *testing.T) {
		err := expectValidationError(t, `
		{
		  "workflow_id":"v","name":"v","version":1,
		  "edges":[],
		  "nodes":[
			{ "id":"start","type":"START" },
			{ "id":"split","type":"GATEWAY","gateway_type":"DYNAMIC_SPLIT",
			  "dynamic_split":{ "paired_join_id":"end","count_variable":"c" } },
			{ "id":"end","type":"END" }
		  ]
		}`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-existent or invalid paired join")
	})

	t.Run("join paired to a non-existent split", func(t *testing.T) {
		err := expectValidationError(t, `
		{
		  "workflow_id":"v","name":"v","version":1,
		  "edges":[],
		  "nodes":[
			{ "id":"start","type":"START" },
			{ "id":"join","type":"GATEWAY","gateway_type":"DYNAMIC_JOIN",
			  "dynamic_join":{ "paired_split_id":"ghost_split" } },
			{ "id":"end","type":"END" }
		  ]
		}`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-existent or invalid paired split")
	})

	t.Run("split with multiple outgoing edges", func(t *testing.T) {
		err := expectValidationError(t, `
		{
		  "workflow_id":"v","name":"v","version":1,
		  "edges":[
			{ "id":"e1","source_id":"start","target_id":"split" },
			{ "id":"e2","source_id":"split","target_id":"t1" },
			{ "id":"e3","source_id":"split","target_id":"t2" },
			{ "id":"e4","source_id":"t1","target_id":"join" },
			{ "id":"e5","source_id":"t2","target_id":"join" },
			{ "id":"e6","source_id":"join","target_id":"end" }
		  ],
		  "nodes":[
			{ "id":"start","type":"START" },
			{ "id":"split","type":"GATEWAY","gateway_type":"DYNAMIC_SPLIT",
			  "dynamic_split":{ "paired_join_id":"join","count_variable":"c" } },
			{ "id":"t1","type":"TASK","task_template_id":"X" },
			{ "id":"t2","type":"TASK","task_template_id":"Y" },
			{ "id":"join","type":"GATEWAY","gateway_type":"DYNAMIC_JOIN","dynamic_join":{ "paired_split_id":"split" } },
			{ "id":"end","type":"END" }
		  ]
		}`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exactly one outgoing edge")
	})

	t.Run("region node entered from outside", func(t *testing.T) {
		err := expectValidationError(t, `
		{
		  "workflow_id":"v","name":"v","version":1,
		  "edges":[
			{ "id":"e1","source_id":"start","target_id":"split" },
			{ "id":"e2","source_id":"split","target_id":"t" },
			{ "id":"e3","source_id":"t","target_id":"join" },
			{ "id":"e4","source_id":"join","target_id":"end" },
			{ "id":"e5","source_id":"start","target_id":"t" }
		  ],
		  "nodes":[
			{ "id":"start","type":"START" },
			{ "id":"split","type":"GATEWAY","gateway_type":"DYNAMIC_SPLIT",
			  "dynamic_split":{ "paired_join_id":"join","count_variable":"c" } },
			{ "id":"t","type":"TASK","task_template_id":"X" },
			{ "id":"join","type":"GATEWAY","gateway_type":"DYNAMIC_JOIN","dynamic_join":{ "paired_split_id":"split" } },
			{ "id":"end","type":"END" }
		  ]
		}`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "from outside the split region")
	})

	t.Run("nested dynamic splits", func(t *testing.T) {
		err := expectValidationError(t, `
		{
		  "workflow_id":"v","name":"v","version":1,
		  "edges":[
			{ "id":"e1","source_id":"start","target_id":"outer_split" },
			{ "id":"e2","source_id":"outer_split","target_id":"inner_split" },
			{ "id":"e3","source_id":"inner_split","target_id":"t" },
			{ "id":"e4","source_id":"t","target_id":"inner_join" },
			{ "id":"e5","source_id":"inner_join","target_id":"outer_join" },
			{ "id":"e6","source_id":"outer_join","target_id":"end" }
		  ],
		  "nodes":[
			{ "id":"start","type":"START" },
			{ "id":"outer_split","type":"GATEWAY","gateway_type":"DYNAMIC_SPLIT",
			  "dynamic_split":{ "paired_join_id":"outer_join","count_variable":"c" } },
			{ "id":"inner_split","type":"GATEWAY","gateway_type":"DYNAMIC_SPLIT",
			  "dynamic_split":{ "paired_join_id":"inner_join","count_variable":"c" } },
			{ "id":"t","type":"TASK","task_template_id":"X" },
			{ "id":"inner_join","type":"GATEWAY","gateway_type":"DYNAMIC_JOIN","dynamic_join":{ "paired_split_id":"inner_split" } },
			{ "id":"outer_join","type":"GATEWAY","gateway_type":"DYNAMIC_JOIN","dynamic_join":{ "paired_split_id":"outer_split" } },
			{ "id":"end","type":"END" }
		  ]
		}`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "nested dynamic splits are not supported")
	})

	t.Run("node ID contains colon", func(t *testing.T) {
		err := expectValidationError(t, `
		{
		  "workflow_id":"v","name":"v","version":1,
		  "edges":[],
		  "nodes":[ { "id":"node:invalid","type":"START" } ]
		}`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot contain ':' character")
	})

	t.Run("task template ID contains colon", func(t *testing.T) {
		err := expectValidationError(t, `
		{
		  "workflow_id":"v","name":"v","version":1,
		  "edges":[],
		  "nodes":[ { "id":"node1","type":"TASK","task_template_id":"task:invalid" } ]
		}`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot contain ':' character")
	})
}

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

func requireResultSlice(t *testing.T, instance WorkflowInstance, key string, wantLen int) []any {
	t.Helper()
	raw, ok := instance.WorkflowVariables[key]
	require.True(t, ok, "expected workflow variable %q to be set", key)
	slice, ok := raw.([]any)
	require.True(t, ok, "expected %q to be a []any, got %T", key, raw)
	require.Len(t, slice, wantLen)
	return slice
}

func TestDynamicSplitScaleLimits(t *testing.T) {
	t.Run("default limit exceeded", func(t *testing.T) {
		env, def := newDynamicEnv(t, dynamicSplitCountWorkflowJSON)
		initialWorkflowVariables := map[string]any{"containerCount": DefaultMaxParallelTasks + 1}
		env.ExecuteWorkflow(GraphInterpreterWorkflow, def, initialWorkflowVariables)
		require.True(t, env.IsWorkflowCompleted())
		require.Error(t, env.GetWorkflowError())
		require.Contains(t, env.GetWorkflowError().Error(), "exceeded maximum parallel tasks limit")
	})
}
