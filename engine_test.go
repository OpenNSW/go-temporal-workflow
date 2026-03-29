package engine

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

// ---------------------------------------------------------------------------
// Workflow definition fixtures
// ---------------------------------------------------------------------------

// customsWorkflowJSON is a realistic customs export workflow that exercises:
//   - sequential task chaining
//   - ExclusiveSplit gateway with condition-based routing (LCL vs FCL)
//   - ExclusiveJoin gateway that merges the two CDN branches before continuing
const customsWorkflowJSON = `
{
  "workflow_id": "customs-export-v1",
  "name": "Customs Export Declaration & Release",
  "version": 1,
  "edges":[
    { "id": "e_customs_start",           "source_id": "customs_0_start",          "target_id": "customs_1_cusdec_submit" },
    { "id": "e_customs_submit_to_pay",   "source_id": "customs_1_cusdec_submit",  "target_id": "customs_2_duty_payment" },
    { "id": "e_customs_pay_to_warrant",  "source_id": "customs_2_duty_payment",   "target_id": "customs_3_warranting_gw" },
    { "id": "e_customs_warrant_lcl",     "source_id": "customs_3_warranting_gw",  "target_id": "customs_4_lcl_cdn_create", "condition": "consignment_type == 'LCL'" },
    { "id": "e_customs_warrant_fcl",     "source_id": "customs_3_warranting_gw",  "target_id": "customs_4_fcl_cdn_create", "condition": "consignment_type == 'FCL'" },
    { "id": "e_customs_lcl_to_join",     "source_id": "customs_4_lcl_cdn_create", "target_id": "customs_5_cdn_join" },
    { "id": "e_customs_fcl_to_join",     "source_id": "customs_4_fcl_cdn_create", "target_id": "customs_5_cdn_join" },
    { "id": "e_customs_join_to_ack",     "source_id": "customs_5_cdn_join",       "target_id": "customs_6_cdn_ack" },
    { "id": "e_customs_ack_bn_create",   "source_id": "customs_6_cdn_ack",        "target_id": "customs_7_boatnote_create" },
    { "id": "e_customs_bn_create_appr",  "source_id": "customs_7_boatnote_create","target_id": "customs_8_boatnote_approve" },
    { "id": "e_customs_bn_done",         "source_id": "customs_8_boatnote_approve","target_id":"customs_9_export_released" }
  ],
  "nodes":[
    { "id": "customs_0_start",            "type": "START" },
    { "id": "customs_1_cusdec_submit",    "type": "TASK",    "task_template_id": "SUBMIT_CUSDEC", "output_mapping": { "consignment_type": "consignment_type" } },
    { "id": "customs_2_duty_payment",     "type": "TASK",    "task_template_id": "PAY_DUTIES" },
    { "id": "customs_3_warranting_gw",    "type": "GATEWAY", "gateway_type": "EXCLUSIVE_SPLIT" },
    { "id": "customs_4_lcl_cdn_create",   "type": "TASK",    "task_template_id": "CREATE_LCL_CDN" },
    { "id": "customs_4_fcl_cdn_create",   "type": "TASK",    "task_template_id": "CREATE_FCL_CDN" },
    { "id": "customs_5_cdn_join",         "type": "GATEWAY", "gateway_type": "EXCLUSIVE_JOIN" },
    { "id": "customs_6_cdn_ack",          "type": "TASK",    "task_template_id": "ACK_CDNS" },
    { "id": "customs_7_boatnote_create",  "type": "TASK",    "task_template_id": "CREATE_BOAT_NOTE" },
    { "id": "customs_8_boatnote_approve", "type": "TASK",    "task_template_id": "APPROVE_BOAT_NOTE" },
    { "id": "customs_9_export_released",  "type": "END" }
  ]
}`

// parallelWorkflowJSON exercises ParallelSplit + ParallelJoin.
// The join must only fire TASK_C once both task_a and task_b have completed.
const parallelWorkflowJSON = `
{
  "workflow_id": "parallel-test",
  "name": "Parallel Split and Join Test",
  "version": 1,
  "edges":[
    { "id": "e1", "source_id": "start",  "target_id": "split"  },
    { "id": "e2", "source_id": "split",  "target_id": "task_a" },
    { "id": "e3", "source_id": "split",  "target_id": "task_b" },
    { "id": "e4", "source_id": "task_a", "target_id": "join"   },
    { "id": "e5", "source_id": "task_b", "target_id": "join"   },
    { "id": "e6", "source_id": "join",   "target_id": "task_c" },
    { "id": "e7", "source_id": "task_c", "target_id": "end"    }
  ],
  "nodes":[
    { "id": "start",  "type": "START" },
    { "id": "split",  "type": "GATEWAY", "gateway_type": "PARALLEL_SPLIT" },
    { "id": "task_a", "type": "TASK", "task_template_id": "TASK_A" },
    { "id": "task_b", "type": "TASK", "task_template_id": "TASK_B" },
    { "id": "join",   "type": "GATEWAY", "gateway_type": "PARALLEL_JOIN" },
    { "id": "task_c", "type": "TASK", "task_template_id": "TASK_C" },
    { "id": "end",    "type": "END" }
  ]
}`

// linearWorkflowJSON is the simplest possible workflow: START → TASK → END.
const linearWorkflowJSON = `
{
  "workflow_id": "linear-test",
  "name": "Linear Workflow",
  "version": 1,
  "edges":[
    { "id": "e1", "source_id": "start", "target_id": "task1" },
    { "id": "e2", "source_id": "task1", "target_id": "end"   }
  ],
  "nodes":[
    { "id": "start", "type": "START" },
    { "id": "task1", "type": "TASK", "task_template_id": "ONLY_TASK" },
    { "id": "end",   "type": "END"  }
  ]
}`

// exclusiveJoinWorkflowJSON has two paths that each converge into an
// ExclusiveJoin gateway. Only one path is taken, so the join must fire exactly
// once.
const exclusiveJoinWorkflowJSON = `
{
  "workflow_id": "xor-join-test",
  "name": "Exclusive Join Test",
  "version": 1,
  "edges":[
    { "id": "e1", "source_id": "start",    "target_id": "split"   },
    { "id": "e2", "source_id": "split",    "target_id": "task_hi", "condition": "score > 50" },
    { "id": "e3", "source_id": "split",    "target_id": "task_lo", "condition": "score <= 50" },
    { "id": "e4", "source_id": "task_hi",  "target_id": "join"    },
    { "id": "e5", "source_id": "task_lo",  "target_id": "join"    },
    { "id": "e6", "source_id": "join",     "target_id": "task_fin"},
    { "id": "e7", "source_id": "task_fin", "target_id": "end"     }
  ],
  "nodes":[
    { "id": "start",    "type": "START" },
    { "id": "split",    "type": "GATEWAY", "gateway_type": "EXCLUSIVE_SPLIT" },
    { "id": "task_hi",  "type": "TASK", "task_template_id": "HIGH_PATH" },
    { "id": "task_lo",  "type": "TASK", "task_template_id": "LOW_PATH"  },
    { "id": "join",     "type": "GATEWAY", "gateway_type": "EXCLUSIVE_JOIN" },
    { "id": "task_fin", "type": "TASK", "task_template_id": "FINAL_TASK" },
    { "id": "end",      "type": "END" }
  ]
}`

// outputMappingWorkflowJSON verifies that task output_mapping correctly
// remaps activity result keys into workflow variable keys.
const outputMappingWorkflowJSON = `
{
  "workflow_id": "mapping-test",
  "name": "Output Mapping Test",
  "version": 1,
  "edges":[
    { "id": "e1", "source_id": "start", "target_id": "mapper" },
    { "id": "e2", "source_id": "mapper", "target_id": "end"   }
  ],
  "nodes":[
    { "id": "start",  "type": "START" },
    { "id": "mapper", "type": "TASK", "task_template_id": "MAP_TASK",
      "output_mapping": { "task_result": "global_result", "task_code": "global_code" } },
    { "id": "end",    "type": "END" }
  ]
}`

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// newEnv creates a standard test workflow environment with EngineActivities registered.
func newEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	env := new(testsuite.WorkflowTestSuite).NewTestWorkflowEnvironment()
	acts := &EngineActivities{}
	env.RegisterActivityWithOptions(acts.ExecuteTaskActivity, activity.RegisterOptions{Name: "ExecuteTaskActivity"})
	env.RegisterActivityWithOptions(acts.WorkflowCompletedActivity, activity.RegisterOptions{Name: "WorkflowCompletedActivity"})
	return env
}

// parseDef unmarshals a JSON workflow definition and asserts no error.
func parseDef(t *testing.T, jsonStr string) WorkflowDefinition {
	t.Helper()
	var def WorkflowDefinition
	require.NoError(t, json.Unmarshal([]byte(jsonStr), &def))
	return def
}

// runWorkflow executes GraphInterpreterWorkflow in the given environment and
// returns the resulting WorkflowInstance. It asserts that the workflow
// completed without a Temporal-level error.
func runWorkflow(t *testing.T, env *testsuite.TestWorkflowEnvironment, def WorkflowDefinition, vars map[string]any) WorkflowInstance {
	t.Helper()
	if vars == nil {
		vars = map[string]any{}
	}
	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, vars)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var inst WorkflowInstance
	require.NoError(t, env.GetWorkflowResult(&inst))
	return inst
}

// expectTaskOnce registers a mock expectation that returns the given output once.
func expectTaskOnce(env *testsuite.TestWorkflowEnvironment, templateID string, output map[string]any) {
	env.OnActivity("ExecuteTaskActivity", mock.Anything, templateID, mock.Anything).
		Return(output, nil).Once()
}

// expectAnyTask registers a catch-all mock that returns an empty map.
func expectAnyTask(env *testsuite.TestWorkflowEnvironment) {
	env.OnActivity("ExecuteTaskActivity", mock.Anything, mock.Anything, mock.Anything).
		Return(map[string]any{}, nil)
}

// expectCompletion registers the WorkflowCompletedActivity expectation.
func expectCompletion(env *testsuite.TestWorkflowEnvironment) {
	env.OnActivity("WorkflowCompletedActivity", mock.Anything, mock.Anything, mock.Anything).
		Return(nil)
}

// ---------------------------------------------------------------------------
// Happy-path tests
// ---------------------------------------------------------------------------

// TestLinearWorkflow verifies the simplest possible graph executes correctly.
func TestLinearWorkflow(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, linearWorkflowJSON)

	expectTaskOnce(env, "ONLY_TASK", map[string]any{"answer": 42})
	expectCompletion(env)

	inst := runWorkflow(t, env, def, nil)

	require.Equal(t, StatusCompleted, inst.Status)
	// JSON round-trip through WorkflowVariables deserialises numbers as float64.
	require.EqualValues(t, 42, inst.WorkflowVariables["answer"])

	env.AssertExpectations(t)
}

// TestCustomsExportLCLFlow verifies the LCL branch is taken and FCL is skipped.
func TestCustomsExportLCLFlow(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, customsWorkflowJSON)
	empty := map[string]any{}

	expectTaskOnce(env, "SUBMIT_CUSDEC", map[string]any{"consignment_type": "LCL"})
	expectTaskOnce(env, "PAY_DUTIES", empty)
	expectTaskOnce(env, "CREATE_LCL_CDN", empty)
	expectTaskOnce(env, "ACK_CDNS", empty)
	expectTaskOnce(env, "CREATE_BOAT_NOTE", empty)
	expectTaskOnce(env, "APPROVE_BOAT_NOTE", empty)
	env.OnActivity("WorkflowCompletedActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	inst := runWorkflow(t, env, def, nil)

	require.Equal(t, StatusCompleted, inst.Status)
	require.Equal(t, "LCL", inst.WorkflowVariables["consignment_type"])

	// FCL task must never have been called.
	env.AssertNotCalled(t, "ExecuteTaskActivity", mock.Anything, "CREATE_FCL_CDN", mock.Anything)
	env.AssertExpectations(t)
}

// TestCustomsExportFCLFlow verifies the FCL branch is taken and LCL is skipped.
func TestCustomsExportFCLFlow(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, customsWorkflowJSON)
	empty := map[string]any{}

	expectTaskOnce(env, "SUBMIT_CUSDEC", map[string]any{"consignment_type": "FCL"})
	expectTaskOnce(env, "PAY_DUTIES", empty)
	expectTaskOnce(env, "CREATE_FCL_CDN", empty)
	expectTaskOnce(env, "ACK_CDNS", empty)
	expectTaskOnce(env, "CREATE_BOAT_NOTE", empty)
	expectTaskOnce(env, "APPROVE_BOAT_NOTE", empty)
	env.OnActivity("WorkflowCompletedActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	inst := runWorkflow(t, env, def, nil)

	require.Equal(t, StatusCompleted, inst.Status)
	require.Equal(t, "FCL", inst.WorkflowVariables["consignment_type"])

	env.AssertNotCalled(t, "ExecuteTaskActivity", mock.Anything, "CREATE_LCL_CDN", mock.Anything)
	env.AssertExpectations(t)
}

// TestParallelJoinFlow verifies that TASK_C is called exactly once after both
// parallel branches complete.
func TestParallelJoinFlow(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, parallelWorkflowJSON)
	empty := map[string]any{}

	expectTaskOnce(env, "TASK_A", empty)
	expectTaskOnce(env, "TASK_B", empty)
	expectTaskOnce(env, "TASK_C", empty) // Must be called exactly once.
	env.OnActivity("WorkflowCompletedActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	inst := runWorkflow(t, env, def, nil)

	require.Equal(t, StatusCompleted, inst.Status)
	env.AssertExpectations(t)
}

// TestExclusiveJoinHighPath verifies the ExclusiveJoin only fires once even
// though only one branch ran (score > 50 → high path).
func TestExclusiveJoinHighPath(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, exclusiveJoinWorkflowJSON)
	empty := map[string]any{}

	expectTaskOnce(env, "HIGH_PATH", empty)
	expectTaskOnce(env, "FINAL_TASK", empty)
	expectCompletion(env)

	inst := runWorkflow(t, env, def, map[string]any{"score": 75})

	require.Equal(t, StatusCompleted, inst.Status)
	env.AssertNotCalled(t, "ExecuteTaskActivity", mock.Anything, "LOW_PATH", mock.Anything)
	env.AssertExpectations(t)
}

// TestExclusiveJoinLowPath verifies the low path branch is taken when score ≤ 50.
func TestExclusiveJoinLowPath(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, exclusiveJoinWorkflowJSON)
	empty := map[string]any{}

	expectTaskOnce(env, "LOW_PATH", empty)
	expectTaskOnce(env, "FINAL_TASK", empty)
	expectCompletion(env)

	inst := runWorkflow(t, env, def, map[string]any{"score": 30})

	require.Equal(t, StatusCompleted, inst.Status)
	env.AssertNotCalled(t, "ExecuteTaskActivity", mock.Anything, "HIGH_PATH", mock.Anything)
	env.AssertExpectations(t)
}

// TestOutputMapping verifies that output_mapping remaps task result keys into
// the correct workflow variable keys. It also verifies that an unmapped key
// from the raw result is still merged into WorkflowVariables via the bulk merge.
func TestOutputMapping(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, outputMappingWorkflowJSON)

	// Activity returns both a mapped key and an extra unmapped key.
	expectTaskOnce(env, "MAP_TASK", map[string]any{
		"task_result": "ok",
		"task_code":   200,
		"extra_key":   "should_also_appear",
	})
	expectCompletion(env)

	inst := runWorkflow(t, env, def, nil)

	require.Equal(t, StatusCompleted, inst.Status)
	// Mapped keys appear under their remapped names.
	// JSON round-trip deserialises numbers as float64.
	require.Equal(t, "ok", inst.WorkflowVariables["global_result"])
	require.EqualValues(t, 200, inst.WorkflowVariables["global_code"])
	// Unmapped keys are still merged.
	require.Equal(t, "should_also_appear", inst.WorkflowVariables["extra_key"])
	env.AssertExpectations(t)
}

// TestWorkflowVariablesArePassedToTasks verifies that a task receives
// variables set by a preceding task.
func TestWorkflowVariablesArePassedToTasks(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, linearWorkflowJSON)

	// Capture the inputs the activity actually receives.
	var capturedInputs map[string]any
	env.OnActivity("ExecuteTaskActivity", mock.Anything, "ONLY_TASK", mock.Anything).
		Run(func(args mock.Arguments) {
			capturedInputs, _ = args.Get(2).(map[string]any)
		}).
		Return(map[string]any{}, nil).Once()
	expectCompletion(env)

	initialVars := map[string]any{"order_id": "ORD-001", "amount": 99.9}
	runWorkflow(t, env, def, initialVars)

	require.Equal(t, "ORD-001", capturedInputs["order_id"])
	require.Equal(t, 99.9, capturedInputs["amount"])
	env.AssertExpectations(t)
}

// ---------------------------------------------------------------------------
// Edge / instance structure tests
// ---------------------------------------------------------------------------

// TestEdgesAreReturnedInWorkflowInstance checks that the returned instance
// contains the correct number of edges with IDs matching the definition,
// and that SourceID / TargetID have been rewritten to instance node IDs.
func TestEdgesAreReturnedInWorkflowInstance(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, customsWorkflowJSON)

	env.OnActivity("ExecuteTaskActivity", mock.Anything, "SUBMIT_CUSDEC", mock.Anything).
		Return(map[string]any{"consignment_type": "LCL"}, nil)
	expectAnyTask(env)
	expectCompletion(env)

	inst := runWorkflow(t, env, def, nil)

	require.NotNil(t, inst.Edges)
	require.Len(t, inst.Edges, len(def.Edges))

	// Build template nodeID → instance nodeID mapping for assertion.
	nodeIDMap := make(map[string]string, len(inst.NodeInfo))
	for defNodeID, nodeInfo := range inst.NodeInfo {
		nodeIDMap[defNodeID] = nodeInfo.ID
	}
	for i, edge := range inst.Edges {
		require.Equal(t, def.Edges[i].ID, edge.ID)
		require.Equal(t, nodeIDMap[def.Edges[i].SourceID], edge.SourceID)
		require.Equal(t, nodeIDMap[def.Edges[i].TargetID], edge.TargetID)
	}
}

// TestEdgesReferenceValidNodeInstanceIDs ensures every edge's SourceID and
// TargetID correspond to an actual NodeInfo entry in the instance.
func TestEdgesReferenceValidNodeInstanceIDs(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, parallelWorkflowJSON)

	expectAnyTask(env)
	expectCompletion(env)

	inst := runWorkflow(t, env, def, nil)

	validNodeIDs := make(map[string]bool, len(inst.NodeInfo))
	for _, node := range inst.NodeInfo {
		validNodeIDs[node.ID] = true
	}
	for _, edge := range inst.Edges {
		require.True(t, validNodeIDs[edge.SourceID], "edge %s has invalid sourceID: %s", edge.ID, edge.SourceID)
		require.True(t, validNodeIDs[edge.TargetID], "edge %s has invalid targetID: %s", edge.ID, edge.TargetID)
	}
}

// TestNodeInstanceIDsAreUnique verifies that every NodeInfo entry has a distinct
// instance ID (template_id:uuid format).
func TestNodeInstanceIDsAreUnique(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, parallelWorkflowJSON)

	expectAnyTask(env)
	expectCompletion(env)

	inst := runWorkflow(t, env, def, nil)

	seen := make(map[string]bool, len(inst.NodeInfo))
	for defID, info := range inst.NodeInfo {
		require.False(t, seen[info.ID], "duplicate instance ID %s for node %s", info.ID, defID)
		seen[info.ID] = true
	}
	require.Len(t, seen, len(def.Nodes))
}

// ---------------------------------------------------------------------------
// Validation / failure tests
// ---------------------------------------------------------------------------

// TestInvalidEdgeDefinitionFailsWorkflow checks that a definition with an edge
// pointing to a non-existent node is caught by validateDefinition and causes
// the workflow to fail immediately before any execution begins.
func TestInvalidEdgeDefinitionFailsWorkflow(t *testing.T) {
	env := newEnv(t)

	badJSON := `{
	  "workflow_id": "bad",
	  "name": "bad",
	  "version": 1,
	  "edges":[{ "id": "e1", "source_id": "start", "target_id": "missing" }],
	  "nodes":[{ "id": "start", "type": "START" }]
	}`
	def := parseDef(t, badJSON)

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, map[string]any{})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

// TestValidationEdgeWithInvalidTargetID checks that validateDefinition rejects
// an edge whose target node ID does not exist in the node list.
func TestValidationEdgeWithInvalidTargetID(t *testing.T) {
	def := WorkflowDefinition{
		Nodes: []Node{
			{ID: "s", Type: NodeTypeStart},
		},
		Edges: []Edge{
			{ID: "e1", SourceID: "s", TargetID: "nonexistent"},
		},
	}
	require.Error(t, validateDefinition(def))
}

// TestValidationEdgeWithInvalidSourceID checks that validateDefinition rejects
// an edge whose source node ID does not exist in the node list.
func TestValidationEdgeWithInvalidSourceID(t *testing.T) {
	def := WorkflowDefinition{
		Nodes: []Node{
			{ID: "e", Type: NodeTypeEnd},
		},
		Edges: []Edge{
			{ID: "e1", SourceID: "nonexistent", TargetID: "e"},
		},
	}
	require.Error(t, validateDefinition(def))
}

// TestValidationStartNodeNoOutgoingEdge checks that a START node with no
// outgoing edges fails validation.
func TestValidationStartNodeNoOutgoingEdge(t *testing.T) {
	def := WorkflowDefinition{
		Nodes: []Node{{ID: "s", Type: NodeTypeStart}},
	}
	require.Error(t, validateDefinition(def))
}

// TestValidationStartNodeMultipleOutgoing checks that a START node with more
// than one outgoing edge fails validation.
func TestValidationStartNodeMultipleOutgoing(t *testing.T) {
	def := WorkflowDefinition{
		Nodes: []Node{
			{ID: "s", Type: NodeTypeStart},
			{ID: "a", Type: NodeTypeEnd},
			{ID: "b", Type: NodeTypeEnd},
		},
		Edges: []Edge{
			{ID: "e1", SourceID: "s", TargetID: "a"},
			{ID: "e2", SourceID: "s", TargetID: "b"},
		},
	}
	require.Error(t, validateDefinition(def))
}

// TestValidationEndNodeWithOutgoingEdge checks that an END node that has
// outgoing edges fails validation.
func TestValidationEndNodeWithOutgoingEdge(t *testing.T) {
	def := WorkflowDefinition{
		Nodes: []Node{
			{ID: "s", Type: NodeTypeStart},
			{ID: "e", Type: NodeTypeEnd},
			{ID: "x", Type: NodeTypeEnd},
		},
		Edges: []Edge{
			{ID: "e1", SourceID: "s", TargetID: "e"},
			{ID: "e2", SourceID: "e", TargetID: "x"},
		},
	}
	require.Error(t, validateDefinition(def))
}

// TestValidationTaskNodeMissingOutgoing checks that a TASK node without an
// outgoing edge fails validation (exactly 1 outgoing is required).
func TestValidationTaskNodeMissingOutgoing(t *testing.T) {
	def := WorkflowDefinition{
		Nodes: []Node{
			{ID: "s", Type: NodeTypeStart},
			{ID: "t", Type: NodeTypeTask},
		},
		Edges: []Edge{
			{ID: "e1", SourceID: "s", TargetID: "t"},
		},
	}
	require.Error(t, validateDefinition(def))
}

// TestValidationTaskNodeMultipleIncoming checks that a TASK node with more than
// one incoming edge fails validation (exactly 1 incoming is required; merges
// must go through an ExclusiveJoin or ParallelJoin gateway).
func TestValidationTaskNodeMultipleIncoming(t *testing.T) {
	def := WorkflowDefinition{
		Nodes: []Node{
			{ID: "s", Type: NodeTypeStart},
			{ID: "gw", Type: NodeTypeGateway, GatewayType: GatewayTypeExclusiveSplit},
			{ID: "ta", Type: NodeTypeTask},
			{ID: "tb", Type: NodeTypeTask},
			{ID: "merge", Type: NodeTypeTask}, // 2 incoming — must be rejected
			{ID: "e", Type: NodeTypeEnd},
		},
		Edges: []Edge{
			{ID: "e1", SourceID: "s", TargetID: "gw"},
			{ID: "e2", SourceID: "gw", TargetID: "ta"},
			{ID: "e3", SourceID: "gw", TargetID: "tb"},
			{ID: "e4", SourceID: "ta", TargetID: "merge"},
			{ID: "e5", SourceID: "tb", TargetID: "merge"},
			{ID: "e6", SourceID: "merge", TargetID: "e"},
		},
	}
	require.Error(t, validateDefinition(def))
}

// TestValidationSplitGatewayOnlyOneOutgoing ensures a split gateway with a
// single outgoing edge is rejected.
func TestValidationSplitGatewayOnlyOneOutgoing(t *testing.T) {
	def := WorkflowDefinition{
		Nodes: []Node{
			{ID: "s", Type: NodeTypeStart},
			{ID: "gw", Type: NodeTypeGateway, GatewayType: GatewayTypeExclusiveSplit},
			{ID: "e", Type: NodeTypeEnd},
		},
		Edges: []Edge{
			{ID: "e1", SourceID: "s", TargetID: "gw"},
			{ID: "e2", SourceID: "gw", TargetID: "e"},
		},
	}
	require.Error(t, validateDefinition(def))
}

// TestValidationJoinGatewayOnlyOneIncoming ensures a join gateway with a
// single incoming edge is rejected.
func TestValidationJoinGatewayOnlyOneIncoming(t *testing.T) {
	def := WorkflowDefinition{
		Nodes: []Node{
			{ID: "s", Type: NodeTypeStart},
			{ID: "gw", Type: NodeTypeGateway, GatewayType: GatewayTypeParallelJoin},
			{ID: "e", Type: NodeTypeEnd},
		},
		Edges: []Edge{
			{ID: "e1", SourceID: "s", TargetID: "gw"},
			{ID: "e2", SourceID: "gw", TargetID: "e"},
		},
	}
	require.Error(t, validateDefinition(def))
}

// TestValidationUnknownGatewayType checks that an unrecognised GatewayType
// fails validation.
func TestValidationUnknownGatewayType(t *testing.T) {
	def := WorkflowDefinition{
		Nodes: []Node{
			{ID: "s", Type: NodeTypeStart},
			{ID: "gw", Type: NodeTypeGateway, GatewayType: "MYSTERY_GATEWAY"},
			{ID: "e", Type: NodeTypeEnd},
		},
		Edges: []Edge{
			{ID: "e1", SourceID: "s", TargetID: "gw"},
			{ID: "e2", SourceID: "gw", TargetID: "e"},
		},
	}
	require.Error(t, validateDefinition(def))
}

// TestNoMatchingExclusiveSplitConditionFails verifies that a workflow fails
// when an ExclusiveSplit has no matching condition for the current variables.
func TestNoMatchingExclusiveSplitConditionFails(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, customsWorkflowJSON)

	// Return an unknown consignment_type so neither LCL nor FCL edge matches.
	env.OnActivity("ExecuteTaskActivity", mock.Anything, "SUBMIT_CUSDEC", mock.Anything).
		Return(map[string]any{"consignment_type": "AIR"}, nil).Once()
	env.OnActivity("ExecuteTaskActivity", mock.Anything, "PAY_DUTIES", mock.Anything).
		Return(map[string]any{}, nil).Once()

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, map[string]any{})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

// TestActivityErrorFailsWorkflow checks that an activity error propagates and
// marks the workflow as failed.
func TestActivityErrorFailsWorkflow(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, linearWorkflowJSON)

	env.OnActivity("ExecuteTaskActivity", mock.Anything, "ONLY_TASK", mock.Anything).
		Return(nil, fmt.Errorf("downstream system unavailable")).Once()

	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, map[string]any{})

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}

// TestInitialVariablesNilIsNormalised verifies that passing nil as
// initialWorkflowVariables doesn't panic — it should be normalised to an empty map.
func TestInitialVariablesNilIsNormalised(t *testing.T) {
	env := newEnv(t)
	def := parseDef(t, linearWorkflowJSON)

	expectTaskOnce(env, "ONLY_TASK", map[string]any{})
	expectCompletion(env)

	// Pass nil explicitly.
	env.ExecuteWorkflow(GraphInterpreterWorkflow, def, nil)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}
