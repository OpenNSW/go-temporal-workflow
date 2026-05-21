package engine

import (
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/workflow"
)

// graphInterpreter holds the state for a single workflow execution.
type graphInterpreter struct {
	def        WorkflowDefinition
	instance   *WorkflowInstance
	edgeTokens map[string]int

	// Pre-computed indexes for O(1) lookups
	nodes    map[string]*Node
	outEdges map[string][]Edge
	inEdges  map[string][]Edge
}

// GraphInterpreterWorkflow is the entry point for the Temporal workflow that interprets a graph definition.
func GraphInterpreterWorkflow(ctx workflow.Context, def WorkflowDefinition, initialWorkflowVariables map[string]any) (*WorkflowInstance, error) {
	if initialWorkflowVariables == nil {
		initialWorkflowVariables = make(map[string]any)
	}

	instance := &WorkflowInstance{
		ID:                workflow.GetInfo(ctx).WorkflowExecution.ID,
		Status:            StatusRunning,
		WorkflowVariables: initialWorkflowVariables,
		AuditTrail:        make([]string, 0),
		NodeInfo:          make(map[string]*NodeInfo),
		Edges:             make([]Edge, len(def.Edges)),
	}

	// Generate UUIDs deterministically
	var generatedUUIDs map[string]string
	if err := workflow.SideEffect(ctx, func(_ workflow.Context) interface{} {
		uuids := make(map[string]string)
		for _, node := range def.Nodes {
			uuids[node.ID] = uuid.NewString()
		}
		return uuids
	}).Get(&generatedUUIDs); err != nil {
		return nil, fmt.Errorf("failed to generate UUIDs via SideEffect: %w", err)
	}

	for _, node := range def.Nodes {
		instance.NodeInfo[node.ID] = &NodeInfo{
			// Create a unique ID for the node. node.ID is the ID in our template.
			ID:             node.ID + ":" + generatedUUIDs[node.ID],
			Type:           node.Type,
			GatewayType:    node.GatewayType,
			TaskTemplateID: node.TaskTemplateID,
			CreatedAt:      workflow.Now(ctx),
			UpdatedAt:      workflow.Now(ctx),
			Status:         NodeStatusNotStarted,
		}
	}

	// Resolve Source and Target IDs in edges to the generated node instance IDs
	for i, edge := range def.Edges {
		sourceNodeInfo, sourceExists := instance.NodeInfo[edge.SourceID]
		if !sourceExists {
			return nil, fmt.Errorf("invalid edge definition: source node '%s' not found for edge '%s'", edge.SourceID, edge.ID)
		}
		targetNodeInfo, targetExists := instance.NodeInfo[edge.TargetID]
		if !targetExists {
			return nil, fmt.Errorf("invalid edge definition: target node '%s' not found for edge '%s'", edge.TargetID, edge.ID)
		}
		instance.Edges[i] = Edge{
			ID:        edge.ID,
			SourceID:  sourceNodeInfo.ID,
			TargetID:  targetNodeInfo.ID,
			Condition: edge.Condition,
		}
	}

	// Initialize our interpreter struct
	interp := &graphInterpreter{
		def:        def,
		instance:   instance,
		edgeTokens: make(map[string]int),
	}
	interp.buildIndexes()

	if err := workflow.SetQueryHandler(ctx, "GetStatus", func() (*WorkflowInstance, error) {
		return instance, nil
	}); err != nil {
		return nil, fmt.Errorf("failed to set GetStatus query handler: %w", err)
	}

	signalChan := workflow.GetSignalChannel(ctx, "TaskUpdateSignal")
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var updateEvent UpdateEvent
			signalChan.Receive(ctx, &updateEvent)
			// TODO: implement event handling
		}
	})

	ao := workflow.ActivityOptions{StartToCloseTimeout: 24 * time.Hour * 365}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Begin Execution
	startNode := interp.findStartNode()
	if startNode == nil {
		instance.Status = StatusFailed
		return instance, fmt.Errorf("no START node found")
	}

	if err := interp.executeNode(ctx, startNode.ID); err != nil {
		instance.Status = StatusFailed
		return instance, err
	}

	instance.Status = StatusCompleted
	return instance, nil
}

// buildIndexes pre-computes node and edge lookups for performance and cleanliness
func (g *graphInterpreter) buildIndexes() {
	g.nodes = make(map[string]*Node)
	g.outEdges = make(map[string][]Edge)
	g.inEdges = make(map[string][]Edge)

	for i, n := range g.def.Nodes {
		g.nodes[n.ID] = &g.def.Nodes[i]
	}
	for _, e := range g.def.Edges {
		g.outEdges[e.SourceID] = append(g.outEdges[e.SourceID], e)
		g.inEdges[e.TargetID] = append(g.inEdges[e.TargetID], e)
	}
}

func (g *graphInterpreter) findStartNode() *Node {
	for _, n := range g.def.Nodes {
		if n.Type == NodeTypeStart {
			return &n
		}
	}
	return nil
}

func (g *graphInterpreter) transitionTo(ctx workflow.Context, edge Edge) error {
	g.edgeTokens[edge.ID]++
	return g.executeNode(ctx, edge.TargetID)
}

func (g *graphInterpreter) executeNode(ctx workflow.Context, nodeID string) error {
	nodeInfo := g.instance.NodeInfo[nodeID]
	node, exists := g.nodes[nodeID]

	if !exists || nodeInfo == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	// Set node to Running for all node types at entry
	nodeInfo.Status = NodeStatusRunning
	nodeInfo.UpdatedAt = workflow.Now(ctx)

	outEdges := g.outEdges[node.ID]
	var err error

	// Delegate to specific handlers based on node type
	switch node.Type {
	case NodeTypeStart:
		err = g.handleStartNode(ctx, nodeInfo, outEdges)
	case NodeTypeTask:
		err = g.handleTaskNode(ctx, nodeInfo, node, outEdges)
	case NodeTypeGateway:
		err = g.handleGatewayNode(ctx, nodeInfo, node, outEdges)
	case NodeTypeSplitTask:
		err = g.handleSplitTaskNode(ctx, nodeInfo, node, outEdges)
	case NodeTypeEnd:
		err = g.handleEndNode(ctx, nodeInfo)
	default:
		err = fmt.Errorf("unknown node type: %v", node.Type)
	}

	if err != nil {
		nodeInfo.Status = NodeStatusFailed
		return err
	}
	return nil
}

// handleStartNode transitions to the single outgoing edge and marks itself Completed.
func (g *graphInterpreter) handleStartNode(ctx workflow.Context, nodeInfo *NodeInfo, outEdges []Edge) error {
	if len(outEdges) == 0 {
		return fmt.Errorf("START node has no outgoing edges")
	}
	nodeInfo.Status = NodeStatusCompleted
	nodeInfo.UpdatedAt = workflow.Now(ctx)
	return g.transitionTo(ctx, outEdges[0])
}

// handleEndNode fires WorkflowCompletedActivity and marks itself Completed.
func (g *graphInterpreter) handleEndNode(ctx workflow.Context, nodeInfo *NodeInfo) error {
	err := workflow.ExecuteActivity(ctx, "WorkflowCompletedActivity", g.instance.ID, g.instance.WorkflowVariables).Get(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to complete workflow: %w", err)
	}
	nodeInfo.Status = NodeStatusCompleted
	nodeInfo.UpdatedAt = workflow.Now(ctx)
	return nil
}

func (g *graphInterpreter) mapTaskInputs(inputMapping map[string]string) (map[string]any, error) {
	inputs := make(map[string]any, len(inputMapping))
	if len(inputMapping) == 0 {
		return inputs, nil
	}

	for globalKey, localKey := range inputMapping {
		val, exists := getNestedKey(g.instance.WorkflowVariables, globalKey)
		if !exists {
			return nil, fmt.Errorf("input mapping error: required global variable '%s' not found in workflow variables for task node", globalKey)
		}
		setNestedKey(inputs, localKey, val)
	}

	return inputs, nil
}

func (g *graphInterpreter) mapTaskOutputs(workflowVars map[string]any, outputMapping map[string]string, result map[string]any) error {
	if len(outputMapping) == 0 || result == nil {
		return nil
	}

	for taskKey, globalKey := range outputMapping {
		val, exists := getNestedKey(result, taskKey)
		if !exists {
			return fmt.Errorf("output mapping error: required task variable '%s' not found in task result", taskKey)
		}
		setNestedKey(workflowVars, globalKey, val)
	}
	return nil
}

func (g *graphInterpreter) handleTaskNode(ctx workflow.Context, nodeInfo *NodeInfo, node *Node, outEdges []Edge) error {
	nodeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:          nodeInfo.ID,
		StartToCloseTimeout: 24 * time.Hour * 365,
	})

	inputs, err := g.mapTaskInputs(node.InputMapping)
	if err != nil {
		return err
	}

	var result map[string]any
	err = workflow.ExecuteActivity(nodeCtx, "ExecuteTaskActivity", node.TaskTemplateID, inputs).Get(ctx, &result)
	if err != nil {
		return err
	}

	err = g.mapTaskOutputs(g.instance.WorkflowVariables, node.OutputMapping, result)
	if err != nil {
		return err
	}

	nodeInfo.Status = NodeStatusCompleted
	nodeInfo.UpdatedAt = workflow.Now(ctx)

	if len(outEdges) > 0 {
		return g.transitionTo(ctx, outEdges[0])
	}
	return nil
}

func (g *graphInterpreter) handleGatewayNode(ctx workflow.Context, nodeInfo *NodeInfo, node *Node, outEdges []Edge) error {
	inEdges := g.inEdges[node.ID]

	switch node.GatewayType {
	case GatewayTypeExclusiveSplit:
		for _, e := range outEdges {
			match, err := EvaluateCondition(e.Condition, g.instance.WorkflowVariables)
			if err != nil {
				return err
			}
			if match {
				nodeInfo.Status = NodeStatusCompleted
				nodeInfo.UpdatedAt = workflow.Now(ctx)
				return g.transitionTo(ctx, e)
			}
		}
		return fmt.Errorf("no matching conditions found at exclusive gateway %s", node.ID)

	case GatewayTypeParallelSplit:
		nodeInfo.Status = NodeStatusCompleted
		nodeInfo.UpdatedAt = workflow.Now(ctx)
		var futures []workflow.Future
		for _, e := range outEdges {
			match, err := EvaluateCondition(e.Condition, g.instance.WorkflowVariables)
			if err != nil {
				return err
			}
			if match {
				f, s := workflow.NewFuture(ctx)
				edge := e // Capture locally for coroutine
				workflow.Go(ctx, func(c workflow.Context) {
					err := g.transitionTo(c, edge)
					s.Set(nil, err)
				})
				futures = append(futures, f)
			}
		}
		for _, f := range futures {
			if err := f.Get(ctx, nil); err != nil {
				return err
			}
		}
		return nil

	case GatewayTypeParallelJoin:
		for _, e := range inEdges {
			if g.edgeTokens[e.ID] <= 0 {
				return nil // Wait for other branches
			}
		}
		for _, e := range inEdges {
			g.edgeTokens[e.ID]-- // Consume tokens
		}
		if len(outEdges) > 0 {
			nodeInfo.Status = NodeStatusCompleted
			nodeInfo.UpdatedAt = workflow.Now(ctx)
			return g.transitionTo(ctx, outEdges[0])
		}
		return nil

	case GatewayTypeExclusiveJoin:
		for _, e := range inEdges {
			if g.edgeTokens[e.ID] > 0 {
				g.edgeTokens[e.ID]--
				break
			}
		}
		if len(outEdges) > 0 {
			nodeInfo.Status = NodeStatusCompleted
			nodeInfo.UpdatedAt = workflow.Now(ctx)
			return g.transitionTo(ctx, outEdges[0])
		}
		return nil
	default:
		return fmt.Errorf("unknown gateway type: %v", node.GatewayType)
	}
}

// handleSplitTaskNode executes the parallel branch fan-out child workflow execution.
func (g *graphInterpreter) handleSplitTaskNode(ctx workflow.Context, nodeInfo *NodeInfo, node *Node, outEdges []Edge) error {
	config := node.SplitTask
	if config == nil {
		return fmt.Errorf("split task configuration is missing on node %s", node.ID)
	}

	// 1. Resolve Items collection from global workflow context
	itemsRaw, exists := getNestedKey(g.instance.WorkflowVariables, config.ItemsVariable)
	if !exists {
		return fmt.Errorf("items variable '%s' not found in workflow variables", config.ItemsVariable)
	}

	var branchesData []any
	if itemsRaw != nil {
		if val, ok := itemsRaw.([]any); ok {
			branchesData = val
		} else {
			// Convert slice of any concrete type to []any
			switch reflectVal := reflect.ValueOf(itemsRaw); reflectVal.Kind() {
			case reflect.Slice:
				branchesData = make([]any, reflectVal.Len())
				for idx := 0; idx < reflectVal.Len(); idx++ {
					branchesData[idx] = reflectVal.Index(idx).Interface()
				}
			default:
				return fmt.Errorf("items variable '%s' is not a valid list type", config.ItemsVariable)
			}
		}
	}

	iterKey := config.IterationKey
	if iterKey == "" {
		iterKey = DefaultIterationKey
	}

	parentInfo := workflow.GetInfo(ctx)
	activeBranches := make(map[string]workflow.ChildWorkflowFuture)
	branchIndexes := make(map[string]int)

	// 2. Iterate dynamically, load templates via Activity, and spawn child interpreters
	for i, itemRaw := range branchesData {
		branchItem, ok := itemRaw.(map[string]any)
		if !ok {
			return fmt.Errorf("index point %d inside branch resolution array is invalid layout", i)
		}

		templateID := node.TaskTemplateID
		if config.Mode == SplitModeDifferentTemplates {
			templateID, _ = branchItem[ItemTemplateIDKey].(string)
		}
		branchID, _ := branchItem[ItemBranchIDKey].(string)
		payload, _ := branchItem[ItemPayloadKey].(map[string]any)

		if templateID == "" || branchID == "" {
			return fmt.Errorf("index point %d requires non-empty template_id (static or dynamic) and branch_id configurations", i)
		}

		// Execute Activity on-the-fly to retrieve sub-graph definition
		var branchGraphDef WorkflowDefinition
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 15 * time.Second,
		}), "FetchWorkflowDefinitionActivity", templateID).Get(ctx, &branchGraphDef)
		if err != nil {
			return fmt.Errorf("boundary lifecycle error hydrating definition graph for template %s: %w", templateID, err)
		}

		// Configure targeted isolation environment parameters for child execution workspace
		childVars := map[string]any{
			VarSplitNodeID: node.ID,
			iterKey: map[string]any{
				IterIndexKey:    i,
				IterBranchIDKey: branchID,
				IterInputKey:    payload,
			},
		}

		deterministicChildID := fmt.Sprintf("%s-%s-%s", parentInfo.WorkflowExecution.ID, node.ID, branchID)
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: deterministicChildID,
		})

		// Recursively spin up core interpreter executing target hydrated sub-graph schema
		future := workflow.ExecuteChildWorkflow(childCtx, "GraphInterpreterWorkflow", branchGraphDef, childVars)
		activeBranches[deterministicChildID] = future
		branchIndexes[deterministicChildID] = i
	}

	// 3. Initialize the Interactive Event Multiplexor Selector
	selector := workflow.NewSelector(ctx)

	completedCount := 0
	totalBranches := len(activeBranches)
	aggregatedResults := make([]map[string]any, totalBranches)
	var executionError error

	// 4. Register active sub-workflow execution handles on monitoring tracking hooks
	for cid, fut := range activeBranches {
		f := fut
		targetID := cid

		selector.AddFuture(f, func(wf workflow.Future) {
			var childOutput *WorkflowInstance
			err := wf.Get(ctx, &childOutput)

			if err != nil {
				executionError = fmt.Errorf("dynamic execution track %s halted abnormally: %w", targetID, err)
			} else if childOutput != nil {
				// Collect results chronologically mapped back to setup registry indexes
				aggregatedResults[branchIndexes[targetID]] = childOutput.WorkflowVariables
			}

			delete(activeBranches, targetID)
			completedCount++
		})
	}

	// 5. Block Execution thread loop until all tracks successfully resolve
	for completedCount < totalBranches {
		selector.Select(ctx) // Suspends workflow thread awaiting state activation triggers cleanly

		if executionError != nil && config.FailureMode == FailureModeFailFast {
			nodeInfo.Status = NodeStatusFailed
			return executionError
		}
	}

	if executionError != nil && config.FailureMode == FailureModeCollectAll {
		nodeInfo.Status = NodeStatusFailed
		return executionError
	}

	// 6. Commit variable mutation changes back to the primary context map layer
	if config.ResultsVariable != "" {
		setNestedKey(g.instance.WorkflowVariables, config.ResultsVariable, aggregatedResults)
	}

	nodeInfo.Status = NodeStatusCompleted
	nodeInfo.UpdatedAt = workflow.Now(ctx)

	if len(outEdges) > 0 {
		return g.transitionTo(ctx, outEdges[0])
	}
	return nil
}
