package engine

import (
	"fmt"
	"maps"
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

	// activeBranches counts concurrent coroutines still executing.
	// Incremented before spawning a branch, decremented on exit.
	// The main coroutine blocks on workflow.Await until this reaches zero.
	activeBranches int

	// workflowErr captures the first error from any branch (first-error-wins).
	// Once set, workflow.Await unblocks and the workflow terminates.
	workflowErr error
}

// validateDefinition checks the structural integrity of a WorkflowDefinition
// before execution begins, enforcing per-node-type edge count rules and
// ensuring every edge references an existing node.
func validateDefinition(def WorkflowDefinition) error {
	// Build a set of valid node IDs for O(1) edge validation below.
	nodeIDs := make(map[string]struct{}, len(def.Nodes))
	for _, n := range def.Nodes {
		nodeIDs[n.ID] = struct{}{}
	}

	inEdges := make(map[string][]Edge)
	outEdges := make(map[string][]Edge)
	for _, e := range def.Edges {
		if _, ok := nodeIDs[e.SourceID]; !ok {
			return fmt.Errorf("edge %q has invalid source ID %q", e.ID, e.SourceID)
		}
		if _, ok := nodeIDs[e.TargetID]; !ok {
			return fmt.Errorf("edge %q has invalid target ID %q", e.ID, e.TargetID)
		}
		outEdges[e.SourceID] = append(outEdges[e.SourceID], e)
		inEdges[e.TargetID] = append(inEdges[e.TargetID], e)
	}

	for _, n := range def.Nodes {
		inCount := len(inEdges[n.ID])
		outCount := len(outEdges[n.ID])

		switch n.Type {
		case NodeTypeStart:
			if inCount > 0 {
				return fmt.Errorf("start node %s cannot have incoming edges", n.ID)
			}
			if outCount != 1 {
				return fmt.Errorf("start node %s must have exactly 1 outgoing edge, got %d", n.ID, outCount)
			}
		case NodeTypeEnd:
			if outCount > 0 {
				return fmt.Errorf("end node %s cannot have outgoing edges", n.ID)
			}
			if inCount != 1 {
				return fmt.Errorf("end node %s must have exactly 1 incoming edge, got %d", n.ID, inCount)
			}
		case NodeTypeTask:
			if inCount != 1 {
				return fmt.Errorf("task node %s must have exactly 1 incoming edge, got %d", n.ID, inCount)
			}
			if outCount != 1 {
				return fmt.Errorf("task node %s must have exactly 1 outgoing edge, got %d", n.ID, outCount)
			}
		case NodeTypeGateway:
			switch n.GatewayType {
			case GatewayTypeExclusiveSplit, GatewayTypeParallelSplit:
				if inCount != 1 {
					return fmt.Errorf("split gateway %s must have exactly 1 incoming edge, got %d", n.ID, inCount)
				}
				if outCount < 2 {
					return fmt.Errorf("split gateway %s must have at least 2 outgoing edges, got %d", n.ID, outCount)
				}
			case GatewayTypeParallelJoin, GatewayTypeExclusiveJoin:
				if inCount < 2 {
					return fmt.Errorf("join gateway %s must have at least 2 incoming edges, got %d", n.ID, inCount)
				}
				if outCount != 1 {
					return fmt.Errorf("join gateway %s must have exactly 1 outgoing edge, got %d", n.ID, outCount)
				}
			default:
				return fmt.Errorf("unknown gateway type %s for node %s", n.GatewayType, n.ID)
			}
		default:
			return fmt.Errorf("unknown node type %s for node %s", n.Type, n.ID)
		}
	}
	return nil
}

func GraphInterpreterWorkflow(ctx workflow.Context, def WorkflowDefinition, initialWorkflowVariables map[string]any) (*WorkflowInstance, error) {
	// Validate topology up-front so mid-run structural errors are impossible.
	if err := validateDefinition(def); err != nil {
		return nil, err
	}

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
	workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		uuids := make(map[string]string)
		for _, node := range def.Nodes {
			uuids[node.ID] = uuid.NewString()
		}
		return uuids
	}).Get(&generatedUUIDs)

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

	workflow.SetQueryHandler(ctx, "GetStatus", func() (*WorkflowInstance, error) {
		return instance, nil
	})

	signalChan := workflow.GetSignalChannel(ctx, "TaskUpdateSignal")
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var updateEvent UpdateEvent
			signalChan.Receive(ctx, &updateEvent)

			if nodeInfo, ok := instance.NodeInfo[updateEvent.NodeID]; ok {
				if stateStr, ok := updateEvent.Payload["state"].(string); ok {
					// Only transition on known states; ignore unknown values to
					// prevent incorrect status reversions (e.g. "COMPLETED" → RUNNING).
					switch stateStr {
					case "FAILED":
						nodeInfo.Status = NodeStatusFailed
					case "RUNNING":
						nodeInfo.Status = NodeStatusRunning
					}
					// Unknown state strings are ignored to prevent incorrect transitions.
				} else {
					if nodeInfo.Status == NodeStatusNotStarted {
						nodeInfo.Status = NodeStatusRunning
					}
				}
				nodeInfo.UpdatedAt = workflow.Now(ctx)
			}
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

	// Spawn the initial branch. workflow.Await blocks until activeBranches
	// reaches zero or an error is captured by a branch.
	interp.activeBranches = 1
	workflow.Go(ctx, func(c workflow.Context) {
		if err := interp.executeNode(c, startNode.ID); err != nil {
			interp.workflowErr = err
		}
		interp.activeBranches--
	})

	_ = workflow.Await(ctx, func() bool {
		return interp.activeBranches == 0 || interp.workflowErr != nil
	})

	if interp.workflowErr != nil {
		instance.Status = StatusFailed
		return instance, interp.workflowErr
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

// transitionTo records an edge firing and spawns a new coroutine to execute
// the target node. activeBranches is incremented before the goroutine starts
// so workflow.Await cannot prematurely unblock.
func (g *graphInterpreter) transitionTo(ctx workflow.Context, edge Edge) error {
	g.edgeTokens[edge.ID]++
	g.activeBranches++
	workflow.Go(ctx, func(c workflow.Context) {
		if err := g.executeNode(c, edge.TargetID); err != nil {
			if g.workflowErr == nil {
				g.workflowErr = err
			}
		}
		g.activeBranches--
	})
	return nil
}

func (g *graphInterpreter) executeNode(ctx workflow.Context, nodeID string) error {
	nodeInfo := g.instance.NodeInfo[nodeID]
	node, exists := g.nodes[nodeID]

	if !exists || nodeInfo == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	// TASK nodes manage their own status transitions internally.
	if node.Type != NodeTypeTask {
		nodeInfo.Status = NodeStatusRunning
		nodeInfo.UpdatedAt = workflow.Now(ctx)
	}

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
	case NodeTypeEnd:
		err = g.handleEndNode(ctx, nodeInfo, outEdges)
	default:
		err = fmt.Errorf("unknown node type: %v", node.Type)
	}

	if err != nil {
		nodeInfo.Status = NodeStatusFailed
		return err
	}
	return nil
}

func (g *graphInterpreter) handleStartNode(ctx workflow.Context, nodeInfo *NodeInfo, outEdges []Edge) error {
	err := g.transitionTo(ctx, outEdges[0])
	if err == nil {
		nodeInfo.Status = NodeStatusCompleted
		nodeInfo.UpdatedAt = workflow.Now(ctx)
	}
	return err
}

func (g *graphInterpreter) handleEndNode(ctx workflow.Context, nodeInfo *NodeInfo, _ []Edge) error {
	err := workflow.ExecuteActivity(ctx, "WorkflowCompletedActivity", g.instance.ID, g.instance.WorkflowVariables).Get(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to complete workflow: %w", err)
	}
	nodeInfo.Status = NodeStatusCompleted
	nodeInfo.UpdatedAt = workflow.Now(ctx)
	return nil
}

func (g *graphInterpreter) handleTaskNode(ctx workflow.Context, nodeInfo *NodeInfo, node *Node, outEdges []Edge) error {
	nodeInfo.Status = NodeStatusRunning
	nodeInfo.UpdatedAt = workflow.Now(ctx)

	nodeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:          nodeInfo.ID,
		StartToCloseTimeout: 24 * time.Hour * 365,
	})

	var result map[string]any
	if err := workflow.ExecuteActivity(nodeCtx, "ExecuteTaskActivity", node.TaskTemplateID, g.instance.WorkflowVariables).Get(ctx, &result); err != nil {
		return err
	}

	// Bulk-merge all result keys first, then apply OutputMapping on top so
	// explicit remappings always win over the raw activity output keys.
	maps.Copy(g.instance.WorkflowVariables, result)

	if len(node.OutputMapping) > 0 {
		for taskKey, globalKey := range node.OutputMapping {
			if val, exists := result[taskKey]; exists {
				g.instance.WorkflowVariables[globalKey] = val
			}
		}
	}

	nodeInfo.Status = NodeStatusCompleted
	nodeInfo.UpdatedAt = workflow.Now(ctx)

	// validateDefinition guarantees exactly one outgoing edge, so [0] is safe.
	_ = g.transitionTo(ctx, outEdges[0])
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
				if err := g.transitionTo(ctx, e); err != nil {
					return err
				}
				nodeInfo.Status = NodeStatusCompleted
				nodeInfo.UpdatedAt = workflow.Now(ctx)
				return nil
			}
		}
		return fmt.Errorf("no matching conditions found at exclusive gateway %s", node.ID)

	// ParallelSplit fires all outgoing edges unconditionally (AND-split behaviour).
	case GatewayTypeParallelSplit:
		var futures []workflow.Future
		for _, e := range outEdges {
			f, s := workflow.NewFuture(ctx)
			edge := e
			workflow.Go(ctx, func(c workflow.Context) {
				s.Set(nil, g.transitionTo(c, edge))
			})
			futures = append(futures, f)
		}
		for _, f := range futures {
			if err := f.Get(ctx, nil); err != nil {
				return err
			}
		}
		nodeInfo.Status = NodeStatusCompleted
		nodeInfo.UpdatedAt = workflow.Now(ctx)
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
		if err := g.transitionTo(ctx, outEdges[0]); err != nil {
			return err
		}
		nodeInfo.Status = NodeStatusCompleted
		nodeInfo.UpdatedAt = workflow.Now(ctx)
		return nil

	case GatewayTypeExclusiveJoin:
		for _, e := range inEdges {
			if g.edgeTokens[e.ID] > 0 {
				g.edgeTokens[e.ID]--
				break
			}
		}
		if err := g.transitionTo(ctx, outEdges[0]); err != nil {
			return err
		}
		nodeInfo.Status = NodeStatusCompleted
		nodeInfo.UpdatedAt = workflow.Now(ctx)
		return nil

	default:
		return fmt.Errorf("unknown gateway type: %v", node.GatewayType)
	}
}
