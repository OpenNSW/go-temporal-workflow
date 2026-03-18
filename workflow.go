package engine

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// GraphInterpreterWorkflow is the generic Temporal workflow that executes the JSON DAG.
func GraphInterpreterWorkflow(ctx workflow.Context, def WorkflowDefinition, initialContext map[string]any) (*WorkflowInstance, error) {
	// Initialize instance state
	instance := &WorkflowInstance{
		ID:            workflow.GetInfo(ctx).WorkflowExecution.ID,
		Status:        StatusRunning,
		GlobalContext: initialContext,
		AuditTrail:    make([]string, 0),
		EdgeTokens:    make(map[string]int),
	}

	// Setup Query handler
	workflow.SetQueryHandler(ctx, "GetStatus", func() (*WorkflowInstance, error) {
		return instance, nil
	})

	// Setup Signal listener for background audit trail updates
	signalChan := workflow.GetSignalChannel(ctx, "TaskUpdateSignal")
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			var msg string
			signalChan.Receive(ctx, &msg)
			instance.AuditTrail = append(instance.AuditTrail, msg)
		}
	})

	ao := workflow.ActivityOptions{StartToCloseTimeout: 24 * time.Hour * 365}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Helper functions for graph lookups
	getNodeByID := func(id string) *Node {
		for _, n := range def.Nodes {
			if n.ID == id {
				return &n
			}
		}
		return nil
	}

	getOutgoingEdges := func(nodeID string) []Edge {
		var edges []Edge
		for _, e := range def.Edges {
			if e.SourceID == nodeID {
				edges = append(edges, e)
			}
		}
		return edges
	}

	getIncomingEdges := func(nodeID string) []Edge {
		var edges []Edge
		for _, e := range def.Edges {
			if e.TargetID == nodeID {
				edges = append(edges, e)
			}
		}
		return edges
	}

	var executeNode func(ctx workflow.Context, nodeID string) error

	// Places a token on an edge and then proceeds to the target node
	transitionTo := func(ctx workflow.Context, edge Edge) error {
		instance.EdgeTokens[edge.ID]++
		return executeNode(ctx, edge.TargetID)
	}

	executeNode = func(ctx workflow.Context, nodeID string) error {
		node := getNodeByID(nodeID)
		if node == nil {
			return fmt.Errorf("node %s not found", nodeID)
		}

		outEdges := getOutgoingEdges(node.ID)

		switch node.Type {
		case NodeTypeStart:
			if len(outEdges) == 0 {
				return fmt.Errorf("START node has no outgoing edges")
			}
			return transitionTo(ctx, outEdges[0])

		case NodeTypeTask:
			var result map[string]any

			// 1. Create a specific context for this node to force the ActivityID
			nodeActOpts := workflow.ActivityOptions{
				ActivityID:          node.ID, // Forces Temporal to use your exact Node ID
				StartToCloseTimeout: 24 * time.Hour * 365,
			}
			nodeCtx := workflow.WithActivityOptions(ctx, nodeActOpts)

			// 2. Use nodeCtx instead of the generic ctx
			err := workflow.ExecuteActivity(nodeCtx, "ExecuteTaskActivity", node.TaskID, instance.GlobalContext).Get(ctx, &result)
			if err != nil {
				return err
			}

			// Map Task Outputs to Global Context
			if len(node.OutputMapping) > 0 && result != nil {
				for taskKey, globalKey := range node.OutputMapping {
					if val, exists := result[taskKey]; exists {
						instance.GlobalContext[globalKey] = val
					}
				}
			}

			// Transition to next node if available
			if len(outEdges) > 0 {
				return transitionTo(ctx, outEdges[0])
			}
			return nil

		case NodeTypeGateway:
			switch node.GatewayType {
			case GatewayTypeExclusiveSplit:
				// Pick the FIRST condition that matches
				for _, e := range outEdges {
					match, err := EvaluateCondition(e.Condition, instance.GlobalContext)
					if err != nil {
						return err
					}
					if match {
						return transitionTo(ctx, e)
					}
				}
				return fmt.Errorf("no matching conditions found at exclusive gateway %s", node.ID)

			case GatewayTypeParallelSplit:
				// Spawn a coroutine for EVERY matching condition
				var futures []workflow.Future
				for _, e := range outEdges {
					match, err := EvaluateCondition(e.Condition, instance.GlobalContext)
					if err != nil {
						return err
					}
					if match {
						f, s := workflow.NewFuture(ctx)
						edge := e // capture locally for coroutine closure
						workflow.Go(ctx, func(c workflow.Context) {
							err := transitionTo(c, edge)
							s.Set(nil, err)
						})
						futures = append(futures, f)
					}
				}

				// Await completion of spawned parallel branches (within this node scope)
				for _, f := range futures {
					if err := f.Get(ctx, nil); err != nil {
						return err
					}
				}
				return nil

			case GatewayTypeParallelJoin:
				inEdges := getIncomingEdges(node.ID)

				// 1. Check if ALL incoming edges have a token
				for _, e := range inEdges {
					if instance.EdgeTokens[e.ID] <= 0 {
						return nil // End coroutine. Another branch arriving later will continue.
					}
				}

				// 2. Consume all tokens safely
				for _, e := range inEdges {
					instance.EdgeTokens[e.ID]--
				}

				// 3. Proceed
				if len(outEdges) > 0 {
					return transitionTo(ctx, outEdges[0])
				}
				return nil

			case GatewayTypeExclusiveJoin:
				inEdges := getIncomingEdges(node.ID)

				// Consume ONLY the token that brought us here
				for _, e := range inEdges {
					if instance.EdgeTokens[e.ID] > 0 {
						instance.EdgeTokens[e.ID]--
						break
					}
				}

				if len(outEdges) > 0 {
					return transitionTo(ctx, outEdges[0])
				}
				return nil
			}

		case NodeTypeEnd:
			return workflow.ExecuteActivity(ctx, "WorkflowCompletedActivity", instance.ID, instance.GlobalContext).Get(ctx, nil)
		}

		return nil
	}

	// 1. Find START Node
	var startNode *Node
	for _, n := range def.Nodes {
		if n.Type == NodeTypeStart {
			startNode = &n
			break
		}
	}

	if startNode == nil {
		instance.Status = StatusFailed
		return instance, fmt.Errorf("no START node found")
	}

	// 2. Begin Execution Traversal
	if err := executeNode(ctx, startNode.ID); err != nil {
		instance.Status = StatusFailed
		return instance, err
	}

	instance.Status = StatusCompleted
	return instance, nil
}
