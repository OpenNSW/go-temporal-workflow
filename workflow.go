package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/workflow"
)

type splitInvocation struct {
	groupKey      string
	pairedJoinID  string
	expected      int
	completed     int
	branchResults []map[string]any // index -> branch's iter.local snapshot
	firstError    error            // for fail_fast mode
	failureMode   string
}

type iterationContext struct {
	GroupKey     string
	Index        int
	Item         any            // nil if CountVariable was used
	Local        map[string]any // per-branch scratch
	PairedJoinID string         // the join this branch is headed toward
	IterationKey string         // e.g. "_iter"
}

// graphInterpreter holds the state for a single workflow execution.
type graphInterpreter struct {
	def              WorkflowDefinition
	instance         *WorkflowInstance
	edgeTokens       map[string]int
	nodes            map[string]*Node
	outEdges         map[string][]Edge
	inEdges          map[string][]Edge
	splitInvocations map[string]*splitInvocation
}

// GraphInterpreterWorkflow is the entry point for the Temporal workflow that interprets a graph definition.
func GraphInterpreterWorkflow(ctx workflow.Context, def WorkflowDefinition, initialWorkflowVariables map[string]any) (*WorkflowInstance, error) {
	if initialWorkflowVariables == nil {
		initialWorkflowVariables = make(map[string]any)
	}

	if err := validateDefinition(def); err != nil {
		instance := &WorkflowInstance{
			ID:                workflow.GetInfo(ctx).WorkflowExecution.ID,
			Status:            StatusFailed,
			WorkflowVariables: initialWorkflowVariables,
			AuditTrail:        make([]string, 0),
		}
		return instance, err
	}

	instance := &WorkflowInstance{
		ID:                workflow.GetInfo(ctx).WorkflowExecution.ID,
		Status:            StatusRunning,
		WorkflowVariables: initialWorkflowVariables,
		AuditTrail:        make([]string, 0),
		NodeInfo:          make(map[string][]*NodeInfo),
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
		instance.NodeInfo[node.ID] = []*NodeInfo{
			{
				// Create a unique ID for the node. node.ID is the ID in our template.
				ID:             node.ID + ":" + generatedUUIDs[node.ID],
				Type:           node.Type,
				GatewayType:    node.GatewayType,
				TaskTemplateID: node.TaskTemplateID,
				CreatedAt:      workflow.Now(ctx),
				UpdatedAt:      workflow.Now(ctx),
				Status:         NodeStatusNotStarted,
			},
		}
	}

	// Resolve Source and Target IDs in edges to the generated node instance IDs
	for i, edge := range def.Edges {
		sourceNodeInfoSlice, sourceExists := instance.NodeInfo[edge.SourceID]
		if !sourceExists || len(sourceNodeInfoSlice) == 0 {
			return nil, fmt.Errorf("invalid edge definition: source node '%s' not found for edge '%s'", edge.SourceID, edge.ID)
		}
		targetNodeInfoSlice, targetExists := instance.NodeInfo[edge.TargetID]
		if !targetExists || len(targetNodeInfoSlice) == 0 {
			return nil, fmt.Errorf("invalid edge definition: target node '%s' not found for edge '%s'", edge.TargetID, edge.ID)
		}
		instance.Edges[i] = Edge{
			ID:        edge.ID,
			SourceID:  sourceNodeInfoSlice[0].ID,
			TargetID:  targetNodeInfoSlice[0].ID,
			Condition: edge.Condition,
		}
	}

	// Initialize our interpreter struct
	interp := &graphInterpreter{
		def:              def,
		instance:         instance,
		edgeTokens:       make(map[string]int),
		splitInvocations: make(map[string]*splitInvocation),
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

	if err := interp.executeNode(ctx, startNode.ID, nil); err != nil {
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

func (g *graphInterpreter) transitionTo(ctx workflow.Context, edge Edge, iter *iterationContext) error {
	g.edgeTokens[edge.ID]++
	return g.executeNode(ctx, edge.TargetID, iter)
}

func (g *graphInterpreter) executeNode(ctx workflow.Context, nodeID string, iter *iterationContext) error {
	nodeInfo := g.ensureInstanceNodeInfo(ctx, nodeID, iter)
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
		err = g.handleStartNode(ctx, nodeInfo, outEdges, iter)
	case NodeTypeTask:
		err = g.handleTaskNode(ctx, nodeInfo, node, outEdges, iter)
	case NodeTypeGateway:
		err = g.handleGatewayNode(ctx, nodeInfo, node, outEdges, iter)
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
func (g *graphInterpreter) handleStartNode(ctx workflow.Context, nodeInfo *NodeInfo, outEdges []Edge, iter *iterationContext) error {
	if len(outEdges) == 0 {
		return fmt.Errorf("START node has no outgoing edges")
	}
	nodeInfo.Status = NodeStatusCompleted
	nodeInfo.UpdatedAt = workflow.Now(ctx)
	return g.transitionTo(ctx, outEdges[0], iter)
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

func (g *graphInterpreter) mapTaskInputs(inputMapping map[string]string, iter *iterationContext) (map[string]any, error) {
	inputs := make(map[string]any, len(inputMapping))
	if len(inputMapping) == 0 {
		return inputs, nil
	}

	for globalKey, localKey := range inputMapping {
		val, ok := g.resolveVariable(globalKey, iter)
		if !ok {
			return nil, fmt.Errorf("input mapping error: %q not found", globalKey)
		}
		setNestedKey(inputs, localKey, val)
	}

	return inputs, nil
}

func (g *graphInterpreter) resolveVariable(path string, iter *iterationContext) (any, bool) {
	if iter != nil && strings.HasPrefix(path, iter.IterationKey+".") {
		rest := path[len(iter.IterationKey)+1:]
		switch {
		case rest == "index":
			return iter.Index, true
		case rest == "item":
			return iter.Item, true
		case rest == "local":
			return iter.Local, true
		case strings.HasPrefix(rest, "item."):
			if m, ok := iter.Item.(map[string]any); ok {
				return getNestedKey(m, rest[len("item."):])
			}
			return nil, false
		case strings.HasPrefix(rest, "local."):
			return getNestedKey(iter.Local, rest[len("local."):])
		}
	}
	return getNestedKey(g.instance.WorkflowVariables, path)
}

func (g *graphInterpreter) mapTaskOutputs(
	workflowVars map[string]any,
	outputMapping map[string]string,
	result map[string]any,
	iter *iterationContext,
) error {
	if len(outputMapping) == 0 || result == nil {
		return nil
	}

	for taskKey, globalKey := range outputMapping {
		val, exists := getNestedKey(result, taskKey)
		if !exists {
			return fmt.Errorf("output mapping error: required task variable '%s' not found in task result", taskKey)
		}

		if iter != nil && strings.HasPrefix(globalKey, iter.IterationKey+".") {
			rest := globalKey[len(iter.IterationKey)+1:]
			if rest == "index" || rest == "item" || strings.HasPrefix(rest, "item.") {
				return fmt.Errorf("output mapping error: cannot write to read-only iteration key %q", globalKey)
			}
			if rest == "local" {
				if m, ok := val.(map[string]any); ok {
					iter.Local = m
				} else {
					return fmt.Errorf("output mapping error: cannot write non-map value to %q", globalKey)
				}
			} else if strings.HasPrefix(rest, "local.") {
				setNestedKey(iter.Local, rest[len("local."):], val)
			} else {
				return fmt.Errorf("output mapping error: invalid write to iteration key %q", globalKey)
			}
		} else {
			setNestedKey(workflowVars, globalKey, val)
		}
	}
	return nil
}

func (g *graphInterpreter) handleTaskNode(ctx workflow.Context, nodeInfo *NodeInfo, node *Node, outEdges []Edge, iter *iterationContext) error {
	activityID := nodeInfo.ID
	nodeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:          activityID,
		StartToCloseTimeout: 24 * time.Hour * 365,
	})

	inputs, err := g.mapTaskInputs(node.InputMapping, iter)
	if err != nil {
		return err
	}

	var result map[string]any
	err = workflow.ExecuteActivity(nodeCtx, "ExecuteTaskActivity", node.TaskTemplateID, inputs).Get(ctx, &result)
	if err != nil {
		return err
	}

	err = g.mapTaskOutputs(g.instance.WorkflowVariables, node.OutputMapping, result, iter)
	if err != nil {
		return err
	}

	nodeInfo.Status = NodeStatusCompleted
	nodeInfo.UpdatedAt = workflow.Now(ctx)

	if len(outEdges) > 0 {
		return g.transitionTo(ctx, outEdges[0], iter)
	}
	return nil
}

func (g *graphInterpreter) handleGatewayNode(ctx workflow.Context, nodeInfo *NodeInfo, node *Node, outEdges []Edge, iter *iterationContext) error {
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
				return g.transitionTo(ctx, e, iter)
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
					err := g.transitionTo(c, edge, iter)
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
			return g.transitionTo(ctx, outEdges[0], iter)
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
			return g.transitionTo(ctx, outEdges[0], iter)
		}
		return nil

	case GatewayTypeDynamicSplit:
		cfg := node.DynamicSplit
		if cfg == nil {
			return fmt.Errorf("DYNAMIC_SPLIT node %s missing dynamic_split config", node.ID)
		}

		n, items, err := g.resolveIterationSize(cfg)
		if err != nil {
			return err
		}

		// Determine group key deterministically. Use SideEffect so the UUID is
		// recorded in workflow history and replays consistently.
		var groupKey string
		workflow.SideEffect(ctx, func(workflow.Context) any {
			return uuid.NewString()
		}).Get(&groupKey)

		inv := &splitInvocation{
			groupKey:      groupKey,
			pairedJoinID:  cfg.PairedJoinID,
			expected:      n,
			branchResults: make([]map[string]any, n),
			failureMode:   defaultIfEmpty(g.nodes[cfg.PairedJoinID].DynamicJoin.FailureMode, "fail_fast"),
		}
		g.splitInvocations[node.ID] = inv

		nodeInfo.Status = NodeStatusCompleted
		nodeInfo.UpdatedAt = workflow.Now(ctx)

		if n == 0 {
			// Zero-iteration shortcut: skip the region entirely, transition the
			// paired join directly. Aggregate empty result.
			return g.completeDynamicJoin(ctx, node.ID, cfg.PairedJoinID, inv)
		}

		if len(outEdges) != 1 {
			return fmt.Errorf("DYNAMIC_SPLIT %s must have exactly one outgoing edge", node.ID)
		}
		firstEdge := outEdges[0]

		var futures []workflow.Future
		for i := 0; i < n; i++ {
			idx := i
			f, settable := workflow.NewFuture(ctx)
			branchIter := &iterationContext{
				GroupKey:     groupKey,
				Index:        idx,
				Item:         itemAt(items, idx),
				Local:        make(map[string]any),
				PairedJoinID: cfg.PairedJoinID,
				IterationKey: defaultIfEmpty(cfg.IterationKey, "_iter"),
			}
			workflow.Go(ctx, func(c workflow.Context) {
				g.edgeTokens[firstEdge.ID]++
				err := g.executeNode(c, firstEdge.TargetID, branchIter)
				// Capture branch's final local state for aggregation
				inv.branchResults[idx] = branchIter.Local
				settable.Set(nil, err)
			})
			futures = append(futures, f)
		}

		var firstErr error
		for _, f := range futures {
			if err := f.Get(ctx, nil); err != nil && firstErr == nil {
				firstErr = err
				if inv.failureMode == "fail_fast" {
					return err
				}
			}
		}
		if firstErr != nil {
			if inv.failureMode == "collect_all" {
				joinNode := g.nodes[cfg.PairedJoinID]
				joinCfg := joinNode.DynamicJoin
				if joinCfg != nil && joinCfg.ResultsVariable != "" {
					results := make([]any, len(inv.branchResults))
					for i, r := range inv.branchResults {
						results[i] = r
					}
					setNestedKey(g.instance.WorkflowVariables, joinCfg.ResultsVariable, results)
				}
			}
			return firstErr
		}
		return nil

	case GatewayTypeDynamicJoin:
		cfg := node.DynamicJoin
		if cfg == nil {
			return fmt.Errorf("DYNAMIC_JOIN node %s missing dynamic_join config", node.ID)
		}
		inv, ok := g.splitInvocations[cfg.PairedSplitID]
		if !ok {
			return fmt.Errorf("DYNAMIC_JOIN %s: no active invocation for split %s", node.ID, cfg.PairedSplitID)
		}
		inv.completed++

		// Update or create per-iteration NodeInfo for this join
		iterJoinInfo := g.ensureInstanceNodeInfo(ctx, node.ID, iter)
		if iterJoinInfo != nil {
			iterJoinInfo.Status = NodeStatusCompleted
			iterJoinInfo.UpdatedAt = workflow.Now(ctx)
		}

		if inv.completed < inv.expected {
			// Wait — other branches still in flight. This branch's goroutine
			// returns; the split's f.Get will keep waiting for siblings.
			return nil
		}

		// Last branch: finalize and transition out of the join.
		return g.completeDynamicJoin(ctx, cfg.PairedSplitID, node.ID, inv)

	default:
		return fmt.Errorf("unknown gateway type: %v", node.GatewayType)
	}
}

func (g *graphInterpreter) completeDynamicJoin(
	ctx workflow.Context,
	splitID, joinID string,
	inv *splitInvocation,
) error {
	joinNode := g.nodes[joinID]
	cfg := joinNode.DynamicJoin

	if cfg.ResultsVariable != "" {
		// Aggregate per-branch local maps into []map[string]any
		results := make([]any, len(inv.branchResults))
		for i, r := range inv.branchResults {
			results[i] = r
		}
		setNestedKey(g.instance.WorkflowVariables, cfg.ResultsVariable, results)
	}

	// Mark the join's first (or only) NodeInfo entry as completed for the
	// "outer" workflow view.
	joinNodeInfo := g.ensureInstanceNodeInfo(ctx, joinID, nil)
	if joinNodeInfo != nil {
		joinNodeInfo.Status = NodeStatusCompleted
		joinNodeInfo.UpdatedAt = workflow.Now(ctx)
	}

	delete(g.splitInvocations, splitID)

	outEdges := g.outEdges[joinID]
	if len(outEdges) == 0 {
		return nil
	}
	if len(outEdges) != 1 {
		return fmt.Errorf("DYNAMIC_JOIN %s must have exactly one outgoing edge", joinID)
	}
	// Transition with iter=nil — we're back in the outer scope.
	return g.transitionTo(ctx, outEdges[0], nil)
}

func (g *graphInterpreter) resolveIterationSize(cfg *DynamicSplitConfig) (int, []any, error) {
	if cfg.CountVariable != "" && cfg.ItemsVariable != "" {
		return 0, nil, fmt.Errorf("dynamic split: cannot set both count_variable and items_variable")
	}
	if cfg.CountVariable != "" {
		v, ok := getNestedKey(g.instance.WorkflowVariables, cfg.CountVariable)
		if !ok {
			return 0, nil, fmt.Errorf("dynamic split: count_variable %q not found", cfg.CountVariable)
		}
		n, err := toInt(v)
		if err != nil {
			return 0, nil, fmt.Errorf("dynamic split: count_variable %q: %w", cfg.CountVariable, err)
		}
		if n < 0 {
			return 0, nil, fmt.Errorf("dynamic split: count_variable %q is negative", cfg.CountVariable)
		}
		return n, nil, nil
	}
	if cfg.ItemsVariable != "" {
		v, ok := getNestedKey(g.instance.WorkflowVariables, cfg.ItemsVariable)
		if !ok {
			return 0, nil, fmt.Errorf("dynamic split: items_variable %q not found", cfg.ItemsVariable)
		}
		items, ok := v.([]any)
		if !ok {
			return 0, nil, fmt.Errorf("dynamic split: items_variable %q is not []any", cfg.ItemsVariable)
		}
		return len(items), items, nil
	}
	return 0, nil, fmt.Errorf("dynamic split: must set one of count_variable / items_variable")
}

func (g *graphInterpreter) ensureInstanceNodeInfo(
	ctx workflow.Context,
	templateNodeID string,
	iter *iterationContext,
) *NodeInfo {
	if iter == nil {
		slice := g.instance.NodeInfo[templateNodeID]
		if len(slice) == 0 {
			return nil
		}
		return slice[0]
	}
	instances := g.instance.NodeInfo[templateNodeID]
	// Pad slice if needed (branches may arrive out of order)
	for len(instances) <= iter.Index {
		instances = append(instances, nil)
	}
	if instances[iter.Index] == nil || (iter.Index == 0 && instances[0].GroupKey == "") {
		template := g.nodes[templateNodeID]
		instances[iter.Index] = &NodeInfo{
			ID:             fmt.Sprintf("%s:%s:%d", templateNodeID, iter.GroupKey, iter.Index),
			Type:           template.Type,
			GatewayType:    template.GatewayType,
			TaskTemplateID: template.TaskTemplateID,
			CreatedAt:      workflow.Now(ctx),
			UpdatedAt:      workflow.Now(ctx),
			Status:         NodeStatusRunning,
			GroupKey:       iter.GroupKey,
			IterationIndex: iter.Index,
		}
	}
	g.instance.NodeInfo[templateNodeID] = instances
	return instances[iter.Index]
}

func validateDefinition(def WorkflowDefinition) error {
	nodes := make(map[string]Node)
	for _, n := range def.Nodes {
		nodes[n.ID] = n
	}

	outEdges := make(map[string][]Edge)
	inEdges := make(map[string][]Edge)
	for _, e := range def.Edges {
		outEdges[e.SourceID] = append(outEdges[e.SourceID], e)
		inEdges[e.TargetID] = append(inEdges[e.TargetID], e)
	}

	splits := make(map[string]Node)
	joins := make(map[string]Node)
	splitToJoin := make(map[string]string)
	joinToSplit := make(map[string]string)

	for _, n := range def.Nodes {
		if n.Type == NodeTypeGateway {
			if n.GatewayType == GatewayTypeDynamicSplit {
				splits[n.ID] = n
				if n.DynamicSplit == nil {
					return fmt.Errorf("node %s has GatewayType DYNAMIC_SPLIT but is missing dynamic_split config", n.ID)
				}
				if n.DynamicSplit.PairedJoinID == "" {
					return fmt.Errorf("node %s dynamic_split config missing paired_join_id", n.ID)
				}
				splitToJoin[n.ID] = n.DynamicSplit.PairedJoinID
			} else if n.GatewayType == GatewayTypeDynamicJoin {
				joins[n.ID] = n
				if n.DynamicJoin == nil {
					return fmt.Errorf("node %s has GatewayType DYNAMIC_JOIN but is missing dynamic_join config", n.ID)
				}
				if n.DynamicJoin.PairedSplitID == "" {
					return fmt.Errorf("node %s dynamic_join config missing paired_split_id", n.ID)
				}
				joinToSplit[n.ID] = n.DynamicJoin.PairedSplitID
			}
		}
	}

	// 1. & 2. Bijective pairing validation
	for sID, jID := range splitToJoin {
		jNode, ok := joins[jID]
		if !ok {
			return fmt.Errorf("split %s references non-existent or invalid paired join %s", sID, jID)
		}
		if jNode.DynamicJoin.PairedSplitID != sID {
			return fmt.Errorf("split %s paired with join %s, but join is paired with %s", sID, jID, jNode.DynamicJoin.PairedSplitID)
		}
	}
	for jID, sID := range joinToSplit {
		_, ok := splits[sID]
		if !ok {
			return fmt.Errorf("join %s references non-existent or invalid paired split %s", jID, sID)
		}
	}

	// 6. Exactly one of CountVariable / ItemsVariable is set
	for _, n := range splits {
		cfg := n.DynamicSplit
		if (cfg.CountVariable == "" && cfg.ItemsVariable == "") || (cfg.CountVariable != "" && cfg.ItemsVariable != "") {
			return fmt.Errorf("dynamic split %s must set exactly one of count_variable or items_variable", n.ID)
		}
	}

	// For each pair, do validation
	for sID, jID := range splitToJoin {
		// 3. Exactly one outgoing edge from split.
		sOut := outEdges[sID]
		if len(sOut) != 1 {
			return fmt.Errorf("split %s must have exactly one outgoing edge, found %d", sID, len(sOut))
		}

		// Find the nodes inside the region by doing a DFS/BFS from the split to the join
		visited := make(map[string]bool)
		var regionNodes []string
		var findRegion func(curr string) error
		findRegion = func(curr string) error {
			if curr == jID {
				return nil
			}
			if curr != sID {
				if visited[curr] {
					return nil
				}
				visited[curr] = true
				regionNodes = append(regionNodes, curr)
			}
			for _, e := range outEdges[curr] {
				if nodes[e.TargetID].GatewayType == GatewayTypeDynamicSplit {
					// 5. Nested fan-outs are rejected
					return fmt.Errorf("nested dynamic splits are not supported: split %s is nested inside split %s", e.TargetID, sID)
				}
				if err := findRegion(e.TargetID); err != nil {
					return err
				}
			}
			return nil
		}

		if err := findRegion(sID); err != nil {
			return err
		}

		// 3. Exactly one incoming edge on join from inside the region.
		inToJoinFromRegion := 0
		for _, e := range inEdges[jID] {
			if e.SourceID == sID || visited[e.SourceID] {
				inToJoinFromRegion++
			}
		}
		if inToJoinFromRegion != 1 {
			return fmt.Errorf("join %s must have exactly one incoming edge from inside its paired split region, found %d", jID, inToJoinFromRegion)
		}

		// 4. Single-entry, single-exit subgraph validation.
		for _, rNodeID := range regionNodes {
			for _, e := range inEdges[rNodeID] {
				if e.SourceID != sID && !visited[e.SourceID] {
					return fmt.Errorf("region node %s has an incoming edge %s from outside the split region (source: %s)", rNodeID, e.ID, e.SourceID)
				}
			}
			for _, e := range outEdges[rNodeID] {
				if e.TargetID != jID && !visited[e.TargetID] {
					return fmt.Errorf("region node %s has an outgoing edge %s to outside the split region (target: %s)", rNodeID, e.ID, e.TargetID)
				}
			}
		}
	}

	return nil
}

func defaultIfEmpty(val, fallback string) string {
	if val == "" {
		return fallback
	}
	return val
}

func itemAt(items []any, idx int) any {
	if idx < 0 || idx >= len(items) {
		return nil
	}
	return items[idx]
}

func toInt(v any) (int, error) {
	switch val := v.(type) {
	case int:
		return val, nil
	case int32:
		return int(val), nil
	case int64:
		return int(val), nil
	case float64:
		return int(val), nil
	case float32:
		return int(val), nil
	default:
		return 0, fmt.Errorf("value %v is not an integer", v)
	}
}
