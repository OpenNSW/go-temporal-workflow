package engine

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"go.temporal.io/sdk/workflow"
)

// --- Types ---

// splitInvocation tracks runtime state for an active DYNAMIC_SPLIT invocation.
type splitInvocation struct {
	expected      int
	branchResults []map[string]any // index -> branch's iter.local snapshot
	failureMode   string
}

// iterationContext carries per-branch state for a single parallel execution
// inside a DYNAMIC_SPLIT region.
type iterationContext struct {
	GroupKey       string
	GroupItemIndex int
	Item           any            // nil if CountVariable was used
	Local          map[string]any // per-branch scratch
	IterationKey   string         // e.g. "_iter"
}

// --- Gateway handlers ---

// handleDynamicSplit orchestrates the fan-out: resolves the iteration size,
// spawns N parallel branches, waits for all futures, and aggregates results.
func (g *graphInterpreter) handleDynamicSplit(ctx workflow.Context, nodeInfo *NodeInfo, node *Node, outEdges []Edge) error {
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

	failureMode := FailureModeFailFast
	if joinNode := g.nodes[cfg.PairedJoinID]; joinNode != nil && joinNode.DynamicJoin != nil && joinNode.DynamicJoin.FailureMode != "" {
		failureMode = joinNode.DynamicJoin.FailureMode
	}

	inv := &splitInvocation{
		expected:      n,
		branchResults: make([]map[string]any, n),
		failureMode:   failureMode,
	}
	g.splitInvocations[node.ID] = inv

	nodeInfo.Status = NodeStatusCompleted
	nodeInfo.UpdatedAt = workflow.Now(ctx)

	if n == 0 {
		// Zero-iteration shortcut: this can happen if the items list is empty (e.g., a shipment has 0 containers).
		// Instead of blocking indefinitely waiting for 0 branches, we skip the region entirely, transition
		// the paired join directly, and aggregate an empty result slice.
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
			GroupKey:       groupKey,
			GroupItemIndex: idx,
			Item:           itemAt(items, idx),
			Local:          make(map[string]any),
			IterationKey:   defaultIfEmpty(cfg.IterationKey, "_iter"),
		}
		workflow.Go(ctx, func(c workflow.Context) {
			err := g.transitionTo(c, firstEdge, branchIter)
			// Capture branch's final local state for aggregation
			inv.branchResults[idx] = branchIter.Local
			settable.Set(nil, err)
		})
		futures = append(futures, f)
	}

	// Wait on branches by completion order (not index order) so fail_fast can
	// return the moment any branch errors, rather than blocking on a slow
	// lower-indexed branch (e.g. one still awaiting a Customs ack).
	var firstErr error
	selector := workflow.NewSelector(ctx)
	for _, f := range futures {
		selector.AddFuture(f, func(f workflow.Future) {
			if err := f.Get(ctx, nil); err != nil && firstErr == nil {
				firstErr = err
			}
		})
	}
	for i := 0; i < n; i++ {
		selector.Select(ctx)
		if firstErr != nil && inv.failureMode == FailureModeFailFast {
			return firstErr
		}
	}
	if firstErr != nil {
		if inv.failureMode == FailureModeCollectAll {
			g.aggregateBranchResults(inv, cfg.PairedJoinID)
		}
		return firstErr
	}

	// All branches successfully completed -> safe to aggregate results and transition out
	return g.completeDynamicJoin(ctx, node.ID, cfg.PairedJoinID, inv)
}

// handleDynamicJoin records a branch arrival at the join. The actual transition
// past the join is handled by the DYNAMIC_SPLIT epilogue after all futures resolve.
func (g *graphInterpreter) handleDynamicJoin(ctx workflow.Context, nodeInfo *NodeInfo, node *Node, iter *iterationContext) error {
	cfg := node.DynamicJoin
	if cfg == nil {
		return fmt.Errorf("DYNAMIC_JOIN node %s missing dynamic_join config", node.ID)
	}
	if _, ok := g.splitInvocations[cfg.PairedSplitID]; !ok {
		return fmt.Errorf("DYNAMIC_JOIN %s: no active invocation for split %s", node.ID, cfg.PairedSplitID)
	}

	// Record arrival for this branch's parallel join node state
	iterJoinInfo := g.ensureInstanceNodeInfo(ctx, node.ID, iter)
	if iterJoinInfo != nil {
		iterJoinInfo.Status = NodeStatusCompleted
		iterJoinInfo.UpdatedAt = workflow.Now(ctx)
	}

	// Join blocks. Transition is handled in the DYNAMIC_SPLIT epilogue.
	return nil
}

// aggregateBranchResults writes each branch's iter.local snapshot into the
// join's ResultsVariable (when configured), preserving branch order by index.
// No-op if the join is missing, has no DynamicJoin config, or sets no ResultsVariable.
func (g *graphInterpreter) aggregateBranchResults(inv *splitInvocation, joinID string) {
	joinNode := g.nodes[joinID]
	if joinNode == nil || joinNode.DynamicJoin == nil || joinNode.DynamicJoin.ResultsVariable == "" {
		return
	}
	results := make([]any, len(inv.branchResults))
	for i, r := range inv.branchResults {
		results[i] = r
	}
	setNestedKey(g.instance.WorkflowVariables, joinNode.DynamicJoin.ResultsVariable, results)
}

// completeDynamicJoin aggregates branch results, marks the join completed, and
// transitions to the join's single outgoing edge (back in the outer scope).
func (g *graphInterpreter) completeDynamicJoin(
	ctx workflow.Context,
	splitID, joinID string,
	inv *splitInvocation,
) error {
	g.aggregateBranchResults(inv, joinID)

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

// --- Iteration variable resolution ---

// resolveDynamicVariable resolves a variable path against the iteration context
// (e.g. "_iter.item", "_iter.local.xxx"). Returns (value, true) if the path
// targets the iteration namespace, or (nil, false) to signal the caller should
// fall back to global workflow variables.
func (g *graphInterpreter) resolveDynamicVariable(path string, iter *iterationContext) (any, bool) {
	if !strings.HasPrefix(path, iter.IterationKey+".") {
		return nil, false
	}
	rest := path[len(iter.IterationKey)+1:]
	switch {
	case rest == "index":
		return iter.GroupItemIndex, true
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
	return nil, false
}

// mapDynamicOutputs routes an output value into the iteration-scoped local
// variables when the destination key targets the iteration namespace.
// Returns (true, nil) if the key was handled, (false, nil) if the key does not
// belong to the iteration namespace and should fall through to global variables.
func (g *graphInterpreter) mapDynamicOutputs(iter *iterationContext, globalKey string, val any) (bool, error) {
	if !strings.HasPrefix(globalKey, iter.IterationKey+".") {
		return false, nil
	}
	rest := globalKey[len(iter.IterationKey)+1:]
	if rest == "index" || rest == "item" || strings.HasPrefix(rest, "item.") {
		return false, fmt.Errorf("output mapping error: cannot write to read-only iteration key %q", globalKey)
	}
	if rest == "local" {
		if m, ok := val.(map[string]any); ok {
			iter.Local = m
		} else {
			return false, fmt.Errorf("output mapping error: cannot write non-map value to %q", globalKey)
		}
	} else if strings.HasPrefix(rest, "local.") {
		setNestedKey(iter.Local, rest[len("local."):], val)
	} else {
		return false, fmt.Errorf("output mapping error: invalid write to iteration key %q", globalKey)
	}
	return true, nil
}

// --- Iteration helpers ---

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

// ensureInstanceNodeInfo returns or creates a NodeInfo entry for a given node.
// For nodes outside a fan-out region (iter == nil), it returns the pre-allocated
// slot at index 0. For nodes inside a fan-out region, it creates or retrieves the
// execution record at the GroupItemIndex position, padding the slice as needed
// since branches may arrive out of order.
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
	for len(instances) <= iter.GroupItemIndex {
		instances = append(instances, nil)
	}
	if instances[iter.GroupItemIndex] == nil || instances[iter.GroupItemIndex].GroupKey == "" {
		template := g.nodes[templateNodeID]
		instances[iter.GroupItemIndex] = &NodeInfo{
			ID:             fmt.Sprintf("%s:%s:%d", templateNodeID, iter.GroupKey, iter.GroupItemIndex),
			Type:           template.Type,
			GatewayType:    template.GatewayType,
			TaskTemplateID: template.TaskTemplateID,
			CreatedAt:      workflow.Now(ctx),
			UpdatedAt:      workflow.Now(ctx),
			Status:         NodeStatusRunning,
			GroupKey:       iter.GroupKey,
			GroupItemIndex: iter.GroupItemIndex,
		}
	}
	g.instance.NodeInfo[templateNodeID] = instances
	return instances[iter.GroupItemIndex]
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
