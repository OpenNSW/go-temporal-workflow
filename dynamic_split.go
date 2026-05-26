package engine

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"
)

type activeBranch struct {
	Future   workflow.ChildWorkflowFuture
	BranchID string
	Index    int
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

	branchesData, err := resolveBranchesData(itemsRaw, config.ItemsVariable)
	if err != nil {
		return err
	}

	iterKey := config.IterationKey
	if iterKey == "" {
		iterKey = DefaultIterationKey
	}

	// 2. Spawn child workflow interpreters
	activeBranches, err := g.spawnChildWorkflows(ctx, node, config, branchesData, iterKey)
	if err != nil {
		return err
	}

	// 3. Monitor executions and collect outputs/errors
	aggregatedResults := make([]map[string]any, len(activeBranches))
	if err := g.monitorChildWorkflows(ctx, activeBranches, aggregatedResults, config, nodeInfo); err != nil {
		return err
	}

	nodeInfo.Status = NodeStatusCompleted
	nodeInfo.UpdatedAt = workflow.Now(ctx)

	if len(outEdges) > 0 {
		return g.transitionTo(ctx, outEdges[0])
	}
	return nil
}

// resolveBranchesData converts a raw items variable interface into a slice of interface values.
func resolveBranchesData(itemsRaw any, itemsVarName string) ([]any, error) {
	var branchesData []any
	if itemsRaw == nil {
		return nil, nil
	}

	if val, ok := itemsRaw.([]any); ok {
		return val, nil
	}

	// Convert slice of any concrete type to []any
	switch reflectVal := reflect.ValueOf(itemsRaw); reflectVal.Kind() {
	case reflect.Slice:
		branchesData = make([]any, reflectVal.Len())
		for idx := 0; idx < reflectVal.Len(); idx++ {
			branchesData[idx] = reflectVal.Index(idx).Interface()
		}
		return branchesData, nil
	default:
		return nil, fmt.Errorf("items variable '%s' is not a valid list type", itemsVarName)
	}
}

// spawnChildWorkflows spins up child interpreters for all active branches and waits for them to successfully start.
func (g *graphInterpreter) spawnChildWorkflows(
	ctx workflow.Context,
	node *Node,
	config *SplitTaskConfig,
	branchesData []any,
	iterKey string,
) (map[string]*activeBranch, error) {
	parentInfo := workflow.GetInfo(ctx)
	activeBranches := make(map[string]*activeBranch)
	branchIDs := make(map[string]bool)

	for i, itemRaw := range branchesData {
		item, err := ParseSplitTaskItem(itemRaw)
		if err != nil {
			return nil, fmt.Errorf("index point %d inside branch resolution array is invalid layout: %w", i, err)
		}

		templateID := node.TaskTemplateID
		if config.Mode == SplitModeDifferentTemplates {
			templateID = item.TemplateID
		}
		branchID := item.BranchID
		if config.Mode == SplitModeSameTemplate {
			branchID = fmt.Sprintf("%s-%d", branchID, i)
		}
		payload := item.Payload

		if templateID == "" || branchID == "" {
			return nil, fmt.Errorf("index point %d requires non-empty template_id (static or dynamic) and branch_id configurations", i)
		}

		if _, exists := branchIDs[branchID]; exists {
			return nil, fmt.Errorf("branch ID %s is duplicated", branchID)
		}
		branchIDs[branchID] = true

		// Execute Activity on-the-fly to retrieve sub-graph definition
		// TODO: Execute FetchWorkflowDefinitionActivity calls in parallel (potentially with a batch size limit) to avoid
		// sequential IO bottlenecks on larger datasets.
		var branchGraphDef WorkflowDefinition
		err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 15 * time.Second,
		}), "FetchWorkflowDefinitionActivity", templateID).Get(ctx, &branchGraphDef)
		if err != nil {
			return nil, fmt.Errorf("boundary lifecycle error hydrating definition graph for template %s: %w", templateID, err)
		}

		// Configure targeted isolation environment parameters for child execution workspace
		childVars := map[string]any{
			VarParentWorkflowID: parentInfo.WorkflowExecution.ID,
			VarSplitNodeID:      node.ID,
			iterKey: map[string]any{
				IterIndexKey:    i,
				IterBranchIDKey: branchID,
				IterInputKey:    payload,
			},
		}

		deterministicChildID := FormatChildWorkflowID(parentInfo.WorkflowExecution.ID, node.ID, branchID)
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: deterministicChildID,
		})

		// Recursively spin up core interpreter executing target hydrated sub-graph schema
		future := workflow.ExecuteChildWorkflow(childCtx, "GraphInterpreterWorkflow", branchGraphDef, childVars)
		activeBranches[deterministicChildID] = &activeBranch{
			Future:   future,
			BranchID: branchID,
			Index:    i,
		}
	}

	// Wait for all child workflows to start to ensure their execution environments (and signal handlers) are initialized
	for targetChildID, info := range activeBranches {
		var childExec workflow.Execution
		if err := info.Future.GetChildWorkflowExecution().Get(ctx, &childExec); err != nil {
			return nil, fmt.Errorf("failed to start child workflow %s: %w", targetChildID, err)
		}
	}

	return activeBranches, nil
}

// monitorChildWorkflows tracks and waits for spawned child workflows and collects outputs/errors.
func (g *graphInterpreter) monitorChildWorkflows(
	ctx workflow.Context,
	activeBranches map[string]*activeBranch,
	aggregatedResults []map[string]any,
	config *SplitTaskConfig,
	nodeInfo *NodeInfo,
) error {
	selector := workflow.NewSelector(ctx)

	completedCount := 0
	totalBranches := len(activeBranches)
	var executionError error
	var failedBranchesErrors []error

	// 4. Register active sub-workflow execution handles on monitoring tracking hooks
	for cid, info := range activeBranches {
		targetID := cid
		branchInfo := info

		selector.AddFuture(branchInfo.Future, func(wf workflow.Future) {
			var childOutput *WorkflowInstance
			err := wf.Get(ctx, &childOutput)

			if err != nil {
				executionError = fmt.Errorf("dynamic execution track %s halted abnormally: %w", targetID, err)
				failedBranchesErrors = append(failedBranchesErrors, executionError)
				aggregatedResults[branchInfo.Index] = map[string]any{
					"error":     err.Error(),
					"branch_id": branchInfo.BranchID,
				}
			} else if childOutput != nil {
				// Collect results chronologically mapped back to setup registry indexes
				aggregatedResults[branchInfo.Index] = childOutput.WorkflowVariables
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

	// 6. Commit variable mutation changes back to the primary context map layer
	if config.ResultsVariable != "" {
		setNestedKey(g.instance.WorkflowVariables, config.ResultsVariable, aggregatedResults)
	}

	if len(failedBranchesErrors) > 0 && config.FailureMode == FailureModeCollectAll {
		nodeInfo.Status = NodeStatusFailed
		logger := workflow.GetLogger(ctx)
		for _, e := range failedBranchesErrors {
			logger.Error("Split task branch execution failure", "error", e.Error())
		}
		var errMsgs []string
		for _, e := range failedBranchesErrors {
			errMsgs = append(errMsgs, e.Error())
		}
		return fmt.Errorf("multiple branches failed: [%s]", strings.Join(errMsgs, "; "))
	}

	return nil
}
