package engine

// NodeType represents the type of a workflow node (e.g. START, END, TASK, GATEWAY).
type NodeType string

// Core node types supported by the engine.
const (
	NodeTypeStart   NodeType = "START"
	NodeTypeEnd     NodeType = "END"
	NodeTypeTask    NodeType = "TASK"
	NodeTypeGateway NodeType = "GATEWAY"
)

// GatewayType represents the type of a gateway controlling execution flow.
type GatewayType string

// Gateway types controlling branching and merging.
const (
	GatewayTypeExclusiveSplit GatewayType = "EXCLUSIVE_SPLIT" // XOR Split
	GatewayTypeParallelSplit  GatewayType = "PARALLEL_SPLIT"  // AND Split
	GatewayTypeExclusiveJoin  GatewayType = "EXCLUSIVE_JOIN"  // XOR Join
	GatewayTypeParallelJoin   GatewayType = "PARALLEL_JOIN"   // AND Join
	GatewayTypeDynamicSplit   GatewayType = "DYNAMIC_SPLIT"   // NEW: Dynamic Fan-Out Split
	GatewayTypeDynamicJoin    GatewayType = "DYNAMIC_JOIN"    // NEW: Dynamic Fan-In Join
)

// DynamicSplitConfig describes how the engine should expand a DYNAMIC_SPLIT
// at runtime. Exactly one of CountVariable / ItemsVariable must be set.
type DynamicSplitConfig struct {
	// PairedJoinID is the ID of the DYNAMIC_JOIN node that closes this region.
	// Required.
	PairedJoinID string `json:"paired_join_id"`

	// CountVariable is a dot-path into WorkflowVariables resolving to an int (or
	// numeric type convertible to int). The engine spawns this many branches.
	// Mutually exclusive with ItemsVariable.
	CountVariable string `json:"count_variable,omitempty"`

	// ItemsVariable is a dot-path into WorkflowVariables resolving to a []any.
	// The engine spawns len(items) branches, exposing items[i] to branch i via
	// the iteration context (see IterationKey).
	// Mutually exclusive with CountVariable.
	ItemsVariable string `json:"items_variable,omitempty"`

	// IterationKey is the WorkflowVariables key under which the engine exposes
	// per-branch iteration state to nodes inside the region. Default: "_iter".
	// Each branch sees:
	//   <IterationKey>.index   int  (0-based)
	//   <IterationKey>.item    any  (only when ItemsVariable is set)
	//   <IterationKey>.local   map[string]any  (writable scratch space)
	IterationKey string `json:"iteration_key,omitempty"`
}

// DynamicJoinConfig describes how the engine should aggregate and resume after
// all N branches arrive at the join.
type DynamicJoinConfig struct {
	// PairedSplitID is the ID of the matching DYNAMIC_SPLIT. Required.
	PairedSplitID string `json:"paired_split_id"`

	// ResultsVariable, if set, is the dot-path under which the engine writes
	// an []map[string]any of each branch's <IterationKey>.local map after all
	// branches complete. Useful for downstream nodes that need to enumerate
	// per-branch results.
	ResultsVariable string `json:"results_variable,omitempty"`

	// FailureMode controls how the join treats branch failures.
	//   "fail_fast"   (default) — first branch failure fails the workflow
	//   "collect_all" — wait for all branches; mark workflow failed only if
	//                   any failed, but expose successes via ResultsVariable
	FailureMode string `json:"failure_mode,omitempty"`
}

const (
	// FailureModeFailFast specifies that any single branch failure immediately fails the workflow.
	FailureModeFailFast   = "FAIL_FAST"
	// FailureModeCollectAll specifies that the engine waits for all parallel branches to complete;
	// it will report failure if any branch failed, but still exposes successful branch outputs.
	FailureModeCollectAll = "COLLECT_ALL"
)

// Node represents a step in the workflow graph.
type Node struct {
	// ID must be unique within the workflow definition and must not contain the ":" character.
	ID string `json:"id"`
	// Type represents the kind of node: START, END, TASK, or GATEWAY.
	Type NodeType `json:"type"`
	// GatewayType specifies the specific gateway category, if Type is GATEWAY. See Gateway Types constants.
	GatewayType GatewayType `json:"gateway_type,omitempty"`
	// TaskTemplateID identifies the task template to run and must not contain the ":" character.
	TaskTemplateID string `json:"task_template_id,omitempty"`
	// InputMapping maps WorkflowVariables Key -> Task Input Key.
	InputMapping map[string]string `json:"input_mapping,omitempty"`
	// OutputMapping maps Task Output Key -> WorkflowVariables Key.
	OutputMapping map[string]string `json:"output_mapping,omitempty"`
	// DynamicSplit is the configuration for the DYNAMIC_SPLIT gateway.
	DynamicSplit *DynamicSplitConfig `json:"dynamic_split,omitempty"`
	// DynamicJoin is the configuration for the DYNAMIC_JOIN gateway.
	DynamicJoin *DynamicJoinConfig `json:"dynamic_join,omitempty"`
}

// Edge represents a directed connection between two nodes.
type Edge struct {
	ID        string `json:"id"`
	SourceID  string `json:"source_id"`
	TargetID  string `json:"target_id"`
	Condition string `json:"condition,omitempty"` // Expression mapped against WorkflowVariables
}

// WorkflowDefinition represents the structural blueprint of a workflow process.
// It serves as the parsed representation of the JSON DSL, defining how nodes
// and edges form a directed graph for the execution engine.
type WorkflowDefinition struct {
	// ID is the unique identifier for this specific workflow template.
	ID string `json:"id"`

	// Name is a human-readable label used for display and organizational purposes.
	Name string `json:"name"`

	// Version tracks iterations of the workflow logic, allowing for side-by-side
	// deployment of different logic versions.
	Version int `json:"version"`

	// Nodes defines the individual steps, gateways, and boundary events
	// that make up the workflow.
	Nodes []Node `json:"nodes"`

	// Edges defines the directed connections between nodes, including
	// any conditional logic required for branching.
	Edges []Edge `json:"edges"`
}
