package engine

// Core Node Types
type NodeType string

const (
	NodeTypeStart   NodeType = "START"
	NodeTypeEnd     NodeType = "END"
	NodeTypeTask    NodeType = "TASK"
	NodeTypeGateway NodeType = "GATEWAY"
)

// Gateway Types
type GatewayType string

const (
	GatewayTypeExclusiveSplit GatewayType = "EXCLUSIVE_SPLIT" // XOR Split
	GatewayTypeParallelSplit  GatewayType = "PARALLEL_SPLIT"  // AND Split
	GatewayTypeExclusiveJoin  GatewayType = "EXCLUSIVE_JOIN"  // XOR Join
	GatewayTypeParallelJoin   GatewayType = "PARALLEL_JOIN"   // AND Join
)

// Node represents a step in the workflow graph.
type Node struct {
	ID             string            `json:"id"`
	Type           NodeType          `json:"type"`                       // START, END, TASK, or GATEWAY
	GatewayType    GatewayType       `json:"gateway_type,omitempty"`     // See Gateway Types constants
	TaskTemplateID string            `json:"task_template_id,omitempty"` // Identifier for the task template to run
	InputMapping   map[string]string `json:"input_mapping,omitempty"`    // Maps WorkflowVariables Key -> Task Input Key
	OutputMapping  map[string]string `json:"output_mapping,omitempty"`   // Maps Task Output Key -> WorkflowVariables Key
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
