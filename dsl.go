package engine

// Core Node Types
const (
	NodeTypeStart   = "START"
	NodeTypeEnd     = "END"
	NodeTypeTask    = "TASK"
	NodeTypeGateway = "GATEWAY"
)

// Gateway Types
const (
	GatewayTypeExclusiveSplit = "EXCLUSIVE_SPLIT" // XOR Split
	GatewayTypeParallelSplit  = "PARALLEL_SPLIT"  // AND Split
	GatewayTypeExclusiveJoin  = "EXCLUSIVE_JOIN"  // XOR Join
	GatewayTypeParallelJoin   = "PARALLEL_JOIN"   // AND Join
)

// Workflow Statuses
const (
	StatusRunning   = "RUNNING"
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
)

// Node represents a step in the workflow graph.
type Node struct {
	ID            string            `json:"id"`
	Type          string            `json:"type"`                     // START, END, TASK, or GATEWAY
	GatewayType   string            `json:"gateway_type,omitempty"`   // See Gateway Types constants
	TaskID        string            `json:"task_id,omitempty"`        // Identifier for the task to run
	OutputMapping map[string]string `json:"output_mapping,omitempty"` // Maps Task Output Key -> WorkflowVariables Key
}

// Edge represents a directed connection between two nodes.
type Edge struct {
	ID        string `json:"id"`
	SourceID  string `json:"source_id"`
	TargetID  string `json:"target_id"`
	Condition string `json:"condition,omitempty"` // Expression mapped against WorkflowVariables
}

// WorkflowDefinition is the parsed structural definition of the JSON DSL.
type WorkflowDefinition struct {
	WorkflowID string `json:"workflow_id"`
	Name       string `json:"name"`
	Version    int    `json:"version"`
	Nodes      []Node `json:"nodes"`
	Edges      []Edge `json:"edges"`
}

// WorkflowInstance holds the dynamic runtime state of the workflow execution.
type WorkflowInstance struct {
	ID                string         `json:"id"`
	Status            string         `json:"status"`
	WorkflowVariables map[string]any `json:"workflow_variables"`
	AuditTrail        []string       `json:"audit_trail"`
	EdgeTokens        map[string]int `json:"edge_tokens"` // Tracks tokens for synchronizing Parallel Joins
}
