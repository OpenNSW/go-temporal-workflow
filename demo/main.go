package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	engine "github.com/OpenNSW/go-temporal-workflow"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
)

// ActiveTask represents a pending human-in-the-loop task
type ActiveTask struct {
	WorkflowID     string         `json:"workflowId"`
	RunID          string         `json:"runId"`
	NodeID         string         `json:"nodeId"`
	TaskTemplateID string         `json:"taskTemplateId"`
	Inputs         map[string]any `json:"inputs"`
}

// Global state variables
var (
	temporalClient   client.Client
	temporalManager  engine.TemporalManager
	activeWorkflows  = []string{}
	activeWorkflowsMu sync.Mutex

	pendingTasks   = make(map[string]ActiveTask) // key: workflowId + "_" + nodeId
	pendingTasksMu sync.Mutex
)

func main() {
	// 1. Establish connection to local Temporal Server
	var err error
	temporalClient, err = client.Dial(client.Options{
		HostPort: "localhost:7233",
	})
	if err != nil {
		log.Fatalf("Unable to connect to Temporal Server on localhost:7233: %v. Please make sure Temporal server is running.", err)
	}
	defer temporalClient.Close()

	// 2. Instantiate engine's TemporalManager
	taskQueue := "consignment-demo-queue"
	temporalManager = engine.NewTemporalManager(
		temporalClient,
		taskQueue,
		taskActivationHandler,
		workflowCompletionHandler,
		fetchWorkflowDefinitionHandler,
	)

	// 3. Start the Temporal worker
	err = temporalManager.StartWorker()
	if err != nil {
		log.Fatalf("Failed to start Temporal worker: %v", err)
	}
	defer temporalManager.StopWorker()
	log.Printf("Temporal worker started on task queue: %s", taskQueue)

	// 4. Configure HTTP routing
	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/api/workflow/start", handleStartWorkflow)
	http.HandleFunc("/api/workflow/list", handleListWorkflows)
	http.HandleFunc("/api/workflow/status", handleGetWorkflowStatus)
	http.HandleFunc("/api/tasks/pending", handleGetPendingTasks)
	http.HandleFunc("/api/tasks/complete", handleCompleteTask)

	port := ":8080"
	log.Printf("Demo web server running on http://localhost%s", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

// serveIndex serves the single page HTML dashboard
func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	// Try "demo/index.html" (if run from root directory)
	if _, err := os.Stat("demo/index.html"); err == nil {
		http.ServeFile(w, r, "demo/index.html")
		return
	}
	// Fallback to "index.html" (if run from inside the demo directory)
	http.ServeFile(w, r, "index.html")
}

// taskActivationHandler intercepts workflow task executions
func taskActivationHandler(payload engine.TaskPayload) (map[string]any, error) {
	log.Printf("[TASK REACHED] NodeID: %s, TaskTemplateID: %s, WorkflowID: %s", payload.NodeID, payload.TaskTemplateID, payload.WorkflowID)

	pendingTasksMu.Lock()
	key := payload.WorkflowID + "_" + payload.NodeID
	pendingTasks[key] = ActiveTask{
		WorkflowID:     payload.WorkflowID,
		RunID:          payload.RunID,
		NodeID:         payload.NodeID,
		TaskTemplateID: payload.TaskTemplateID,
		Inputs:         payload.Inputs,
	}
	pendingTasksMu.Unlock()

	// Return activity.ErrResultPending to pause the workflow until completed via the UI
	return nil, activity.ErrResultPending
}

// workflowCompletionHandler processes finalized workflows
func workflowCompletionHandler(workflowID string, finalVariables map[string]any) error {
	log.Printf("[WORKFLOW COMPLETED] ID: %s", workflowID)
	return nil
}

// fetchWorkflowDefinitionHandler resolves definitions dynamically during split-task executions
func fetchWorkflowDefinitionHandler(templateID string) (engine.WorkflowDefinition, error) {
	def, exists := WorkflowDefinitions[templateID]
	if !exists {
		return engine.WorkflowDefinition{}, fmt.Errorf("workflow definition not found for template ID: %s", templateID)
	}
	return def, nil
}

// HTTP API Handlers

func handleStartWorkflow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	workflowID := fmt.Sprintf("consignment-%d", time.Now().Unix())

	// Configure initial workflow variables including items for split tasks (which start empty and get determined dynamically)
	initialVars := map[string]any{
		"split_items": []any{},
		"consignment": map[string]any{
			"status": "INITIATED",
		},
	}

	def := WorkflowDefinitions[TemplateMainConsignment]

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := temporalManager.StartWorkflow(ctx, workflowID, def, initialVars)
	if err != nil {
		log.Printf("Error starting workflow: %v", err)
		http.Error(w, fmt.Sprintf("Failed to start workflow: %v", err), http.StatusInternalServerError)
		return
	}

	activeWorkflowsMu.Lock()
	activeWorkflows = append([]string{workflowID}, activeWorkflows...)
	activeWorkflowsMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":     "Started",
		"workflowId": workflowID,
	})
}

func handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	activeWorkflowsMu.Lock()
	defer activeWorkflowsMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(activeWorkflows)
}

func handleGetWorkflowStatus(w http.ResponseWriter, r *http.Request) {
	workflowID := r.URL.Query().Get("id")
	if workflowID == "" {
		http.Error(w, "Missing 'id' query parameter", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Query main parent workflow status
	parentStatus, err := temporalManager.GetStatus(ctx, workflowID)
	if err != nil {
		log.Printf("Error querying status for %s: %v", workflowID, err)
		http.Error(w, fmt.Sprintf("Failed to query status: %v", err), http.StatusInternalServerError)
		return
	}

	// Dynamically construct and query child workflows statuses (if active/completed)
	children := make(map[string]*engine.WorkflowInstance)
	childBranches := []string{"customs", "phyto", "health"}
	for _, branch := range childBranches {
		childID := fmt.Sprintf("%s-split_task-%s", workflowID, branch)
		cStatus, cErr := temporalManager.GetStatus(ctx, childID)
		if cErr == nil {
			children[branch] = cStatus
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"parent":   parentStatus,
		"children": children,
	})
}

func handleGetPendingTasks(w http.ResponseWriter, r *http.Request) {
	pendingTasksMu.Lock()
	defer pendingTasksMu.Unlock()

	tasksList := make([]ActiveTask, 0, len(pendingTasks))
	for _, task := range pendingTasks {
		tasksList = append(tasksList, task)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tasksList)
}

func handleCompleteTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		WorkflowID string         `json:"workflowId"`
		RunID      string         `json:"runId"`
		NodeID     string         `json:"nodeId"`
		Payload    map[string]any `json:"payload"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Printf("[COMPLETING TASK] NodeID: %s, WorkflowID: %s", req.NodeID, req.WorkflowID)

	// Intercept and compute split_items dynamically based on user-selected HS codes
	if strings.HasPrefix(req.NodeID, "pick_hs_codes") {
		var hsCodes []string
		if rawCodes, exists := req.Payload["hs_codes"]; exists {
			if codeSlice, ok := rawCodes.([]any); ok {
				for _, c := range codeSlice {
					if str, ok := c.(string); ok {
						hsCodes = append(hsCodes, str)
					}
				}
			} else if strSlice, ok := rawCodes.([]string); ok {
				hsCodes = strSlice
			} else if singleStr, ok := rawCodes.(string); ok {
				hsCodes = []string{singleStr}
			}
		}

		hasFood := false
		hasPlant := false
		for _, c := range hsCodes {
			if c == "HS-FOOD" {
				hasFood = true
			}
			if c == "HS-PLANT" {
				hasPlant = true
			}
		}

		var splitItems []map[string]any
		// Customs is always active if any is selected (and we require picking at least one)
		if hasFood || hasPlant {
			splitItems = append(splitItems, map[string]any{
				"template_id": TemplateCustoms,
				"branch_id":   "customs",
				"payload": map[string]any{
					"signal_name": "CDN_ACK_DONE",
				},
			})
		}
		if hasPlant {
			splitItems = append(splitItems, map[string]any{
				"template_id": TemplatePhyto,
				"branch_id":   "phyto",
				"payload": map[string]any{
					"signal_name": "CDN_ACK_DONE",
				},
			})
		}
		if hasFood {
			splitItems = append(splitItems, map[string]any{
				"template_id": TemplateHealth,
				"branch_id":   "health",
				"payload": map[string]any{
					"signal_name": "CDN_ACK_DONE",
				},
			})
		}

		req.Payload["split_items"] = splitItems
	}

	err := temporalManager.TaskDone(ctx, req.WorkflowID, req.RunID, req.NodeID, req.Payload)
	if err != nil {
		log.Printf("Error completing task: %v", err)
		http.Error(w, fmt.Sprintf("Failed to complete task: %v", err), http.StatusInternalServerError)
		return
	}

	// Remove from pending tasks
	pendingTasksMu.Lock()
	delete(pendingTasks, req.WorkflowID+"_"+req.NodeID)
	pendingTasksMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "Success"})
}
