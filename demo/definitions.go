package main

import (
	engine "github.com/OpenNSW/go-temporal-workflow"
)

// Define constant template IDs for resolving child workflows
const (
	TemplateMainConsignment = "main_consignment_flow"
	TemplateCustoms         = "customs_workflow"
	TemplatePhyto           = "phyto_workflow"
	TemplateHealth          = "health_workflow"
)

// WorkflowDefinitions stores all registered workflow definitions for lookup by the engine
var WorkflowDefinitions = map[string]engine.WorkflowDefinition{
	// 1. Primary Master Consignment Workflow
	TemplateMainConsignment: {
		ID:      TemplateMainConsignment,
		Name:    "Master Consignment Clearance Flow",
		Version: 1,
		Nodes: []engine.Node{
			{
				ID:   "start",
				Type: engine.NodeTypeStart,
			},
			{
				ID:             "pick_cha",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "pick_cha",
				OutputMapping: map[string]string{
					"cha_name": "consignment.cha_name",
				},
			},
			{
				ID:             "pick_hs_codes",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "pick_hs_codes",
				OutputMapping: map[string]string{
					"hs_codes":    "consignment.hs_codes",
					"split_items": "split_items",
				},
			},
			{
				ID:   "split_task",
				Type: engine.NodeTypeSplitTask,
				SplitTask: &engine.SplitTaskConfig{
					Mode:            engine.SplitModeDifferentTemplates,
					ItemsVariable:   "split_items",
					ResultsVariable: "sub_workflow_results",
					FailureMode:     engine.FailureModeCollectAll,
				},
			},
			{
				ID:             "confirm_consignment",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "confirm_consignment",
				InputMapping: map[string]string{
					"sub_workflow_results": "sub_workflow_results",
				},
				OutputMapping: map[string]string{
					"confirmation_status": "consignment.confirmation_status",
				},
			},
			{
				ID:   "end",
				Type: engine.NodeTypeEnd,
			},
		},
		Edges: []engine.Edge{
			{ID: "e1", SourceID: "start", TargetID: "pick_cha"},
			{ID: "e2", SourceID: "pick_cha", TargetID: "pick_hs_codes"},
			{ID: "e3", SourceID: "pick_hs_codes", TargetID: "split_task"},
			{ID: "e4", SourceID: "split_task", TargetID: "confirm_consignment"},
			{ID: "e5", SourceID: "confirm_consignment", TargetID: "end"},
		},
	},

	// 2. Customs Sub-workflow
	TemplateCustoms: {
		ID:      TemplateCustoms,
		Name:    "Customs Clearance sub-flow",
		Version: 1,
		Nodes: []engine.Node{
			{
				ID:   "c_start",
				Type: engine.NodeTypeStart,
			},
			{
				ID:             "cusdec",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "cusdec",
				OutputMapping: map[string]string{
					"cusdec_status": "customs.cusdec_status",
				},
			},
			{
				ID:             "warrenting",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "warrenting",
				OutputMapping: map[string]string{
					"warrant_status": "customs.warrant_status",
				},
			},
			{
				ID:             "payment",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "payment",
				OutputMapping: map[string]string{
					"payment_status": "customs.payment_status",
				},
			},
			{
				ID:             "cdn_submission",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "cdn_submission",
				OutputMapping: map[string]string{
					"cdn_submitted": "customs.cdn_submitted",
				},
			},
			{
				ID:             "cdn_ack",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "cdn_ack",
				OutputMapping: map[string]string{
					"cdn_acknowledged": "customs.cdn_acknowledged",
				},
			},
			{
				ID:             "c_emit_cdn_ack",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: engine.SysTaskEmitSignal,
				InputMapping: map[string]string{
					"_iter.input.signal_name": engine.InputSignalName,
				},
			},
			{
				ID:             "boat_note",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "boat_note",
				OutputMapping: map[string]string{
					"boat_note_id": "customs.boat_note_id",
				},
			},
			{
				ID:             "export_release",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "export_release",
				OutputMapping: map[string]string{
					"release_status": "customs.release_status",
				},
			},
			{
				ID:   "c_end",
				Type: engine.NodeTypeEnd,
			},
		},
		Edges: []engine.Edge{
			{ID: "ce1", SourceID: "c_start", TargetID: "cusdec"},
			{ID: "ce2", SourceID: "cusdec", TargetID: "warrenting"},
			{ID: "ce3", SourceID: "warrenting", TargetID: "payment"},
			{ID: "ce4", SourceID: "payment", TargetID: "cdn_submission"},
			{ID: "ce5", SourceID: "cdn_submission", TargetID: "cdn_ack"},
			{ID: "ce6", SourceID: "cdn_ack", TargetID: "c_emit_cdn_ack"},
			{ID: "ce6_2", SourceID: "c_emit_cdn_ack", TargetID: "boat_note"},
			{ID: "ce7", SourceID: "boat_note", TargetID: "export_release"},
			{ID: "ce8", SourceID: "export_release", TargetID: "c_end"},
		},
	},

	// 3. Phytosanitary Certificate Sub-workflow
	TemplatePhyto: {
		ID:      TemplatePhyto,
		Name:    "Phytosanitary Inspection and Cert sub-flow",
		Version: 1,
		Nodes: []engine.Node{
			{
				ID:   "p_start",
				Type: engine.NodeTypeStart,
			},
			{
				ID:             "phyto_app",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "phyto_app",
				OutputMapping: map[string]string{
					"app_status": "phyto.app_status",
				},
			},
			{
				ID:             "phyto_pay",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "phyto_pay",
				OutputMapping: map[string]string{
					"payment_status": "phyto.payment_status",
				},
			},
			{
				ID:             "phyto_wait_cdn",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: engine.SysTaskWaitForSignal,
				InputMapping: map[string]string{
					"_iter.input.signal_name": engine.InputSignalName,
				},
			},
			{
				ID:             "phyto_issue",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "phyto_issue",
				OutputMapping: map[string]string{
					"cert_number": "phyto.cert_number",
				},
			},
			{
				ID:   "p_end",
				Type: engine.NodeTypeEnd,
			},
		},
		Edges: []engine.Edge{
			{ID: "pe1", SourceID: "p_start", TargetID: "phyto_app"},
			{ID: "pe2", SourceID: "phyto_app", TargetID: "phyto_pay"},
			{ID: "pe3", SourceID: "phyto_pay", TargetID: "phyto_wait_cdn"},
			{ID: "pe3_2", SourceID: "phyto_wait_cdn", TargetID: "phyto_issue"},
			{ID: "pe4", SourceID: "phyto_issue", TargetID: "p_end"},
		},
	},

	// 4. Health Certificate Sub-workflow
	TemplateHealth: {
		ID:      TemplateHealth,
		Name:    "Health Certificate sub-flow",
		Version: 1,
		Nodes: []engine.Node{
			{
				ID:   "h_start",
				Type: engine.NodeTypeStart,
			},
			{
				ID:             "health_app",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "health_app",
				OutputMapping: map[string]string{
					"app_status": "health.app_status",
				},
			},
			{
				ID:             "health_pay",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "health_pay",
				OutputMapping: map[string]string{
					"payment_status": "health.payment_status",
				},
			},
			{
				ID:             "health_wait_cdn",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: engine.SysTaskWaitForSignal,
				InputMapping: map[string]string{
					"_iter.input.signal_name": engine.InputSignalName,
				},
			},
			{
				ID:             "health_issue",
				Type:           engine.NodeTypeTask,
				TaskTemplateID: "health_issue",
				OutputMapping: map[string]string{
					"cert_number": "health.cert_number",
				},
			},
			{
				ID:   "h_end",
				Type: engine.NodeTypeEnd,
			},
		},
		Edges: []engine.Edge{
			{ID: "he1", SourceID: "h_start", TargetID: "health_app"},
			{ID: "he2", SourceID: "health_app", TargetID: "health_pay"},
			{ID: "he3", SourceID: "health_pay", TargetID: "health_wait_cdn"},
			{ID: "he3_2", SourceID: "health_wait_cdn", TargetID: "health_issue"},
			{ID: "he4", SourceID: "health_issue", TargetID: "h_end"},
		},
	},
}
