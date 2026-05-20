package engine

import "fmt"

// validateDefinition checks that the workflow definition forms a valid DAG
// with correctly configured split and join gateways:
// 1. Bijective pairing between splits and joins.
// 2. Exactly one of CountVariable or ItemsVariable set on splits.
// 3. Exactly one entry to split and exit from join.
// 4. No nested fan-out blocks.
// 5. Single-entry, single-exit subgraph topology for the region.
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
