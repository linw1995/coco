use super::*;

impl ConsoleGraphSnapshotStore {
    pub fn try_append_new_branch_lane_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: AppendLinearBranchInput<'_>,
        lane_y: i32,
    ) -> crate::Result<bool> {
        let ancestry = store
            .ancestry_nodes(input.head_id)
            .context(crate::error::StoreSnafu)?;
        let (source, source_point, nodes): (Option<String>, Option<Point>, Vec<Node>) = match self
            .first_materialized_ancestry_point(
            connection, input.mode, &ancestry, lane_y,
        )? {
            Some((0, source_point)) => {
                return self.insert_branch_alias_lane(
                    connection,
                    input,
                    lane_y,
                    &ancestry[0],
                    source_point,
                );
            }
            Some((source_index, source_point)) => {
                let source = &ancestry[source_index];
                let mut nodes = ancestry[..source_index].to_vec();
                nodes.reverse();
                if nodes.is_empty() || !is_linear_new_nodes(&source.id, &nodes) {
                    return Ok(false);
                }
                (Some(source.id.clone()), Some(source_point), nodes)
            }
            None => {
                let mut nodes = ancestry
                    .iter()
                    .take_while(|node| !node.is_root())
                    .filter(|node| is_visible_mode_node(input.mode, node))
                    .cloned()
                    .collect::<Vec<_>>();
                nodes.reverse();
                if nodes.is_empty() || !initial_visible_lane_is_linear(input.mode, &nodes) {
                    return Ok(false);
                }
                (None, None, nodes)
            }
        };

        let lane = GraphViewportLane {
            key: lane_key(input.branch),
            label: input.branch.to_owned(),
            y: lane_y,
        };
        let branch_label = branch_label(input.branch, input.state);
        let mut previous = source.zip(source_point);
        let appended_len = nodes.len();
        let event_order =
            self.event_order_by_materialized_and_new_nodes(connection, store, input.mode, &nodes)?;
        for (index, node) in nodes.into_iter().enumerate() {
            let point = match previous.as_ref() {
                Some((previous_id, previous_point)) => Point {
                    x: previous_point.x
                        + required_column_gap(previous_id, &node.id, &event_order)
                            * GRAPH_COLUMN_WIDTH,
                    y: lane_y,
                },
                None => Point {
                    x: GRAPH_LEFT_X,
                    y: lane_y,
                },
            };
            let primary_parent_id = previous
                .as_ref()
                .map(|(previous_id, _)| previous_id.as_str())
                .unwrap_or("");
            let Some(point) = self.point_with_merge_parent_column_constraints(
                connection,
                store,
                MergeColumnConstraintInput {
                    mode: input.mode,
                    node: &node,
                    primary_parent_id,
                    point,
                    event_order: &event_order,
                    reserved_lane_y: Some(lane_y),
                    context_start_id: None,
                },
            )?
            else {
                return Ok(false);
            };
            let labels = if index + 1 == appended_len {
                vec![branch_label.clone()]
            } else {
                Vec::new()
            };
            let viewport_node = graph_viewport_node_from_node(&node, point, labels);
            self.insert_node_location(
                connection,
                NodeLocationInsert {
                    mode: input.mode,
                    node: &viewport_node,
                    lane: &lane,
                    bounds: node_bounds(&viewport_node),
                },
            )?;
            if let Some((previous_id, previous_point)) = previous.as_ref() {
                let edge = if index == 0 {
                    routed_edge(
                        GraphViewportEdgeKind::Fork,
                        previous_id,
                        *previous_point,
                        &node.id,
                        point,
                        self.next_routed_edge_slot_in_connection(
                            connection,
                            input.mode,
                            *previous_point,
                            point,
                        )?,
                    )
                } else {
                    primary_parent_edge(previous_id, *previous_point, &node.id, point)
                };
                self.insert_edge_route(
                    connection,
                    EdgeRouteInsert {
                        mode: input.mode,
                        edge: &edge,
                        bounds: edge_bounds(&edge),
                    },
                )?;
                if !self.insert_node_merge_edges(
                    connection,
                    store,
                    NodeMergeEdgesInput {
                        mode: input.mode,
                        node: &node,
                        primary_parent_id: previous_id,
                        target: point,
                        context_start_id: None,
                    },
                )? {
                    return Ok(false);
                }
            } else if !self.insert_node_merge_edges(
                connection,
                store,
                NodeMergeEdgesInput {
                    mode: input.mode,
                    node: &node,
                    primary_parent_id: "",
                    target: point,
                    context_start_id: None,
                },
            )? {
                return Ok(false);
            }
            if matches!(
                self.try_append_skill_invocation_subtree_in_transaction(
                    connection, store, input.mode, &node.id, point, &lane,
                )?,
                SkillSubtreeAppend::Unsupported
            ) {
                return Ok(false);
            }
            previous = Some((node.id, point));
        }
        Ok(true)
    }

    pub fn try_append_skill_invocation_subtree_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        mode: GraphMode,
        source_id: &str,
        source_point: Point,
        lane: &GraphViewportLane,
    ) -> crate::Result<SkillSubtreeAppend> {
        if mode != GraphMode::All {
            return Ok(SkillSubtreeAppend::Absent);
        }
        let source = store.node(source_id).context(crate::error::StoreSnafu)?;
        if source.kind.as_tool_uses().is_none() {
            return Ok(SkillSubtreeAppend::Absent);
        }
        let subtrees = visible_skill_invocation_linear_subtrees(
            source_id,
            visible_skill_invocation_subtree_nodes_with_lookup(
                mode,
                source_id,
                |id| store.node(id).context(crate::error::StoreSnafu),
                |id| store.children(id).context(crate::error::StoreSnafu),
            )?,
        );
        let Some(subtrees) = subtrees else {
            return Ok(SkillSubtreeAppend::Unsupported);
        };
        if subtrees.is_empty() {
            return Ok(SkillSubtreeAppend::Absent);
        }

        for nodes in subtrees {
            let (subtree_lane, fork_first_inserted) = match self
                .materialized_skill_subtree_attach_row_in_connection(connection, mode, &nodes)?
            {
                Some((row, fork_first_inserted)) => (
                    GraphViewportLane {
                        key: row.lane_key,
                        label: row.lane_label,
                        y: row.lane_y,
                    },
                    fork_first_inserted,
                ),
                None => {
                    let subtree_source_id = nodes
                        .last()
                        .map(|node| node.id.as_str())
                        .unwrap_or(source_id);
                    (
                        skill_invocation_subtree_lane(
                            subtree_source_id,
                            self.next_materialized_lane_y_after_reserved(
                                connection,
                                mode,
                                Some(lane.y),
                            )?,
                        ),
                        false,
                    )
                }
            };
            let event_order =
                self.event_order_by_materialized_and_new_nodes(connection, store, mode, &nodes)?;
            let mut previous_id = source_id.to_owned();
            let mut previous_point = source_point;
            let mut first_inserted_node = true;
            for node in nodes {
                if let Some(row) = self.materialized_node_row_by_id_on_lane_in_connection(
                    connection,
                    mode,
                    &node.id,
                    &subtree_lane.key,
                )? {
                    let point = Point { x: row.x, y: row.y };
                    previous_id = node.id;
                    previous_point = point;
                    continue;
                }
                let candidate = Point {
                    x: previous_point.x
                        + required_column_gap(&previous_id, &node.id, &event_order)
                            * GRAPH_COLUMN_WIDTH,
                    y: subtree_lane.y,
                };
                let Some(point) = self.point_with_merge_parent_column_constraints(
                    connection,
                    store,
                    MergeColumnConstraintInput {
                        mode,
                        node: &node,
                        primary_parent_id: &previous_id,
                        point: candidate,
                        event_order: &event_order,
                        reserved_lane_y: Some(subtree_lane.y),
                        context_start_id: None,
                    },
                )?
                else {
                    return Ok(SkillSubtreeAppend::Unsupported);
                };
                let viewport_node = graph_viewport_node_from_node(&node, point, Vec::new());
                self.insert_node_location(
                    connection,
                    NodeLocationInsert {
                        mode,
                        node: &viewport_node,
                        lane: &subtree_lane,
                        bounds: node_bounds(&viewport_node),
                    },
                )?;
                let previous = (previous_id.clone(), previous_point);
                if !self.insert_orphan_merge_parent_node_edges(
                    connection,
                    store,
                    OrphanMergeParentNodeEdgeInput {
                        mode,
                        node: &node,
                        point,
                        previous: Some(&previous),
                        first_node: previous_id == source_id,
                        force_fork: first_inserted_node && fork_first_inserted,
                        context_start_id: None,
                    },
                )? {
                    return Ok(SkillSubtreeAppend::Unsupported);
                }
                previous_id = node.id;
                previous_point = point;
                first_inserted_node = false;
            }
        }
        Ok(SkillSubtreeAppend::Applied)
    }

    pub fn try_append_linear_branches_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        source_version: u64,
        mode: GraphMode,
        session_states: &[(String, SessionState)],
    ) -> crate::Result<bool> {
        let Some(meta) = self.latest_materialization_row_in_connection(connection, mode)? else {
            return Ok(false);
        };
        if meta.source_version >= 0 && source_version <= meta.source_version as u64 {
            return Ok(true);
        }

        let mut materialized_lanes = self.materialized_lanes_in_connection(connection, mode)?;
        let branch_names = session_states
            .iter()
            .map(|(branch, _)| branch.clone())
            .collect::<BTreeSet<_>>();
        let removed_lanes = removed_lanes_in_order(&materialized_lanes, &branch_names);
        if !removed_lanes.is_empty() {
            if self.lanes_have_retained_downstream_edges(connection, mode, &removed_lanes)? {
                return Ok(false);
            }
            self.delete_materialized_lanes(connection, mode, &removed_lanes)?;
            self.shift_lanes_after_deletion(connection, mode, &removed_lanes)?;
            materialized_lanes = self.materialized_lanes_in_connection(connection, mode)?;
        }
        let materialized_lane_labels = materialized_lanes
            .iter()
            .filter(|lane| !is_derived_lane_key(&lane.lane_key))
            .map(|lane| lane.lane_label.clone())
            .collect::<BTreeSet<_>>();
        if !existing_branch_lanes_preserve_order(
            session_states,
            &materialized_lanes,
            &materialized_lane_labels,
        ) {
            return Ok(false);
        }

        if !self.try_update_all_branch_lanes(
            connection,
            store,
            session_states,
            materialized_lane_labels,
        )? {
            return Ok(false);
        }
        self.prune_removable_derived_lanes(connection, mode)?;
        self.rebalance_routed_edge_slots(connection, mode)?;
        let Some(materialized_nodes) =
            self.refresh_materialized_node_labels(connection, store, mode, session_states)?
        else {
            return Ok(false);
        };
        let world_max_x = materialized_nodes
            .iter()
            .map(|row| row.x)
            .max()
            .unwrap_or(meta.world_max_x - 120)
            + 120;
        let world_max_y = self
            .materialized_lanes_in_connection(connection, mode)?
            .iter()
            .map(|lane| lane.lane_y)
            .max()
            .unwrap_or(crate::layout::GRAPH_TOP_Y - GRAPH_LANE_HEIGHT)
            + 120;

        self.put_materialization_meta(
            connection,
            MaterializationMetaInput {
                source_version,
                mode,
                world_min_x: meta.world_min_x,
                world_min_y: meta.world_min_y,
                world_max_x,
                world_max_y,
            },
        )?;
        Ok(true)
    }

    pub fn try_update_all_branch_lanes(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        session_states: &[(String, SessionState)],
        materialized_lane_labels: BTreeSet<String>,
    ) -> crate::Result<bool> {
        let mode = GraphMode::All;
        let mut materialized_lane_labels = materialized_lane_labels;
        let mut next_lane_y = crate::layout::GRAPH_TOP_Y;
        for (branch, state) in session_states {
            let head_id = store
                .branch_head(branch)
                .context(crate::error::StoreSnafu)?;
            let has_visible_nodes = self.branch_has_initial_visible_nodes(store, mode, branch)?;
            let appended = if materialized_lane_labels.contains(branch) {
                if !has_visible_nodes {
                    if !self
                        .delete_materialized_branch_lane_if_isolated(connection, mode, branch)?
                    {
                        return Ok(false);
                    }
                    materialized_lane_labels.remove(branch);
                    continue;
                }
                self.try_append_linear_branch_in_transaction(
                    connection,
                    store,
                    AppendLinearBranchInput {
                        mode,
                        branch,
                        state,
                        head_id: &head_id,
                    },
                )?
            } else {
                if !has_visible_nodes {
                    continue;
                }
                self.shift_lanes_for_insertion(connection, mode, next_lane_y)?;
                let appended = self.try_append_new_branch_lane_in_transaction(
                    connection,
                    store,
                    AppendLinearBranchInput {
                        mode,
                        branch,
                        state,
                        head_id: &head_id,
                    },
                    next_lane_y,
                )?;
                if appended {
                    materialized_lane_labels.insert(branch.clone());
                }
                appended
            };
            if !appended {
                return Ok(false);
            }
            if materialized_lane_labels.contains(branch) {
                next_lane_y += GRAPH_LANE_HEIGHT;
            }
        }
        Ok(true)
    }
}
