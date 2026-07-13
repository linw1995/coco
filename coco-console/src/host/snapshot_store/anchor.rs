use super::*;

impl ConsoleGraphSnapshotStore {
    pub fn try_update_anchor_materialization_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        source_version: u64,
        session_states: &[(String, SessionState)],
    ) -> crate::Result<bool> {
        let mode = GraphMode::Anchors;
        let Some(meta) = self.latest_materialization_row_in_connection(connection, mode)? else {
            return Ok(false);
        };
        if meta.source_version >= 0 && source_version <= meta.source_version as u64 {
            return Ok(true);
        }
        let Some(materialized_lane_labels) =
            self.prune_anchor_materialized_lanes(connection, session_states)?
        else {
            return Ok(false);
        };
        if !self.try_update_anchor_branch_lanes(
            connection,
            store,
            session_states,
            materialized_lane_labels,
        )? {
            return Ok(false);
        }
        self.prune_removable_derived_lanes(connection, mode)?;
        self.rebalance_routed_edge_slots(connection, mode)?;
        let Some(materialized_nodes) = self.refresh_materialized_node_labels(
            connection,
            store,
            GraphMode::Anchors,
            session_states,
        )?
        else {
            return Ok(false);
        };
        let world_max_x = materialized_nodes
            .iter()
            .map(|row| row.x)
            .max()
            .unwrap_or(meta.world_max_x - 120)
            + 120;
        let world_max_y = materialized_nodes
            .iter()
            .map(|row| row.y)
            .max()
            .unwrap_or(meta.world_max_y - 120)
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

    pub fn prune_anchor_materialized_lanes(
        &self,
        connection: &mut SqliteConnection,
        session_states: &[(String, SessionState)],
    ) -> crate::Result<Option<BTreeSet<String>>> {
        let mode = GraphMode::Anchors;
        let mut materialized_lanes = self.materialized_lanes_in_connection(connection, mode)?;
        let branch_names = session_states
            .iter()
            .map(|(branch, _)| branch.clone())
            .collect::<BTreeSet<_>>();
        let removed_lanes = removed_lanes_in_order(&materialized_lanes, &branch_names);
        if !removed_lanes.is_empty() {
            if self.lanes_have_retained_downstream_edges(connection, mode, &removed_lanes)? {
                return Ok(None);
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
            return Ok(None);
        }
        Ok(Some(materialized_lane_labels))
    }

    pub fn try_update_anchor_branch_lanes(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        session_states: &[(String, SessionState)],
        materialized_lane_labels: BTreeSet<String>,
    ) -> crate::Result<bool> {
        let mode = GraphMode::Anchors;
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
                self.try_update_existing_anchor_branch_lane(
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
                let appended = self.try_append_new_anchor_branch_lane_in_transaction(
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
            next_lane_y += GRAPH_LANE_HEIGHT;
        }
        Ok(true)
    }

    pub fn try_update_existing_anchor_branch_lane(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: AppendLinearBranchInput<'_>,
    ) -> crate::Result<bool> {
        let ancestry = store
            .ancestry_nodes(input.head_id)
            .context(crate::error::StoreSnafu)?;
        let scoped_ancestry = provider_context_ancestry_nodes(ancestry);
        let Some(tail) =
            self.latest_lane_tail_in_connection(connection, input.mode, input.branch)?
        else {
            return Ok(false);
        };
        let Some(visible_head) = self.first_materialized_lane_ancestry_node(
            connection,
            input.mode,
            input.branch,
            &scoped_ancestry,
        )?
        else {
            return self.replace_anchor_branch_lane_for_context_shift(
                connection,
                store,
                input,
                tail,
                scoped_ancestry,
            );
        };
        if visible_head.x < tail.x {
            if self.lane_suffix_has_retained_downstream_edges(
                connection,
                input.mode,
                input.branch,
                visible_head.x,
            )? {
                return Ok(false);
            }
            self.delete_materialized_lane_suffix(
                connection,
                input.mode,
                input.branch,
                visible_head.x,
            )?;
        }
        self.try_append_anchor_branch_after_row(connection, store, input, visible_head)
    }

    pub fn replace_anchor_branch_lane_for_context_shift(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: AppendLinearBranchInput<'_>,
        tail: MaterializedTailNodeRow,
        scoped_ancestry: Vec<Node>,
    ) -> crate::Result<bool> {
        if self.lane_suffix_has_retained_downstream_edges(
            connection,
            input.mode,
            input.branch,
            i32::MIN,
        )? {
            return Ok(false);
        }
        let context_start_id = context_start_id_from_scoped_ancestry(&scoped_ancestry);
        let visible_chain = scoped_ancestry
            .iter()
            .rev()
            .filter(|node| is_anchor_node(node))
            .cloned()
            .collect::<Vec<_>>();
        if visible_chain.is_empty() {
            return Ok(false);
        }

        let lane_y = tail.lane_y;
        self.delete_materialized_lanes(
            connection,
            input.mode,
            &[LaneRow {
                lane_key: tail.lane_key,
                lane_label: tail.lane_label,
                lane_y,
            }],
        )?;
        self.insert_anchor_branch_lane_nodes(
            connection,
            store,
            &input,
            AnchorBranchLaneInsert {
                lane_y,
                nodes: visible_chain,
                previous: None,
                context_start_id,
            },
        )
    }

    pub fn refresh_materialized_node_labels(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        mode: GraphMode,
        session_states: &[(String, SessionState)],
    ) -> crate::Result<Option<Vec<MaterializedTailNodeRow>>> {
        let mut labels_by_node_id = BTreeMap::<String, Vec<String>>::new();
        for (branch, state) in session_states {
            if !self.branch_has_initial_visible_nodes(store, mode, branch)? {
                continue;
            }
            let head_id = store
                .branch_head(branch)
                .context(crate::error::StoreSnafu)?;
            let ancestry = store
                .ancestry_nodes(&head_id)
                .context(crate::error::StoreSnafu)?;
            let Some(row) =
                self.first_materialized_lane_ancestry_node(connection, mode, branch, &ancestry)?
            else {
                return Ok(None);
            };
            labels_by_node_id
                .entry(row.node_id)
                .or_default()
                .push(branch_label(branch, state));
        }
        for labels in labels_by_node_id.values_mut() {
            labels.sort();
        }
        let materialized_nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        for row in &materialized_nodes {
            let labels = labels_by_node_id
                .get(&row.node_id)
                .cloned()
                .unwrap_or_default();
            self.update_node_labels(connection, mode, &row.node_key, labels)?;
        }
        Ok(Some(materialized_nodes))
    }

    pub fn try_append_new_anchor_branch_lane_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: AppendLinearBranchInput<'_>,
        lane_y: i32,
    ) -> crate::Result<bool> {
        let ancestry = store
            .ancestry_nodes(input.head_id)
            .context(crate::error::StoreSnafu)?;
        let scoped_ancestry = provider_context_ancestry_nodes(ancestry);
        let context_start_id = context_start_id_from_scoped_ancestry(&scoped_ancestry);
        let visible_chain = scoped_ancestry
            .iter()
            .rev()
            .filter(|node| is_anchor_node(node))
            .cloned()
            .collect::<Vec<_>>();
        if visible_chain.is_empty() {
            return Ok(false);
        }

        let covered_before_lane = self
            .materialized_node_rows_in_connection(connection, input.mode)?
            .into_iter()
            .filter(|row| row.y < lane_y)
            .map(|row| row.node_id)
            .collect::<BTreeSet<_>>();
        let first_new = visible_chain
            .iter()
            .position(|node| !covered_before_lane.contains(&node.id))
            .unwrap_or_else(|| visible_chain.len().saturating_sub(1));
        let nodes = visible_chain[first_new..].to_vec();
        if nodes.is_empty() {
            return Ok(false);
        }

        let fork_source = first_new
            .checked_sub(1)
            .and_then(|index| visible_chain.get(index));
        let mut previous = match fork_source {
            Some(source) => {
                let Some(source_point) =
                    self.materialized_node_point_in_connection(connection, input.mode, &source.id)?
                else {
                    return Ok(false);
                };
                Some((source.id.clone(), source_point))
            }
            None => None,
        };

        let branch_label = branch_label(input.branch, input.state);
        self.insert_anchor_branch_lane_nodes(
            connection,
            store,
            &input,
            AnchorBranchLaneInsert {
                lane_y,
                nodes,
                previous: previous.take(),
                context_start_id,
            },
        )?;
        if let Some(row) =
            self.latest_lane_tail_in_connection(connection, input.mode, input.branch)?
        {
            self.update_node_labels(connection, input.mode, &row.node_key, vec![branch_label])?;
        }
        Ok(true)
    }

    pub fn insert_anchor_branch_lane_nodes(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: &AppendLinearBranchInput<'_>,
        lane_insert: AnchorBranchLaneInsert,
    ) -> crate::Result<bool> {
        let AnchorBranchLaneInsert {
            lane_y,
            nodes,
            mut previous,
            context_start_id,
        } = lane_insert;
        let context_start_id = context_start_id.as_deref();
        let lane = GraphViewportLane {
            key: lane_key(input.branch),
            label: input.branch.to_owned(),
            y: lane_y,
        };
        let branch_label = branch_label(input.branch, input.state);
        let appended_len = nodes.len();
        let starts_from_fork = previous.is_some();
        let event_order =
            self.event_order_by_materialized_and_new_nodes(connection, store, input.mode, &nodes)?;
        for (index, node) in nodes.into_iter().enumerate() {
            let candidate = match previous.as_ref() {
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
                    point: candidate,
                    event_order: &event_order,
                    reserved_lane_y: Some(lane.y),
                    context_start_id,
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
                let edge = if index == 0 && starts_from_fork {
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
                        context_start_id,
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
                    context_start_id,
                },
            )? {
                return Ok(false);
            }
            previous = Some((node.id, point));
        }
        Ok(true)
    }

    pub fn try_append_anchor_branch_after_row(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: AppendLinearBranchInput<'_>,
        tail: MaterializedTailNodeRow,
    ) -> crate::Result<bool> {
        if input.head_id == tail.node_id {
            self.trim_branch_lane_covered_prefix(connection, input.mode, input.branch)?;
            return Ok(true);
        }
        let ancestry = store
            .ancestry_nodes(input.head_id)
            .context(crate::error::StoreSnafu)?;
        let context_start_id = merge_parent_context_start_id(input.mode, &ancestry);
        let Ok(mut chain) = store.log_nodes(&tail.node_id, input.head_id) else {
            return Ok(false);
        };
        chain.reverse();
        if chain.first().is_none_or(|node| node.id != tail.node_id) {
            return Ok(false);
        }
        if !is_linear_primary_chain(&chain) {
            return Ok(false);
        }

        let lane = GraphViewportLane {
            key: tail.lane_key,
            label: tail.lane_label,
            y: tail.lane_y,
        };
        let appended_nodes = chain
            .into_iter()
            .skip(1)
            .filter(is_anchor_node)
            .collect::<Vec<_>>();
        let event_order = self.event_order_by_materialized_and_new_nodes(
            connection,
            store,
            input.mode,
            &appended_nodes,
        )?;
        let mut previous_id = tail.node_id;
        let mut previous_point = Point {
            x: tail.x,
            y: tail.y,
        };
        for node in appended_nodes {
            let point = Point {
                x: previous_point.x
                    + required_column_gap(&previous_id, &node.id, &event_order)
                        * GRAPH_COLUMN_WIDTH,
                y: previous_point.y,
            };
            let Some(point) = self.point_with_merge_parent_column_constraints(
                connection,
                store,
                MergeColumnConstraintInput {
                    mode: input.mode,
                    node: &node,
                    primary_parent_id: &previous_id,
                    point,
                    event_order: &event_order,
                    reserved_lane_y: None,
                    context_start_id: context_start_id.as_deref(),
                },
            )?
            else {
                return Ok(false);
            };
            let viewport_node = graph_viewport_node_from_node(&node, point, Vec::new());
            self.insert_node_location(
                connection,
                NodeLocationInsert {
                    mode: input.mode,
                    node: &viewport_node,
                    lane: &lane,
                    bounds: node_bounds(&viewport_node),
                },
            )?;
            let edge = primary_parent_edge(&previous_id, previous_point, &node.id, point);
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
                    primary_parent_id: &previous_id,
                    target: point,
                    context_start_id: context_start_id.as_deref(),
                },
            )? {
                return Ok(false);
            }
            previous_id = node.id;
            previous_point = point;
        }
        Ok(true)
    }

    pub fn insert_branch_alias_lane(
        &self,
        connection: &mut SqliteConnection,
        input: AppendLinearBranchInput<'_>,
        lane_y: i32,
        node: &Node,
        source_point: Point,
    ) -> crate::Result<bool> {
        let mut labels = self.materialized_node_label_set(connection, input.mode, &node.id)?;
        labels.insert(branch_label(input.branch, input.state));
        let labels = labels.into_iter().collect::<Vec<_>>();
        let lane = GraphViewportLane {
            key: lane_key(input.branch),
            label: input.branch.to_owned(),
            y: lane_y,
        };
        let point = Point {
            x: source_point.x,
            y: lane_y,
        };
        let viewport_node = graph_viewport_node_from_node(node, point, labels.clone());
        self.insert_node_location(
            connection,
            NodeLocationInsert {
                mode: input.mode,
                node: &viewport_node,
                lane: &lane,
                bounds: node_bounds(&viewport_node),
            },
        )?;
        self.migrate_orphan_occurrences_to_point(connection, input.mode, &node.id, point)?;
        self.update_node_id_labels(connection, input.mode, &node.id, labels)?;
        self.insert_branch_alias_fork_edge(connection, input.mode, node, point)?;
        Ok(true)
    }

    pub fn insert_branch_alias_fork_edge(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node: &Node,
        point: Point,
    ) -> crate::Result<()> {
        let Some(parent_point) =
            self.materialized_node_point_in_connection(connection, mode, &node.parent)?
        else {
            return Ok(());
        };
        let edge = routed_edge(
            GraphViewportEdgeKind::Fork,
            &node.parent,
            parent_point,
            &node.id,
            point,
            self.next_routed_edge_slot_in_connection(connection, mode, parent_point, point)?,
        );
        self.insert_edge_route(
            connection,
            EdgeRouteInsert {
                mode,
                edge: &edge,
                bounds: edge_bounds(&edge),
            },
        )?;
        self.rebalance_target_port_offsets(connection, mode, point)
    }

    pub fn migrate_orphan_occurrences_to_point(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        point: Point,
    ) -> crate::Result<()> {
        let lanes = self.orphan_lanes_for_node_in_connection(connection, mode, node_id)?;
        if lanes.is_empty() {
            return Ok(());
        }

        let outgoing_edges =
            self.outgoing_edge_routes_from_lanes(connection, mode, node_id, &lanes)?;
        self.delete_materialized_lanes(connection, mode, &lanes)?;
        self.insert_migrated_outgoing_edge_routes(connection, mode, point, outgoing_edges)
    }

    pub fn orphan_lanes_for_node_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
    ) -> crate::Result<Vec<LaneRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::lane_key.like("derived:orphan:%")),
            )
            .select((
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
            ))
            .distinct()
            .order(node_locations::lane_y)
            .load::<LaneRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn outgoing_edge_routes_from_lanes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        lanes: &[LaneRow],
    ) -> crate::Result<Vec<EdgeRouteRow>> {
        let mut outgoing_edges = Vec::new();
        for lane in lanes {
            outgoing_edges.extend(self.outgoing_edge_routes_from_lane_node(
                connection,
                mode,
                node_id,
                lane.lane_y,
            )?);
        }
        Ok(outgoing_edges)
    }

    pub fn outgoing_edge_routes_from_lane_node(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        lane_y: i32,
    ) -> crate::Result<Vec<EdgeRouteRow>> {
        use console_graph_edge_routes::dsl as edge_routes;

        edge_routes::console_graph_edge_routes
            .filter(
                edge_routes::mode
                    .eq(mode.as_query_value())
                    .and(edge_routes::source_id.eq(node_id))
                    .and(edge_routes::source_y.eq(lane_y)),
            )
            .select((
                edge_routes::edge_key,
                edge_routes::edge_kind,
                edge_routes::source_id,
                edge_routes::target_id,
                edge_routes::source_x,
                edge_routes::source_y,
                edge_routes::target_x,
                edge_routes::target_y,
                edge_routes::route_slot,
                edge_routes::target_port_offset,
            ))
            .load::<EdgeRouteRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn insert_migrated_outgoing_edge_routes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        point: Point,
        rows: Vec<EdgeRouteRow>,
    ) -> crate::Result<()> {
        for row in rows {
            self.insert_migrated_outgoing_edge_route(connection, mode, point, row)?;
        }
        Ok(())
    }

    pub fn insert_migrated_outgoing_edge_route(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        point: Point,
        row: EdgeRouteRow,
    ) -> crate::Result<()> {
        let kind = parse_edge_kind(&row.edge_kind)?;
        let target = Point {
            x: row.target_x,
            y: row.target_y,
        };
        let edge = GraphViewportEdge {
            key: edge_key(kind, &row.source_id, point, &row.target_id, target),
            kind,
            source_id: row.source_id,
            target_id: row.target_id,
            source: point,
            target,
            route_slot: row.route_slot,
            target_port_offset: row.target_port_offset,
        };
        self.insert_edge_route(
            connection,
            EdgeRouteInsert {
                mode,
                edge: &edge,
                bounds: edge_bounds(&edge),
            },
        )?;
        self.rebalance_target_port_offsets(connection, mode, target)
    }

    pub fn point_with_merge_parent_column_constraints(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: MergeColumnConstraintInput<'_>,
    ) -> crate::Result<Option<Point>> {
        let mut parent_ids = BTreeSet::from([input.primary_parent_id.to_owned()]);
        let mut refreshed_event_order = None;
        let mut x = input.point.x;
        for merge_parent in node_anchor_merge_parents(input.node) {
            let source = match self.ensure_visible_merge_parent_point(
                connection,
                store,
                input.mode,
                merge_parent,
                input.reserved_lane_y,
                input.context_start_id,
            )? {
                MergeParentPoint::Visible(source) => source,
                MergeParentPoint::Skipped => continue,
                MergeParentPoint::Unsupported => return Ok(None),
            };
            if parent_ids.insert(source.node_id.clone()) {
                let event_order = if input.event_order.contains_key(&source.node_id) {
                    input.event_order
                } else {
                    refreshed_event_order.get_or_insert(
                        self.event_order_by_materialized_and_new_nodes(
                            connection,
                            store,
                            input.mode,
                            std::slice::from_ref(input.node),
                        )?,
                    )
                };
                x = x.max(
                    source.point.x
                        + required_column_gap(&source.node_id, &input.node.id, event_order)
                            * GRAPH_COLUMN_WIDTH,
                );
            }
        }
        if let Some(event_order) = refreshed_event_order.as_ref()
            && !input.primary_parent_id.is_empty()
            && let Some(primary_point) = self.materialized_node_point_in_connection(
                connection,
                input.mode,
                input.primary_parent_id,
            )?
        {
            x = x.max(
                primary_point.x
                    + required_column_gap(input.primary_parent_id, &input.node.id, event_order)
                        * GRAPH_COLUMN_WIDTH,
            );
        }
        Ok(Some(Point {
            x,
            y: input.point.y,
        }))
    }

    pub fn insert_node_merge_edges(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: NodeMergeEdgesInput<'_>,
    ) -> crate::Result<bool> {
        let mut parent_ids = BTreeSet::from([input.primary_parent_id.to_owned()]);
        for merge_parent in node_anchor_merge_parents(input.node) {
            let source = match self.ensure_visible_merge_parent_point(
                connection,
                store,
                input.mode,
                merge_parent,
                None,
                input.context_start_id,
            )? {
                MergeParentPoint::Visible(source) => source,
                MergeParentPoint::Skipped => continue,
                MergeParentPoint::Unsupported => return Ok(false),
            };
            if !parent_ids.insert(source.node_id.clone()) {
                continue;
            }
            let edge = routed_edge(
                GraphViewportEdgeKind::MergeParent,
                &source.node_id,
                source.point,
                &input.node.id,
                input.target,
                self.next_routed_edge_slot_in_connection(
                    connection,
                    input.mode,
                    source.point,
                    input.target,
                )?,
            );
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode: input.mode,
                    edge: &edge,
                    bounds: edge_bounds(&edge),
                },
            )?;
            self.rebalance_target_port_offsets(connection, input.mode, input.target)?;
        }
        Ok(true)
    }

    pub fn ensure_visible_merge_parent_point(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        mode: GraphMode,
        merge_parent: &MergeParent,
        reserved_lane_y: Option<i32>,
        context_start_id: Option<&str>,
    ) -> crate::Result<MergeParentPoint> {
        let ancestry = store
            .ancestry_nodes(merge_parent.node_id())
            .context(crate::error::StoreSnafu)?;
        let Some(source_index) =
            visible_scoped_merge_parent_source_index(mode, &ancestry, context_start_id)
        else {
            return Ok(MergeParentPoint::Skipped);
        };
        let source = &ancestry[source_index];
        if let Some(point) =
            self.materialized_node_point_in_connection(connection, mode, &source.id)?
        {
            return Ok(MergeParentPoint::Visible(VisibleMergeParentPoint {
                node_id: source.id.clone(),
                point,
            }));
        }
        match self.insert_orphan_merge_parent_lane(
            connection,
            store,
            OrphanMergeParentLaneInput {
                mode,
                ancestry: &ancestry,
                source_index,
                reserved_lane_y,
                context_start_id,
            },
        )? {
            Some(point) => Ok(MergeParentPoint::Visible(point)),
            None => Ok(MergeParentPoint::Unsupported),
        }
    }

    pub fn insert_orphan_merge_parent_lane(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: OrphanMergeParentLaneInput<'_>,
    ) -> crate::Result<Option<VisibleMergeParentPoint>> {
        let Some(orphan) = self.orphan_merge_parent_lane(
            connection,
            input.mode,
            input.ancestry,
            input.source_index,
            input.reserved_lane_y,
            input.context_start_id,
        )?
        else {
            return Ok(None);
        };
        let Some(point) =
            self.insert_orphan_merge_parent_nodes(connection, store, input.mode, &orphan)?
        else {
            return Ok(None);
        };
        Ok(Some(VisibleMergeParentPoint {
            node_id: orphan.source_id,
            point,
        }))
    }

    pub fn orphan_merge_parent_lane(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        ancestry: &[Node],
        source_index: usize,
        reserved_lane_y: Option<i32>,
        context_start_id: Option<&str>,
    ) -> crate::Result<Option<OrphanMergeParentLane>> {
        let (fork_source, end_index) = self.orphan_merge_parent_fork_source(
            connection,
            mode,
            ancestry,
            source_index,
            context_start_id,
        )?;
        let nodes = visible_orphan_merge_parent_nodes(mode, ancestry, end_index);
        let Some(source_id) = nodes.last().map(|source| source.id.clone()) else {
            return Ok(None);
        };
        let lane = orphan_merge_parent_lane(
            source_id.as_str(),
            self.next_materialized_lane_y_after_reserved(connection, mode, reserved_lane_y)?,
        );
        Ok(Some(OrphanMergeParentLane {
            source_id,
            lane,
            nodes,
            fork_source,
            context_start_id: context_start_id.map(str::to_owned),
        }))
    }

    pub fn orphan_merge_parent_fork_source(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        ancestry: &[Node],
        source_index: usize,
        context_start_id: Option<&str>,
    ) -> crate::Result<(Option<(String, Point)>, usize)> {
        let end_index =
            scoped_merge_parent_end_index(ancestry, context_start_id).unwrap_or(ancestry.len());
        for (index, node) in ancestry.iter().enumerate().skip(source_index + 1) {
            if index >= end_index {
                break;
            }
            if let Some(point) =
                self.materialized_node_point_in_connection(connection, mode, &node.id)?
            {
                return Ok((Some((node.id.clone(), point)), index));
            }
        }
        Ok((None, end_index))
    }

    pub fn insert_orphan_merge_parent_nodes(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        mode: GraphMode,
        orphan: &OrphanMergeParentLane,
    ) -> crate::Result<Option<Point>> {
        let event_order =
            self.event_order_by_materialized_and_new_nodes(connection, store, mode, &orphan.nodes)?;
        let mut previous = orphan.fork_source.clone();
        let mut source_point = None;
        for (index, node) in orphan.nodes.iter().enumerate() {
            let point = match previous.as_ref() {
                Some((previous_id, previous_point)) => Point {
                    x: previous_point.x
                        + required_column_gap(previous_id, &node.id, &event_order)
                            * GRAPH_COLUMN_WIDTH,
                    y: orphan.lane.y,
                },
                None => Point {
                    x: GRAPH_LEFT_X,
                    y: orphan.lane.y,
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
                    mode,
                    node,
                    primary_parent_id,
                    point,
                    event_order: &event_order,
                    reserved_lane_y: Some(orphan.lane.y),
                    context_start_id: orphan.context_start_id.as_deref(),
                },
            )?
            else {
                return Ok(None);
            };
            let viewport_node = graph_viewport_node_from_node(node, point, Vec::new());
            self.insert_node_location(
                connection,
                NodeLocationInsert {
                    mode,
                    node: &viewport_node,
                    lane: &orphan.lane,
                    bounds: node_bounds(&viewport_node),
                },
            )?;
            if !self.insert_orphan_merge_parent_node_edges(
                connection,
                store,
                OrphanMergeParentNodeEdgeInput {
                    mode,
                    node,
                    point,
                    previous: previous.as_ref(),
                    first_node: index == 0,
                    force_fork: false,
                    context_start_id: orphan.context_start_id.as_deref(),
                },
            )? {
                return Ok(None);
            }
            if matches!(
                self.try_append_skill_invocation_subtree_in_transaction(
                    connection,
                    store,
                    mode,
                    &node.id,
                    point,
                    &orphan.lane,
                )?,
                SkillSubtreeAppend::Unsupported
            ) {
                return Ok(None);
            }
            source_point = Some(point);
            previous = Some((node.id.clone(), point));
        }
        Ok(source_point)
    }

    pub fn insert_orphan_merge_parent_node_edges(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: OrphanMergeParentNodeEdgeInput<'_>,
    ) -> crate::Result<bool> {
        let Some((previous_id, previous_point)) = input.previous else {
            return self.insert_node_merge_edges(
                connection,
                store,
                NodeMergeEdgesInput {
                    mode: input.mode,
                    node: input.node,
                    primary_parent_id: "",
                    target: input.point,
                    context_start_id: input.context_start_id,
                },
            );
        };
        let edge = if input.force_fork || input.first_node && previous_point.y != input.point.y {
            routed_edge(
                GraphViewportEdgeKind::Fork,
                previous_id,
                *previous_point,
                &input.node.id,
                input.point,
                self.next_routed_edge_slot_in_connection(
                    connection,
                    input.mode,
                    *previous_point,
                    input.point,
                )?,
            )
        } else {
            primary_parent_edge(previous_id, *previous_point, &input.node.id, input.point)
        };
        self.insert_edge_route(
            connection,
            EdgeRouteInsert {
                mode: input.mode,
                edge: &edge,
                bounds: edge_bounds(&edge),
            },
        )?;
        self.insert_node_merge_edges(
            connection,
            store,
            NodeMergeEdgesInput {
                mode: input.mode,
                node: input.node,
                primary_parent_id: previous_id,
                target: input.point,
                context_start_id: input.context_start_id,
            },
        )
    }

    pub fn rebalance_target_port_offsets(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        target: Point,
    ) -> crate::Result<()> {
        use console_graph_edge_routes::dsl as edge_routes;

        let mut rows = edge_routes::console_graph_edge_routes
            .filter(edge_routes::mode.eq(mode.as_query_value()))
            .filter(edge_routes::target_x.eq(target.x))
            .filter(edge_routes::target_y.eq(target.y))
            .select((
                edge_routes::edge_key,
                edge_routes::edge_kind,
                edge_routes::source_id,
                edge_routes::target_id,
                edge_routes::source_x,
                edge_routes::source_y,
                edge_routes::target_x,
                edge_routes::target_y,
                edge_routes::route_slot,
                edge_routes::target_port_offset,
            ))
            .load::<EdgeRouteRow>(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        rows.sort_by(|left, right| {
            target_port_rebalance_order(&left.edge_kind)
                .cmp(&target_port_rebalance_order(&right.edge_kind))
                .then_with(|| left.edge_key.cmp(&right.edge_key))
        });
        let mut primary_edges = Vec::new();
        let mut secondary_edges = Vec::new();
        for row in rows {
            let kind = parse_edge_kind(&row.edge_kind)?;
            let edge = GraphViewportEdge {
                key: row.edge_key,
                kind,
                source_id: row.source_id,
                target_id: row.target_id,
                source: Point {
                    x: row.source_x,
                    y: row.source_y,
                },
                target: Point {
                    x: row.target_x,
                    y: row.target_y,
                },
                route_slot: row.route_slot,
                target_port_offset: row.target_port_offset,
            };
            if kind == GraphViewportEdgeKind::PrimaryParent {
                primary_edges.push(edge);
            } else {
                secondary_edges.push(edge);
            }
        }
        let primary_count = primary_edges.len();
        for (index, edge) in primary_edges.iter_mut().enumerate() {
            edge.target_port_offset = primary_incoming_port_offset(primary_count, index);
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode,
                    edge,
                    bounds: edge_bounds(edge),
                },
            )?;
        }
        let secondary_count = secondary_edges.len();
        for (index, edge) in secondary_edges.iter_mut().enumerate() {
            edge.target_port_offset = if primary_count > 0 {
                secondary_incoming_port_offset(index)
            } else {
                primary_incoming_port_offset(secondary_count, index)
            };
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode,
                    edge,
                    bounds: edge_bounds(edge),
                },
            )?;
        }
        Ok(())
    }

    pub fn rebalance_routed_edge_slots(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<()> {
        use console_graph_edge_routes::dsl as edge_routes;

        let mut rows = edge_routes::console_graph_edge_routes
            .filter(edge_routes::mode.eq(mode.as_query_value()))
            .filter(edge_routes::edge_kind.ne("primary_parent"))
            .select((
                edge_routes::edge_key,
                edge_routes::edge_kind,
                edge_routes::source_id,
                edge_routes::target_id,
                edge_routes::source_x,
                edge_routes::source_y,
                edge_routes::target_x,
                edge_routes::target_y,
                edge_routes::route_slot,
                edge_routes::target_port_offset,
            ))
            .load::<EdgeRouteRow>(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        rows.sort_by(|left, right| {
            left.source_y
                .cmp(&right.source_y)
                .then_with(|| {
                    (left.target_y - left.source_y)
                        .signum()
                        .cmp(&(right.target_y - right.source_y).signum())
                })
                .then_with(|| {
                    routed_edge_kind_order(&left.edge_kind)
                        .cmp(&routed_edge_kind_order(&right.edge_kind))
                })
                .then_with(|| left.target_y.cmp(&right.target_y))
                .then_with(|| left.target_x.cmp(&right.target_x))
                .then_with(|| left.edge_key.cmp(&right.edge_key))
        });
        let mut next_slot_by_corridor = BTreeMap::<(i32, i32), i32>::new();
        for row in rows {
            let kind = parse_edge_kind(&row.edge_kind)?;
            let source = Point {
                x: row.source_x,
                y: row.source_y,
            };
            let target = Point {
                x: row.target_x,
                y: row.target_y,
            };
            let direction = (target.y - source.y).signum();
            let next_slot = next_slot_by_corridor
                .entry((source.y, direction))
                .or_default();
            let edge = GraphViewportEdge {
                key: row.edge_key,
                kind,
                source_id: row.source_id,
                target_id: row.target_id,
                source,
                target,
                route_slot: *next_slot,
                target_port_offset: row.target_port_offset,
            };
            *next_slot += 1;
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode,
                    edge: &edge,
                    bounds: edge_bounds(&edge),
                },
            )?;
        }
        Ok(())
    }
}
