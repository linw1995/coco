use super::*;

impl ConsoleGraphSnapshotStore {
    pub async fn try_append_linear_branch(
        &self,
        source_version: u64,
        mode: GraphMode,
        store: &(impl BranchStore + NodeStore + SessionStore),
    ) -> crate::Result<bool> {
        let mut session_states = store
            .list_session_states()
            .await
            .context(crate::error::StoreSnafu)?
            .into_iter()
            .collect::<Vec<_>>();
        session_states.sort_by(|(left, _), (right, _)| {
            branch_lane_priority(left).cmp(&branch_lane_priority(right))
        });
        let source = MaterializationSourceSnapshot::from_store(store, &session_states).await?;

        if session_states.is_empty() {
            let this = self.clone();
            return self
                .with_connection(move |connection| {
                    this.run_bool_write_transaction(connection, |this, connection| {
                        this.put_empty_materialization_in_transaction(
                            connection,
                            source_version,
                            mode,
                        )
                    })
                })
                .await;
        }

        let this = self.clone();
        let materialization_is_empty = self
            .with_connection(move |connection| {
                let has_materialization = this
                    .latest_materialization_row_in_connection(connection, mode)?
                    .is_some();
                Ok(!has_materialization
                    || this
                        .materialized_node_rows_in_connection(connection, mode)?
                        .is_empty())
            })
            .await?;

        if materialization_is_empty {
            return self
                .try_seed_initial_branch_materialization_in_batches(
                    source,
                    source_version,
                    mode,
                    session_states,
                )
                .await;
        }

        let this = self.clone();
        self.with_connection(move |connection| {
            this.run_bool_write_transaction(connection, |this, connection| match mode {
                GraphMode::Anchors => this.try_update_anchor_materialization_in_transaction(
                    connection,
                    &source,
                    source_version,
                    &session_states,
                ),
                GraphMode::All => this.try_append_linear_branches_in_transaction(
                    connection,
                    &source,
                    source_version,
                    mode,
                    &session_states,
                ),
            })
        })
        .await
    }

    pub async fn try_seed_initial_branch_materialization_in_batches(
        &self,
        source: MaterializationSourceSnapshot,
        source_version: u64,
        mode: GraphMode,
        session_states: Vec<(String, SessionState)>,
    ) -> crate::Result<bool> {
        let this = self.clone();
        self.with_connection(move |connection| {
            this.run_bool_write_transaction(connection, |this, connection| {
                let Some(first_index) =
                    this.first_visible_initial_branch_index(&source, mode, &session_states)?
                else {
                    this.delete_materialization_meta(connection, mode)?;
                    return this.put_empty_materialization_in_transaction(
                        connection,
                        source_version,
                        mode,
                    );
                };

                this.delete_materialization_meta(connection, mode)?;
                this.clear_materialized_mode_facts(connection, mode)?;

                let (first_branch, first_state) = &session_states[first_index];
                if !this.try_seed_first_branch_materialization_in_transaction(
                    connection,
                    &source,
                    mode,
                    first_branch,
                    first_state,
                )? {
                    return Ok(false);
                }

                let mut next_lane_y = crate::layout::GRAPH_TOP_Y + GRAPH_LANE_HEIGHT;
                for (branch, state) in session_states[first_index..].iter().skip(1) {
                    if !this.branch_has_initial_visible_nodes(&source, mode, branch)? {
                        continue;
                    }
                    let head_id = source
                        .branch_head(branch)
                        .context(crate::error::StoreSnafu)?;
                    this.shift_lanes_for_insertion(connection, mode, next_lane_y)?;
                    let input = AppendLinearBranchInput {
                        mode,
                        branch,
                        state,
                        head_id: &head_id,
                    };
                    let appended = match mode {
                        GraphMode::Anchors => this
                            .try_append_new_anchor_branch_lane_in_transaction(
                                connection,
                                &source,
                                input,
                                next_lane_y,
                            ),
                        GraphMode::All => this.try_append_new_branch_lane_in_transaction(
                            connection,
                            &source,
                            input,
                            next_lane_y,
                        ),
                    }?;
                    if !appended {
                        return Ok(false);
                    }
                    next_lane_y += GRAPH_LANE_HEIGHT;
                }

                this.rebalance_routed_edge_slots(connection, mode)?;
                if this
                    .refresh_materialized_node_labels(connection, &source, mode, &session_states)?
                    .is_none()
                {
                    return Ok(false);
                }
                this.put_materialization_meta_from_materialized_rows(
                    connection,
                    source_version,
                    mode,
                )?;
                Ok(true)
            })
        })
        .await
    }

    #[cfg(test)]
    pub fn restore_empty_materialization_after_failed_batch_seed(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        previous_meta: Option<MaterializationRow>,
    ) -> crate::Result<()> {
        self.run_write_transaction(connection, |this, connection| {
            this.delete_materialization_meta(connection, mode)?;
            this.clear_materialized_mode_facts(connection, mode)?;
            if let Some(meta) = previous_meta {
                this.put_materialization_meta(
                    connection,
                    MaterializationMetaInput {
                        source_version: meta.source_version.max(0) as u64,
                        mode,
                        world_min_x: meta.world_min_x,
                        world_min_y: meta.world_min_y,
                        world_max_x: meta.world_max_x,
                        world_max_y: meta.world_max_y,
                    },
                )?;
            }
            Ok(())
        })
    }

    pub async fn replace_materialization_from_viewport(
        &self,
        mode: GraphMode,
        viewport: GraphViewportResponse,
        branch_labels: BTreeSet<String>,
    ) -> crate::Result<()> {
        let this = self.clone();
        self.with_connection(move |connection| {
            this.finish_transaction(connection.immediate_transaction(|connection| {
                this.replace_materialization_from_viewport_in_transaction(
                    connection,
                    mode,
                    viewport,
                    branch_labels,
                )
                .map_err(SnapshotTransactionError::Operation)
            }))
        })
        .await
    }

    pub fn replace_materialization_from_viewport_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        viewport: GraphViewportResponse,
        branch_labels: BTreeSet<String>,
    ) -> crate::Result<()> {
        let mut nodes_by_y = BTreeMap::<i32, Vec<&GraphViewportNode>>::new();
        for node in &viewport.nodes {
            nodes_by_y.entry(node.y).or_default().push(node);
        }
        let lanes_by_y = viewport
            .lanes
            .iter()
            .map(|lane| {
                (
                    lane.y,
                    full_layout_materialization_lane(lane, &nodes_by_y, &branch_labels),
                )
            })
            .collect::<BTreeMap<_, _>>();
        self.clear_materialized_mode_facts(connection, mode)?;
        for node in &viewport.nodes {
            let fallback_lane;
            let lane = if let Some(lane) = lanes_by_y.get(&node.y) {
                lane
            } else {
                fallback_lane = GraphViewportLane {
                    key: format!("layout:y:{}", node.y),
                    label: String::new(),
                    y: node.y,
                };
                &fallback_lane
            };
            self.insert_node_location(
                connection,
                NodeLocationInsert {
                    mode,
                    node,
                    lane,
                    bounds: node_bounds(node),
                },
            )?;
        }
        for edge in &viewport.edges {
            self.insert_edge_route(
                connection,
                EdgeRouteInsert {
                    mode,
                    edge,
                    bounds: edge_bounds(edge),
                },
            )?;
        }
        self.put_materialization_meta(
            connection,
            MaterializationMetaInput {
                source_version: viewport.version,
                mode,
                world_min_x: 0,
                world_min_y: 0,
                world_max_x: viewport.canvas.width,
                world_max_y: viewport.canvas.height,
            },
        )
    }

    pub fn put_empty_materialization_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        source_version: u64,
        mode: GraphMode,
    ) -> crate::Result<bool> {
        self.clear_materialized_mode_facts(connection, mode)?;
        self.put_materialization_meta(
            connection,
            MaterializationMetaInput {
                source_version,
                mode,
                world_min_x: 0,
                world_min_y: 0,
                world_max_x: GRAPH_LEFT_X + 120,
                world_max_y: crate::layout::GRAPH_TOP_Y + 120,
            },
        )?;
        Ok(true)
    }

    pub fn first_visible_initial_branch_index(
        &self,
        store: &MaterializationSourceSnapshot,
        mode: GraphMode,
        session_states: &[(String, SessionState)],
    ) -> crate::Result<Option<usize>> {
        for (index, (branch, _)) in session_states.iter().enumerate() {
            if self.branch_has_initial_visible_nodes(store, mode, branch)? {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }

    pub fn branch_has_initial_visible_nodes(
        &self,
        store: &MaterializationSourceSnapshot,
        mode: GraphMode,
        branch: &str,
    ) -> crate::Result<bool> {
        let head_id = store
            .branch_head(branch)
            .context(crate::error::StoreSnafu)?;
        let ancestry = store
            .ancestry_nodes(&head_id)
            .context(crate::error::StoreSnafu)?;
        Ok(!initial_visible_graph_lane_nodes(store, mode, ancestry)?.is_empty())
    }

    pub fn try_seed_first_branch_materialization_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        mode: GraphMode,
        first_branch: &str,
        first_state: &SessionState,
    ) -> crate::Result<bool> {
        let head_id = store
            .branch_head(first_branch)
            .context(crate::error::StoreSnafu)?;
        let ancestry = store
            .ancestry_nodes(&head_id)
            .context(crate::error::StoreSnafu)?;
        let context_start_id = merge_parent_context_start_id(mode, &ancestry);
        let nodes = initial_visible_graph_lane_nodes(store, mode, ancestry)?;
        if nodes.is_empty() || !initial_visible_lane_is_linear(mode, &nodes) {
            return Ok(false);
        }

        let lane = GraphViewportLane {
            key: lane_key(first_branch),
            label: first_branch.to_owned(),
            y: crate::layout::GRAPH_TOP_Y,
        };
        let branch_label = branch_label(first_branch, first_state);
        let event_order =
            self.event_order_by_materialized_and_new_nodes(connection, store, mode, &nodes)?;
        let mut previous = None::<(String, Point)>;
        let appended_len = nodes.len();
        for (index, node) in nodes.into_iter().enumerate() {
            let candidate = match previous.as_ref() {
                Some((previous_id, previous_point)) => Point {
                    x: previous_point.x
                        + required_column_gap(previous_id, &node.id, &event_order)
                            * GRAPH_COLUMN_WIDTH,
                    y: lane.y,
                },
                None => Point {
                    x: GRAPH_LEFT_X,
                    y: lane.y,
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
                    node: &node,
                    primary_parent_id,
                    point: candidate,
                    event_order: &event_order,
                    reserved_lane_y: Some(lane.y),
                    context_start_id: context_start_id.as_deref(),
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
                    mode,
                    node: &viewport_node,
                    lane: &lane,
                    bounds: node_bounds(&viewport_node),
                },
            )?;
            if let Some((previous_id, previous_point)) = previous.as_ref() {
                let edge = primary_parent_edge(previous_id, *previous_point, &node.id, point);
                self.insert_edge_route(
                    connection,
                    EdgeRouteInsert {
                        mode,
                        edge: &edge,
                        bounds: edge_bounds(&edge),
                    },
                )?;
                if !self.insert_node_merge_edges(
                    connection,
                    store,
                    NodeMergeEdgesInput {
                        mode,
                        node: &node,
                        primary_parent_id: previous_id,
                        target: point,
                        context_start_id: context_start_id.as_deref(),
                    },
                )? {
                    return Ok(false);
                }
            } else if !self.insert_node_merge_edges(
                connection,
                store,
                NodeMergeEdgesInput {
                    mode,
                    node: &node,
                    primary_parent_id: "",
                    target: point,
                    context_start_id: context_start_id.as_deref(),
                },
            )? {
                return Ok(false);
            }
            if matches!(
                self.try_append_skill_invocation_subtree_in_transaction(
                    connection, store, mode, &node.id, point, &lane,
                )?,
                SkillSubtreeAppend::Unsupported
            ) {
                return Ok(false);
            }
            previous = Some((node.id, point));
        }

        Ok(true)
    }

    pub fn put_materialization_meta_from_materialized_rows(
        &self,
        connection: &mut SqliteConnection,
        source_version: u64,
        mode: GraphMode,
    ) -> crate::Result<()> {
        let materialized_nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        let world_max_x = materialized_nodes
            .iter()
            .map(|row| row.x)
            .max()
            .unwrap_or(GRAPH_LEFT_X)
            + 120;
        let world_max_y = self
            .materialized_lanes_in_connection(connection, mode)?
            .iter()
            .map(|lane| lane.lane_y)
            .max()
            .unwrap_or(crate::layout::GRAPH_TOP_Y)
            + 120;
        self.put_materialization_meta(
            connection,
            MaterializationMetaInput {
                source_version,
                mode,
                world_min_x: 0,
                world_min_y: 0,
                world_max_x,
                world_max_y,
            },
        )?;
        Ok(())
    }

    pub fn delete_materialized_branch_lane_if_isolated(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
    ) -> crate::Result<bool> {
        let Some(lane) = self
            .materialized_lanes_in_connection(connection, mode)?
            .into_iter()
            .find(|lane| lane.lane_key == lane_key(branch))
        else {
            return Ok(true);
        };
        if self.lanes_have_retained_downstream_edges(
            connection,
            mode,
            std::slice::from_ref(&lane),
        )? {
            return Ok(false);
        }
        self.delete_materialized_lanes(connection, mode, std::slice::from_ref(&lane))?;
        self.shift_lanes_after_deletion(connection, mode, std::slice::from_ref(&lane))?;
        Ok(true)
    }

    pub fn try_append_linear_branch_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: AppendLinearBranchInput<'_>,
    ) -> crate::Result<bool> {
        let Some(tail) =
            self.latest_lane_tail_in_connection(connection, input.mode, input.branch)?
        else {
            return Ok(false);
        };
        let branch_label = branch_label(input.branch, input.state);
        if let Some(appended) = self.try_append_unchanged_head_skill_subtree_in_transaction(
            connection, store, &input, &tail,
        )? {
            return Ok(appended);
        }
        if let Some(appended) = self.try_refresh_materialized_branch_head_in_transaction(
            connection,
            store,
            &input,
            &tail,
            &branch_label,
        )? {
            return Ok(appended);
        }
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

        self.update_node_labels(connection, input.mode, &tail.node_key, Vec::new())?;
        let lane = GraphViewportLane {
            key: tail.lane_key,
            label: tail.lane_label,
            y: tail.lane_y,
        };
        if matches!(
            self.try_append_skill_invocation_subtree_in_transaction(
                connection,
                store,
                input.mode,
                &tail.node_id,
                Point {
                    x: tail.x,
                    y: tail.y,
                },
                &lane,
            )?,
            SkillSubtreeAppend::Unsupported
        ) {
            return Ok(false);
        }
        let appended_nodes = chain.into_iter().skip(1).collect::<Vec<_>>();
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
        let appended_len = appended_nodes.len();
        for (index, node) in appended_nodes.into_iter().enumerate() {
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
            previous_id = node.id;
            previous_point = point;
        }
        Ok(true)
    }

    pub fn try_append_unchanged_head_skill_subtree_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: &AppendLinearBranchInput<'_>,
        tail: &MaterializedTailNodeRow,
    ) -> crate::Result<Option<bool>> {
        if input.head_id != tail.node_id {
            return Ok(None);
        }
        let lane = GraphViewportLane {
            key: tail.lane_key.clone(),
            label: tail.lane_label.clone(),
            y: tail.lane_y,
        };
        match self.try_append_skill_invocation_subtree_in_transaction(
            connection,
            store,
            input.mode,
            &tail.node_id,
            Point {
                x: tail.x,
                y: tail.y,
            },
            &lane,
        )? {
            SkillSubtreeAppend::Unsupported => Ok(Some(false)),
            SkillSubtreeAppend::Absent | SkillSubtreeAppend::Applied => {
                self.trim_branch_lane_covered_prefix(connection, input.mode, input.branch)?;
                Ok(Some(true))
            }
        }
    }

    pub fn try_refresh_materialized_branch_head_in_transaction(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        input: &AppendLinearBranchInput<'_>,
        tail: &MaterializedTailNodeRow,
        branch_label: &str,
    ) -> crate::Result<Option<bool>> {
        let Some(head) = self.materialized_lane_node_in_connection(
            connection,
            input.mode,
            input.branch,
            input.head_id,
        )?
        else {
            return Ok(None);
        };
        if head.x >= tail.x {
            return Ok(None);
        }

        let lane = GraphViewportLane {
            key: head.lane_key.clone(),
            label: head.lane_label.clone(),
            y: head.lane_y,
        };
        match self.try_append_skill_invocation_subtree_in_transaction(
            connection,
            store,
            input.mode,
            input.head_id,
            Point {
                x: head.x,
                y: head.y,
            },
            &lane,
        )? {
            SkillSubtreeAppend::Applied => {
                if self.lane_suffix_has_retained_downstream_edges(
                    connection,
                    input.mode,
                    input.branch,
                    head.x,
                )? {
                    return Ok(Some(false));
                }
                self.delete_materialized_lane_suffix(connection, input.mode, input.branch, head.x)?;
                self.update_node_labels(
                    connection,
                    input.mode,
                    &head.node_key,
                    vec![branch_label.to_owned()],
                )?;
                return Ok(Some(true));
            }
            SkillSubtreeAppend::Unsupported => return Ok(Some(false)),
            SkillSubtreeAppend::Absent => {}
        }
        if self.lane_suffix_has_retained_downstream_edges(
            connection,
            input.mode,
            input.branch,
            head.x,
        )? {
            return Ok(Some(false));
        }
        self.delete_materialized_lane_suffix(connection, input.mode, input.branch, head.x)?;
        self.update_node_labels(
            connection,
            input.mode,
            &head.node_key,
            vec![branch_label.to_owned()],
        )?;
        Ok(Some(true))
    }
}
