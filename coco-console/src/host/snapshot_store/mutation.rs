use super::*;

impl ConsoleGraphSnapshotStore {
    pub fn delete_materialization_meta(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<()> {
        use console_graph_materializations::dsl as materializations;

        diesel::delete(
            materializations::console_graph_materializations
                .filter(materializations::mode.eq(mode.as_query_value())),
        )
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    pub fn put_materialization_meta(
        &self,
        connection: &mut SqliteConnection,
        input: MaterializationMetaInput,
    ) -> crate::Result<()> {
        use console_graph_materializations::dsl as materializations;

        let row = MaterializationInsert {
            mode: input.mode.as_query_value(),
            source_version: input.source_version as i64,
            coordinate_space: COORDINATE_SPACE,
            world_min_x: input.world_min_x,
            world_min_y: input.world_min_y,
            world_max_x: input.world_max_x,
            world_max_y: input.world_max_y,
        };
        let updated_at = jiff::Timestamp::now().to_string();
        diesel::insert_into(materializations::console_graph_materializations)
            .values(&row)
            .on_conflict(materializations::mode)
            .do_update()
            .set((
                materializations::source_version
                    .eq(diesel::upsert::excluded(materializations::source_version)),
                materializations::coordinate_space
                    .eq(diesel::upsert::excluded(materializations::coordinate_space)),
                materializations::world_min_x
                    .eq(diesel::upsert::excluded(materializations::world_min_x)),
                materializations::world_min_y
                    .eq(diesel::upsert::excluded(materializations::world_min_y)),
                materializations::world_max_x
                    .eq(diesel::upsert::excluded(materializations::world_max_x)),
                materializations::world_max_y
                    .eq(diesel::upsert::excluded(materializations::world_max_y)),
                materializations::updated_at.eq(updated_at),
            ))
            .execute(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        Ok(())
    }

    pub fn insert_node_location(
        &self,
        connection: &mut SqliteConnection,
        insert: NodeLocationInsert<'_>,
    ) -> crate::Result<()> {
        use console_graph_node_locations::dsl as node_locations;

        let labels_json = serde_json::to_string(&insert.node.labels).context(
            ParseGraphSnapshotStoreValueSnafu {
                column: "console_graph_node_locations.labels_json",
            },
        )?;
        diesel::query_dsl::methods::FilterDsl::filter(
            diesel::insert_into(node_locations::console_graph_node_locations)
                .values((
                    node_locations::mode.eq(insert.mode.as_query_value()),
                    node_locations::node_key.eq(&insert.node.key),
                    node_locations::node_id.eq(&insert.node.id),
                    node_locations::node_target.eq(&insert.node.node_target),
                    node_locations::short_id.eq(&insert.node.short_id),
                    node_locations::node_kind.eq(&insert.node.kind),
                    node_locations::summary.eq(&insert.node.summary),
                    node_locations::labels_json.eq(labels_json),
                    node_locations::lane_key.eq(&insert.lane.key),
                    node_locations::lane_label.eq(&insert.lane.label),
                    node_locations::lane_y.eq(insert.lane.y),
                    node_locations::x.eq(insert.node.x),
                    node_locations::y.eq(insert.node.y),
                    node_locations::min_x.eq(insert.bounds.left),
                    node_locations::min_y.eq(insert.bounds.top),
                    node_locations::max_x.eq(insert.bounds.right),
                    node_locations::max_y.eq(insert.bounds.bottom),
                ))
                .on_conflict((node_locations::mode, node_locations::node_key))
                .do_update()
                .set((
                    node_locations::node_id.eq(diesel::upsert::excluded(node_locations::node_id)),
                    node_locations::node_target
                        .eq(diesel::upsert::excluded(node_locations::node_target)),
                    node_locations::short_id.eq(diesel::upsert::excluded(node_locations::short_id)),
                    node_locations::node_kind
                        .eq(diesel::upsert::excluded(node_locations::node_kind)),
                    node_locations::summary.eq(diesel::upsert::excluded(node_locations::summary)),
                    node_locations::labels_json
                        .eq(diesel::upsert::excluded(node_locations::labels_json)),
                    node_locations::lane_key.eq(diesel::upsert::excluded(node_locations::lane_key)),
                    node_locations::lane_label
                        .eq(diesel::upsert::excluded(node_locations::lane_label)),
                    node_locations::lane_y.eq(diesel::upsert::excluded(node_locations::lane_y)),
                    node_locations::x.eq(diesel::upsert::excluded(node_locations::x)),
                    node_locations::y.eq(diesel::upsert::excluded(node_locations::y)),
                    node_locations::min_x.eq(diesel::upsert::excluded(node_locations::min_x)),
                    node_locations::min_y.eq(diesel::upsert::excluded(node_locations::min_y)),
                    node_locations::max_x.eq(diesel::upsert::excluded(node_locations::max_x)),
                    node_locations::max_y.eq(diesel::upsert::excluded(node_locations::max_y)),
                )),
            node_locations::node_id
                .ne(diesel::upsert::excluded(node_locations::node_id))
                .or(node_locations::node_target
                    .ne(diesel::upsert::excluded(node_locations::node_target)))
                .or(node_locations::short_id.ne(diesel::upsert::excluded(node_locations::short_id)))
                .or(node_locations::node_kind
                    .ne(diesel::upsert::excluded(node_locations::node_kind)))
                .or(node_locations::summary.ne(diesel::upsert::excluded(node_locations::summary)))
                .or(node_locations::labels_json
                    .ne(diesel::upsert::excluded(node_locations::labels_json)))
                .or(node_locations::lane_key.ne(diesel::upsert::excluded(node_locations::lane_key)))
                .or(node_locations::lane_label
                    .ne(diesel::upsert::excluded(node_locations::lane_label)))
                .or(node_locations::lane_y.ne(diesel::upsert::excluded(node_locations::lane_y)))
                .or(node_locations::x.ne(diesel::upsert::excluded(node_locations::x)))
                .or(node_locations::y.ne(diesel::upsert::excluded(node_locations::y)))
                .or(node_locations::min_x.ne(diesel::upsert::excluded(node_locations::min_x)))
                .or(node_locations::min_y.ne(diesel::upsert::excluded(node_locations::min_y)))
                .or(node_locations::max_x.ne(diesel::upsert::excluded(node_locations::max_x)))
                .or(node_locations::max_y.ne(diesel::upsert::excluded(node_locations::max_y))),
        )
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    pub fn insert_edge_route(
        &self,
        connection: &mut SqliteConnection,
        insert: EdgeRouteInsert<'_>,
    ) -> crate::Result<()> {
        use console_graph_edge_routes::dsl as edge_routes;

        diesel::query_dsl::methods::FilterDsl::filter(
            diesel::insert_into(edge_routes::console_graph_edge_routes)
                .values((
                    edge_routes::mode.eq(insert.mode.as_query_value()),
                    edge_routes::edge_key.eq(&insert.edge.key),
                    edge_routes::edge_kind.eq(edge_kind_query_value(insert.edge.kind)),
                    edge_routes::source_id.eq(&insert.edge.source_id),
                    edge_routes::target_id.eq(&insert.edge.target_id),
                    edge_routes::source_x.eq(insert.edge.source.x),
                    edge_routes::source_y.eq(insert.edge.source.y),
                    edge_routes::target_x.eq(insert.edge.target.x),
                    edge_routes::target_y.eq(insert.edge.target.y),
                    edge_routes::route_slot.eq(insert.edge.route_slot),
                    edge_routes::target_port_offset.eq(insert.edge.target_port_offset),
                    edge_routes::min_x.eq(insert.bounds.left),
                    edge_routes::min_y.eq(insert.bounds.top),
                    edge_routes::max_x.eq(insert.bounds.right),
                    edge_routes::max_y.eq(insert.bounds.bottom),
                ))
                .on_conflict((edge_routes::mode, edge_routes::edge_key))
                .do_update()
                .set((
                    edge_routes::edge_kind.eq(diesel::upsert::excluded(edge_routes::edge_kind)),
                    edge_routes::source_id.eq(diesel::upsert::excluded(edge_routes::source_id)),
                    edge_routes::target_id.eq(diesel::upsert::excluded(edge_routes::target_id)),
                    edge_routes::source_x.eq(diesel::upsert::excluded(edge_routes::source_x)),
                    edge_routes::source_y.eq(diesel::upsert::excluded(edge_routes::source_y)),
                    edge_routes::target_x.eq(diesel::upsert::excluded(edge_routes::target_x)),
                    edge_routes::target_y.eq(diesel::upsert::excluded(edge_routes::target_y)),
                    edge_routes::route_slot.eq(diesel::upsert::excluded(edge_routes::route_slot)),
                    edge_routes::target_port_offset
                        .eq(diesel::upsert::excluded(edge_routes::target_port_offset)),
                    edge_routes::min_x.eq(diesel::upsert::excluded(edge_routes::min_x)),
                    edge_routes::min_y.eq(diesel::upsert::excluded(edge_routes::min_y)),
                    edge_routes::max_x.eq(diesel::upsert::excluded(edge_routes::max_x)),
                    edge_routes::max_y.eq(diesel::upsert::excluded(edge_routes::max_y)),
                )),
            edge_routes::edge_kind
                .ne(diesel::upsert::excluded(edge_routes::edge_kind))
                .or(edge_routes::source_id.ne(diesel::upsert::excluded(edge_routes::source_id)))
                .or(edge_routes::target_id.ne(diesel::upsert::excluded(edge_routes::target_id)))
                .or(edge_routes::source_x.ne(diesel::upsert::excluded(edge_routes::source_x)))
                .or(edge_routes::source_y.ne(diesel::upsert::excluded(edge_routes::source_y)))
                .or(edge_routes::target_x.ne(diesel::upsert::excluded(edge_routes::target_x)))
                .or(edge_routes::target_y.ne(diesel::upsert::excluded(edge_routes::target_y)))
                .or(edge_routes::route_slot.ne(diesel::upsert::excluded(edge_routes::route_slot)))
                .or(edge_routes::target_port_offset
                    .ne(diesel::upsert::excluded(edge_routes::target_port_offset)))
                .or(edge_routes::min_x.ne(diesel::upsert::excluded(edge_routes::min_x)))
                .or(edge_routes::min_y.ne(diesel::upsert::excluded(edge_routes::min_y)))
                .or(edge_routes::max_x.ne(diesel::upsert::excluded(edge_routes::max_x)))
                .or(edge_routes::max_y.ne(diesel::upsert::excluded(edge_routes::max_y))),
        )
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    pub fn update_node_labels(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_key: &str,
        labels: Vec<String>,
    ) -> crate::Result<()> {
        let labels_json =
            serde_json::to_string(&labels).context(ParseGraphSnapshotStoreValueSnafu {
                column: "console_graph_node_locations.labels_json",
            })?;
        use console_graph_node_locations::dsl as node_locations;

        diesel::update(
            node_locations::console_graph_node_locations.filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_key.eq(node_key))
                    .and(node_locations::labels_json.ne(&labels_json)),
            ),
        )
        .set(node_locations::labels_json.eq(&labels_json))
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    pub fn update_node_id_labels(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        labels: Vec<String>,
    ) -> crate::Result<()> {
        let labels_json =
            serde_json::to_string(&labels).context(ParseGraphSnapshotStoreValueSnafu {
                column: "console_graph_node_locations.labels_json",
            })?;
        use console_graph_node_locations::dsl as node_locations;

        diesel::update(
            node_locations::console_graph_node_locations.filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::labels_json.ne(&labels_json)),
            ),
        )
        .set(node_locations::labels_json.eq(&labels_json))
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    pub fn materialized_node_label_set(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
    ) -> crate::Result<BTreeSet<String>> {
        use console_graph_node_locations::dsl as node_locations;

        let rows = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id)),
            )
            .select(node_locations::labels_json)
            .load::<String>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        let mut labels = BTreeSet::new();
        for labels_json in rows {
            let row_labels = serde_json::from_str::<Vec<String>>(&labels_json).context(
                ParseGraphSnapshotStoreValueSnafu {
                    column: "console_graph_node_locations.labels_json",
                },
            )?;
            labels.extend(row_labels);
        }
        Ok(labels)
    }

    pub fn delete_materialized_lanes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lanes: &[LaneRow],
    ) -> crate::Result<()> {
        for lane in lanes {
            use console_graph_edge_routes::dsl as edge_routes;
            use console_graph_node_locations::dsl as node_locations;

            diesel::delete(
                edge_routes::console_graph_edge_routes.filter(
                    edge_routes::mode.eq(mode.as_query_value()).and(
                        edge_routes::source_y
                            .eq(lane.lane_y)
                            .or(edge_routes::target_y.eq(lane.lane_y)),
                    ),
                ),
            )
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
            diesel::delete(
                node_locations::console_graph_node_locations.filter(
                    node_locations::mode
                        .eq(mode.as_query_value())
                        .and(node_locations::lane_key.eq(&lane.lane_key)),
                ),
            )
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        }
        Ok(())
    }

    pub fn delete_materialized_node_occurrences(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        nodes: &[MaterializedTailNodeRow],
    ) -> crate::Result<()> {
        for node in nodes {
            use console_graph_edge_routes::dsl as edge_routes;
            use console_graph_node_locations::dsl as node_locations;

            diesel::delete(
                edge_routes::console_graph_edge_routes.filter(
                    edge_routes::mode.eq(mode.as_query_value()).and(
                        edge_routes::source_id
                            .eq(&node.node_id)
                            .and(edge_routes::source_x.eq(node.x))
                            .and(edge_routes::source_y.eq(node.y))
                            .or(edge_routes::target_id
                                .eq(&node.node_id)
                                .and(edge_routes::target_x.eq(node.x))
                                .and(edge_routes::target_y.eq(node.y))),
                    ),
                ),
            )
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
            diesel::delete(
                node_locations::console_graph_node_locations.filter(
                    node_locations::mode
                        .eq(mode.as_query_value())
                        .and(node_locations::node_key.eq(&node.node_key)),
                ),
            )
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        }
        Ok(())
    }

    pub fn prune_removable_derived_lanes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<()> {
        let mut lanes = Vec::new();
        for lane in self.materialized_lanes_in_connection(connection, mode)? {
            self.trim_covered_derived_lane_prefix(connection, mode, &lane.lane_key)?;
            let covered = is_derived_lane_key(&lane.lane_key)
                && self.derived_lane_nodes_are_covered_by_branch_lanes(
                    connection,
                    mode,
                    &lane.lane_key,
                )?;
            let should_prune = is_orphan_lane_key(&lane.lane_key)
                && (!self.lane_has_external_outgoing_edge(connection, mode, &lane.lane_key)?
                    || covered);
            let should_prune = should_prune
                || is_skill_invocation_lane_key(&lane.lane_key)
                    && (!self.lane_has_external_edge(connection, mode, &lane.lane_key)? || covered);
            if should_prune {
                if covered {
                    self.migrate_covered_derived_lane_outgoing_edges(
                        connection,
                        mode,
                        &lane.lane_key,
                    )?;
                }
                lanes.push(lane);
            }
        }
        self.delete_materialized_lanes(connection, mode, &lanes)?;
        self.shift_lanes_after_deletion(connection, mode, &lanes)
    }

    pub fn trim_covered_derived_lane_prefix(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<()> {
        if !is_derived_lane_key(lane_key) {
            return Ok(());
        }
        let nodes =
            self.materialized_node_rows_by_lane_key_in_connection(connection, mode, lane_key)?;
        let mut covered_prefix = Vec::new();
        for node in &nodes {
            let Some(cover) =
                self.materialized_branch_node_point_in_connection(connection, mode, &node.node_id)?
            else {
                break;
            };
            covered_prefix.push((node.clone(), cover));
        }
        if covered_prefix.is_empty() || covered_prefix.len() == nodes.len() {
            return Ok(());
        }
        if is_skill_invocation_lane_key(lane_key) && covered_prefix.len() < 2 {
            return Ok(());
        }

        self.migrate_covered_derived_lane_outgoing_edges(connection, mode, lane_key)?;
        self.delete_materialized_node_occurrences(
            connection,
            mode,
            &covered_prefix
                .iter()
                .map(|(node, _)| node.clone())
                .collect::<Vec<_>>(),
        )?;
        let (source, source_point) = covered_prefix.last().expect("prefix is not empty");
        let target = &nodes[covered_prefix.len()];
        let target_point = Point {
            x: target.x,
            y: target.y,
        };
        let edge = routed_edge(
            GraphViewportEdgeKind::Fork,
            &source.node_id,
            *source_point,
            &target.node_id,
            target_point,
            self.next_routed_edge_slot_in_connection(
                connection,
                mode,
                *source_point,
                target_point,
            )?,
        );
        self.insert_edge_route(
            connection,
            EdgeRouteInsert {
                mode,
                edge: &edge,
                bounds: edge_bounds(&edge),
            },
        )?;
        self.rebalance_target_port_offsets(connection, mode, target_point)
    }

    pub fn trim_branch_lane_covered_prefix(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
    ) -> crate::Result<()> {
        let lane_key = lane_key(branch);
        let nodes =
            self.materialized_node_rows_by_lane_key_in_connection(connection, mode, &lane_key)?;
        if nodes.len() < 2 {
            return Ok(());
        }
        let lane_y = nodes[0].y;
        let mut covered_prefix = Vec::new();
        for node in &nodes {
            let Some(cover) = self.materialized_branch_node_point_before_lane_in_connection(
                connection,
                mode,
                &node.node_id,
                lane_y,
            )?
            else {
                break;
            };
            covered_prefix.push((node.clone(), cover));
        }
        if covered_prefix.is_empty() {
            return Ok(());
        }

        if covered_prefix.len() == nodes.len() {
            self.trim_fully_covered_branch_lane(connection, mode, &nodes, &covered_prefix)
        } else {
            self.trim_partially_covered_branch_lane_prefix(
                connection,
                mode,
                &nodes,
                &covered_prefix,
            )
        }
    }

    pub fn trim_fully_covered_branch_lane(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        nodes: &[MaterializedTailNodeRow],
        covered_prefix: &[(MaterializedTailNodeRow, Point)],
    ) -> crate::Result<()> {
        let Some((alias, _)) = covered_prefix.last() else {
            return Ok(());
        };
        let incoming = self.primary_incoming_edge_to_node_occurrence(
            connection,
            mode,
            &alias.node_id,
            alias.x,
            alias.y,
        )?;
        self.delete_materialized_node_occurrences(
            connection,
            mode,
            &nodes[..nodes.len().saturating_sub(1)],
        )?;
        let Some(incoming) = incoming else {
            return Ok(());
        };
        let Some(source) = self.materialized_branch_node_point_before_lane_in_connection(
            connection,
            mode,
            &incoming.source_id,
            alias.y,
        )?
        else {
            return Ok(());
        };
        let target = Point {
            x: alias.x,
            y: alias.y,
        };
        self.insert_trimmed_branch_fork_edge(
            connection,
            mode,
            &incoming.source_id,
            source,
            alias,
            target,
        )
    }

    pub fn trim_partially_covered_branch_lane_prefix(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        nodes: &[MaterializedTailNodeRow],
        covered_prefix: &[(MaterializedTailNodeRow, Point)],
    ) -> crate::Result<()> {
        self.delete_materialized_node_occurrences(
            connection,
            mode,
            &covered_prefix
                .iter()
                .map(|(node, _)| node.clone())
                .collect::<Vec<_>>(),
        )?;
        let (source, source_point) = covered_prefix.last().expect("prefix is not empty");
        let target = &nodes[covered_prefix.len()];
        let target_point = Point {
            x: target.x,
            y: target.y,
        };
        self.insert_trimmed_branch_fork_edge(
            connection,
            mode,
            &source.node_id,
            *source_point,
            target,
            target_point,
        )
    }

    pub fn insert_trimmed_branch_fork_edge(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        source_id: &str,
        source: Point,
        target: &MaterializedTailNodeRow,
        target_point: Point,
    ) -> crate::Result<()> {
        let edge = routed_edge(
            GraphViewportEdgeKind::Fork,
            source_id,
            source,
            &target.node_id,
            target_point,
            self.next_routed_edge_slot_in_connection(connection, mode, source, target_point)?,
        );
        self.insert_edge_route(
            connection,
            EdgeRouteInsert {
                mode,
                edge: &edge,
                bounds: edge_bounds(&edge),
            },
        )?;
        self.rebalance_target_port_offsets(connection, mode, target_point)
    }

    pub fn migrate_covered_derived_lane_outgoing_edges(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<()> {
        let edges = self.materialized_edge_route_rows_in_connection(connection, mode)?;
        let nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        let mut rows = Vec::new();
        for edge in edges {
            if !node_point_on_lane(
                &nodes,
                lane_key,
                &edge.source_id,
                Point {
                    x: edge.source_x,
                    y: edge.source_y,
                },
            ) || node_point_on_lane(
                &nodes,
                lane_key,
                &edge.target_id,
                Point {
                    x: edge.target_x,
                    y: edge.target_y,
                },
            ) {
                continue;
            }
            for cover in nodes.iter().filter(|node| {
                node.node_id == edge.source_id
                    && node.lane_key != lane_key
                    && !is_derived_orphan_or_skill_lane(&node.lane_key)
            }) {
                rows.push((
                    edge.clone(),
                    Point {
                        x: cover.x,
                        y: cover.y,
                    },
                ));
            }
        }
        rows.sort_by(|(left_edge, left_cover), (right_edge, right_cover)| {
            left_edge
                .edge_key
                .cmp(&right_edge.edge_key)
                .then_with(|| left_cover.y.cmp(&right_cover.y))
                .then_with(|| left_cover.x.cmp(&right_cover.x))
        });
        for (row, source) in rows {
            let kind = parse_edge_kind(&row.edge_kind)?;
            let target = Point {
                x: row.target_x,
                y: row.target_y,
            };
            let edge = GraphViewportEdge {
                key: edge_key(kind, &row.source_id, source, &row.target_id, target),
                kind,
                source_id: row.source_id,
                target_id: row.target_id,
                source,
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
            self.rebalance_target_port_offsets(connection, mode, target)?;
        }
        Ok(())
    }

    pub fn lane_has_external_outgoing_edge(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<bool> {
        let edges = self.materialized_edge_route_rows_in_connection(connection, mode)?;
        let nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        Ok(edges.iter().any(|edge| {
            node_point_on_lane(
                &nodes,
                lane_key,
                &edge.source_id,
                Point {
                    x: edge.source_x,
                    y: edge.source_y,
                },
            ) && !node_point_on_lane(
                &nodes,
                lane_key,
                &edge.target_id,
                Point {
                    x: edge.target_x,
                    y: edge.target_y,
                },
            )
        }))
    }

    pub fn lane_has_external_edge(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<bool> {
        let edges = self.materialized_edge_route_rows_in_connection(connection, mode)?;
        let nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        Ok(edges.iter().any(|edge| {
            let source_on_lane = node_point_on_lane(
                &nodes,
                lane_key,
                &edge.source_id,
                Point {
                    x: edge.source_x,
                    y: edge.source_y,
                },
            );
            let target_on_lane = node_point_on_lane(
                &nodes,
                lane_key,
                &edge.target_id,
                Point {
                    x: edge.target_x,
                    y: edge.target_y,
                },
            );
            source_on_lane != target_on_lane
        }))
    }

    pub fn derived_lane_nodes_are_covered_by_branch_lanes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<bool> {
        let nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        Ok(nodes
            .iter()
            .filter(|node| node.lane_key == lane_key)
            .all(|node| {
                nodes.iter().any(|cover| {
                    cover.node_id == node.node_id
                        && cover.lane_key != node.lane_key
                        && !is_derived_orphan_or_skill_lane(&cover.lane_key)
                })
            }))
    }

    pub fn clear_materialized_mode_facts(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<()> {
        use console_graph_edge_routes::dsl as edge_routes;
        use console_graph_node_locations::dsl as node_locations;

        diesel::delete(
            edge_routes::console_graph_edge_routes
                .filter(edge_routes::mode.eq(mode.as_query_value())),
        )
        .execute(&mut *connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        diesel::delete(
            node_locations::console_graph_node_locations
                .filter(node_locations::mode.eq(mode.as_query_value())),
        )
        .execute(&mut *connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    pub fn lane_suffix_has_retained_downstream_edges(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
        head_x: i32,
    ) -> crate::Result<bool> {
        let branch_lane_key = lane_key(branch);
        let edges = self.materialized_edge_route_rows_in_connection(connection, mode)?;
        let nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        Ok(edges.iter().any(|edge| {
            node_point_on_lane_suffix(
                &nodes,
                &branch_lane_key,
                head_x,
                &edge.source_id,
                Point {
                    x: edge.source_x,
                    y: edge.source_y,
                },
            ) && !node_point_on_lane_suffix(
                &nodes,
                &branch_lane_key,
                head_x,
                &edge.target_id,
                Point {
                    x: edge.target_x,
                    y: edge.target_y,
                },
            ) && !node_point_on_derived_lane(
                &nodes,
                &edge.target_id,
                Point {
                    x: edge.target_x,
                    y: edge.target_y,
                },
            )
        }))
    }

    pub fn lanes_have_retained_downstream_edges(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lanes: &[LaneRow],
    ) -> crate::Result<bool> {
        for lane in lanes {
            if self.lane_suffix_has_retained_downstream_edges(
                connection,
                mode,
                &lane.lane_label,
                i32::MIN,
            )? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn delete_materialized_lane_suffix(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
        head_x: i32,
    ) -> crate::Result<()> {
        let branch_lane_key = lane_key(branch);
        let edges = self.materialized_edge_route_rows_in_connection(connection, mode)?;
        let nodes = self.materialized_node_rows_in_connection(connection, mode)?;
        let edge_keys = edges
            .iter()
            .filter(|edge| {
                node_point_on_lane_suffix(
                    &nodes,
                    &branch_lane_key,
                    head_x,
                    &edge.source_id,
                    Point {
                        x: edge.source_x,
                        y: edge.source_y,
                    },
                ) || node_point_on_lane_suffix(
                    &nodes,
                    &branch_lane_key,
                    head_x,
                    &edge.target_id,
                    Point {
                        x: edge.target_x,
                        y: edge.target_y,
                    },
                )
            })
            .map(|edge| edge.edge_key.clone())
            .collect::<Vec<_>>();
        use console_graph_edge_routes::dsl as edge_routes;
        use console_graph_node_locations::dsl as node_locations;

        for edge_key in edge_keys {
            diesel::delete(
                edge_routes::console_graph_edge_routes.filter(
                    edge_routes::mode
                        .eq(mode.as_query_value())
                        .and(edge_routes::edge_key.eq(edge_key)),
                ),
            )
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        }

        diesel::delete(
            node_locations::console_graph_node_locations.filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::lane_key.eq(&branch_lane_key))
                    .and(node_locations::x.gt(head_x)),
            ),
        )
        .execute(connection)
        .context(QueryGraphSnapshotStoreSnafu {
            path: self.path.as_ref().clone(),
        })?;
        Ok(())
    }

    pub fn shift_lanes_for_insertion(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        insert_y: i32,
    ) -> crate::Result<()> {
        let mut lanes = self
            .materialized_lanes_in_connection(connection, mode)?
            .into_iter()
            .filter(|lane| lane.lane_y >= insert_y)
            .collect::<Vec<_>>();
        lanes.sort_by(|left, right| {
            right
                .lane_y
                .cmp(&left.lane_y)
                .then_with(|| right.lane_key.cmp(&left.lane_key))
        });
        for lane in lanes {
            self.shift_lane_nodes(connection, mode, &lane, -GRAPH_LANE_HEIGHT)?;
            self.shift_lane_edges(connection, mode, lane.lane_y, -GRAPH_LANE_HEIGHT)?;
        }
        Ok(())
    }

    pub fn shift_lanes_after_deletion(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        removed_lanes: &[LaneRow],
    ) -> crate::Result<()> {
        for lane in self.lane_shifts_after_deletion(connection, mode, removed_lanes)? {
            let delta = GRAPH_LANE_HEIGHT * removed_lane_count_before(removed_lanes, lane.lane_y);
            if delta == 0 {
                continue;
            }
            self.shift_lane_nodes(connection, mode, &lane, delta)?;
            self.shift_lane_edges(connection, mode, lane.lane_y, delta)?;
        }
        Ok(())
    }

    pub fn lane_shifts_after_deletion(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        removed_lanes: &[LaneRow],
    ) -> crate::Result<Vec<LaneRow>> {
        let first_removed_y = removed_lanes
            .iter()
            .map(|lane| lane.lane_y)
            .min()
            .unwrap_or(i32::MAX);
        Ok(self
            .materialized_lanes_in_connection(connection, mode)?
            .into_iter()
            .filter(|lane| lane.lane_y > first_removed_y)
            .collect())
    }

    pub fn shift_lane_nodes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane: &LaneRow,
        delta: i32,
    ) -> crate::Result<()> {
        use console_graph_node_locations::dsl as node_locations;

        let rows = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::lane_key.eq(&lane.lane_key)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::x,
                node_locations::y,
                node_locations::lane_y,
                node_locations::min_y,
                node_locations::max_y,
            ))
            .load::<(String, String, i32, i32, i32, i32, i32)>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        for (node_key, node_id, x, y, lane_y, min_y, max_y) in rows {
            let next_y = y - delta;
            diesel::update(
                node_locations::console_graph_node_locations.filter(
                    node_locations::mode
                        .eq(mode.as_query_value())
                        .and(node_locations::node_key.eq(&node_key)),
                ),
            )
            .set((
                node_locations::node_key.eq(format!("node:{node_id}:{x}:{next_y}")),
                node_locations::lane_y.eq(lane_y - delta),
                node_locations::y.eq(next_y),
                node_locations::min_y.eq(min_y - delta),
                node_locations::max_y.eq(max_y - delta),
            ))
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        }
        Ok(())
    }

    pub fn shift_lane_edges(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_y: i32,
        delta: i32,
    ) -> crate::Result<()> {
        use console_graph_edge_routes::dsl as edge_routes;

        let rows = edge_routes::console_graph_edge_routes
            .filter(
                edge_routes::mode.eq(mode.as_query_value()).and(
                    edge_routes::source_y
                        .eq(lane_y)
                        .or(edge_routes::target_y.eq(lane_y)),
                ),
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
            .load::<EdgeRouteRow>(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        for row in rows {
            diesel::delete(
                edge_routes::console_graph_edge_routes.filter(
                    edge_routes::mode
                        .eq(mode.as_query_value())
                        .and(edge_routes::edge_key.eq(&row.edge_key)),
                ),
            )
            .execute(&mut *connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
            let kind = parse_edge_kind(&row.edge_kind)?;
            let source = Point {
                x: row.source_x,
                y: if row.source_y == lane_y {
                    row.source_y - delta
                } else {
                    row.source_y
                },
            };
            let target = Point {
                x: row.target_x,
                y: if row.target_y == lane_y {
                    row.target_y - delta
                } else {
                    row.target_y
                },
            };
            let edge = GraphViewportEdge {
                key: edge_key(kind, &row.source_id, source, &row.target_id, target),
                kind,
                source_id: row.source_id,
                target_id: row.target_id,
                source,
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
        }
        Ok(())
    }
}
