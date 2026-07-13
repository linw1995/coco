use super::*;

impl ConsoleGraphSnapshotStore {
    pub async fn latest_viewport(
        &self,
        mode: GraphMode,
        request: GraphViewportRequest,
    ) -> crate::Result<Option<GraphViewportResponse>> {
        let this = self.clone();
        self.with_connection(move |connection| {
            this.run_read_transaction(connection, |this, connection| {
                let Some(meta) = this.latest_materialization_row_in_connection(connection, mode)?
                else {
                    return Ok(None);
                };
                this.viewport_from_row(connection, mode, meta, request)
            })
        })
        .await
    }

    pub async fn latest_viewport_diff(
        &self,
        mode: GraphMode,
        request: GraphViewportDiffRequest,
    ) -> crate::Result<Option<GraphViewportDiffResponse>> {
        let this = self.clone();
        self.with_connection(move |connection| {
            this.run_read_transaction(connection, |this, connection| {
                let Some(meta) = this.latest_materialization_row_in_connection(connection, mode)?
                else {
                    return Ok(None);
                };
                this.viewport_diff_from_row(connection, mode, meta, request)
            })
        })
        .await
    }

    pub(crate) async fn materialized_node_reference(
        &self,
        mode: GraphMode,
        target: &str,
    ) -> crate::Result<Option<MaterializedNodeReference>> {
        let this = self.clone();
        let target = target.to_owned();
        self.with_connection(move |connection| {
            this.run_read_transaction(connection, |this, connection| {
                this.materialized_node_reference_in_connection(connection, mode, &target)
            })
        })
        .await
    }

    pub(crate) async fn materialized_node_points(
        &self,
        mode: GraphMode,
        node_ids: &BTreeSet<String>,
    ) -> crate::Result<BTreeMap<String, Point>> {
        let this = self.clone();
        let node_ids = node_ids.clone();
        self.with_connection(move |connection| {
            this.run_read_transaction(connection, |this, connection| {
                this.materialized_node_points_in_connection(connection, mode, &node_ids)
            })
        })
        .await
    }

    pub(crate) async fn has_materialization(&self, mode: GraphMode) -> crate::Result<bool> {
        Ok(self.latest_materialization_row(mode).await?.is_some())
    }

    pub(crate) async fn has_non_empty_materialization(
        &self,
        mode: GraphMode,
    ) -> crate::Result<bool> {
        let this = self.clone();
        self.with_connection(move |connection| {
            Ok(this
                .latest_materialization_row_in_connection(connection, mode)?
                .is_some()
                && !this
                    .materialized_node_rows_in_connection(connection, mode)?
                    .is_empty())
        })
        .await
    }

    pub(crate) async fn latest_materialization_version(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<u64>> {
        Ok(self
            .latest_materialization_row(mode)
            .await?
            .map(|meta| meta.source_version.max(0) as u64))
    }

    pub(crate) async fn materialized_shell_facts(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializedGraphShellFacts>> {
        let this = self.clone();
        self.with_connection(move |connection| {
            this.run_read_transaction(connection, |this, connection| {
                let Some(meta) = this.latest_materialization_row_in_connection(connection, mode)?
                else {
                    return Ok(None);
                };
                let lanes = this
                    .materialized_lanes_in_connection(connection, mode)?
                    .into_iter()
                    .map(|row| GraphViewportLane {
                        key: row.lane_key,
                        label: row.lane_label,
                        y: row.lane_y,
                    })
                    .collect();
                let mut nodes_by_id = BTreeMap::new();
                for row in this.materialized_node_rows_in_connection(connection, mode)? {
                    nodes_by_id
                        .entry(row.node_id)
                        .or_insert(Point { x: row.x, y: row.y });
                }
                let nodes = nodes_by_id
                    .into_iter()
                    .map(|(node_id, point)| MaterializedGraphShellNode { node_id, point })
                    .collect();
                Ok(Some(MaterializedGraphShellFacts {
                    version: meta.source_version.max(0) as u64,
                    lanes,
                    nodes,
                    edge_count: this.materialized_edge_count_in_connection(connection, mode)?,
                }))
            })
        })
        .await
    }

    pub async fn latest_materialization_row(
        &self,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializationRow>> {
        let this = self.clone();
        self.with_connection(move |connection| {
            this.latest_materialization_row_in_connection(connection, mode)
        })
        .await
    }

    pub fn latest_materialization_row_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<Option<MaterializationRow>> {
        use console_graph_materializations::dsl as materializations;

        materializations::console_graph_materializations
            .filter(
                materializations::mode
                    .eq(mode.as_query_value())
                    .and(materializations::coordinate_space.eq(COORDINATE_SPACE)),
            )
            .select((
                materializations::source_version,
                materializations::world_min_x,
                materializations::world_min_y,
                materializations::world_max_x,
                materializations::world_max_y,
            ))
            .get_result::<MaterializationRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn latest_lane_tail_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        self.latest_lane_tail_by_key_in_connection(connection, mode, &lane_key(branch))
    }

    pub fn materialized_lane_node_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
        node_id: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::lane_key.eq(lane_key(branch)))
                    .and(node_locations::node_id.eq(node_id)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((node_locations::x.desc(), node_locations::node_key.desc()))
            .limit(1)
            .get_result::<MaterializedTailNodeRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn materialized_lanes_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<Vec<LaneRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(node_locations::mode.eq(mode.as_query_value()))
            .select((
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
            ))
            .distinct()
            .order((node_locations::lane_y, node_locations::lane_key))
            .load::<LaneRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn next_materialized_lane_y(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<i32> {
        Ok(self
            .materialized_lanes_in_connection(connection, mode)?
            .iter()
            .map(|lane| lane.lane_y)
            .max()
            .unwrap_or(crate::layout::GRAPH_TOP_Y - GRAPH_LANE_HEIGHT)
            + GRAPH_LANE_HEIGHT)
    }

    pub fn next_materialized_lane_y_after_reserved(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        reserved_lane_y: Option<i32>,
    ) -> crate::Result<i32> {
        let next_y = self.next_materialized_lane_y(connection, mode)?;
        Ok(reserved_lane_y
            .map(|lane_y| next_y.max(lane_y + GRAPH_LANE_HEIGHT))
            .unwrap_or(next_y))
    }

    pub fn first_materialized_ancestry_point(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        ancestry: &[Node],
        before_lane_y: i32,
    ) -> crate::Result<Option<(usize, Point)>> {
        for (index, node) in ancestry.iter().enumerate() {
            let Some(row) = self
                .materialized_non_skill_node_row_by_id_in_connection(connection, mode, &node.id)?
            else {
                continue;
            };
            if row.y >= before_lane_y || is_orphan_lane_key(&row.lane_key) {
                continue;
            }
            return Ok(Some((index, Point { x: row.x, y: row.y })));
        }
        Ok(None)
    }

    pub fn first_materialized_lane_ancestry_node(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        branch: &str,
        ancestry: &[Node],
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        for node in ancestry {
            let Some(row) =
                self.materialized_lane_node_in_connection(connection, mode, branch, &node.id)?
            else {
                continue;
            };
            return Ok(Some(row));
        }
        Ok(None)
    }

    pub fn materialized_skill_subtree_attach_row_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        nodes: &[Node],
    ) -> crate::Result<Option<(MaterializedTailNodeRow, bool)>> {
        for node in nodes.iter().rev() {
            let Some(row) = self.materialized_node_row_by_id_with_lane_prefix_in_connection(
                connection,
                mode,
                &node.id,
                DERIVED_SKILL_LANE_KEY_PREFIX,
            )?
            else {
                continue;
            };
            let Some(tail) =
                self.latest_lane_tail_by_key_in_connection(connection, mode, &row.lane_key)?
            else {
                continue;
            };
            let fork_first_inserted = tail.node_key != row.node_key;
            return Ok(Some((row, fork_first_inserted)));
        }
        Ok(None)
    }

    pub fn latest_lane_tail_by_key_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::lane_key.eq(lane_key)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((node_locations::x.desc(), node_locations::node_key.desc()))
            .limit(1)
            .get_result::<MaterializedTailNodeRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn materialized_node_row_by_id_on_lane_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        lane_key: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::lane_key.eq(lane_key)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<MaterializedTailNodeRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn materialized_node_row_by_id_with_lane_prefix_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        lane_key_prefix: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::lane_key.like(format!("{lane_key_prefix}%"))),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<MaterializedTailNodeRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn materialized_non_skill_node_row_by_id_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
    ) -> crate::Result<Option<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::lane_key.not_like("derived:skill:%")),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<MaterializedTailNodeRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn materialized_node_rows_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<Vec<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(node_locations::mode.eq(mode.as_query_value()))
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .load::<MaterializedTailNodeRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn materialized_node_rows_by_lane_key_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        lane_key: &str,
    ) -> crate::Result<Vec<MaterializedTailNodeRow>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::lane_key.eq(lane_key)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
                node_locations::x,
                node_locations::y,
            ))
            .order((node_locations::x, node_locations::node_key))
            .load::<MaterializedTailNodeRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn materialized_branch_node_point_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
    ) -> crate::Result<Option<Point>> {
        use console_graph_node_locations::dsl as node_locations;

        let row = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::lane_key.not_like("derived:orphan:%"))
                    .and(node_locations::lane_key.not_like("derived:skill:%")),
            )
            .select((node_locations::x, node_locations::y))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<MaterializedNodePointRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        Ok(row.map(|row| Point { x: row.x, y: row.y }))
    }

    pub fn materialized_branch_node_point_before_lane_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        before_lane_y: i32,
    ) -> crate::Result<Option<Point>> {
        use console_graph_node_locations::dsl as node_locations;

        let row = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id))
                    .and(node_locations::y.lt(before_lane_y))
                    .and(node_locations::lane_key.not_like("derived:orphan:%"))
                    .and(node_locations::lane_key.not_like("derived:skill:%")),
            )
            .select((node_locations::x, node_locations::y))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<MaterializedNodePointRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        Ok(row.map(|row| Point { x: row.x, y: row.y }))
    }

    pub fn primary_incoming_edge_to_node_occurrence(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
        x: i32,
        y: i32,
    ) -> crate::Result<Option<EdgeRouteRow>> {
        use console_graph_edge_routes::dsl as edge_routes;

        edge_routes::console_graph_edge_routes
            .filter(
                edge_routes::mode
                    .eq(mode.as_query_value())
                    .and(edge_routes::edge_kind.eq("primary_parent"))
                    .and(edge_routes::target_id.eq(node_id))
                    .and(edge_routes::target_x.eq(x))
                    .and(edge_routes::target_y.eq(y)),
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
            .limit(1)
            .get_result::<EdgeRouteRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn materialized_node_point_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_id: &str,
    ) -> crate::Result<Option<Point>> {
        use console_graph_node_locations::dsl as node_locations;

        node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_id.eq(node_id)),
            )
            .select((node_locations::x, node_locations::y))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<MaterializedNodePointRow>(connection)
            .optional()
            .map(|row| row.map(|row| Point { x: row.x, y: row.y }))
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn materialized_node_reference_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        target: &str,
    ) -> crate::Result<Option<MaterializedNodeReference>> {
        if self
            .latest_materialization_row_in_connection(connection, mode)?
            .is_none()
        {
            return Ok(None);
        }
        use console_graph_node_locations::dsl as node_locations;

        let row = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::node_target.eq(target)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::node_target,
                node_locations::short_id,
                node_locations::node_kind,
                node_locations::summary,
                node_locations::labels_json,
                node_locations::x,
                node_locations::y,
            ))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .limit(1)
            .get_result::<NodeLocationRow>(connection)
            .optional()
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        row.map(|row| {
            let labels = serde_json::from_str::<Vec<String>>(&row.labels_json).context(
                ParseGraphSnapshotStoreValueSnafu {
                    column: "console_graph_node_locations.labels_json",
                },
            )?;
            Ok(MaterializedNodeReference {
                node_id: row.node_id,
                labels,
            })
        })
        .transpose()
    }

    pub fn materialized_node_points_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        node_ids: &BTreeSet<String>,
    ) -> crate::Result<BTreeMap<String, Point>> {
        if self
            .latest_materialization_row_in_connection(connection, mode)?
            .is_none()
        {
            return Ok(BTreeMap::new());
        }
        let mut points = BTreeMap::new();
        for node_id in node_ids {
            if let Some(point) =
                self.materialized_node_point_in_connection(connection, mode, node_id)?
            {
                points.insert(node_id.clone(), point);
            }
        }
        Ok(points)
    }

    pub fn materialized_edge_count_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<usize> {
        use console_graph_edge_routes::dsl as edge_routes;

        edge_routes::console_graph_edge_routes
            .filter(edge_routes::mode.eq(mode.as_query_value()))
            .select(diesel::dsl::count(edge_routes::edge_key).aggregate_distinct())
            .get_result::<i64>(connection)
            .map(|count| count.max(0) as usize)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })
    }

    pub fn materialized_edge_route_rows_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
    ) -> crate::Result<Vec<EdgeRouteRow>> {
        use console_graph_edge_routes::dsl as edge_routes;

        edge_routes::console_graph_edge_routes
            .filter(edge_routes::mode.eq(mode.as_query_value()))
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

    pub fn event_order_by_materialized_and_new_nodes(
        &self,
        connection: &mut SqliteConnection,
        store: &MaterializationSourceSnapshot,
        mode: GraphMode,
        new_nodes: &[Node],
    ) -> crate::Result<BTreeMap<String, usize>> {
        let mut nodes_by_id = new_nodes
            .iter()
            .map(|node| (node.id.clone(), node.clone()))
            .collect::<BTreeMap<_, _>>();
        for row in self.materialized_node_rows_in_connection(connection, mode)? {
            if nodes_by_id.contains_key(&row.node_id) {
                continue;
            }
            let node = store.node(&row.node_id).context(crate::error::StoreSnafu)?;
            nodes_by_id.insert(row.node_id, node);
        }

        let mut nodes = nodes_by_id.into_values().collect::<Vec<_>>();
        nodes.sort_by(|left, right| {
            left.created_at
                .as_nanosecond()
                .cmp(&right.created_at.as_nanosecond())
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(nodes
            .into_iter()
            .enumerate()
            .map(|(index, node)| (node.id, index))
            .collect())
    }

    pub fn next_routed_edge_slot_in_connection(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        source: Point,
        target: Point,
    ) -> crate::Result<i32> {
        let direction = (target.y - source.y).signum();
        Ok(self
            .materialized_edge_route_rows_in_connection(connection, mode)?
            .into_iter()
            .filter(|edge| {
                edge.edge_kind != "primary_parent"
                    && edge.source_y == source.y
                    && (edge.target_y - edge.source_y).signum() == direction
            })
            .map(|edge| edge.route_slot + 1)
            .max()
            .unwrap_or(0))
    }

    pub fn viewport_from_row(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        meta: MaterializationRow,
        request: GraphViewportRequest,
    ) -> crate::Result<Option<GraphViewportResponse>> {
        let request = request.normalized();
        let bounds = ViewportItemBounds::from_request(request);
        Ok(Some(GraphViewportResponse {
            version: meta.source_version as u64,
            canvas: GraphCanvas {
                width: meta.world_max_x.saturating_sub(meta.world_min_x),
                height: meta.world_max_y.saturating_sub(meta.world_min_y),
            },
            viewport: GraphViewport {
                x: request.x,
                y: request.y,
                width: request.width,
                height: request.height,
                overscan: request.overscan,
            },
            lanes: self.viewport_lanes(connection, mode, bounds)?,
            nodes: self.viewport_nodes(connection, mode, bounds)?,
            edges: self.viewport_edges(connection, mode, bounds)?,
        }))
    }

    pub fn viewport_diff_from_row(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        meta: MaterializationRow,
        request: GraphViewportDiffRequest,
    ) -> crate::Result<Option<GraphViewportDiffResponse>> {
        let previous = self
            .viewport_from_row(connection, mode, meta.clone(), request.previous)?
            .expect("viewport metadata should produce a response");
        let current = self
            .viewport_from_row(connection, mode, meta, request.current)?
            .expect("viewport metadata should produce a response");
        Ok(Some(diff_graph_viewport_responses(
            previous,
            current,
            request.known.as_ref(),
        )))
    }

    pub fn viewport_lanes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        bounds: ViewportItemBounds,
    ) -> crate::Result<Vec<GraphViewportLane>> {
        use console_graph_node_locations::dsl as node_locations;

        if bounds.right < 0 || bounds.left > crate::layout::GRAPH_LEFT_X {
            return Ok(Vec::new());
        }
        let lane_top = bounds.top.saturating_sub(24);
        let lane_bottom = bounds.bottom.saturating_add(24);
        let rows = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::lane_y.le(lane_bottom))
                    .and(node_locations::lane_y.ge(lane_top)),
            )
            .select((
                node_locations::lane_key,
                node_locations::lane_label,
                node_locations::lane_y,
            ))
            .distinct()
            .order((node_locations::lane_y, node_locations::lane_key))
            .load::<LaneRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        Ok(rows
            .into_iter()
            .map(|row| GraphViewportLane {
                key: row.lane_key,
                label: row.lane_label,
                y: row.lane_y,
            })
            .collect())
    }

    pub fn viewport_nodes(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        bounds: ViewportItemBounds,
    ) -> crate::Result<Vec<GraphViewportNode>> {
        use console_graph_node_locations::dsl as node_locations;

        let rows = node_locations::console_graph_node_locations
            .filter(
                node_locations::mode
                    .eq(mode.as_query_value())
                    .and(node_locations::min_x.le(bounds.right))
                    .and(node_locations::max_x.ge(bounds.left))
                    .and(node_locations::min_y.le(bounds.bottom))
                    .and(node_locations::max_y.ge(bounds.top)),
            )
            .select((
                node_locations::node_key,
                node_locations::node_id,
                node_locations::node_target,
                node_locations::short_id,
                node_locations::node_kind,
                node_locations::summary,
                node_locations::labels_json,
                node_locations::x,
                node_locations::y,
            ))
            .order((
                node_locations::y,
                node_locations::x,
                node_locations::node_key,
            ))
            .load::<NodeLocationRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        rows.into_iter()
            .map(|row| {
                let labels = serde_json::from_str(&row.labels_json).context(
                    ParseGraphSnapshotStoreValueSnafu {
                        column: "console_graph_node_locations.labels_json",
                    },
                )?;
                Ok(GraphViewportNode {
                    key: row.node_key,
                    id: row.node_id,
                    node_target: row.node_target,
                    short_id: row.short_id,
                    kind: row.node_kind,
                    summary: row.summary,
                    labels,
                    x: row.x,
                    y: row.y,
                })
            })
            .collect()
    }

    pub fn viewport_edges(
        &self,
        connection: &mut SqliteConnection,
        mode: GraphMode,
        bounds: ViewportItemBounds,
    ) -> crate::Result<Vec<GraphViewportEdge>> {
        use console_graph_edge_routes::dsl as edge_routes;

        let rows = edge_routes::console_graph_edge_routes
            .filter(
                edge_routes::mode
                    .eq(mode.as_query_value())
                    .and(edge_routes::min_x.le(bounds.right))
                    .and(edge_routes::max_x.ge(bounds.left))
                    .and(edge_routes::min_y.le(bounds.bottom))
                    .and(edge_routes::max_y.ge(bounds.top)),
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
            .order((
                edge_routes::min_y,
                edge_routes::min_x,
                edge_routes::edge_key,
            ))
            .load::<EdgeRouteRow>(connection)
            .context(QueryGraphSnapshotStoreSnafu {
                path: self.path.as_ref().clone(),
            })?;
        rows.into_iter()
            .map(|row| {
                Ok(GraphViewportEdge {
                    key: row.edge_key,
                    kind: parse_edge_kind(&row.edge_kind)?,
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
                })
            })
            .collect()
    }
}
