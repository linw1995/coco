use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use diesel::sqlite::SqliteConnection;
use diesel::{
    BoolExpressionMethods, ExpressionMethods, Insertable, JoinOnDsl, NullableExpressionMethods,
    OptionalExtension, QueryDsl, SelectableHelper,
};

use super::schema::{
    web_graph_edge_routes, web_graph_node_placements, web_graph_node_spatial_index,
    web_graph_node_spatial_items, web_graph_route_spatial_index, web_graph_route_spatial_items,
};
use super::*;
use crate::host::web_graph_view::GRAPH_ROW_STEP;

const NODE_BOUNDS_HALF_WIDTH: i32 = 64;
const NODE_BOUNDS_TOP: i32 = 24;
const NODE_BOUNDS_BOTTOM: i32 = 52;
const ROW_COLLISION_RADIUS: i32 = GRAPH_ROW_STEP / 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Viewport {
    pub x: i32,
    pub y: i32,
    pub width: i32,
    pub height: i32,
    pub overscan: i32,
}

impl Viewport {
    pub fn validate(self) -> Result<()> {
        ensure!(
            self.width > 0 && self.height > 0 && self.overscan >= 0,
            InvalidViewportSnafu {
                width: self.width,
                height: self.height,
                overscan: self.overscan,
            }
        );
        Ok(())
    }

    fn bounds(self) -> SpatialBounds {
        SpatialBounds {
            min_x: self.x.saturating_sub(self.overscan),
            max_x: self
                .x
                .saturating_add(self.width)
                .saturating_add(self.overscan),
            min_y: self.y.saturating_sub(self.overscan),
            max_y: self
                .y
                .saturating_add(self.height)
                .saturating_add(self.overscan),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewportCursor {
    revision: Revision,
    layout: LayoutKind,
    viewport: Viewport,
    node_after: Option<i32>,
    route_after: Option<i32>,
    nodes_done: bool,
    routes_done: bool,
}

impl ViewportCursor {
    pub fn revision(&self) -> Revision {
        self.revision
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewportPage {
    pub canvas: Canvas,
    pub viewport: Viewport,
    pub nodes: Vec<NodePlacement>,
    pub edges: Vec<RoutedEdge>,
    pub next_cursor: Option<ViewportCursor>,
}

#[derive(Debug, Clone, Copy)]
struct SpatialBounds {
    min_x: i32,
    max_x: i32,
    min_y: i32,
    max_y: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = web_graph_node_spatial_items)]
pub struct NodeSpatialItemRow {
    spatial_id: i32,
    layout_kind: String,
    node_id: String,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = web_graph_route_spatial_items)]
pub struct RouteSpatialItemRow {
    spatial_id: i32,
    layout_kind: String,
    edge_kind: String,
    source_id: String,
    target_id: String,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = web_graph_node_spatial_index)]
pub struct NodeSpatialIndexRow {
    spatial_id: Option<i32>,
    min_x: Option<i32>,
    max_x: Option<i32>,
    min_y: Option<i32>,
    max_y: Option<i32>,
    layout_kind: Option<Vec<u8>>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = web_graph_route_spatial_index)]
pub struct RouteSpatialIndexRow {
    spatial_id: Option<i32>,
    min_x: Option<i32>,
    max_x: Option<i32>,
    min_y: Option<i32>,
    max_y: Option<i32>,
    layout_kind: Option<Vec<u8>>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = web_graph_node_spatial_items)]
struct NewNodeSpatialItemRow<'a> {
    layout_kind: &'a str,
    node_id: &'a str,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = web_graph_route_spatial_items)]
struct NewRouteSpatialItemRow<'a> {
    layout_kind: &'a str,
    edge_kind: &'a str,
    source_id: &'a str,
    target_id: &'a str,
}

pub struct SnapshotSpatialRows {
    pub node_items: Vec<NodeSpatialItemRow>,
    pub node_index: Vec<NodeSpatialIndexRow>,
    pub route_items: Vec<RouteSpatialItemRow>,
    pub route_index: Vec<RouteSpatialIndexRow>,
}

pub fn snapshot_rows(
    placements: &[PlacementRow],
    routes: &[RouteRow],
) -> std::result::Result<SnapshotSpatialRows, TransactionError> {
    let mut node_items = Vec::with_capacity(placements.len());
    let mut node_index = Vec::with_capacity(placements.len());
    for (index, placement) in placements.iter().enumerate() {
        let spatial_id = snapshot_spatial_id(index)?;
        node_items.push(NodeSpatialItemRow {
            spatial_id,
            layout_kind: placement.layout_kind.clone(),
            node_id: placement.node_id.clone(),
        });
        node_index.push(node_index_row(
            spatial_id,
            &placement.layout_kind,
            Point {
                x: placement.x,
                y: placement.y,
            },
        ));
    }
    let mut route_items = Vec::with_capacity(routes.len());
    let mut route_index = Vec::with_capacity(routes.len());
    for (index, route) in routes.iter().enumerate() {
        let spatial_id = snapshot_spatial_id(index)?;
        route_items.push(RouteSpatialItemRow {
            spatial_id,
            layout_kind: route.layout_kind.clone(),
            edge_kind: route.edge_kind.clone(),
            source_id: route.source_id.clone(),
            target_id: route.target_id.clone(),
        });
        route_index.push(route_index_row(
            spatial_id,
            &route.layout_kind,
            route_from_row(route),
        ));
    }
    Ok(SnapshotSpatialRows {
        node_items,
        node_index,
        route_items,
        route_index,
    })
}

pub async fn load_viewport_page(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    viewport: Viewport,
    cursor: Option<ViewportCursor>,
    limit_per_kind: NonZeroUsize,
    revision: Revision,
) -> std::result::Result<ViewportPage, TransactionError> {
    validate_viewport_cursor(cursor.as_ref(), revision, kind, viewport)?;
    let canvas = load_canvas(connection, kind).await?;
    let (node_after, route_after, nodes_done, routes_done) = cursor
        .map(|cursor| {
            (
                cursor.node_after,
                cursor.route_after,
                cursor.nodes_done,
                cursor.routes_done,
            )
        })
        .unwrap_or((None, None, false, false));
    let bounds = viewport.bounds();
    let (nodes, node_after, nodes_done) = if nodes_done {
        (Vec::new(), node_after, true)
    } else {
        load_viewport_nodes(connection, kind, bounds, node_after, limit_per_kind).await?
    };
    let (edges, route_after, routes_done) = if routes_done {
        (Vec::new(), route_after, true)
    } else {
        load_viewport_routes(connection, kind, bounds, route_after, limit_per_kind).await?
    };
    let next_cursor = (!nodes_done || !routes_done).then_some(ViewportCursor {
        revision,
        layout: kind,
        viewport,
        node_after,
        route_after,
        nodes_done,
        routes_done,
    });
    Ok(ViewportPage {
        canvas,
        viewport,
        nodes,
        edges,
        next_cursor,
    })
}

pub async fn load_routes_intersecting_nodes(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    nodes: &[NodePlacement],
) -> std::result::Result<Vec<RoutedEdge>, TransactionError> {
    use diesel_async::RunQueryDsl;

    let mut routes = BTreeMap::new();
    for node in nodes {
        let bounds = node_row_collision_bounds(node.point);
        let rows = web_graph_route_spatial_index::table
            .inner_join(
                web_graph_route_spatial_items::table.on(web_graph_route_spatial_index::spatial_id
                    .eq(web_graph_route_spatial_items::spatial_id.nullable())),
            )
            .inner_join(
                web_graph_edge_routes::table.on(web_graph_route_spatial_items::layout_kind
                    .eq(web_graph_edge_routes::layout_kind)
                    .and(
                        web_graph_route_spatial_items::edge_kind
                            .eq(web_graph_edge_routes::edge_kind),
                    )
                    .and(
                        web_graph_route_spatial_items::source_id
                            .eq(web_graph_edge_routes::source_id),
                    )
                    .and(
                        web_graph_route_spatial_items::target_id
                            .eq(web_graph_edge_routes::target_id),
                    )),
            )
            .filter(
                web_graph_route_spatial_index::layout_kind
                    .eq(Some(layout_kind_value(kind).as_bytes().to_vec())),
            )
            .filter(web_graph_route_spatial_index::min_x.le(Some(bounds.max_x)))
            .filter(web_graph_route_spatial_index::max_x.ge(Some(bounds.min_x)))
            .filter(web_graph_route_spatial_index::min_y.le(Some(bounds.max_y)))
            .filter(web_graph_route_spatial_index::max_y.ge(Some(bounds.min_y)))
            .select(RouteRow::as_select())
            .load::<RouteRow>(connection)
            .await?;
        for route in rows.into_iter().map(stored_route) {
            let route = route?;
            routes.insert(route.edge.clone(), route);
        }
    }
    Ok(routes.into_values().collect())
}

pub async fn load_nodes_intersecting_routes(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    routes: &[RoutedEdge],
) -> std::result::Result<Vec<NodePlacement>, TransactionError> {
    use diesel_async::RunQueryDsl;

    let mut nodes = BTreeMap::new();
    for route in routes {
        let bounds = route_row_collision_bounds(route.route);
        let rows = web_graph_node_spatial_index::table
            .inner_join(
                web_graph_node_spatial_items::table.on(web_graph_node_spatial_index::spatial_id
                    .eq(web_graph_node_spatial_items::spatial_id.nullable())),
            )
            .inner_join(
                web_graph_node_placements::table.on(web_graph_node_spatial_items::layout_kind
                    .eq(web_graph_node_placements::layout_kind)
                    .and(
                        web_graph_node_spatial_items::node_id
                            .eq(web_graph_node_placements::node_id),
                    )),
            )
            .filter(
                web_graph_node_spatial_index::layout_kind
                    .eq(Some(layout_kind_value(kind).as_bytes().to_vec())),
            )
            .filter(web_graph_node_spatial_index::min_x.le(Some(bounds.max_x)))
            .filter(web_graph_node_spatial_index::max_x.ge(Some(bounds.min_x)))
            .filter(web_graph_node_spatial_index::min_y.le(Some(bounds.max_y)))
            .filter(web_graph_node_spatial_index::max_y.ge(Some(bounds.min_y)))
            .select(PlacementRow::as_select())
            .load::<PlacementRow>(connection)
            .await?;
        for node in rows.into_iter().map(stored_placement) {
            let node = node?;
            nodes.insert(node.node.clone(), node);
        }
    }
    Ok(nodes.into_values().collect())
}

async fn load_viewport_nodes(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    bounds: SpatialBounds,
    after: Option<i32>,
    limit: NonZeroUsize,
) -> std::result::Result<(Vec<NodePlacement>, Option<i32>, bool), TransactionError> {
    use diesel_async::RunQueryDsl;

    let mut query = web_graph_node_spatial_index::table
        .filter(
            web_graph_node_spatial_index::layout_kind
                .eq(Some(layout_kind_value(kind).as_bytes().to_vec())),
        )
        .filter(web_graph_node_spatial_index::min_x.le(Some(bounds.max_x)))
        .filter(web_graph_node_spatial_index::max_x.ge(Some(bounds.min_x)))
        .filter(web_graph_node_spatial_index::min_y.le(Some(bounds.max_y)))
        .filter(web_graph_node_spatial_index::max_y.ge(Some(bounds.min_y)))
        .into_boxed();
    if let Some(after) = after {
        query = query.filter(web_graph_node_spatial_index::spatial_id.gt(Some(after)));
    }
    let ids = query
        .order(web_graph_node_spatial_index::spatial_id.asc())
        .limit(page_query_limit(limit))
        .select(web_graph_node_spatial_index::spatial_id)
        .load::<Option<i32>>(connection)
        .await?;
    let (ids, next_after, done) = finish_spatial_ids(ids, after, limit)?;
    if ids.is_empty() {
        return Ok((Vec::new(), next_after, done));
    }
    let rows = web_graph_node_spatial_items::table
        .inner_join(
            web_graph_node_placements::table.on(web_graph_node_spatial_items::layout_kind
                .eq(web_graph_node_placements::layout_kind)
                .and(web_graph_node_spatial_items::node_id.eq(web_graph_node_placements::node_id))),
        )
        .filter(web_graph_node_spatial_items::spatial_id.eq_any(&ids))
        .order(web_graph_node_spatial_items::spatial_id.asc())
        .select(PlacementRow::as_select())
        .load::<PlacementRow>(connection)
        .await?
        .into_iter()
        .map(stored_placement)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok((rows, next_after, done))
}

async fn load_viewport_routes(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    bounds: SpatialBounds,
    after: Option<i32>,
    limit: NonZeroUsize,
) -> std::result::Result<(Vec<RoutedEdge>, Option<i32>, bool), TransactionError> {
    use diesel_async::RunQueryDsl;

    // A bounding-box hit alone selects every route leaving a visible high-fanout node. Requiring
    // both endpoints to enter the overscanned viewport keeps browser work proportional to the
    // visible subgraph while still preloading edges before either endpoint reaches the screen.
    let mut query = web_graph_route_spatial_index::table
        .inner_join(
            web_graph_route_spatial_items::table.on(web_graph_route_spatial_index::spatial_id
                .eq(web_graph_route_spatial_items::spatial_id.nullable())),
        )
        .inner_join(
            web_graph_edge_routes::table.on(web_graph_route_spatial_items::layout_kind
                .eq(web_graph_edge_routes::layout_kind)
                .and(web_graph_route_spatial_items::edge_kind.eq(web_graph_edge_routes::edge_kind))
                .and(web_graph_route_spatial_items::source_id.eq(web_graph_edge_routes::source_id))
                .and(
                    web_graph_route_spatial_items::target_id.eq(web_graph_edge_routes::target_id),
                )),
        )
        .filter(
            web_graph_route_spatial_index::layout_kind
                .eq(Some(layout_kind_value(kind).as_bytes().to_vec())),
        )
        .filter(web_graph_route_spatial_index::min_x.le(Some(bounds.max_x)))
        .filter(web_graph_route_spatial_index::max_x.ge(Some(bounds.min_x)))
        .filter(web_graph_route_spatial_index::min_y.le(Some(bounds.max_y)))
        .filter(web_graph_route_spatial_index::max_y.ge(Some(bounds.min_y)))
        .filter(web_graph_edge_routes::source_x.ge(bounds.min_x))
        .filter(web_graph_edge_routes::source_x.le(bounds.max_x))
        .filter(web_graph_edge_routes::source_y.ge(bounds.min_y))
        .filter(web_graph_edge_routes::source_y.le(bounds.max_y))
        .filter(web_graph_edge_routes::target_x.ge(bounds.min_x))
        .filter(web_graph_edge_routes::target_x.le(bounds.max_x))
        .filter(web_graph_edge_routes::target_y.ge(bounds.min_y))
        .filter(web_graph_edge_routes::target_y.le(bounds.max_y))
        .into_boxed();
    if let Some(after) = after {
        query = query.filter(web_graph_route_spatial_index::spatial_id.gt(Some(after)));
    }
    let mut rows = query
        .order(web_graph_route_spatial_index::spatial_id.asc())
        .limit(page_query_limit(limit))
        .select((
            web_graph_route_spatial_index::spatial_id,
            RouteRow::as_select(),
        ))
        .load::<(Option<i32>, RouteRow)>(connection)
        .await?
        .into_iter()
        .map(|(spatial_id, row)| {
            spatial_id
                .ok_or_else(|| invalid_value("spatial_id", "NULL"))
                .map(|spatial_id| (spatial_id, row))
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let has_more = rows.len() > limit.get();
    rows.truncate(limit.get());
    let next_after = rows.last().map(|(spatial_id, _)| *spatial_id).or(after);
    let routes = rows
        .into_iter()
        .map(|(_, row)| stored_route(row))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok((routes, next_after, !has_more))
}

fn finish_spatial_ids(
    mut ids: Vec<Option<i32>>,
    previous_after: Option<i32>,
    limit: NonZeroUsize,
) -> std::result::Result<(Vec<i32>, Option<i32>, bool), TransactionError> {
    let has_more = ids.len() > limit.get();
    ids.truncate(limit.get());
    let ids = ids
        .into_iter()
        .map(|id| id.ok_or_else(|| invalid_value("spatial_id", "NULL")))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let next_after = ids.last().copied().or(previous_after);
    Ok((ids, next_after, !has_more))
}

fn page_query_limit(limit: NonZeroUsize) -> i64 {
    i64::try_from(limit.get() + 1).expect("bounded read limit fits in SQLite INTEGER")
}

fn validate_viewport_cursor(
    cursor: Option<&ViewportCursor>,
    current: Revision,
    kind: LayoutKind,
    viewport: Viewport,
) -> std::result::Result<(), TransactionError> {
    if let Some(cursor) = cursor {
        if cursor.revision != current {
            return Err(TransactionError::Operation(Error::StaleCursor {
                cursor: cursor.revision,
                current,
            }));
        }
        if cursor.layout != kind || cursor.viewport != viewport {
            return Err(TransactionError::Operation(Error::CursorQueryMismatch));
        }
    }
    Ok(())
}

pub async fn clear(
    connection: &mut AsyncSqliteConnection,
) -> std::result::Result<(), TransactionError> {
    use diesel_async::RunQueryDsl;

    diesel::delete(web_graph_route_spatial_index::table)
        .execute(connection)
        .await?;
    diesel::delete(web_graph_node_spatial_index::table)
        .execute(connection)
        .await?;
    diesel::delete(web_graph_route_spatial_items::table)
        .execute(connection)
        .await?;
    diesel::delete(web_graph_node_spatial_items::table)
        .execute(connection)
        .await?;
    Ok(())
}

pub fn insert_snapshot(
    connection: &mut SqliteConnection,
    rows: &SnapshotSpatialRows,
) -> diesel::QueryResult<()> {
    for chunk in rows.node_items.chunks(WRITE_BATCH_SIZE) {
        diesel::RunQueryDsl::execute(
            diesel::insert_into(web_graph_node_spatial_items::table).values(chunk),
            connection,
        )?;
    }
    for row in &rows.node_index {
        diesel::RunQueryDsl::execute(
            diesel::insert_into(web_graph_node_spatial_index::table).values(row),
            connection,
        )?;
    }
    for chunk in rows.route_items.chunks(WRITE_BATCH_SIZE) {
        diesel::RunQueryDsl::execute(
            diesel::insert_into(web_graph_route_spatial_items::table).values(chunk),
            connection,
        )?;
    }
    for row in &rows.route_index {
        diesel::RunQueryDsl::execute(
            diesel::insert_into(web_graph_route_spatial_index::table).values(row),
            connection,
        )?;
    }
    Ok(())
}

pub async fn remove_node(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    node: &NodeId,
) -> std::result::Result<(), TransactionError> {
    use diesel_async::RunQueryDsl;

    let spatial_id = node_spatial_id(connection, kind, node).await?;
    let changed = diesel::delete(
        web_graph_node_spatial_index::table
            .filter(web_graph_node_spatial_index::spatial_id.eq(Some(spatial_id))),
    )
    .execute(connection)
    .await?;
    expect_row_count("remove_node_spatial_index", 1, changed)?;
    let changed = diesel::delete(
        web_graph_node_spatial_items::table
            .filter(web_graph_node_spatial_items::spatial_id.eq(spatial_id)),
    )
    .execute(connection)
    .await?;
    expect_row_count("remove_node_spatial_item", 1, changed)
}

pub async fn remove_route(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    edge: &EdgeId,
) -> std::result::Result<(), TransactionError> {
    use diesel_async::RunQueryDsl;

    let spatial_id = route_spatial_id(connection, kind, edge).await?;
    let changed = diesel::delete(
        web_graph_route_spatial_index::table
            .filter(web_graph_route_spatial_index::spatial_id.eq(Some(spatial_id))),
    )
    .execute(connection)
    .await?;
    expect_row_count("remove_route_spatial_index", 1, changed)?;
    let changed = diesel::delete(
        web_graph_route_spatial_items::table
            .filter(web_graph_route_spatial_items::spatial_id.eq(spatial_id)),
    )
    .execute(connection)
    .await?;
    expect_row_count("remove_route_spatial_item", 1, changed)
}

pub async fn upsert_node(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    placement: &NodePlacement,
) -> std::result::Result<(), TransactionError> {
    use diesel_async::RunQueryDsl;

    let spatial_id = match optional_node_spatial_id(connection, kind, &placement.node).await? {
        Some(spatial_id) => spatial_id,
        None => {
            diesel::insert_into(web_graph_node_spatial_items::table)
                .values(&NewNodeSpatialItemRow {
                    layout_kind: layout_kind_value(kind),
                    node_id: placement.node.as_str(),
                })
                .execute(connection)
                .await?;
            node_spatial_id(connection, kind, &placement.node).await?
        }
    };
    diesel::delete(
        web_graph_node_spatial_index::table
            .filter(web_graph_node_spatial_index::spatial_id.eq(Some(spatial_id))),
    )
    .execute(connection)
    .await?;
    diesel::insert_into(web_graph_node_spatial_index::table)
        .values(&node_index_row(
            spatial_id,
            layout_kind_value(kind),
            placement.point,
        ))
        .execute(connection)
        .await?;
    Ok(())
}

pub async fn upsert_route(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    routed: &RoutedEdge,
) -> std::result::Result<(), TransactionError> {
    use diesel_async::RunQueryDsl;

    let spatial_id = match optional_route_spatial_id(connection, kind, &routed.edge).await? {
        Some(spatial_id) => spatial_id,
        None => {
            diesel::insert_into(web_graph_route_spatial_items::table)
                .values(&NewRouteSpatialItemRow {
                    layout_kind: layout_kind_value(kind),
                    edge_kind: edge_kind_value(routed.edge.kind),
                    source_id: routed.edge.source.as_str(),
                    target_id: routed.edge.target.as_str(),
                })
                .execute(connection)
                .await?;
            route_spatial_id(connection, kind, &routed.edge).await?
        }
    };
    diesel::delete(
        web_graph_route_spatial_index::table
            .filter(web_graph_route_spatial_index::spatial_id.eq(Some(spatial_id))),
    )
    .execute(connection)
    .await?;
    diesel::insert_into(web_graph_route_spatial_index::table)
        .values(&route_index_row(
            spatial_id,
            layout_kind_value(kind),
            routed.route,
        ))
        .execute(connection)
        .await?;
    Ok(())
}

async fn node_spatial_id(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    node: &NodeId,
) -> std::result::Result<i32, TransactionError> {
    optional_node_spatial_id(connection, kind, node)
        .await?
        .ok_or_else(|| {
            invalid_value(
                "web_graph_node_spatial_items",
                format!("{}:{node}", layout_kind_value(kind)),
            )
        })
}

async fn optional_node_spatial_id(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    node: &NodeId,
) -> std::result::Result<Option<i32>, TransactionError> {
    use diesel_async::RunQueryDsl;

    Ok(web_graph_node_spatial_items::table
        .filter(web_graph_node_spatial_items::layout_kind.eq(layout_kind_value(kind)))
        .filter(web_graph_node_spatial_items::node_id.eq(node.as_str()))
        .select(web_graph_node_spatial_items::spatial_id)
        .first::<i32>(connection)
        .await
        .optional()?)
}

async fn route_spatial_id(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    edge: &EdgeId,
) -> std::result::Result<i32, TransactionError> {
    optional_route_spatial_id(connection, kind, edge)
        .await?
        .ok_or_else(|| {
            invalid_value(
                "web_graph_route_spatial_items",
                format!("{}:{edge}", layout_kind_value(kind)),
            )
        })
}

async fn optional_route_spatial_id(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    edge: &EdgeId,
) -> std::result::Result<Option<i32>, TransactionError> {
    use diesel_async::RunQueryDsl;

    Ok(web_graph_route_spatial_items::table
        .filter(web_graph_route_spatial_items::layout_kind.eq(layout_kind_value(kind)))
        .filter(web_graph_route_spatial_items::edge_kind.eq(edge_kind_value(edge.kind)))
        .filter(web_graph_route_spatial_items::source_id.eq(edge.source.as_str()))
        .filter(web_graph_route_spatial_items::target_id.eq(edge.target.as_str()))
        .select(web_graph_route_spatial_items::spatial_id)
        .first::<i32>(connection)
        .await
        .optional()?)
}

fn node_index_row(spatial_id: i32, layout_kind: &str, point: Point) -> NodeSpatialIndexRow {
    let bounds = node_bounds(point);
    NodeSpatialIndexRow {
        spatial_id: Some(spatial_id),
        min_x: Some(bounds.min_x),
        max_x: Some(bounds.max_x),
        min_y: Some(bounds.min_y),
        max_y: Some(bounds.max_y),
        layout_kind: Some(layout_kind.as_bytes().to_vec()),
    }
}

fn route_index_row(spatial_id: i32, layout_kind: &str, route: BezierRoute) -> RouteSpatialIndexRow {
    let bounds = route_bounds(route);
    RouteSpatialIndexRow {
        spatial_id: Some(spatial_id),
        min_x: Some(bounds.min_x),
        max_x: Some(bounds.max_x),
        min_y: Some(bounds.min_y),
        max_y: Some(bounds.max_y),
        layout_kind: Some(layout_kind.as_bytes().to_vec()),
    }
}

fn node_bounds(point: Point) -> SpatialBounds {
    SpatialBounds {
        min_x: point.x.saturating_sub(NODE_BOUNDS_HALF_WIDTH),
        max_x: point.x.saturating_add(NODE_BOUNDS_HALF_WIDTH),
        min_y: point.y.saturating_sub(NODE_BOUNDS_TOP),
        max_y: point.y.saturating_add(NODE_BOUNDS_BOTTOM),
    }
}

fn node_row_collision_bounds(point: Point) -> SpatialBounds {
    SpatialBounds {
        min_y: point.y.saturating_sub(ROW_COLLISION_RADIUS),
        max_y: point.y.saturating_add(ROW_COLLISION_RADIUS),
        ..node_bounds(point)
    }
}

fn route_bounds(route: BezierRoute) -> SpatialBounds {
    let points = [route.source, route.control_1, route.control_2, route.target];
    SpatialBounds {
        min_x: points.iter().map(|point| point.x).min().unwrap_or_default(),
        max_x: points.iter().map(|point| point.x).max().unwrap_or_default(),
        min_y: points.iter().map(|point| point.y).min().unwrap_or_default(),
        max_y: points.iter().map(|point| point.y).max().unwrap_or_default(),
    }
}

fn route_row_collision_bounds(route: BezierRoute) -> SpatialBounds {
    let bounds = route_bounds(route);
    SpatialBounds {
        min_y: bounds.min_y.saturating_sub(ROW_COLLISION_RADIUS),
        max_y: bounds.max_y.saturating_add(ROW_COLLISION_RADIUS),
        ..bounds
    }
}

fn route_from_row(row: &RouteRow) -> BezierRoute {
    BezierRoute {
        source: Point {
            x: row.source_x,
            y: row.source_y,
        },
        control_1: Point {
            x: row.control_1_x,
            y: row.control_1_y,
        },
        control_2: Point {
            x: row.control_2_x,
            y: row.control_2_y,
        },
        target: Point {
            x: row.target_x,
            y: row.target_y,
        },
    }
}

fn snapshot_spatial_id(index: usize) -> std::result::Result<i32, TransactionError> {
    let value = index
        .checked_add(1)
        .expect("a snapshot cannot contain usize::MAX spatial rows");
    i32::try_from(value).map_err(|_| {
        TransactionError::Operation(Error::IntegerOutOfRange {
            column: "spatial_id",
            value: u64::try_from(value).unwrap_or(u64::MAX),
        })
    })
}
