use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::{Duration, Instant};

use coco_mem::{GRAPH_READ_BATCH_SIZE, GraphNodeCursor, GraphNodeRecord, Node, SqliteGraphStore};
use snafu::{IntoError, prelude::*};
use tokio::sync::watch;

use super::error::{
    StoreSnafu, WebGraphModelSnafu, WebGraphNotInitializedSnafu, WebGraphOrderSnafu,
    WebGraphParentPlacementMissingSnafu, WebGraphRevisionExhaustedSnafu,
    WebGraphSourceCursorMismatchSnafu, WebGraphSourceCursorRegressedSnafu,
    WebGraphSourceCursorStalledSnafu, WebGraphSourceNodeMissingSnafu,
    WebGraphSourceVersionExhaustedSnafu, WebGraphStoreSnafu,
};
use super::publisher::ConsolePublisher;
use super::web_graph_order::{
    Error as OrderError, IncomingEdge, Result as OrderResult, nearest_row_for_y,
    reserved_rows_from_placements, stable_column_order,
};
use super::web_graph_store::{Error as StoreError, StoredGraphState, Viewport, WebGraphStore};
use super::web_graph_view::{
    EndpointPortOffsets, EndpointPortSlots, GRAPH_PADDING, GRAPH_RANK_STEP, GRAPH_ROW_STEP,
    ViewMode, diff_graph_viewport_responses, edge_key, edge_port_offset, graph_kind_name, node_key,
    node_target_id, route_edge, route_edge_with_offsets, shorten_id, summarize_node,
};
use crate::api::{
    GraphBezierRoute, GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportEdge,
    GraphViewportEdgeKind, GraphViewportNode, GraphViewportResponse, Point as ApiPoint,
};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportRequest};
use crate::web_graph::{
    BezierRoute, Canvas, EdgeId, EdgeKind, FORMAT_VERSION, Graph, LayoutKind, LayoutPatch,
    LayoutPatches, LayoutSnapshot, LayoutSnapshots, NodeId, NodePlacement, Patch, Point, Revision,
    RoutedEdge, Snapshot, SourceVersion, TopologyPatch, TopologySnapshot,
};

const RETRY_MIN_DELAY: Duration = Duration::from_millis(25);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
const CATCH_UP_PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
// Full node payloads can be large, so release the source connection between small batches.
const SOURCE_NODE_HYDRATION_BATCH_SIZE: usize = 16;

#[derive(Clone)]
pub(crate) struct WebGraphRuntime {
    store: WebGraphStore,
    source: SqliteGraphStore,
    publisher: ConsolePublisher,
    ready: watch::Sender<u64>,
}

#[derive(Debug)]
enum EnsureStep {
    Visit(String),
    Build(Box<GraphNodeRecord>),
}

#[derive(Debug, Clone)]
struct ParentEdge {
    kind: EdgeKind,
    node_id: String,
}

#[derive(Debug, Default)]
struct EnsureResult {
    changed: bool,
    added_nodes: Vec<String>,
}

#[derive(Debug)]
struct CatchUpProgress {
    started_at: Instant,
    last_logged_at: Instant,
    start_row_id: i64,
    high_watermark_row_id: i64,
    processed_nodes: u64,
    changed_source_nodes: u64,
    pages: u64,
}

impl CatchUpProgress {
    fn new(cursor: Option<&GraphNodeCursor>, through: &GraphNodeCursor) -> Self {
        let now = Instant::now();
        Self {
            started_at: now,
            last_logged_at: now,
            start_row_id: cursor.map_or(0, |cursor| cursor.row_id),
            high_watermark_row_id: through.row_id,
            processed_nodes: 0,
            changed_source_nodes: 0,
            pages: 0,
        }
    }

    fn observe_high_watermark(&mut self, through: &GraphNodeCursor) {
        self.high_watermark_row_id = self.high_watermark_row_id.max(through.row_id);
    }

    fn record_page(&mut self, processed_nodes: usize, changed_source_nodes: u64) {
        self.processed_nodes = self.processed_nodes.saturating_add(
            u64::try_from(processed_nodes).expect("graph page size should fit in u64"),
        );
        self.changed_source_nodes = self
            .changed_source_nodes
            .saturating_add(changed_source_nodes);
        self.pages = self.pages.saturating_add(1);
    }

    fn total_nodes(&self) -> u64 {
        row_id_distance(self.start_row_id, self.high_watermark_row_id)
    }

    fn pending_nodes(&self, current_row_id: i64) -> u64 {
        row_id_distance(current_row_id, self.high_watermark_row_id)
    }

    fn unchanged_source_nodes(&self) -> u64 {
        self.processed_nodes
            .saturating_sub(self.changed_source_nodes)
    }

    fn elapsed_millis(&self) -> u64 {
        u64::try_from(self.started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn nodes_per_second(&self) -> u64 {
        let elapsed_millis = self.started_at.elapsed().as_millis().max(1);
        let rate = u128::from(self.processed_nodes).saturating_mul(1_000) / elapsed_millis;
        u64::try_from(rate).unwrap_or(u64::MAX)
    }

    fn log_started(&self, current_revision: u64) {
        self.log("started", self.start_row_id, current_revision);
    }

    fn log_progress_if_due(&mut self, current_row_id: i64, current_revision: u64) {
        if self.last_logged_at.elapsed() < CATCH_UP_PROGRESS_INTERVAL {
            return;
        }
        self.last_logged_at = Instant::now();
        self.log("progress", current_row_id, current_revision);
    }

    fn log_completed(&self, current_row_id: i64, current_revision: u64) {
        self.log("completed", current_row_id, current_revision);
    }

    fn log(&self, phase: &'static str, current_row_id: i64, current_revision: u64) {
        tracing::info!(
            phase,
            elapsed_ms = self.elapsed_millis(),
            total_nodes = self.total_nodes(),
            processed_nodes = self.processed_nodes,
            pending_nodes = self.pending_nodes(current_row_id),
            changed_source_nodes = self.changed_source_nodes,
            unchanged_source_nodes = self.unchanged_source_nodes(),
            pages = self.pages,
            nodes_per_second = self.nodes_per_second(),
            current_revision,
            source_start_row_id = self.start_row_id,
            source_current_row_id = current_row_id,
            source_high_watermark_row_id = self.high_watermark_row_id,
            "web graph catch-up"
        );
    }
}

fn row_id_distance(start: i64, end: i64) -> u64 {
    // Source nodes are append-only, so implicit SQLite row IDs are contiguous while the maximum
    // row remains present. This keeps backlog reporting exact without holding a COUNT query open.
    u64::try_from(end.saturating_sub(start)).unwrap_or_default()
}

fn route_endpoint_ids<'a>(routes: impl Iterator<Item = &'a RoutedEdge>) -> BTreeSet<NodeId> {
    routes
        .flat_map(|route| [route.edge.source.clone(), route.edge.target.clone()])
        .collect()
}

fn route_y_at_x(route: BezierRoute, x: i32) -> i32 {
    const SCALE: i64 = 1 << 16;

    if x <= route.source.x {
        return route.source.y;
    }
    if x >= route.target.x {
        return route.target.y;
    }

    let mut lower = 0_i64;
    let mut upper = SCALE;
    while upper - lower > 1 {
        let middle = lower + (upper - lower) / 2;
        let middle_x = cubic_coordinate(
            route.source.x,
            route.control_1.x,
            route.control_2.x,
            route.target.x,
            middle,
            SCALE,
        );
        if middle_x < x {
            lower = middle;
        } else {
            upper = middle;
        }
    }
    cubic_coordinate(
        route.source.y,
        route.control_1.y,
        route.control_2.y,
        route.target.y,
        lower + (upper - lower) / 2,
        SCALE,
    )
}

fn cubic_coordinate(
    start: i32,
    control_1: i32,
    control_2: i32,
    end: i32,
    t: i64,
    scale: i64,
) -> i32 {
    let t = i128::from(t);
    let scale = i128::from(scale);
    let inverse = scale - t;
    let denominator = scale.saturating_pow(3);
    let numerator = i128::from(start)
        .saturating_mul(inverse.saturating_pow(3))
        .saturating_add(
            i128::from(control_1)
                .saturating_mul(3)
                .saturating_mul(inverse.saturating_pow(2))
                .saturating_mul(t),
        )
        .saturating_add(
            i128::from(control_2)
                .saturating_mul(3)
                .saturating_mul(inverse)
                .saturating_mul(t.saturating_pow(2)),
        )
        .saturating_add(i128::from(end).saturating_mul(t.saturating_pow(3)));
    let value = numerator / denominator;
    i32::try_from(value).unwrap_or_else(|_| {
        if value.is_negative() {
            i32::MIN
        } else {
            i32::MAX
        }
    })
}

fn endpoint_source_offsets(
    routes: &BTreeMap<EdgeId, RoutedEdge>,
    sources: &BTreeSet<NodeId>,
    points: &BTreeMap<NodeId, Point>,
) -> BTreeMap<EdgeId, i32> {
    let mut offsets = BTreeMap::new();
    for source in sources {
        let mut outgoing = routes
            .values()
            .filter(|route| &route.edge.source == source)
            .collect::<Vec<_>>();
        outgoing.sort_by(|left, right| {
            let left_target = points[&left.edge.target];
            let right_target = points[&right.edge.target];
            left_target
                .y
                .cmp(&right_target.y)
                .then_with(|| left_target.x.cmp(&right_target.x))
                .then_with(|| left.edge.kind.cmp(&right.edge.kind))
                .then_with(|| left.edge.target.cmp(&right.edge.target))
        });
        let count = outgoing.len();
        offsets.extend(
            outgoing
                .into_iter()
                .enumerate()
                .map(|(slot, route)| (route.edge.clone(), edge_port_offset(slot, count))),
        );
    }
    offsets
}

fn endpoint_target_offsets(
    routes: &BTreeMap<EdgeId, RoutedEdge>,
    targets: &BTreeSet<NodeId>,
    points: &BTreeMap<NodeId, Point>,
) -> BTreeMap<EdgeId, i32> {
    let mut offsets = BTreeMap::new();
    for target in targets {
        let mut incoming = routes
            .values()
            .filter(|route| &route.edge.target == target)
            .collect::<Vec<_>>();
        incoming.sort_by(|left, right| {
            let left_source = points[&left.edge.source];
            let right_source = points[&right.edge.source];
            left_source
                .y
                .cmp(&right_source.y)
                .then_with(|| left_source.x.cmp(&right_source.x))
                .then_with(|| left.edge.kind.cmp(&right.edge.kind))
                .then_with(|| left.edge.source.cmp(&right.edge.source))
        });
        let count = incoming.len();
        offsets.extend(
            incoming
                .into_iter()
                .enumerate()
                .map(|(slot, route)| (route.edge.clone(), edge_port_offset(slot, count))),
        );
    }
    offsets
}

fn reroute_at_points(
    routes: &BTreeMap<EdgeId, RoutedEdge>,
    points: &BTreeMap<NodeId, Point>,
) -> BTreeMap<EdgeId, RoutedEdge> {
    let sources = routes
        .values()
        .map(|route| route.edge.source.clone())
        .collect::<BTreeSet<_>>();
    let targets = routes
        .values()
        .map(|route| route.edge.target.clone())
        .collect::<BTreeSet<_>>();
    let source_offsets = endpoint_source_offsets(routes, &sources, points);
    let target_offsets = endpoint_target_offsets(routes, &targets, points);
    routes
        .values()
        .map(|routed| {
            let route = route_edge_with_offsets(
                points[&routed.edge.source],
                points[&routed.edge.target],
                EndpointPortOffsets {
                    source: source_offsets[&routed.edge],
                    target: target_offsets[&routed.edge],
                },
            );
            (
                routed.edge.clone(),
                RoutedEdge {
                    edge: routed.edge.clone(),
                    route,
                },
            )
        })
        .collect()
}

fn intermediate_columns(
    routes: &BTreeMap<EdgeId, RoutedEdge>,
    points: &BTreeMap<NodeId, Point>,
) -> BTreeSet<i32> {
    let mut columns = BTreeSet::new();
    for routed in routes.values() {
        let source = points[&routed.edge.source];
        let target = points[&routed.edge.target];
        let mut x = source.x.saturating_add(GRAPH_RANK_STEP);
        while x < target.x {
            columns.insert(x);
            let next = x.saturating_add(GRAPH_RANK_STEP);
            if next <= x {
                break;
            }
            x = next;
        }
    }
    columns
}

fn reserved_edge_rows(
    routes: &BTreeMap<EdgeId, RoutedEdge>,
    points: &BTreeMap<NodeId, Point>,
) -> BTreeMap<i32, BTreeSet<usize>> {
    let mut rows = BTreeMap::<i32, BTreeSet<usize>>::new();
    for routed in routes.values() {
        let source = points[&routed.edge.source];
        let target = points[&routed.edge.target];
        let mut x = source.x.saturating_add(GRAPH_RANK_STEP);
        while x < target.x {
            rows.entry(x)
                .or_default()
                .insert(nearest_row_for_y(route_y_at_x(routed.route, x)));
            let next = x.saturating_add(GRAPH_RANK_STEP);
            if next <= x {
                break;
            }
            x = next;
        }
    }
    rows
}

fn reflow_points(
    columns: &BTreeMap<i32, Vec<NodePlacement>>,
    new_nodes_by_column: &BTreeMap<i32, BTreeMap<NodeId, usize>>,
    routes: &BTreeMap<EdgeId, RoutedEdge>,
    old_points: &BTreeMap<NodeId, Point>,
) -> OrderResult<BTreeMap<NodeId, Point>> {
    let baseline_reserved_rows = columns
        .iter()
        .map(|(x, placements)| (*x, reserved_rows_from_placements(placements)))
        .collect::<BTreeMap<_, _>>();
    let empty_new_nodes = BTreeMap::new();
    let maximum_iterations = columns
        .values()
        .map(Vec::len)
        .sum::<usize>()
        .saturating_add(routes.len())
        .max(8);
    let mut final_points = old_points.clone();
    let mut seen = BTreeSet::new();
    let mut accumulated_edge_rows = BTreeMap::<i32, BTreeSet<usize>>::new();
    for _ in 0..maximum_iterations {
        let projected_routes = reroute_at_points(routes, &final_points);
        let edge_rows_by_column = reserved_edge_rows(&projected_routes, &final_points);
        for (x, rows) in edge_rows_by_column {
            accumulated_edge_rows.entry(x).or_default().extend(rows);
        }
        let mut next_points = final_points.clone();
        for (x, placements) in columns {
            let incoming = placements
                .iter()
                .map(|placement| {
                    let edges = projected_routes
                        .values()
                        .filter(|route| route.edge.target == placement.node)
                        .map(|route| IncomingEdge {
                            kind: route.edge.kind,
                            source_y: next_points[&route.edge.source].y,
                        })
                        .collect::<Vec<_>>();
                    (placement.node.clone(), edges)
                })
                .collect::<BTreeMap<_, _>>();
            let mut reserved_rows = baseline_reserved_rows[x].clone();
            if let Some(edge_rows) = accumulated_edge_rows.get(x) {
                reserved_rows.extend(edge_rows);
            }
            let ordered = stable_column_order(
                placements,
                new_nodes_by_column.get(x).unwrap_or(&empty_new_nodes),
                &incoming,
                &reserved_rows,
            )?;
            next_points.extend(
                ordered
                    .into_iter()
                    .map(|placement| (placement.node, placement.point)),
            );
        }
        let next_routes = reroute_at_points(routes, &next_points);
        let next_edge_rows = reserved_edge_rows(&next_routes, &next_points);
        let has_overlap = columns.iter().any(|(x, placements)| {
            next_edge_rows.get(x).is_some_and(|edge_rows| {
                placements.iter().any(|placement| {
                    edge_rows.contains(&nearest_row_for_y(next_points[&placement.node].y))
                })
            })
        });
        if !has_overlap {
            return Ok(next_points);
        }
        let signature = (
            next_points
                .iter()
                .map(|(node, point)| (node.clone(), point.x, point.y))
                .collect::<Vec<_>>(),
            accumulated_edge_rows
                .iter()
                .map(|(x, rows)| (*x, rows.iter().copied().collect::<Vec<_>>()))
                .collect::<Vec<_>>(),
        );
        if !seen.insert(signature) {
            break;
        }
        final_points = next_points;
    }
    Err(OrderError::ReflowDidNotConverge {
        iterations: maximum_iterations,
    })
}

impl WebGraphRuntime {
    pub async fn open(path: impl AsRef<Path>, publisher: ConsolePublisher) -> crate::Result<Self> {
        let path = path.as_ref();
        let source = SqliteGraphStore::open_read_only(path)
            .await
            .context(StoreSnafu)?;
        let store = WebGraphStore::open(path)
            .await
            .context(WebGraphStoreSnafu)?;
        let empty = empty_graph()?;
        store.initialize(&empty).await.context(WebGraphStoreSnafu)?;
        let state = store
            .state()
            .await
            .context(WebGraphStoreSnafu)?
            .context(WebGraphNotInitializedSnafu)?;
        let (ready, _) = watch::channel(state.revision.get());
        Ok(Self {
            store,
            source,
            publisher,
            ready,
        })
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.ready.subscribe()
    }

    pub fn current_revision(&self) -> u64 {
        *self.ready.borrow()
    }

    fn publish_revision(&self, revision: u64) {
        self.ready.send_if_modified(|current| {
            if *current >= revision {
                return false;
            }
            *current = revision;
            true
        });
    }

    pub fn subscribe_source_changes(&self) -> watch::Receiver<u64> {
        self.publisher.subscribe_source_changes()
    }

    pub async fn node_points(
        &self,
        mode: ViewMode,
        node_ids: &[String],
    ) -> crate::Result<BTreeMap<String, Point>> {
        let node_ids = node_ids
            .iter()
            .map(|node_id| NodeId::new(node_id.clone()).context(WebGraphModelSnafu))
            .collect::<crate::Result<Vec<_>>>()?;
        if node_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        'retry: loop {
            let mut revision = None;
            let mut points = BTreeMap::new();
            for chunk in node_ids.chunks(GRAPH_READ_BATCH_SIZE) {
                let read = self
                    .store
                    .node_placements(layout_kind(mode), chunk)
                    .await
                    .context(WebGraphStoreSnafu)?
                    .context(WebGraphNotInitializedSnafu)?;
                if revision.is_some_and(|revision| revision != read.state.revision) {
                    tokio::task::yield_now().await;
                    continue 'retry;
                }
                revision = Some(read.state.revision);
                points.extend(
                    read.value
                        .into_iter()
                        .map(|placement| (placement.node.to_string(), placement.point)),
                );
            }
            return Ok(points);
        }
    }

    pub async fn drive(self, mut source_changes: watch::Receiver<u64>) -> Infallible {
        let mut retry_delay = RETRY_MIN_DELAY;
        loop {
            source_changes.borrow_and_update();
            match self.catch_up().await {
                Ok(()) => retry_delay = RETRY_MIN_DELAY,
                Err(error) => {
                    tracing::warn!(%error, "retrying web graph synchronization");
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(RETRY_MAX_DELAY);
                    continue;
                }
            }
            tokio::select! {
                changed = source_changes.changed() => {
                    changed.expect("web graph runtime retains the source change publisher");
                }
                () = tokio::time::sleep(RECONCILE_INTERVAL) => {}
            }
        }
    }

    pub async fn catch_up(&self) -> crate::Result<()> {
        let page_size =
            NonZeroUsize::new(GRAPH_READ_BATCH_SIZE).expect("graph read batch size is non-zero");
        let mut progress: Option<CatchUpProgress> = None;
        loop {
            let state = self
                .store
                .state()
                .await
                .context(WebGraphStoreSnafu)?
                .context(WebGraphNotInitializedSnafu)?;
            if let Some(cursor) = state.source_cursor.as_ref()
                && !self
                    .source
                    .graph_node_cursor_matches(cursor)
                    .await
                    .context(StoreSnafu)?
            {
                return WebGraphSourceCursorMismatchSnafu {
                    row_id: cursor.row_id,
                    node_id: cursor.node_id.clone(),
                }
                .fail();
            }
            let Some(through) = self
                .source
                .graph_node_high_watermark()
                .await
                .context(StoreSnafu)?
            else {
                return Ok(());
            };
            if let Some(cursor) = state.source_cursor.as_ref() {
                if cursor.row_id > through.row_id {
                    return WebGraphSourceCursorRegressedSnafu {
                        stored_row_id: cursor.row_id,
                        source_row_id: through.row_id,
                    }
                    .fail();
                }
                if cursor == &through {
                    let current_revision = state.revision.get();
                    self.publish_revision(current_revision);
                    if let Some(progress) = progress.as_mut() {
                        progress.observe_high_watermark(&through);
                        progress.log_completed(cursor.row_id, current_revision);
                    }
                    return Ok(());
                }
            }

            match progress.as_mut() {
                Some(progress) => progress.observe_high_watermark(&through),
                None => {
                    let started = CatchUpProgress::new(state.source_cursor.as_ref(), &through);
                    started.log_started(self.current_revision());
                    progress = Some(started);
                }
            }

            // Keep source pool checkouts scoped to the cursor query, before layout work starts.
            let page = self
                .source
                .graph_nodes_page(state.source_cursor.as_ref(), &through, page_size)
                .await
                .context(StoreSnafu)?;
            if page.entries.is_empty() {
                return WebGraphSourceCursorStalledSnafu {
                    stored_row_id: state.source_cursor.as_ref().map(|cursor| cursor.row_id),
                    source_row_id: through.row_id,
                }
                .fail();
            }
            let page_complete = page.complete;
            let processed_nodes = page.entries.len();
            let mut changed_source_nodes = 0_u64;
            let mut nodes_to_reflow = Vec::new();
            for entry in &page.entries {
                let result = self.ensure_source_node(entry).await?;
                if result.changed {
                    changed_source_nodes = changed_source_nodes.saturating_add(1);
                }
                nodes_to_reflow.extend(result.added_nodes);
                nodes_to_reflow.push(entry.node_id.clone());
            }
            let page_cursor = page
                .entries
                .last()
                .expect("non-empty graph page should end with a cursor");
            let finalized = self
                .reflow_and_advance_source(&nodes_to_reflow, page_cursor)
                .await?;
            let current_revision = finalized.revision.get();
            self.publish_revision(current_revision);
            let current_row_id = page_cursor.row_id;
            let progress = progress
                .as_mut()
                .expect("catch-up progress starts before reading a graph page");
            progress.record_page(processed_nodes, changed_source_nodes);
            if page_complete {
                progress.log_completed(current_row_id, current_revision);
                return Ok(());
            }
            progress.log_progress_if_due(current_row_id, current_revision);
            tokio::task::yield_now().await;
        }
    }

    pub async fn viewport(
        &self,
        mode: ViewMode,
        request: GraphViewportRequest,
    ) -> crate::Result<GraphViewportResponse> {
        loop {
            if let Some(response) = self.viewport_once(mode, request).await? {
                return Ok(response);
            }
            tokio::task::yield_now().await;
        }
    }

    pub async fn viewport_after(
        &self,
        mode: ViewMode,
        observed_revision: u64,
        request: GraphViewportRequest,
    ) -> crate::Result<GraphViewportResponse> {
        self.wait_after(observed_revision).await;
        self.viewport(mode, request).await
    }

    pub async fn viewport_diff(
        &self,
        mode: ViewMode,
        request: GraphViewportDiffRequest,
    ) -> crate::Result<GraphViewportDiffResponse> {
        loop {
            let current = self.viewport(mode, request.current).await?;
            let previous = if request.known.is_some() {
                empty_viewport_response(current.version, current.canvas, request.previous)
            } else {
                self.viewport(mode, request.previous).await?
            };
            if previous.version == current.version {
                return Ok(diff_graph_viewport_responses(
                    previous,
                    current,
                    request.known.as_ref(),
                ));
            }
        }
    }

    pub async fn viewport_diff_after(
        &self,
        mode: ViewMode,
        observed_revision: u64,
        request: GraphViewportDiffRequest,
    ) -> crate::Result<GraphViewportDiffResponse> {
        self.wait_after(observed_revision).await;
        self.viewport_diff(mode, request).await
    }

    async fn wait_after(&self, observed_revision: u64) {
        let mut ready = self.subscribe();
        loop {
            if *ready.borrow_and_update() > observed_revision {
                return;
            }
            ready
                .changed()
                .await
                .expect("web graph runtime retains the revision publisher");
        }
    }

    async fn viewport_once(
        &self,
        mode: ViewMode,
        request: GraphViewportRequest,
    ) -> crate::Result<Option<GraphViewportResponse>> {
        let request = request.normalized();
        let viewport = Viewport {
            x: request.x,
            y: request.y,
            width: request.width,
            height: request.height,
            overscan: request.overscan,
        };
        let kind = layout_kind(mode);
        let limit =
            NonZeroUsize::new(GRAPH_READ_BATCH_SIZE).expect("graph read batch size is non-zero");
        let mut cursor = None;
        let mut revision = None;
        let mut canvas = None;
        let mut placements = Vec::new();
        let mut routes = Vec::new();
        loop {
            let page = match self
                .store
                .viewport_page(kind, viewport, cursor.as_ref(), limit)
                .await
            {
                Ok(Some(page)) => page,
                Ok(None) => return Err(WebGraphNotInitializedSnafu.build()),
                Err(StoreError::StaleCursor { .. }) => return Ok(None),
                Err(source) => return Err(source).context(WebGraphStoreSnafu),
            };
            if revision.is_some_and(|revision| revision != page.state.revision) {
                return Ok(None);
            }
            revision = Some(page.state.revision);
            if canvas.is_none() {
                canvas = Some(page.value.canvas);
            }
            placements.extend(page.value.nodes);
            routes.extend(page.value.edges);
            cursor = page.value.next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        let mut nodes = Vec::with_capacity(placements.len());
        for chunk in placements.chunks(SOURCE_NODE_HYDRATION_BATCH_SIZE) {
            let ids = chunk
                .iter()
                .map(|placement| placement.node.as_str().to_owned())
                .collect::<Vec<_>>();
            let source_nodes = self
                .source
                .graph_nodes_by_ids(&ids)
                .await
                .context(StoreSnafu)?
                .into_iter()
                .map(|node| (node.id.clone(), node))
                .collect::<BTreeMap<_, _>>();
            for placement in chunk {
                let node = source_nodes.get(placement.node.as_str()).with_context(|| {
                    WebGraphSourceNodeMissingSnafu {
                        node_id: placement.node.to_string(),
                    }
                })?;
                nodes.push(viewport_node(node, placement.point));
            }
            tokio::task::yield_now().await;
        }
        let edges = routes.into_iter().map(viewport_edge).collect();
        let canvas = canvas.context(WebGraphNotInitializedSnafu)?;
        Ok(Some(GraphViewportResponse {
            version: revision.context(WebGraphNotInitializedSnafu)?.get(),
            canvas: GraphCanvas {
                width: canvas.width,
                height: canvas.height,
            },
            viewport: graph_viewport(request),
            nodes,
            edges,
        }))
    }

    async fn ensure_source_node(
        &self,
        source_cursor: &GraphNodeCursor,
    ) -> crate::Result<EnsureResult> {
        let state = self
            .store
            .state()
            .await
            .context(WebGraphStoreSnafu)?
            .context(WebGraphNotInitializedSnafu)?;
        if let Some(current) = state.source_cursor.as_ref()
            && current.row_id >= source_cursor.row_id
        {
            if current.row_id == source_cursor.row_id && current.node_id != source_cursor.node_id {
                return WebGraphSourceCursorMismatchSnafu {
                    row_id: current.row_id,
                    node_id: current.node_id.clone(),
                }
                .fail();
            }
            return Ok(EnsureResult::default());
        }
        if self.node_exists(&source_cursor.node_id).await? {
            return Ok(EnsureResult::default());
        }

        let mut steps = vec![EnsureStep::Visit(source_cursor.node_id.clone())];
        let mut visiting = BTreeSet::new();
        let mut result = EnsureResult::default();
        while let Some(step) = steps.pop() {
            match step {
                EnsureStep::Visit(node_id) => {
                    if self.node_exists(&node_id).await? {
                        continue;
                    }
                    if !visiting.insert(node_id.clone()) {
                        let node = NodeId::new(node_id).context(WebGraphModelSnafu)?;
                        return Err(WebGraphModelSnafu.into_error(
                            crate::web_graph::Error::Cycle {
                                layout: LayoutKind::All,
                                node,
                            },
                        ));
                    }
                    let node = self.source_node(&node_id).await?;
                    let parents = raw_parent_edges(&node);
                    steps.push(EnsureStep::Build(Box::new(node)));
                    for parent in parents.into_iter().rev() {
                        steps.push(EnsureStep::Visit(parent.node_id));
                    }
                }
                EnsureStep::Build(node) => {
                    visiting.remove(&node.id);
                    if self.apply_node(&node).await? {
                        result.changed = true;
                        result.added_nodes.push(node.id.clone());
                    }
                }
            }
        }
        Ok(result)
    }

    async fn reflow_and_advance_source(
        &self,
        added_nodes: &[String],
        source_cursor: &GraphNodeCursor,
    ) -> crate::Result<StoredGraphState> {
        let mut new_node_order = BTreeMap::new();
        for (order, node_id) in added_nodes.iter().enumerate() {
            let node = NodeId::new(node_id.clone()).context(WebGraphModelSnafu)?;
            new_node_order.entry(node).or_insert(order);
        }
        loop {
            let state = self
                .store
                .state()
                .await
                .context(WebGraphStoreSnafu)?
                .context(WebGraphNotInitializedSnafu)?;
            if let Some(current) = state.source_cursor.as_ref()
                && current.row_id >= source_cursor.row_id
            {
                if current.row_id == source_cursor.row_id
                    && current.node_id != source_cursor.node_id
                {
                    return WebGraphSourceCursorMismatchSnafu {
                        row_id: current.row_id,
                        node_id: current.node_id.clone(),
                    }
                    .fail();
                }
                return Ok(state);
            }
            let Some(anchors) = self
                .build_reflow_layout_patch(LayoutKind::Anchors, &new_node_order, state.revision)
                .await?
            else {
                tokio::task::yield_now().await;
                continue;
            };
            let Some(all) = self
                .build_reflow_layout_patch(LayoutKind::All, &new_node_order, state.revision)
                .await?
            else {
                tokio::task::yield_now().await;
                continue;
            };
            let revision =
                state
                    .revision
                    .get()
                    .checked_add(1)
                    .context(WebGraphRevisionExhaustedSnafu {
                        revision: state.revision.get(),
                    })?;
            let current_row_id = state
                .source_cursor
                .as_ref()
                .map_or(0, |cursor| cursor.row_id);
            let advanced_rows = u64::try_from(source_cursor.row_id - current_row_id)
                .expect("source cursor advancement is positive");
            let source_version = state
                .source_version
                .get()
                .checked_add(advanced_rows)
                .context(WebGraphSourceVersionExhaustedSnafu {
                    source_version: state.source_version.get(),
                })?;
            let patch = Patch {
                format_version: FORMAT_VERSION,
                base_revision: state.revision,
                revision: Revision::new(revision),
                source_version: SourceVersion::new(source_version),
                topology: TopologyPatch::default(),
                layouts: LayoutPatches { anchors, all },
            };
            match self
                .store
                .apply_patch_and_advance_source(patch, source_cursor.clone())
                .await
            {
                Ok(state) => return Ok(state),
                Err(StoreError::InvalidGraph {
                    source: crate::web_graph::Error::RevisionMismatch { .. },
                }) => tokio::task::yield_now().await,
                Err(source) => return Err(source).context(WebGraphStoreSnafu),
            }
        }
    }

    async fn build_reflow_layout_patch(
        &self,
        kind: LayoutKind,
        new_node_order: &BTreeMap<NodeId, usize>,
        revision: Revision,
    ) -> crate::Result<Option<LayoutPatch>> {
        let new_node_ids = new_node_order.keys().cloned().collect::<Vec<_>>();
        let Some(new_placements) = self
            .placements_at_revision(kind, &new_node_ids, revision)
            .await?
        else {
            return Ok(None);
        };
        let mut new_nodes_by_column = BTreeMap::<i32, BTreeMap<NodeId, usize>>::new();
        for placement in new_placements {
            new_nodes_by_column
                .entry(placement.point.x)
                .or_default()
                .insert(placement.node.clone(), new_node_order[&placement.node]);
        }
        if new_nodes_by_column.is_empty() {
            return Ok(Some(LayoutPatch::default()));
        }

        let mut columns = BTreeMap::<i32, Vec<NodePlacement>>::new();
        let mut column_nodes = BTreeSet::new();
        let mut old_points = BTreeMap::new();
        for x in new_nodes_by_column.keys().copied() {
            let Some(placements) = self.column_at_revision(kind, x, revision).await? else {
                return Ok(None);
            };
            for placement in &placements {
                column_nodes.insert(placement.node.clone());
                old_points.insert(placement.node.clone(), placement.point);
            }
            columns.insert(x, placements);
        }
        let Some(mut routes) = self
            .incident_routes_at_revision(kind, &column_nodes, revision)
            .await?
        else {
            return Ok(None);
        };
        let route_endpoints = route_endpoint_ids(routes.values());
        let Some(endpoint_placements) = self
            .placements_at_revision(
                kind,
                &route_endpoints.into_iter().collect::<Vec<_>>(),
                revision,
            )
            .await?
        else {
            return Ok(None);
        };
        for placement in endpoint_placements {
            old_points.insert(placement.node, placement.point);
        }

        for x in intermediate_columns(&routes, &old_points) {
            if columns.contains_key(&x) {
                continue;
            }
            let Some(placements) = self.column_at_revision(kind, x, revision).await? else {
                return Ok(None);
            };
            for placement in &placements {
                old_points.insert(placement.node.clone(), placement.point);
            }
            columns.insert(x, placements);
        }

        let mut final_points = reflow_points(&columns, &new_nodes_by_column, &routes, &old_points)
            .context(WebGraphOrderSnafu)?;
        let placement_updates = columns
            .values()
            .flatten()
            .filter_map(|placement| {
                let point = final_points[&placement.node];
                (point != placement.point).then(|| {
                    (
                        placement.node.clone(),
                        NodePlacement {
                            node: placement.node.clone(),
                            point,
                        },
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();

        let mut affected_nodes = new_nodes_by_column
            .values()
            .flat_map(BTreeMap::keys)
            .cloned()
            .collect::<BTreeSet<_>>();
        affected_nodes.extend(placement_updates.keys().cloned());
        let mut affected_sources = affected_nodes.clone();
        let mut affected_targets = affected_nodes.clone();
        for route in routes.values() {
            if affected_nodes.contains(&route.edge.target) {
                affected_sources.insert(route.edge.source.clone());
            }
            if affected_nodes.contains(&route.edge.source) {
                affected_targets.insert(route.edge.target.clone());
            }
        }
        let affected_endpoints = affected_sources
            .union(&affected_targets)
            .cloned()
            .collect::<BTreeSet<_>>();
        let Some(endpoint_routes) = self
            .incident_routes_at_revision(kind, &affected_endpoints, revision)
            .await?
        else {
            return Ok(None);
        };
        routes.extend(endpoint_routes);

        let route_endpoints = route_endpoint_ids(routes.values());
        let missing_endpoints = route_endpoints
            .into_iter()
            .filter(|node| !old_points.contains_key(node))
            .collect::<Vec<_>>();
        let Some(endpoint_placements) = self
            .placements_at_revision(kind, &missing_endpoints, revision)
            .await?
        else {
            return Ok(None);
        };
        for placement in endpoint_placements {
            final_points.insert(placement.node.clone(), placement.point);
            old_points.insert(placement.node, placement.point);
        }

        let source_offsets = endpoint_source_offsets(&routes, &affected_sources, &final_points);
        let target_offsets = endpoint_target_offsets(&routes, &affected_targets, &final_points);
        let mut route_updates = Vec::new();
        for routed in routes.values() {
            if !affected_sources.contains(&routed.edge.source)
                && !affected_targets.contains(&routed.edge.target)
            {
                continue;
            }
            let old_source = old_points[&routed.edge.source];
            let old_target = old_points[&routed.edge.target];
            let source_offset = source_offsets
                .get(&routed.edge)
                .copied()
                .unwrap_or_else(|| routed.route.source.y.saturating_sub(old_source.y));
            let target_offset = target_offsets
                .get(&routed.edge)
                .copied()
                .unwrap_or_else(|| routed.route.target.y.saturating_sub(old_target.y));
            let route = route_edge_with_offsets(
                final_points[&routed.edge.source],
                final_points[&routed.edge.target],
                EndpointPortOffsets {
                    source: source_offset,
                    target: target_offset,
                },
            );
            if route != routed.route {
                route_updates.push(RoutedEdge {
                    edge: routed.edge.clone(),
                    route,
                });
            }
        }

        let canvas = self
            .store
            .canvas(kind)
            .await
            .context(WebGraphStoreSnafu)?
            .context(WebGraphNotInitializedSnafu)?;
        if canvas.state.revision != revision {
            return Ok(None);
        }
        let next_height = placement_updates
            .values()
            .map(|placement| placement.point.y.saturating_add(GRAPH_PADDING))
            .max()
            .unwrap_or(canvas.value.height)
            .max(canvas.value.height);
        let next_canvas = Canvas {
            width: canvas.value.width,
            height: next_height,
        };

        Ok(Some(LayoutPatch {
            canvas: (next_canvas != canvas.value).then_some(next_canvas),
            upsert_nodes: placement_updates.into_values().collect(),
            upsert_edges: route_updates,
            ..LayoutPatch::default()
        }))
    }

    async fn placements_at_revision(
        &self,
        kind: LayoutKind,
        node_ids: &[NodeId],
        revision: Revision,
    ) -> crate::Result<Option<Vec<NodePlacement>>> {
        let mut placements = Vec::new();
        for chunk in node_ids.chunks(GRAPH_READ_BATCH_SIZE) {
            let read = self
                .store
                .node_placements(kind, chunk)
                .await
                .context(WebGraphStoreSnafu)?
                .context(WebGraphNotInitializedSnafu)?;
            if read.state.revision != revision {
                return Ok(None);
            }
            placements.extend(read.value);
        }
        Ok(Some(placements))
    }

    async fn column_at_revision(
        &self,
        kind: LayoutKind,
        x: i32,
        revision: Revision,
    ) -> crate::Result<Option<Vec<NodePlacement>>> {
        let limit =
            NonZeroUsize::new(GRAPH_READ_BATCH_SIZE).expect("graph read batch size is non-zero");
        let mut placements = Vec::new();
        let mut cursor = None;
        loop {
            let read = match self
                .store
                .layout_column_placements_page(kind, x, cursor.as_ref(), limit)
                .await
            {
                Ok(Some(read)) => read,
                Ok(None) => return Err(WebGraphNotInitializedSnafu.build()),
                Err(StoreError::StaleCursor { .. }) => return Ok(None),
                Err(source) => return Err(source).context(WebGraphStoreSnafu),
            };
            if read.state.revision != revision {
                return Ok(None);
            }
            placements.extend(read.value.items);
            cursor = read.value.next_cursor;
            if cursor.is_none() {
                return Ok(Some(placements));
            }
        }
    }

    async fn incident_routes_at_revision(
        &self,
        kind: LayoutKind,
        node_ids: &BTreeSet<NodeId>,
        revision: Revision,
    ) -> crate::Result<Option<BTreeMap<EdgeId, RoutedEdge>>> {
        let limit =
            NonZeroUsize::new(GRAPH_READ_BATCH_SIZE).expect("graph read batch size is non-zero");
        let node_ids = node_ids.iter().cloned().collect::<Vec<_>>();
        let mut routes = BTreeMap::new();
        for chunk in node_ids.chunks(GRAPH_READ_BATCH_SIZE) {
            let mut cursor = None;
            loop {
                let read = match self
                    .store
                    .incident_edge_routes_page(kind, chunk, cursor.as_ref(), limit)
                    .await
                {
                    Ok(Some(read)) => read,
                    Ok(None) => return Err(WebGraphNotInitializedSnafu.build()),
                    Err(StoreError::StaleCursor { .. }) => return Ok(None),
                    Err(source) => return Err(source).context(WebGraphStoreSnafu),
                };
                if read.state.revision != revision {
                    return Ok(None);
                }
                routes.extend(
                    read.value
                        .items
                        .into_iter()
                        .map(|route| (route.edge.clone(), route)),
                );
                cursor = read.value.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
        }
        Ok(Some(routes))
    }

    async fn node_exists(&self, node_id: &str) -> crate::Result<bool> {
        let node_id = NodeId::new(node_id).context(WebGraphModelSnafu)?;
        let placements = self
            .store
            .node_placements(LayoutKind::All, &[node_id])
            .await
            .context(WebGraphStoreSnafu)?
            .context(WebGraphNotInitializedSnafu)?;
        Ok(!placements.value.is_empty())
    }

    async fn source_node(&self, node_id: &str) -> crate::Result<GraphNodeRecord> {
        self.source
            .graph_node_records_by_ids(&[node_id.to_owned()])
            .await
            .context(StoreSnafu)?
            .pop()
            .with_context(|| WebGraphSourceNodeMissingSnafu {
                node_id: node_id.to_owned(),
            })
    }

    async fn apply_node(&self, node: &GraphNodeRecord) -> crate::Result<bool> {
        loop {
            if self.node_exists(&node.id).await? {
                return Ok(false);
            }
            let state = self
                .store
                .state()
                .await
                .context(WebGraphStoreSnafu)?
                .context(WebGraphNotInitializedSnafu)?;
            let Some(patch) = self.build_node_patch(node, &state).await? else {
                tokio::task::yield_now().await;
                continue;
            };
            match self.store.apply_patch(patch).await {
                Ok(_) => return Ok(true),
                Err(StoreError::InvalidGraph {
                    source: crate::web_graph::Error::RevisionMismatch { .. },
                }) => tokio::task::yield_now().await,
                Err(source) => return Err(source).context(WebGraphStoreSnafu),
            }
        }
    }

    async fn build_node_patch(
        &self,
        node: &GraphNodeRecord,
        state: &StoredGraphState,
    ) -> crate::Result<Option<Patch>> {
        let node_id = NodeId::new(node.id.clone()).context(WebGraphModelSnafu)?;
        let all_parents = raw_parent_edges(node);
        let anchor_parents = if node.is_anchor {
            self.anchor_parent_edges(&all_parents).await?
        } else {
            Vec::new()
        };
        let Some(all) = self
            .build_layout_patch(LayoutKind::All, &node_id, &all_parents, state.revision)
            .await?
        else {
            return Ok(None);
        };
        let anchors = if node.is_anchor {
            let Some(layout) = self
                .build_layout_patch(
                    LayoutKind::Anchors,
                    &node_id,
                    &anchor_parents,
                    state.revision,
                )
                .await?
            else {
                return Ok(None);
            };
            layout
        } else {
            LayoutPatch::default()
        };

        let all_edges = parent_edge_ids(&all_parents, &node_id)?;
        let anchor_edges = parent_edge_ids(&anchor_parents, &node_id)?;
        let add_edges = all_edges
            .into_iter()
            .chain(anchor_edges)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let revision =
            state
                .revision
                .get()
                .checked_add(1)
                .context(WebGraphRevisionExhaustedSnafu {
                    revision: state.revision.get(),
                })?;
        Ok(Some(Patch {
            format_version: FORMAT_VERSION,
            base_revision: state.revision,
            revision: Revision::new(revision),
            source_version: state.source_version,
            topology: TopologyPatch {
                add_nodes: vec![node_id],
                add_edges,
                ..TopologyPatch::default()
            },
            layouts: LayoutPatches { anchors, all },
        }))
    }

    async fn build_layout_patch(
        &self,
        kind: LayoutKind,
        node_id: &NodeId,
        parents: &[ParentEdge],
        revision: Revision,
    ) -> crate::Result<Option<LayoutPatch>> {
        let parent_ids = parents
            .iter()
            .map(|parent| NodeId::new(parent.node_id.clone()).context(WebGraphModelSnafu))
            .collect::<crate::Result<BTreeSet<_>>>()?
            .into_iter()
            .collect::<Vec<_>>();
        let read = self
            .store
            .layout_node_states(kind, &parent_ids)
            .await
            .context(WebGraphStoreSnafu)?
            .context(WebGraphNotInitializedSnafu)?;
        if read.state.revision != revision {
            return Ok(None);
        }
        let parent_states = read
            .value
            .into_iter()
            .map(|state| (state.placement.node.to_string(), state))
            .collect::<BTreeMap<_, _>>();
        let mut parent_points = BTreeMap::new();
        let mut source_slots = BTreeMap::<String, usize>::new();
        let mut source_counts =
            parents
                .iter()
                .fold(BTreeMap::<String, usize>::new(), |mut counts, parent| {
                    *counts.entry(parent.node_id.clone()).or_default() += 1;
                    counts
                });
        for parent in parents {
            let state = parent_states.get(&parent.node_id).with_context(|| {
                WebGraphParentPlacementMissingSnafu {
                    layout: layout_name(kind),
                    node_id: node_id.to_string(),
                    parent_id: parent.node_id.clone(),
                }
            })?;
            parent_points.insert(parent.node_id.clone(), state.placement.point);
            source_slots.insert(parent.node_id.clone(), state.outgoing_edge_count);
            source_counts
                .entry(parent.node_id.clone())
                .and_modify(|count| *count = count.saturating_add(state.outgoing_edge_count));
        }

        let x = parent_points
            .values()
            .map(|point| point.x.saturating_add(GRAPH_RANK_STEP))
            .max()
            .unwrap_or(GRAPH_PADDING);
        let bottom = self
            .store
            .layout_column_bottom(kind, x)
            .await
            .context(WebGraphStoreSnafu)?
            .context(WebGraphNotInitializedSnafu)?;
        if bottom.state.revision != revision {
            return Ok(None);
        }
        let y = bottom
            .value
            .map(|bottom| bottom.saturating_add(GRAPH_ROW_STEP))
            .unwrap_or(GRAPH_PADDING);
        let canvas = self
            .store
            .canvas(kind)
            .await
            .context(WebGraphStoreSnafu)?
            .context(WebGraphNotInitializedSnafu)?;
        if canvas.state.revision != revision {
            return Ok(None);
        }
        let next_canvas = Canvas {
            width: canvas.value.width.max(x.saturating_add(GRAPH_PADDING)),
            height: canvas.value.height.max(y.saturating_add(GRAPH_PADDING)),
        };
        let point = Point { x, y };
        let mut routes = Vec::with_capacity(parents.len());
        for (target_slot, parent) in parents.iter().enumerate() {
            let slot = source_slots
                .get_mut(&parent.node_id)
                .expect("parent layout state is loaded before routing");
            let source_slot = *slot;
            *slot = slot.saturating_add(1);
            let edge = EdgeId::new(
                parent.kind,
                NodeId::new(parent.node_id.clone()).context(WebGraphModelSnafu)?,
                node_id.clone(),
            );
            let source = parent_points
                .get(&parent.node_id)
                .copied()
                .expect("parent points are loaded before routing");
            routes.push(RoutedEdge {
                edge,
                route: route_edge(
                    source,
                    point,
                    EndpointPortSlots {
                        source: source_slot,
                        source_count: source_counts[&parent.node_id],
                        target: target_slot,
                        target_count: parents.len(),
                    },
                ),
            });
        }
        Ok(Some(LayoutPatch {
            canvas: (next_canvas != canvas.value).then_some(next_canvas),
            upsert_nodes: vec![NodePlacement {
                node: node_id.clone(),
                point,
            }],
            upsert_edges: routes,
            ..LayoutPatch::default()
        }))
    }

    async fn anchor_parent_edges(&self, parents: &[ParentEdge]) -> crate::Result<Vec<ParentEdge>> {
        let mut resolved = Vec::new();
        let mut sources = BTreeSet::new();
        for parent in parents {
            let Some(source) = self.nearest_anchor(&parent.node_id).await? else {
                continue;
            };
            if sources.insert(source.clone()) {
                resolved.push(ParentEdge {
                    kind: parent.kind,
                    node_id: source,
                });
            }
        }
        Ok(resolved)
    }

    async fn nearest_anchor(&self, start_id: &str) -> crate::Result<Option<String>> {
        let mut current = start_id.to_owned();
        while !current.is_empty() {
            let node = self.source_node(&current).await?;
            if node.is_anchor {
                return Ok(Some(node.id));
            }
            current = node.parent;
        }
        Ok(None)
    }
}

fn empty_graph() -> crate::Result<Graph> {
    let canvas = Canvas {
        width: GRAPH_PADDING * 2,
        height: GRAPH_PADDING * 2,
    };
    Graph::from_snapshot(Snapshot {
        format_version: FORMAT_VERSION,
        revision: Revision::new(0),
        source_version: SourceVersion::new(0),
        topology: TopologySnapshot {
            nodes: Vec::new(),
            edges: Vec::new(),
        },
        layouts: LayoutSnapshots {
            anchors: LayoutSnapshot {
                canvas,
                nodes: Vec::new(),
                edges: Vec::new(),
            },
            all: LayoutSnapshot {
                canvas,
                nodes: Vec::new(),
                edges: Vec::new(),
            },
        },
    })
    .context(WebGraphModelSnafu)
}

fn raw_parent_edges(node: &GraphNodeRecord) -> Vec<ParentEdge> {
    let mut parents = Vec::new();
    if !node.parent.is_empty() {
        parents.push(ParentEdge {
            kind: EdgeKind::Primary,
            node_id: node.parent.clone(),
        });
    }
    if node.is_anchor {
        parents.extend(node.merge_parents.iter().map(|parent| ParentEdge {
            kind: if parent.is_shadow() {
                EdgeKind::Shadow
            } else {
                EdgeKind::Merge
            },
            node_id: parent.node_id().to_owned(),
        }));
    }
    parents
}

fn parent_edge_ids(parents: &[ParentEdge], target: &NodeId) -> crate::Result<Vec<EdgeId>> {
    parents
        .iter()
        .map(|parent| {
            Ok(EdgeId::new(
                parent.kind,
                NodeId::new(parent.node_id.clone()).context(WebGraphModelSnafu)?,
                target.clone(),
            ))
        })
        .collect()
}

fn layout_kind(mode: ViewMode) -> LayoutKind {
    match mode {
        ViewMode::Anchors => LayoutKind::Anchors,
        ViewMode::All => LayoutKind::All,
    }
}

fn layout_name(kind: LayoutKind) -> &'static str {
    match kind {
        LayoutKind::Anchors => "anchors",
        LayoutKind::All => "all",
    }
}

fn viewport_node(node: &Node, point: Point) -> GraphViewportNode {
    GraphViewportNode {
        key: node_key(&node.id),
        id: node.id.clone(),
        node_target: node_target_id(&node.id),
        short_id: shorten_id(&node.id),
        kind: graph_kind_name(node).to_owned(),
        summary: summarize_node(node),
        labels: Vec::new(),
        x: point.x,
        y: point.y,
    }
}

fn viewport_edge(route: RoutedEdge) -> GraphViewportEdge {
    let kind = match route.edge.kind {
        EdgeKind::Primary => GraphViewportEdgeKind::Primary,
        EdgeKind::Merge => GraphViewportEdgeKind::Merge,
        EdgeKind::Shadow => GraphViewportEdgeKind::Shadow,
    };
    GraphViewportEdge {
        key: edge_key(kind, route.edge.source.as_str(), route.edge.target.as_str()),
        kind,
        source_id: route.edge.source.to_string(),
        target_id: route.edge.target.to_string(),
        route: api_route(route.route),
    }
}

fn api_route(route: BezierRoute) -> GraphBezierRoute {
    GraphBezierRoute {
        source: api_point(route.source),
        control_1: api_point(route.control_1),
        control_2: api_point(route.control_2),
        target: api_point(route.target),
    }
}

fn api_point(point: Point) -> ApiPoint {
    ApiPoint {
        x: point.x,
        y: point.y,
    }
}

fn graph_viewport(request: GraphViewportRequest) -> GraphViewport {
    let request = request.normalized();
    GraphViewport {
        x: request.x,
        y: request.y,
        width: request.width,
        height: request.height,
        overscan: request.overscan,
    }
}

fn empty_viewport_response(
    version: u64,
    canvas: GraphCanvas,
    request: GraphViewportRequest,
) -> GraphViewportResponse {
    GraphViewportResponse {
        version,
        canvas,
        viewport: graph_viewport(request),
        nodes: Vec::new(),
        edges: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use coco_mem::{
        Anchor, BranchStore, Kind, MergeParent, NewNode, NewNodeContent, NodeStore, PromptAnchor,
        Role, SqliteStore,
    };
    use diesel::Connection;
    use diesel::connection::SimpleConnection;
    use diesel::sqlite::SqliteConnection;
    use tokio::time::timeout;

    use super::*;
    use crate::host::store::ConsoleStore;

    #[test]
    fn catch_up_progress_tracks_dynamic_backlog_and_page_stats() {
        let cursor = GraphNodeCursor {
            row_id: 4,
            node_id: "cursor".to_owned(),
        };
        let mut through = GraphNodeCursor {
            row_id: 9,
            node_id: "through".to_owned(),
        };
        let mut progress = CatchUpProgress::new(Some(&cursor), &through);

        assert_eq!(progress.total_nodes(), 5);
        assert_eq!(progress.pending_nodes(cursor.row_id), 5);
        progress.record_page(2, 1);
        assert_eq!(progress.processed_nodes, 2);
        assert_eq!(progress.changed_source_nodes, 1);
        assert_eq!(progress.unchanged_source_nodes(), 1);
        assert_eq!(progress.pages, 1);
        assert_eq!(progress.pending_nodes(6), 3);

        through.row_id = 12;
        progress.observe_high_watermark(&through);
        assert_eq!(progress.total_nodes(), 8);
        assert_eq!(progress.pending_nodes(6), 6);
    }

    #[test]
    fn endpoint_ports_follow_the_opposite_endpoint_order() {
        let source = NodeId::new("source").unwrap();
        let upper_target = NodeId::new("upper-target").unwrap();
        let lower_target = NodeId::new("lower-target").unwrap();
        let upper_source = NodeId::new("upper-source").unwrap();
        let lower_source = NodeId::new("lower-source").unwrap();
        let target = NodeId::new("target").unwrap();
        let source_to_upper = EdgeId::new(EdgeKind::Primary, source.clone(), upper_target.clone());
        let source_to_lower = EdgeId::new(EdgeKind::Primary, source.clone(), lower_target.clone());
        let upper_to_target = EdgeId::new(EdgeKind::Primary, upper_source.clone(), target.clone());
        let lower_to_target = EdgeId::new(EdgeKind::Primary, lower_source.clone(), target.clone());
        let points = BTreeMap::from([
            (source.clone(), Point { x: 56, y: 128 }),
            (upper_target.clone(), Point { x: 168, y: 56 }),
            (lower_target.clone(), Point { x: 168, y: 200 }),
            (upper_source.clone(), Point { x: 56, y: 56 }),
            (lower_source.clone(), Point { x: 56, y: 200 }),
            (target.clone(), Point { x: 168, y: 128 }),
        ]);
        let routes = [
            source_to_lower.clone(),
            source_to_upper.clone(),
            lower_to_target.clone(),
            upper_to_target.clone(),
        ]
        .into_iter()
        .map(|edge| {
            let route = route_edge(
                points[&edge.source],
                points[&edge.target],
                EndpointPortSlots {
                    source: 0,
                    source_count: 1,
                    target: 0,
                    target_count: 1,
                },
            );
            (edge.clone(), RoutedEdge { edge, route })
        })
        .collect::<BTreeMap<_, _>>();

        let source_offsets = endpoint_source_offsets(&routes, &BTreeSet::from([source]), &points);
        assert_eq!(source_offsets[&source_to_upper], edge_port_offset(0, 2));
        assert_eq!(source_offsets[&source_to_lower], edge_port_offset(1, 2));
        let target_offsets = endpoint_target_offsets(&routes, &BTreeSet::from([target]), &points);
        assert_eq!(target_offsets[&upper_to_target], edge_port_offset(0, 2));
        assert_eq!(target_offsets[&lower_to_target], edge_port_offset(1, 2));
    }

    #[tokio::test]
    async fn late_child_is_inserted_by_parent_order_and_reroutes_affected_edges() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let upper_parent = append_text(&writer, &root, "upper parent").await;
        let middle_parent = append_text(&writer, &root, "middle parent").await;
        let lower_parent = append_text(&writer, &root, "lower parent").await;
        let upper_child = append_text(&writer, &upper_parent, "upper child").await;
        let lower_child = append_text(&writer, &lower_parent, "lower child").await;
        let runtime = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();
        runtime.catch_up().await.unwrap();
        let upper_child_id = NodeId::new(upper_child.clone()).unwrap();
        let lower_child_id = NodeId::new(lower_child.clone()).unwrap();
        let before = runtime
            .store
            .node_placements(
                LayoutKind::All,
                &[upper_child_id.clone(), lower_child_id.clone()],
            )
            .await
            .unwrap()
            .unwrap()
            .value
            .into_iter()
            .map(|placement| (placement.node, placement.point))
            .collect::<BTreeMap<_, _>>();
        assert!(before[&upper_child_id].y < before[&lower_child_id].y);
        let state_before = runtime.store.state().await.unwrap().unwrap();

        let middle_child = append_text(&writer, &middle_parent, "middle child").await;
        runtime.catch_up().await.unwrap();

        let middle_child_id = NodeId::new(middle_child.clone()).unwrap();
        let after = runtime
            .store
            .node_placements(
                LayoutKind::All,
                &[
                    upper_child_id.clone(),
                    middle_child_id.clone(),
                    lower_child_id.clone(),
                ],
            )
            .await
            .unwrap()
            .unwrap()
            .value
            .into_iter()
            .map(|placement| (placement.node, placement.point))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(after[&upper_child_id], before[&upper_child_id]);
        assert!(after[&upper_child_id].y < after[&middle_child_id].y);
        assert!(after[&middle_child_id].y < after[&lower_child_id].y);
        assert_eq!(
            after[&lower_child_id].y,
            before[&lower_child_id].y + GRAPH_ROW_STEP
        );

        let state_after = runtime.store.state().await.unwrap().unwrap();
        assert_eq!(
            state_after.source_version.get(),
            state_before.source_version.get() + 1
        );
        assert_eq!(state_after.revision.get(), state_before.revision.get() + 2);
        let viewport = runtime
            .viewport(ViewMode::All, complete_viewport())
            .await
            .unwrap();
        let lower_route = viewport
            .edges
            .iter()
            .find(|edge| edge.source_id == lower_parent && edge.target_id == lower_child)
            .unwrap();
        assert_eq!(
            lower_route.route.target.y,
            after[&lower_child_id].y + edge_port_offset(0, 1)
        );
    }

    #[tokio::test]
    async fn edges_reserve_rows_in_intermediate_columns_for_every_layout() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let upper = append_anchor(&writer, &root, Vec::new(), "upper").await;
        let lower = append_anchor(&writer, &root, Vec::new(), "lower").await;
        let middle = append_anchor(&writer, &lower, Vec::new(), "middle").await;
        let target = append_anchor(
            &writer,
            &middle,
            vec![MergeParent::shadow(upper.clone())],
            "target",
        )
        .await;
        let runtime = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();

        runtime.catch_up().await.unwrap();

        let middle_id = NodeId::new(middle).unwrap();
        for (layout, mode) in [
            (LayoutKind::Anchors, ViewMode::Anchors),
            (LayoutKind::All, ViewMode::All),
        ] {
            let middle_point = runtime
                .store
                .node_placements(layout, std::slice::from_ref(&middle_id))
                .await
                .unwrap()
                .unwrap()
                .value[0]
                .point;
            assert_eq!(middle_point.y, GRAPH_PADDING + GRAPH_ROW_STEP);
            let viewport = runtime.viewport(mode, complete_viewport()).await.unwrap();
            let crossing = viewport
                .edges
                .iter()
                .find(|edge| {
                    edge.kind == GraphViewportEdgeKind::Shadow
                        && edge.source_id == upper
                        && edge.target_id == target
                })
                .unwrap();
            let route = BezierRoute {
                source: Point {
                    x: crossing.route.source.x,
                    y: crossing.route.source.y,
                },
                control_1: Point {
                    x: crossing.route.control_1.x,
                    y: crossing.route.control_1.y,
                },
                control_2: Point {
                    x: crossing.route.control_2.x,
                    y: crossing.route.control_2.y,
                },
                target: Point {
                    x: crossing.route.target.x,
                    y: crossing.route.target.y,
                },
            };
            assert_eq!(nearest_row_for_y(route_y_at_x(route, middle_point.x)), 0);
            assert!(viewport.canvas.height >= middle_point.y + GRAPH_PADDING);
        }
    }

    #[tokio::test]
    async fn long_edge_reflows_every_intermediate_column_in_the_same_catch_up_page() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let upper = append_anchor(&writer, &root, Vec::new(), "upper").await;
        let lower = append_anchor(&writer, &root, Vec::new(), "lower").await;
        let mut primary_parent = append_anchor(&writer, &lower, Vec::new(), "primary 0").await;
        let merge_source = append_anchor(&writer, &upper, Vec::new(), "merge source").await;
        let mut middle = None;
        for index in 1..12 {
            primary_parent = append_anchor(
                &writer,
                &primary_parent,
                Vec::new(),
                &format!("primary {index}"),
            )
            .await;
            if index == 3 {
                middle = Some(primary_parent.clone());
            }
        }
        let target = append_anchor(
            &writer,
            &primary_parent,
            vec![MergeParent::merge(merge_source.clone())],
            "target",
        )
        .await;
        let runtime = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();

        runtime.catch_up().await.unwrap();

        let middle_id = NodeId::new(middle.unwrap()).unwrap();
        for (layout, mode) in [
            (LayoutKind::Anchors, ViewMode::Anchors),
            (LayoutKind::All, ViewMode::All),
        ] {
            let point = runtime
                .store
                .node_placements(layout, std::slice::from_ref(&middle_id))
                .await
                .unwrap()
                .unwrap()
                .value[0]
                .point;
            let viewport = runtime.viewport(mode, complete_viewport()).await.unwrap();
            let crossing = viewport
                .edges
                .iter()
                .find(|edge| {
                    edge.kind == GraphViewportEdgeKind::Merge
                        && edge.source_id == merge_source
                        && edge.target_id == target
                })
                .unwrap();
            let route = BezierRoute {
                source: Point {
                    x: crossing.route.source.x,
                    y: crossing.route.source.y,
                },
                control_1: Point {
                    x: crossing.route.control_1.x,
                    y: crossing.route.control_1.y,
                },
                control_2: Point {
                    x: crossing.route.control_2.x,
                    y: crossing.route.control_2.y,
                },
                target: Point {
                    x: crossing.route.target.x,
                    y: crossing.route.target.y,
                },
            };
            assert_ne!(
                nearest_row_for_y(point.y),
                nearest_row_for_y(route_y_at_x(route, point.x))
            );
        }
    }

    #[tokio::test]
    async fn edge_lane_reflow_preserves_parallel_branch_order_across_columns() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let upper_0 = append_anchor(&writer, &root, Vec::new(), "upper 0").await;
        let lower_0 = append_anchor(&writer, &root, Vec::new(), "lower 0").await;
        let lower_1 = append_anchor(&writer, &lower_0, Vec::new(), "lower 1").await;
        let lower_2 = append_anchor(&writer, &lower_1, Vec::new(), "lower 2").await;
        let lower_3 = append_anchor(
            &writer,
            &lower_2,
            vec![MergeParent::merge(upper_0.clone())],
            "lower 3",
        )
        .await;
        let upper_1 = append_anchor(&writer, &upper_0, Vec::new(), "upper 1").await;
        let upper_2 = append_anchor(&writer, &upper_1, Vec::new(), "upper 2").await;
        let upper_3 = append_anchor(&writer, &upper_2, Vec::new(), "upper 3").await;
        let runtime = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();

        runtime.catch_up().await.unwrap();

        for layout in [LayoutKind::Anchors, LayoutKind::All] {
            for (upper, lower) in [
                (&upper_0, &lower_0),
                (&upper_1, &lower_1),
                (&upper_2, &lower_2),
                (&upper_3, &lower_3),
            ] {
                let ids = [
                    NodeId::new(upper.clone()).unwrap(),
                    NodeId::new(lower.clone()).unwrap(),
                ];
                let points = runtime
                    .store
                    .node_placements(layout, &ids)
                    .await
                    .unwrap()
                    .unwrap()
                    .value
                    .into_iter()
                    .map(|placement| (placement.node, placement.point))
                    .collect::<BTreeMap<_, _>>();
                assert!(points[&ids[0]].y < points[&ids[1]].y);
            }
        }
    }

    #[tokio::test]
    async fn outdated_layout_is_rebuilt_from_source_on_restart() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let upper = append_anchor(&writer, &root, Vec::new(), "upper").await;
        let lower = append_anchor(&writer, &root, Vec::new(), "lower").await;
        let middle = append_anchor(&writer, &lower, Vec::new(), "middle").await;
        append_anchor(&writer, &middle, vec![MergeParent::shadow(upper)], "target").await;
        let runtime = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();
        runtime.catch_up().await.unwrap();
        let source_version = runtime.store.state().await.unwrap().unwrap().source_version;
        let derived_path = runtime.store.path().to_owned();
        drop(runtime);

        let mut connection = SqliteConnection::establish(derived_path.to_str().unwrap()).unwrap();
        connection
            .batch_execute("UPDATE web_graph_state SET layout_version = 0")
            .unwrap();
        drop(connection);

        let runtime = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();
        let reset = runtime.store.state().await.unwrap().unwrap();
        assert_eq!(reset.revision.get(), 0);
        assert_eq!(reset.source_version.get(), 0);
        assert_eq!(reset.source_cursor, None);

        runtime.catch_up().await.unwrap();

        let rebuilt = runtime.store.state().await.unwrap().unwrap();
        assert_eq!(rebuilt.source_version, source_version);
        assert!(rebuilt.source_cursor.is_some());
        let middle_id = NodeId::new(middle).unwrap();
        for layout in [LayoutKind::Anchors, LayoutKind::All] {
            let middle_point = runtime
                .store
                .node_placements(layout, std::slice::from_ref(&middle_id))
                .await
                .unwrap()
                .unwrap()
                .value[0]
                .point;
            assert_eq!(middle_point.y, GRAPH_PADDING + GRAPH_ROW_STEP);
        }
    }

    #[tokio::test]
    async fn restart_reflows_nodes_persisted_before_source_cursor_advances() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let upper_parent = append_text(&writer, &root, "upper parent").await;
        let middle_parent = append_text(&writer, &root, "middle parent").await;
        let lower_parent = append_text(&writer, &root, "lower parent").await;
        let upper_child = append_text(&writer, &upper_parent, "upper child").await;
        let lower_child = append_text(&writer, &lower_parent, "lower child").await;
        let runtime = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();
        runtime.catch_up().await.unwrap();
        let finalized = runtime.store.state().await.unwrap().unwrap();

        let middle_child = append_text(&writer, &middle_parent, "middle child").await;
        let middle_record = runtime.source_node(&middle_child).await.unwrap();
        assert!(runtime.apply_node(&middle_record).await.unwrap());
        let partial = runtime.store.state().await.unwrap().unwrap();
        assert_eq!(partial.source_cursor, finalized.source_cursor);
        assert_eq!(partial.source_version, finalized.source_version);
        assert_eq!(partial.revision.get(), finalized.revision.get() + 1);

        let upper_child_id = NodeId::new(upper_child).unwrap();
        let middle_child_id = NodeId::new(middle_child).unwrap();
        let lower_child_id = NodeId::new(lower_child).unwrap();
        let partial_points = runtime
            .store
            .node_placements(
                LayoutKind::All,
                &[
                    upper_child_id.clone(),
                    middle_child_id.clone(),
                    lower_child_id.clone(),
                ],
            )
            .await
            .unwrap()
            .unwrap()
            .value
            .into_iter()
            .map(|placement| (placement.node, placement.point))
            .collect::<BTreeMap<_, _>>();
        assert!(partial_points[&lower_child_id].y < partial_points[&middle_child_id].y);
        drop(runtime);

        let runtime = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();
        runtime.catch_up().await.unwrap();

        let recovered = runtime.store.state().await.unwrap().unwrap();
        assert_eq!(recovered.revision.get(), partial.revision.get() + 1);
        assert_eq!(
            recovered.source_version.get(),
            finalized.source_version.get() + 1
        );
        assert_eq!(
            recovered.source_cursor,
            runtime.source.graph_node_high_watermark().await.unwrap()
        );
        let recovered_points = runtime
            .store
            .node_placements(
                LayoutKind::All,
                &[
                    upper_child_id.clone(),
                    middle_child_id.clone(),
                    lower_child_id.clone(),
                ],
            )
            .await
            .unwrap()
            .unwrap()
            .value
            .into_iter()
            .map(|placement| (placement.node, placement.point))
            .collect::<BTreeMap<_, _>>();
        assert!(recovered_points[&upper_child_id].y < recovered_points[&middle_child_id].y);
        assert!(recovered_points[&middle_child_id].y < recovered_points[&lower_child_id].y);
    }

    #[tokio::test]
    async fn synchronization_builds_global_topology_and_both_layouts_incrementally() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let hidden_before_first = append_text(&writer, &root, "hidden before first").await;
        let first_anchor = append_anchor(&writer, &hidden_before_first, Vec::new(), "first").await;
        let hidden_between = append_text(&writer, &first_anchor, "hidden between").await;
        let second_anchor = append_anchor(&writer, &hidden_between, Vec::new(), "second").await;
        let third_anchor = append_anchor(
            &writer,
            &second_anchor,
            vec![MergeParent::shadow(first_anchor.clone())],
            "third",
        )
        .await;
        let publisher = ConsolePublisher::new();
        let runtime = WebGraphRuntime::open(writer.store_path(), publisher)
            .await
            .unwrap();

        runtime.catch_up().await.unwrap();

        let all = runtime
            .viewport(ViewMode::All, complete_viewport())
            .await
            .unwrap();
        assert_eq!(all.nodes.len(), 6);
        assert_eq!(all.edges.len(), 6);
        let anchors = runtime
            .viewport(ViewMode::Anchors, complete_viewport())
            .await
            .unwrap();
        assert_eq!(
            anchors
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                first_anchor.as_str(),
                second_anchor.as_str(),
                third_anchor.as_str(),
            ])
        );
        assert!(anchors.edges.iter().any(|edge| {
            edge.kind == GraphViewportEdgeKind::Primary
                && edge.source_id == first_anchor
                && edge.target_id == second_anchor
        }));
        assert!(anchors.edges.iter().any(|edge| {
            edge.kind == GraphViewportEdgeKind::Shadow
                && edge.source_id == first_anchor
                && edge.target_id == third_anchor
        }));
        let state = runtime.store.state().await.unwrap().unwrap();
        assert_eq!(state.source_version.get(), 6);
        assert_eq!(state.revision.get(), state.source_version.get() + 1);
        assert_eq!(
            state.source_cursor,
            runtime.source.graph_node_high_watermark().await.unwrap()
        );
    }

    #[tokio::test]
    async fn viewport_diff_supports_server_and_client_known_state() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let runtime = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();
        runtime.catch_up().await.unwrap();
        let viewport = complete_viewport();

        let server_diff = runtime
            .viewport_diff(
                ViewMode::All,
                GraphViewportDiffRequest {
                    previous: viewport,
                    current: viewport,
                    known: None,
                },
            )
            .await
            .unwrap();
        assert!(server_diff.added.nodes.is_empty());
        assert!(server_diff.updated.nodes.is_empty());
        assert!(server_diff.removed.is_empty());

        let client_diff = runtime
            .viewport_diff(
                ViewMode::All,
                GraphViewportDiffRequest {
                    previous: viewport,
                    current: viewport,
                    known: Some(crate::host::api::GraphViewportKnownItems::default()),
                },
            )
            .await
            .unwrap();
        assert_eq!(client_diff.added.nodes.len(), 1);
        assert!(client_diff.updated.nodes.is_empty());
        assert!(client_diff.removed.is_empty());
    }

    #[tokio::test]
    async fn source_dirty_wakeup_persists_cursor_before_publishing_revision() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let publisher = ConsolePublisher::new();
        let runtime = WebGraphRuntime::open(writer.store_path(), publisher.clone())
            .await
            .unwrap();
        runtime.catch_up().await.unwrap();
        let root = writer.root_id();
        let root_id = NodeId::new(root.clone()).unwrap();
        let root_before = runtime
            .store
            .node_placements(LayoutKind::All, std::slice::from_ref(&root_id))
            .await
            .unwrap()
            .unwrap()
            .value[0]
            .point;
        let mut revisions = runtime.subscribe();
        revisions.borrow_and_update();
        let source_changes = runtime.subscribe_source_changes();
        let driver = tokio::spawn(runtime.clone().drive(source_changes));
        let console_store = ConsoleStore::new(writer.clone(), publisher);

        let child = append_text(&console_store, &root, "live child").await;
        let child_cursor = runtime
            .source
            .graph_node_high_watermark()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(child_cursor.node_id, child);
        timeout(Duration::from_secs(2), async {
            loop {
                revisions.changed().await.unwrap();
                let revision = *revisions.borrow_and_update();
                let state = runtime.store.state().await.unwrap().unwrap();
                if state.revision.get() == revision
                    && state.source_cursor.as_ref() == Some(&child_cursor)
                {
                    let child_id = NodeId::new(child.clone()).unwrap();
                    if !runtime
                        .store
                        .node_placements(LayoutKind::All, &[child_id])
                        .await
                        .unwrap()
                        .unwrap()
                        .value
                        .is_empty()
                    {
                        break;
                    }
                }
            }
        })
        .await
        .expect("node creation should publish a persisted web graph revision");

        let root_after = runtime
            .store
            .node_placements(LayoutKind::All, &[root_id])
            .await
            .unwrap()
            .unwrap()
            .value[0]
            .point;
        assert_eq!(root_after, root_before);
        let viewport = runtime
            .viewport(ViewMode::All, complete_viewport())
            .await
            .unwrap();
        assert!(viewport.nodes.iter().any(|node| node.id == child));

        driver.abort();
        assert!(driver.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn catch_up_publishes_revision_committed_by_another_runtime() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let first = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();
        first.catch_up().await.unwrap();
        let second = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();
        let observed_revision = second.current_revision();
        let mut revisions = second.subscribe();
        revisions.borrow_and_update();

        append_text(&writer, &writer.root_id(), "committed by first runtime").await;
        first.catch_up().await.unwrap();
        let committed_revision = first.current_revision();
        assert!(committed_revision > observed_revision);
        assert_eq!(second.current_revision(), observed_revision);

        second.catch_up().await.unwrap();

        timeout(Duration::from_secs(2), revisions.changed())
            .await
            .expect("catch-up should publish the persisted revision")
            .unwrap();
        assert_eq!(*revisions.borrow_and_update(), committed_revision);
        assert_eq!(second.current_revision(), committed_revision);
    }

    #[tokio::test]
    async fn batch_node_event_builds_every_missing_ancestor() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let publisher = ConsolePublisher::new();
        let runtime = WebGraphRuntime::open(writer.store_path(), publisher.clone())
            .await
            .unwrap();
        runtime.catch_up().await.unwrap();
        let root = writer.root_id();
        let console_store = ConsoleStore::new(writer.clone(), publisher);
        console_store.fork("main", &root).await.unwrap();
        runtime.catch_up().await.unwrap();
        let source_changes = runtime.subscribe_source_changes();

        let head = console_store
            .append_nodes_and_set_branch_head(
                "main",
                &root,
                &root,
                vec![
                    NewNodeContent {
                        role: Role::User,
                        metadata: None,
                        kind: Kind::Text("first batch node".to_owned()),
                    },
                    NewNodeContent {
                        role: Role::LLM,
                        metadata: None,
                        kind: Kind::Text("second batch node".to_owned()),
                    },
                ],
            )
            .await
            .unwrap();
        let driver = tokio::spawn(runtime.clone().drive(source_changes));
        let ancestry = writer.ancestry(&head).await.unwrap();
        let batch_node_ids = ancestry
            .iter()
            .filter(|node| node.id != root)
            .map(|node| node.id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(batch_node_ids.len(), 2);
        let expected_cursor = runtime
            .source
            .graph_node_high_watermark()
            .await
            .unwrap()
            .unwrap();

        timeout(Duration::from_secs(2), async {
            loop {
                let viewport = runtime
                    .viewport(ViewMode::All, complete_viewport())
                    .await
                    .unwrap();
                let visible = viewport
                    .nodes
                    .iter()
                    .map(|node| node.id.as_str())
                    .collect::<BTreeSet<_>>();
                let state = runtime.store.state().await.unwrap().unwrap();
                if batch_node_ids.is_subset(&visible)
                    && state.source_cursor.as_ref() == Some(&expected_cursor)
                    && runtime.current_revision() == state.revision.get()
                {
                    assert_eq!(viewport.edges.len(), 2);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the final batch event should build its missing ancestry");
        let state = runtime.store.state().await.unwrap().unwrap();
        assert_eq!(state.revision.get(), 5);
        assert_eq!(state.source_version.get(), 3);
        assert_eq!(
            state.source_cursor,
            runtime.source.graph_node_high_watermark().await.unwrap()
        );

        driver.abort();
        assert!(driver.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn restart_resumes_after_the_persisted_source_cursor() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let root = writer.root_id();
        let runtime = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();
        runtime.catch_up().await.unwrap();
        let first = append_text(&writer, &root, "before restart").await;
        runtime.catch_up().await.unwrap();
        let first_id = NodeId::new(first.clone()).unwrap();
        let first_point = runtime
            .store
            .node_placements(LayoutKind::All, std::slice::from_ref(&first_id))
            .await
            .unwrap()
            .unwrap()
            .value[0]
            .point;
        let before = runtime.store.state().await.unwrap().unwrap();
        drop(runtime);

        let second = append_text(&writer, &first, "after restart one").await;
        append_text(&writer, &second, "after restart two").await;
        let runtime = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();
        assert_eq!(runtime.store.state().await.unwrap().unwrap(), before);

        runtime.catch_up().await.unwrap();

        let after = runtime.store.state().await.unwrap().unwrap();
        assert_eq!(after.source_version.get(), before.source_version.get() + 2);
        assert_eq!(after.revision.get(), before.revision.get() + 3);
        assert_eq!(
            after.source_cursor,
            runtime.source.graph_node_high_watermark().await.unwrap()
        );
        assert_eq!(
            runtime
                .store
                .node_placements(LayoutKind::All, &[first_id])
                .await
                .unwrap()
                .unwrap()
                .value[0]
                .point,
            first_point
        );
        assert_eq!(
            runtime
                .viewport(ViewMode::All, complete_viewport())
                .await
                .unwrap()
                .nodes
                .len(),
            4
        );
    }

    #[tokio::test]
    async fn catch_up_rejects_a_cursor_from_a_different_source_store() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let runtime = WebGraphRuntime::open(writer.store_path(), ConsolePublisher::new())
            .await
            .unwrap();
        runtime.catch_up().await.unwrap();
        append_text(&writer, &writer.root_id(), "next source node").await;
        let next = runtime
            .source
            .graph_node_high_watermark()
            .await
            .unwrap()
            .unwrap();
        let state = runtime.store.state().await.unwrap().unwrap();
        let patch = Patch {
            format_version: FORMAT_VERSION,
            base_revision: state.revision,
            revision: Revision::new(state.revision.get() + 1),
            source_version: SourceVersion::new(state.source_version.get() + 1),
            topology: TopologyPatch::default(),
            layouts: LayoutPatches::default(),
        };
        runtime
            .store
            .apply_patch_and_advance_source(
                patch,
                GraphNodeCursor {
                    row_id: next.row_id,
                    node_id: "node-from-another-store".to_owned(),
                },
            )
            .await
            .unwrap();

        let error = runtime.catch_up().await.unwrap_err();

        assert!(matches!(
            error,
            crate::Error::WebGraphSourceCursorMismatch { row_id, .. }
                if row_id == next.row_id
        ));
    }

    async fn append_text(store: &impl NodeStore, parent: &str, text: &str) -> String {
        store
            .append(NewNode {
                parent: parent.to_owned(),
                role: Role::User,
                metadata: None,
                kind: Kind::Text(text.to_owned()),
            })
            .await
            .unwrap()
    }

    async fn append_anchor(
        store: &impl NodeStore,
        parent: &str,
        merge_parents: Vec<MergeParent>,
        prompt: &str,
    ) -> String {
        store
            .append(NewNode {
                parent: parent.to_owned(),
                role: Role::User,
                metadata: None,
                kind: Kind::Anchor(Anchor::prompt(
                    merge_parents,
                    PromptAnchor {
                        prompt: prompt.to_owned(),
                        attachments: Vec::new(),
                    },
                )),
            })
            .await
            .unwrap()
    }

    fn complete_viewport() -> GraphViewportRequest {
        GraphViewportRequest {
            x: 0,
            y: 0,
            width: 10_000,
            height: 10_000,
            overscan: 0,
        }
    }
}
