use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

use coco_mem::{
    GRAPH_READ_BATCH_SIZE, GraphChildPageCursor, Kind, Node, NodeStore, SqliteGraphStore,
};
use snafu::{IntoError, prelude::*};
use tokio::sync::{broadcast, watch};

use super::incremental_layout::{EndpointPortSlots, LayoutPoint, StableLayoutConfig, route_edge};
use super::web_graph_store::{Error as StoreError, StoredGraphState, Viewport, WebGraphStore};
use crate::api::{
    GraphBezierRoute, GraphCanvas, GraphViewport, GraphViewportDiffResponse, GraphViewportEdge,
    GraphViewportEdgeKind, GraphViewportNode, GraphViewportResponse, Point as ApiPoint,
};
use crate::error::{
    StoreSnafu, WebGraphModelSnafu, WebGraphNotInitializedSnafu,
    WebGraphParentPlacementMissingSnafu, WebGraphRevisionExhaustedSnafu,
    WebGraphSourceNodeMissingSnafu, WebGraphStoreSnafu,
};
use crate::graph::{GraphMode, graph_kind_name, node_target_id, shorten_id, summarize_node};
use crate::host::api::{GraphViewportDiffRequest, GraphViewportRequest};
use crate::layout::{
    GRAPH_PADDING, GRAPH_RANK_STEP, GRAPH_ROW_STEP, GraphLayoutEdgeKind,
    diff_graph_viewport_responses, edge_key, node_key,
};
use crate::publisher::{ConsoleNodeCreated, ConsolePublisher};
use crate::web_graph::{
    BezierRoute, Canvas, EdgeId, EdgeKind, FORMAT_VERSION, Graph, LayoutKind, LayoutPatch,
    LayoutPatches, LayoutSnapshot, LayoutSnapshots, NodeId, NodePlacement, Patch, Point, Revision,
    RoutedEdge, Snapshot, SourceVersion, TopologyPatch, TopologySnapshot,
};

const RETRY_MIN_DELAY: Duration = Duration::from_millis(25);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

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
    Build(Box<Node>),
}

#[derive(Debug)]
struct ScanFrame {
    parent_id: String,
    cursor: Option<GraphChildPageCursor>,
    pending: VecDeque<String>,
    complete: bool,
}

#[derive(Debug, Clone)]
struct ParentEdge {
    kind: EdgeKind,
    node_id: String,
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
        publisher.advance_to(state.source_version.get());
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

    pub fn subscribe_node_creations(&self) -> broadcast::Receiver<ConsoleNodeCreated> {
        self.publisher.subscribe_node_creations()
    }

    pub async fn drive(
        self,
        mut node_creations: broadcast::Receiver<ConsoleNodeCreated>,
    ) -> Infallible {
        let mut retry_delay = RETRY_MIN_DELAY;
        loop {
            match self.synchronize().await {
                Ok(()) => retry_delay = RETRY_MIN_DELAY,
                Err(error) => {
                    tracing::warn!(%error, "retrying web graph synchronization");
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(RETRY_MAX_DELAY);
                    continue;
                }
            }

            loop {
                match node_creations.recv().await {
                    Ok(created) => {
                        if let Err(error) = self
                            .ensure_node(&created.node_id, created.source_version)
                            .await
                        {
                            tracing::warn!(
                                node_id = %created.node_id,
                                source_version = created.source_version,
                                %error,
                                "retrying web graph node build",
                            );
                            tokio::time::sleep(retry_delay).await;
                            retry_delay = (retry_delay * 2).min(RETRY_MAX_DELAY);
                            break;
                        }
                        retry_delay = RETRY_MIN_DELAY;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "web graph node events lagged; rescanning source");
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        std::future::pending::<()>().await;
                    }
                }
            }
        }
    }

    pub async fn synchronize(&self) -> crate::Result<()> {
        let source_version = self.publisher.current_version();
        let root_id = self.source.root_id();
        self.ensure_node_without_advancing(&root_id, source_version)
            .await?;

        let mut stack = vec![ScanFrame {
            parent_id: root_id,
            cursor: None,
            pending: VecDeque::new(),
            complete: false,
        }];
        while let Some(frame) = stack.last_mut() {
            if let Some(child_id) = frame.pending.pop_front() {
                let child = self.source.get_node(&child_id).await.context(StoreSnafu)?;
                if child.parent != frame.parent_id {
                    continue;
                }
                self.ensure_node_without_advancing(&child.id, source_version)
                    .await?;
                stack.push(ScanFrame {
                    parent_id: child.id,
                    cursor: None,
                    pending: VecDeque::new(),
                    complete: false,
                });
                continue;
            }
            if frame.complete {
                stack.pop();
                continue;
            }
            let page = self
                .source
                .graph_child_ids_page(
                    &frame.parent_id,
                    frame.cursor.as_ref(),
                    NonZeroUsize::new(GRAPH_READ_BATCH_SIZE)
                        .expect("graph read batch size is non-zero"),
                )
                .await
                .context(StoreSnafu)?;
            frame.cursor = page.next_cursor;
            frame.complete = page.complete;
            frame.pending.extend(page.child_ids);
        }

        self.advance_source_version(source_version).await
    }

    pub async fn viewport(
        &self,
        mode: GraphMode,
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
        mode: GraphMode,
        observed_revision: u64,
        request: GraphViewportRequest,
    ) -> crate::Result<GraphViewportResponse> {
        self.wait_after(observed_revision).await;
        self.viewport(mode, request).await
    }

    pub async fn viewport_diff(
        &self,
        mode: GraphMode,
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
        mode: GraphMode,
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
        mode: GraphMode,
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
        for chunk in placements.chunks(GRAPH_READ_BATCH_SIZE) {
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

    async fn ensure_node(&self, node_id: &str, source_version: u64) -> crate::Result<bool> {
        let changed = self
            .ensure_node_without_advancing(node_id, source_version)
            .await?;
        self.advance_source_version(source_version).await?;
        Ok(changed)
    }

    async fn ensure_node_without_advancing(
        &self,
        node_id: &str,
        source_version: u64,
    ) -> crate::Result<bool> {
        let mut steps = vec![EnsureStep::Visit(node_id.to_owned())];
        let mut visiting = BTreeSet::new();
        let mut changed = false;
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
                    let node = self.source.get_node(&node_id).await.context(StoreSnafu)?;
                    let parents = raw_parent_edges(&node);
                    steps.push(EnsureStep::Build(Box::new(node)));
                    for parent in parents.into_iter().rev() {
                        steps.push(EnsureStep::Visit(parent.node_id));
                    }
                }
                EnsureStep::Build(node) => {
                    visiting.remove(&node.id);
                    if !self.node_exists(&node.id).await? {
                        changed |= self.apply_node(&node, source_version).await?;
                    }
                }
            }
        }
        Ok(changed)
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

    async fn apply_node(&self, node: &Node, source_version: u64) -> crate::Result<bool> {
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
            let Some(patch) = self.build_node_patch(node, source_version, state).await? else {
                tokio::task::yield_now().await;
                continue;
            };
            match self.store.apply_patch(patch).await {
                Ok(state) => {
                    self.ready.send_replace(state.revision.get());
                    return Ok(true);
                }
                Err(StoreError::InvalidGraph {
                    source: crate::web_graph::Error::RevisionMismatch { .. },
                }) => tokio::task::yield_now().await,
                Err(source) => return Err(source).context(WebGraphStoreSnafu),
            }
        }
    }

    async fn build_node_patch(
        &self,
        node: &Node,
        source_version: u64,
        state: StoredGraphState,
    ) -> crate::Result<Option<Patch>> {
        let node_id = NodeId::new(node.id.clone()).context(WebGraphModelSnafu)?;
        let all_parents = raw_parent_edges(node);
        let anchor_parents = if matches!(node.kind, Kind::Anchor(_)) {
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
        let anchors = if matches!(node.kind, Kind::Anchor(_)) {
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
            source_version: SourceVersion::new(source_version.max(state.source_version.get())),
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
        let mut parent_points = BTreeMap::new();
        for parent in parents {
            let parent_id = NodeId::new(parent.node_id.clone()).context(WebGraphModelSnafu)?;
            let read = self
                .store
                .node_placements(kind, std::slice::from_ref(&parent_id))
                .await
                .context(WebGraphStoreSnafu)?
                .context(WebGraphNotInitializedSnafu)?;
            if read.state.revision != revision {
                return Ok(None);
            }
            let point = read
                .value
                .into_iter()
                .next()
                .with_context(|| WebGraphParentPlacementMissingSnafu {
                    layout: layout_name(kind),
                    node_id: node_id.to_string(),
                    parent_id: parent.node_id.clone(),
                })?
                .point;
            parent_points.insert(parent.node_id.clone(), point);
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
        let mut source_slots = BTreeMap::<String, usize>::new();
        let mut routes = Vec::with_capacity(parents.len());
        for (target_slot, parent) in parents.iter().enumerate() {
            let source_slot = match source_slots.get_mut(&parent.node_id) {
                Some(slot) => {
                    let current = *slot;
                    *slot = slot.saturating_add(1);
                    current
                }
                None => {
                    let Some(count) = self
                        .outgoing_route_count(kind, &parent.node_id, revision)
                        .await?
                    else {
                        return Ok(None);
                    };
                    source_slots.insert(parent.node_id.clone(), count.saturating_add(1));
                    count
                }
            };
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
                route: web_route(route_edge(
                    layout_point(source),
                    layout_point(point),
                    EndpointPortSlots {
                        source: source_slot,
                        target: target_slot,
                    },
                    StableLayoutConfig::default(),
                )),
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

    async fn outgoing_route_count(
        &self,
        kind: LayoutKind,
        node_id: &str,
        revision: Revision,
    ) -> crate::Result<Option<usize>> {
        let node_id = NodeId::new(node_id).context(WebGraphModelSnafu)?;
        let limit =
            NonZeroUsize::new(GRAPH_READ_BATCH_SIZE).expect("graph read batch size is non-zero");
        let mut cursor = None;
        let mut count = 0_usize;
        loop {
            let page = match self
                .store
                .incident_edge_routes_page(
                    kind,
                    std::slice::from_ref(&node_id),
                    cursor.as_ref(),
                    limit,
                )
                .await
            {
                Ok(Some(page)) => page,
                Ok(None) => return Err(WebGraphNotInitializedSnafu.build()),
                Err(StoreError::StaleCursor { .. }) => return Ok(None),
                Err(source) => return Err(source).context(WebGraphStoreSnafu),
            };
            if page.state.revision != revision {
                return Ok(None);
            }
            count = count.saturating_add(
                page.value
                    .items
                    .iter()
                    .filter(|route| route.edge.source == node_id)
                    .count(),
            );
            cursor = page.value.next_cursor;
            if cursor.is_none() {
                return Ok(Some(count));
            }
        }
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
            let node = self.source.get_node(&current).await.context(StoreSnafu)?;
            if matches!(node.kind, Kind::Anchor(_)) {
                return Ok(Some(node.id));
            }
            current = node.parent;
        }
        Ok(None)
    }

    async fn advance_source_version(&self, source_version: u64) -> crate::Result<()> {
        loop {
            let state = self
                .store
                .state()
                .await
                .context(WebGraphStoreSnafu)?
                .context(WebGraphNotInitializedSnafu)?;
            if state.source_version.get() >= source_version {
                return Ok(());
            }
            let revision =
                state
                    .revision
                    .get()
                    .checked_add(1)
                    .context(WebGraphRevisionExhaustedSnafu {
                        revision: state.revision.get(),
                    })?;
            let patch = Patch {
                format_version: FORMAT_VERSION,
                base_revision: state.revision,
                revision: Revision::new(revision),
                source_version: SourceVersion::new(source_version),
                topology: TopologyPatch::default(),
                layouts: LayoutPatches::default(),
            };
            match self.store.apply_patch(patch).await {
                Ok(state) => {
                    self.ready.send_replace(state.revision.get());
                    return Ok(());
                }
                Err(StoreError::InvalidGraph {
                    source: crate::web_graph::Error::RevisionMismatch { .. },
                }) => tokio::task::yield_now().await,
                Err(source) => return Err(source).context(WebGraphStoreSnafu),
            }
        }
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

fn raw_parent_edges(node: &Node) -> Vec<ParentEdge> {
    let mut parents = Vec::new();
    if !node.parent.is_empty() {
        parents.push(ParentEdge {
            kind: EdgeKind::Primary,
            node_id: node.parent.clone(),
        });
    }
    if let Kind::Anchor(anchor) = &node.kind {
        parents.extend(anchor.merge_parents().iter().map(|parent| ParentEdge {
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

fn layout_kind(mode: GraphMode) -> LayoutKind {
    match mode {
        GraphMode::Anchors => LayoutKind::Anchors,
        GraphMode::All => LayoutKind::All,
    }
}

fn layout_name(kind: LayoutKind) -> &'static str {
    match kind {
        LayoutKind::Anchors => "anchors",
        LayoutKind::All => "all",
    }
}

fn layout_point(point: Point) -> LayoutPoint {
    LayoutPoint {
        x: point.x,
        y: point.y,
    }
}

fn web_route(route: super::incremental_layout::CubicRoute) -> BezierRoute {
    BezierRoute {
        source: Point {
            x: route.source.x,
            y: route.source.y,
        },
        control_1: Point {
            x: route.control_1.x,
            y: route.control_1.y,
        },
        control_2: Point {
            x: route.control_2.x,
            y: route.control_2.y,
        },
        target: Point {
            x: route.target.x,
            y: route.target.y,
        },
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
    let layout_kind = match route.edge.kind {
        EdgeKind::Primary => GraphLayoutEdgeKind::Primary,
        EdgeKind::Merge => GraphLayoutEdgeKind::Merge,
        EdgeKind::Shadow => GraphLayoutEdgeKind::Shadow,
    };
    GraphViewportEdge {
        key: edge_key(
            layout_kind,
            route.edge.source.as_str(),
            route.edge.target.as_str(),
        ),
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
        Anchor, BranchStore, MergeParent, NewNode, NewNodeContent, PromptAnchor, Role, SqliteStore,
    };
    use tokio::time::timeout;

    use super::*;
    use crate::host::store::ConsoleStore;

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

        runtime.synchronize().await.unwrap();

        let all = runtime
            .viewport(GraphMode::All, complete_viewport())
            .await
            .unwrap();
        assert_eq!(all.nodes.len(), 6);
        assert_eq!(all.edges.len(), 6);
        let anchors = runtime
            .viewport(GraphMode::Anchors, complete_viewport())
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
        assert_eq!(
            runtime.store.state().await.unwrap().unwrap().revision.get(),
            6
        );
    }

    #[tokio::test]
    async fn node_creation_event_persists_a_patch_before_publishing_revision() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let publisher = ConsolePublisher::new();
        let runtime = WebGraphRuntime::open(writer.store_path(), publisher.clone())
            .await
            .unwrap();
        runtime.synchronize().await.unwrap();
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
        let node_creations = runtime.subscribe_node_creations();
        let driver = tokio::spawn(runtime.clone().drive(node_creations));
        let console_store = ConsoleStore::new(writer.clone(), publisher);

        let child = append_text(&console_store, &root, "live child").await;
        timeout(Duration::from_secs(2), async {
            loop {
                revisions.changed().await.unwrap();
                let revision = *revisions.borrow_and_update();
                let state = runtime.store.state().await.unwrap().unwrap();
                if state.revision.get() == revision && state.source_version.get() == 1 {
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
            .viewport(GraphMode::All, complete_viewport())
            .await
            .unwrap();
        assert!(viewport.nodes.iter().any(|node| node.id == child));

        driver.abort();
        assert!(driver.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn batch_node_event_builds_every_missing_ancestor() {
        let writer = SqliteStore::open_temporary().await.unwrap();
        let publisher = ConsolePublisher::new();
        let runtime = WebGraphRuntime::open(writer.store_path(), publisher.clone())
            .await
            .unwrap();
        runtime.synchronize().await.unwrap();
        let root = writer.root_id();
        let console_store = ConsoleStore::new(writer.clone(), publisher);
        console_store.fork("main", &root).await.unwrap();
        runtime.synchronize().await.unwrap();
        let node_creations = runtime.subscribe_node_creations();

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
        let driver = tokio::spawn(runtime.clone().drive(node_creations));
        let ancestry = writer.ancestry(&head).await.unwrap();
        let batch_node_ids = ancestry
            .iter()
            .filter(|node| node.id != root)
            .map(|node| node.id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(batch_node_ids.len(), 2);

        timeout(Duration::from_secs(2), async {
            loop {
                let viewport = runtime
                    .viewport(GraphMode::All, complete_viewport())
                    .await
                    .unwrap();
                let visible = viewport
                    .nodes
                    .iter()
                    .map(|node| node.id.as_str())
                    .collect::<BTreeSet<_>>();
                if batch_node_ids.is_subset(&visible) {
                    assert_eq!(viewport.edges.len(), 2);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the final batch event should build its missing ancestry");
        let state = runtime.store.state().await.unwrap().unwrap();
        assert_eq!(state.revision.get(), 4);
        assert_eq!(state.source_version.get(), 2);

        driver.abort();
        assert!(driver.await.unwrap_err().is_cancelled());
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
