use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Duration;

use coco_mem::{GRAPH_READ_BATCH_SIZE, GraphNodeCursor, GraphNodeRecord, Node, SqliteGraphStore};
use snafu::{IntoError, prelude::*};
use tokio::sync::watch;

use super::error::{
    StoreSnafu, WebGraphModelSnafu, WebGraphNotInitializedSnafu,
    WebGraphParentPlacementMissingSnafu, WebGraphRevisionExhaustedSnafu,
    WebGraphSourceCursorMismatchSnafu, WebGraphSourceCursorRegressedSnafu,
    WebGraphSourceCursorStalledSnafu, WebGraphSourceNodeMissingSnafu,
    WebGraphSourceVersionExhaustedSnafu, WebGraphStoreSnafu,
};
use super::publisher::ConsolePublisher;
use super::web_graph_store::{Error as StoreError, StoredGraphState, Viewport, WebGraphStore};
use super::web_graph_view::{
    EndpointPortSlots, GRAPH_PADDING, GRAPH_RANK_STEP, GRAPH_ROW_STEP, ViewMode,
    diff_graph_viewport_responses, edge_key, graph_kind_name, node_key, node_target_id, route_edge,
    shorten_id, summarize_node,
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
                    return Ok(());
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
            for entry in page.entries {
                self.ensure_source_node(&entry).await?;
            }
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

    async fn ensure_source_node(&self, source_cursor: &GraphNodeCursor) -> crate::Result<bool> {
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
            return Ok(false);
        }
        if self.node_exists(&source_cursor.node_id).await? {
            self.advance_source_cursor(source_cursor).await?;
            return Ok(false);
        }

        let mut steps = vec![EnsureStep::Visit(source_cursor.node_id.clone())];
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
                    let node = self.source_node(&node_id).await?;
                    let parents = raw_parent_edges(&node);
                    steps.push(EnsureStep::Build(Box::new(node)));
                    for parent in parents.into_iter().rev() {
                        steps.push(EnsureStep::Visit(parent.node_id));
                    }
                }
                EnsureStep::Build(node) => {
                    visiting.remove(&node.id);
                    let is_source_node = node.id == source_cursor.node_id;
                    if is_source_node || !self.node_exists(&node.id).await? {
                        changed |= self
                            .apply_node(&node, is_source_node.then_some(source_cursor))
                            .await?;
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

    async fn apply_node(
        &self,
        node: &GraphNodeRecord,
        source_cursor: Option<&GraphNodeCursor>,
    ) -> crate::Result<bool> {
        loop {
            if self.node_exists(&node.id).await? {
                if let Some(source_cursor) = source_cursor {
                    self.advance_source_cursor(source_cursor).await?;
                }
                return Ok(false);
            }
            let state = self
                .store
                .state()
                .await
                .context(WebGraphStoreSnafu)?
                .context(WebGraphNotInitializedSnafu)?;
            let Some(patch) = self
                .build_node_patch(node, source_cursor.is_some(), &state)
                .await?
            else {
                tokio::task::yield_now().await;
                continue;
            };
            let result = if let Some(source_cursor) = source_cursor {
                self.store
                    .apply_patch_and_advance_source(patch, source_cursor.clone())
                    .await
            } else {
                self.store.apply_patch(patch).await
            };
            match result {
                Ok(state) => {
                    if source_cursor.is_some() {
                        self.ready.send_replace(state.revision.get());
                    }
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
        node: &GraphNodeRecord,
        consume_source: bool,
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
        let source_version = if consume_source {
            state.source_version.get().checked_add(1).context(
                WebGraphSourceVersionExhaustedSnafu {
                    source_version: state.source_version.get(),
                },
            )?
        } else {
            state.source_version.get()
        };
        Ok(Some(Patch {
            format_version: FORMAT_VERSION,
            base_revision: state.revision,
            revision: Revision::new(revision),
            source_version: SourceVersion::new(source_version),
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
                route: route_edge(
                    source,
                    point,
                    EndpointPortSlots {
                        source: source_slot,
                        target: target_slot,
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
            let node = self.source_node(&current).await?;
            if node.is_anchor {
                return Ok(Some(node.id));
            }
            current = node.parent;
        }
        Ok(None)
    }

    async fn advance_source_cursor(&self, source_cursor: &GraphNodeCursor) -> crate::Result<()> {
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
            let source_version = state.source_version.get().checked_add(1).context(
                WebGraphSourceVersionExhaustedSnafu {
                    source_version: state.source_version.get(),
                },
            )?;
            let patch = Patch {
                format_version: FORMAT_VERSION,
                base_revision: state.revision,
                revision: Revision::new(revision),
                source_version: SourceVersion::new(source_version),
                topology: TopologyPatch::default(),
                layouts: LayoutPatches::default(),
            };
            match self
                .store
                .apply_patch_and_advance_source(patch, source_cursor.clone())
                .await
            {
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
        assert_eq!(
            runtime.store.state().await.unwrap().unwrap().revision.get(),
            6
        );
        let state = runtime.store.state().await.unwrap().unwrap();
        assert_eq!(state.source_version.get(), 6);
        assert_eq!(
            state.source_cursor,
            runtime.source.graph_node_high_watermark().await.unwrap()
        );
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
        assert_eq!(state.revision.get(), 3);
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
        assert_eq!(after.revision.get(), before.revision.get() + 2);
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
