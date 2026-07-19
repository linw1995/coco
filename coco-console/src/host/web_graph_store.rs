use std::collections::{BTreeSet, VecDeque};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use diesel::{
    BoolExpressionMethods, ExpressionMethods, Insertable, OptionalExtension, QueryDsl, Queryable,
    Selectable, SelectableHelper,
};
use diesel_async::AsyncConnection;
use diesel_migrations::{EmbeddedMigrations, embed_migrations};
use snafu::prelude::*;

use crate::web_graph::{
    BezierRoute, Canvas, EdgeId, EdgeKind, Graph, LayoutKind, LayoutPatch, LayoutSnapshot, NodeId,
    NodePlacement, Patch, Point, Revision, RoutedEdge, Snapshot, SourceVersion, TopologyPatch,
};

const DATABASE_FILE_NAME: &str = "web-graph.sqlite3";
const WRITE_BATCH_SIZE: usize = 64;
const MAX_READ_BATCH_SIZE: usize = 128;
const MIGRATIONS: EmbeddedMigrations = embed_migrations!("web-graph-migrations");

mod database;
mod schema;
mod spatial;

use database::{AsyncSqliteConnection, Database};
use schema::{
    web_graph_edge_routes, web_graph_edges, web_graph_layouts, web_graph_node_placements,
    web_graph_nodes, web_graph_state,
};
pub use spatial::{Viewport, ViewportCursor, ViewportPage};

#[derive(Clone)]
pub struct WebGraphStore {
    path: PathBuf,
    database: Database,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredGraphState {
    pub format_version: u32,
    pub revision: Revision,
    pub source_version: SourceVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRead<T> {
    pub state: StoredGraphState,
    pub value: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageCursor<T> {
    revision: Revision,
    layout: LayoutKind,
    scope: CursorScope,
    after: T,
}

impl<T> PageCursor<T> {
    pub fn revision(&self) -> Revision {
        self.revision
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CursorScope {
    All,
    Incident(Vec<NodeId>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPage<T, C> {
    pub items: Vec<T>,
    pub next_cursor: Option<PageCursor<C>>,
}

impl std::fmt::Debug for WebGraphStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WebGraphStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl WebGraphStore {
    pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).context(CreateDirectorySnafu {
            path: dir.to_owned(),
        })?;
        let path = dir.join(DATABASE_FILE_NAME);
        let database = Database::open(path.clone()).await?;
        Ok(Self { path, database })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn state(&self) -> Result<Option<StoredGraphState>> {
        let path = self.path.clone();
        let mut connection = self.database.acquire().await?;
        load_state(&mut connection)
            .await
            .map_err(|error| error.into_store_error(path))
    }

    pub async fn canvas(&self, kind: LayoutKind) -> Result<Option<GraphRead<Canvas>>> {
        let path = self.path.clone();
        let mut connection = self.database.acquire().await?;
        connection
            .transaction::<_, TransactionError, _>(async |connection| {
                let Some(state) = load_state(connection).await? else {
                    return Ok(None);
                };
                let canvas = load_canvas(connection, kind).await?;
                Ok(Some(GraphRead {
                    state,
                    value: canvas,
                }))
            })
            .await
            .map_err(|error| error.into_store_error(path))
    }

    pub async fn layout_column_bottom(
        &self,
        kind: LayoutKind,
        x: i32,
    ) -> Result<Option<GraphRead<Option<i32>>>> {
        use diesel::dsl::max;
        use diesel_async::RunQueryDsl;

        let path = self.path.clone();
        let mut connection = self.database.acquire().await?;
        connection
            .transaction::<_, TransactionError, _>(async |connection| {
                let Some(state) = load_state(connection).await? else {
                    return Ok(None);
                };
                let bottom = web_graph_node_placements::table
                    .filter(web_graph_node_placements::layout_kind.eq(layout_kind_value(kind)))
                    .filter(web_graph_node_placements::x.eq(x))
                    .select(max(web_graph_node_placements::y))
                    .get_result::<Option<i32>>(connection)
                    .await?;
                Ok(Some(GraphRead {
                    state,
                    value: bottom,
                }))
            })
            .await
            .map_err(|error| error.into_store_error(path))
    }

    pub async fn node_placements(
        &self,
        kind: LayoutKind,
        node_ids: &[NodeId],
    ) -> Result<Option<GraphRead<Vec<NodePlacement>>>> {
        validate_read_batch_size(node_ids.len())?;
        let path = self.path.clone();
        let node_ids = node_ids
            .iter()
            .map(|node| node.as_str().to_owned())
            .collect::<Vec<_>>();
        let mut connection = self.database.acquire().await?;
        connection
            .transaction::<_, TransactionError, _>(async |connection| {
                let Some(state) = load_state(connection).await? else {
                    return Ok(None);
                };
                let placements = load_placements_by_node_ids(connection, kind, &node_ids).await?;
                Ok(Some(GraphRead {
                    state,
                    value: placements,
                }))
            })
            .await
            .map_err(|error| error.into_store_error(path))
    }

    pub async fn incident_edge_routes_page(
        &self,
        kind: LayoutKind,
        node_ids: &[NodeId],
        cursor: Option<&PageCursor<EdgeId>>,
        limit: NonZeroUsize,
    ) -> Result<Option<GraphRead<GraphPage<RoutedEdge, EdgeId>>>> {
        validate_read_batch_size(node_ids.len())?;
        validate_read_batch_size(limit.get())?;
        let path = self.path.clone();
        let scope = CursorScope::Incident(
            node_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        );
        let cursor = cursor.cloned();
        let mut connection = self.database.acquire().await?;
        connection
            .transaction::<_, TransactionError, _>(async |connection| {
                let Some(state) = load_state(connection).await? else {
                    return Ok(None);
                };
                validate_cursor(cursor.as_ref(), state.revision, kind, &scope)?;
                let page = load_routes_page(connection, kind, cursor, limit, state.revision, scope)
                    .await?;
                Ok(Some(GraphRead { state, value: page }))
            })
            .await
            .map_err(|error| error.into_store_error(path))
    }

    pub async fn node_placements_page(
        &self,
        kind: LayoutKind,
        cursor: Option<&PageCursor<NodeId>>,
        limit: NonZeroUsize,
    ) -> Result<Option<GraphRead<GraphPage<NodePlacement, NodeId>>>> {
        validate_read_batch_size(limit.get())?;
        let path = self.path.clone();
        let cursor = cursor.cloned();
        let mut connection = self.database.acquire().await?;
        connection
            .transaction::<_, TransactionError, _>(async |connection| {
                let Some(state) = load_state(connection).await? else {
                    return Ok(None);
                };
                validate_cursor(cursor.as_ref(), state.revision, kind, &CursorScope::All)?;
                let page =
                    load_placements_page(connection, kind, cursor, limit, state.revision).await?;
                Ok(Some(GraphRead { state, value: page }))
            })
            .await
            .map_err(|error| error.into_store_error(path))
    }

    pub async fn edge_routes_page(
        &self,
        kind: LayoutKind,
        cursor: Option<&PageCursor<EdgeId>>,
        limit: NonZeroUsize,
    ) -> Result<Option<GraphRead<GraphPage<RoutedEdge, EdgeId>>>> {
        validate_read_batch_size(limit.get())?;
        let path = self.path.clone();
        let cursor = cursor.cloned();
        let mut connection = self.database.acquire().await?;
        connection
            .transaction::<_, TransactionError, _>(async |connection| {
                let Some(state) = load_state(connection).await? else {
                    return Ok(None);
                };
                validate_cursor(cursor.as_ref(), state.revision, kind, &CursorScope::All)?;
                let page = load_routes_page(
                    connection,
                    kind,
                    cursor,
                    limit,
                    state.revision,
                    CursorScope::All,
                )
                .await?;
                Ok(Some(GraphRead { state, value: page }))
            })
            .await
            .map_err(|error| error.into_store_error(path))
    }

    pub async fn viewport_page(
        &self,
        kind: LayoutKind,
        viewport: Viewport,
        cursor: Option<&ViewportCursor>,
        limit_per_kind: NonZeroUsize,
    ) -> Result<Option<GraphRead<ViewportPage>>> {
        validate_read_batch_size(limit_per_kind.get())?;
        viewport.validate()?;
        let path = self.path.clone();
        let cursor = cursor.cloned();
        let mut connection = self.database.acquire().await?;
        connection
            .transaction::<_, TransactionError, _>(async |connection| {
                let Some(state) = load_state(connection).await? else {
                    return Ok(None);
                };
                let page = spatial::load_viewport_page(
                    connection,
                    kind,
                    viewport,
                    cursor,
                    limit_per_kind,
                    state.revision,
                )
                .await?;
                Ok(Some(GraphRead { state, value: page }))
            })
            .await
            .map_err(|error| error.into_store_error(path))
    }

    pub async fn replace(&self, graph: &Graph) -> Result<()> {
        let snapshot = graph.snapshot();
        let path = self.path.clone();
        let mut connection = self.database.acquire().await?;
        connection
            .immediate_transaction::<_, TransactionError, _>(async |connection| {
                replace_snapshot(connection, &snapshot).await
            })
            .await
            .map_err(|error| error.into_store_error(path))
    }

    pub async fn initialize(&self, graph: &Graph) -> Result<bool> {
        let snapshot = graph.snapshot();
        let path = self.path.clone();
        let mut connection = self.database.acquire().await?;
        connection
            .immediate_transaction::<_, TransactionError, _>(async |connection| {
                if load_state(connection).await?.is_some() {
                    return Ok(false);
                }
                replace_snapshot(connection, &snapshot).await?;
                Ok(true)
            })
            .await
            .map_err(|error| error.into_store_error(path))
    }

    pub async fn apply_patch(&self, patch: Patch) -> Result<StoredGraphState> {
        let path = self.path.clone();
        let mut connection = self.database.acquire().await?;
        connection
            .immediate_transaction::<_, TransactionError, _>(async |connection| {
                let state = load_state(connection).await?.ok_or_else(|| {
                    TransactionError::Operation(Error::NotInitialized { path: path.clone() })
                })?;
                patch
                    .validate_against(state.revision, state.source_version)
                    .map_err(invalid_graph)?;
                validate_patch_candidate(connection, &patch).await?;
                apply_patch_rows(connection, &patch).await?;
                validate_new_cycles(connection, &patch).await?;
                Ok(StoredGraphState {
                    format_version: state.format_version,
                    revision: patch.revision,
                    source_version: patch.source_version,
                })
            })
            .await
            .map_err(|error| error.into_store_error(path))
    }
}

#[derive(Debug, Queryable, Selectable, Insertable)]
#[diesel(table_name = web_graph_state)]
struct StateRow {
    id: i32,
    format_version: i32,
    revision: i64,
    source_version: i64,
}

#[derive(Debug, Queryable, Selectable, Insertable)]
#[diesel(table_name = web_graph_nodes)]
struct NodeRow {
    node_id: String,
}

#[derive(Debug, Queryable, Selectable, Insertable)]
#[diesel(table_name = web_graph_edges)]
struct EdgeRow {
    edge_kind: String,
    source_id: String,
    target_id: String,
}

#[derive(Debug, Queryable, Selectable, Insertable)]
#[diesel(table_name = web_graph_layouts)]
struct LayoutRow {
    layout_kind: String,
    canvas_width: i32,
    canvas_height: i32,
}

#[derive(Debug, Queryable, Selectable, Insertable)]
#[diesel(table_name = web_graph_node_placements)]
struct PlacementRow {
    layout_kind: String,
    node_id: String,
    x: i32,
    y: i32,
}

#[derive(Debug, Queryable, Selectable, Insertable)]
#[diesel(table_name = web_graph_edge_routes)]
struct RouteRow {
    layout_kind: String,
    edge_kind: String,
    source_id: String,
    target_id: String,
    source_x: i32,
    source_y: i32,
    control_1_x: i32,
    control_1_y: i32,
    control_2_x: i32,
    control_2_y: i32,
    target_x: i32,
    target_y: i32,
}

struct SnapshotRows {
    state: StateRow,
    nodes: Vec<NodeRow>,
    edges: Vec<EdgeRow>,
    layouts: Vec<LayoutRow>,
    placements: Vec<PlacementRow>,
    routes: Vec<RouteRow>,
    spatial: spatial::SnapshotSpatialRows,
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create web graph store directory {}: {source}", path.display()))]
    CreateDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to resolve web graph store path {}: {source}", path.display()))]
    ResolvePath {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to create web graph store pool {}: {source}", path.display()))]
    CreatePool {
        path: PathBuf,
        source: diesel_async::pooled_connection::PoolError,
    },

    #[snafu(display("Failed to acquire web graph store connection {}: {source}", path.display()))]
    AcquireConnection {
        path: PathBuf,
        source: diesel_async::pooled_connection::bb8::RunError,
    },

    #[snafu(display("Web graph store {} query failed: {source}", path.display()))]
    Query {
        path: PathBuf,
        source: diesel::result::Error,
    },

    #[snafu(display("Web graph store {} migration failed: {source}", path.display()))]
    Migrate {
        path: PathBuf,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to configure web graph store {}: {message}", path.display()))]
    Configure { path: PathBuf, message: String },

    #[snafu(display("Web graph store {} is not initialized", path.display()))]
    NotInitialized { path: PathBuf },

    #[snafu(display("Web graph read batch contains {actual} items; maximum is {maximum}"))]
    ReadBatchTooLarge { actual: usize, maximum: usize },

    #[snafu(display(
        "Web graph page cursor revision {cursor} does not match current revision {current}"
    ))]
    StaleCursor { cursor: Revision, current: Revision },

    #[snafu(display("Web graph page cursor belongs to a different query"))]
    CursorQueryMismatch,

    #[snafu(display("Invalid web graph viewport {width}x{height} with overscan {overscan}"))]
    InvalidViewport {
        width: i32,
        height: i32,
        overscan: i32,
    },

    #[snafu(display("Stored web graph is invalid: {source}"))]
    InvalidGraph { source: crate::web_graph::Error },

    #[snafu(display("Invalid web graph store value {column}: {value}"))]
    InvalidValue { column: &'static str, value: String },

    #[snafu(display("Web graph store value {column} exceeds SQLite INTEGER: {value}"))]
    IntegerOutOfRange { column: &'static str, value: u64 },

    #[snafu(display(
        "Web graph store operation {operation} changed {actual} rows instead of {expected}"
    ))]
    UnexpectedRowCount {
        operation: &'static str,
        expected: usize,
        actual: usize,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
enum TransactionError {
    Database(diesel::result::Error),
    Operation(Error),
}

impl From<diesel::result::Error> for TransactionError {
    fn from(source: diesel::result::Error) -> Self {
        Self::Database(source)
    }
}

impl TransactionError {
    fn into_store_error(self, path: PathBuf) -> Error {
        match self {
            Self::Database(source) => Error::Query { path, source },
            Self::Operation(error) => error,
        }
    }
}

impl From<Error> for TransactionError {
    fn from(error: Error) -> Self {
        Self::Operation(error)
    }
}

async fn load_state(
    connection: &mut AsyncSqliteConnection,
) -> std::result::Result<Option<StoredGraphState>, TransactionError> {
    use diesel_async::RunQueryDsl;

    web_graph_state::table
        .filter(web_graph_state::id.eq(1))
        .select(StateRow::as_select())
        .first::<StateRow>(connection)
        .await
        .optional()?
        .map(stored_state)
        .transpose()
}

fn stored_state(row: StateRow) -> std::result::Result<StoredGraphState, TransactionError> {
    let format_version = stored_u32(row.format_version, "web_graph_state.format_version")?;
    if format_version != crate::web_graph::FORMAT_VERSION {
        return Err(invalid_graph(crate::web_graph::Error::UnsupportedFormat {
            actual: format_version,
        }));
    }
    Ok(StoredGraphState {
        format_version,
        revision: Revision::new(stored_u64(row.revision, "web_graph_state.revision")?),
        source_version: SourceVersion::new(stored_u64(
            row.source_version,
            "web_graph_state.source_version",
        )?),
    })
}

async fn load_canvas(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
) -> std::result::Result<Canvas, TransactionError> {
    use diesel_async::RunQueryDsl;

    let row = web_graph_layouts::table
        .filter(web_graph_layouts::layout_kind.eq(layout_kind_value(kind)))
        .select(LayoutRow::as_select())
        .first::<LayoutRow>(connection)
        .await
        .optional()?
        .ok_or_else(|| {
            invalid_value(
                "web_graph_layouts.layout_kind",
                format!("missing {} layout", layout_kind_value(kind)),
            )
        })?;
    Ok(Canvas {
        width: row.canvas_width,
        height: row.canvas_height,
    })
}

async fn load_placements_by_node_ids(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    node_ids: &[String],
) -> std::result::Result<Vec<NodePlacement>, TransactionError> {
    use diesel_async::RunQueryDsl;

    if node_ids.is_empty() {
        return Ok(Vec::new());
    }
    web_graph_node_placements::table
        .filter(web_graph_node_placements::layout_kind.eq(layout_kind_value(kind)))
        .filter(web_graph_node_placements::node_id.eq_any(node_ids))
        .order(web_graph_node_placements::node_id.asc())
        .select(PlacementRow::as_select())
        .load::<PlacementRow>(connection)
        .await?
        .into_iter()
        .map(stored_placement)
        .collect()
}

async fn load_placements_page(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    cursor: Option<PageCursor<NodeId>>,
    limit: NonZeroUsize,
    revision: Revision,
) -> std::result::Result<GraphPage<NodePlacement, NodeId>, TransactionError> {
    use diesel_async::RunQueryDsl;

    let after = cursor.map(|cursor| cursor.after.as_str().to_owned());
    let mut query = web_graph_node_placements::table
        .filter(web_graph_node_placements::layout_kind.eq(layout_kind_value(kind)))
        .into_boxed();
    if let Some(after) = &after {
        query = query.filter(web_graph_node_placements::node_id.gt(after));
    }
    let mut items = query
        .order(web_graph_node_placements::node_id.asc())
        .limit(i64::try_from(limit.get() + 1).expect("bounded read limit fits in SQLite INTEGER"))
        .select(PlacementRow::as_select())
        .load::<PlacementRow>(connection)
        .await?
        .into_iter()
        .map(stored_placement)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let has_more = items.len() > limit.get();
    items.truncate(limit.get());
    let next_cursor = has_more.then(|| PageCursor {
        revision,
        layout: kind,
        scope: CursorScope::All,
        after: items
            .last()
            .expect("a non-empty page has a final node")
            .node
            .clone(),
    });
    Ok(GraphPage { items, next_cursor })
}

async fn load_routes_page(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    cursor: Option<PageCursor<EdgeId>>,
    limit: NonZeroUsize,
    revision: Revision,
    scope: CursorScope,
) -> std::result::Result<GraphPage<RoutedEdge, EdgeId>, TransactionError> {
    use diesel_async::RunQueryDsl;

    let node_ids = match &scope {
        CursorScope::All => None,
        CursorScope::Incident(nodes) => Some(
            nodes
                .iter()
                .map(|node| node.as_str().to_owned())
                .collect::<Vec<_>>(),
        ),
    };
    if node_ids.as_ref().is_some_and(Vec::is_empty) {
        return Ok(GraphPage {
            items: Vec::new(),
            next_cursor: None,
        });
    }
    let after = cursor.map(|cursor| {
        (
            edge_kind_value(cursor.after.kind).to_owned(),
            cursor.after.source.as_str().to_owned(),
            cursor.after.target.as_str().to_owned(),
        )
    });
    let mut query = web_graph_edge_routes::table
        .filter(web_graph_edge_routes::layout_kind.eq(layout_kind_value(kind)))
        .into_boxed();
    if let Some(node_ids) = &node_ids {
        query = query.filter(
            web_graph_edge_routes::source_id
                .eq_any(node_ids)
                .or(web_graph_edge_routes::target_id.eq_any(node_ids)),
        );
    }
    if let Some((edge_kind, source, target)) = &after {
        query = query.filter(
            web_graph_edge_routes::edge_kind
                .gt(edge_kind)
                .or(web_graph_edge_routes::edge_kind
                    .eq(edge_kind)
                    .and(web_graph_edge_routes::source_id.gt(source)))
                .or(web_graph_edge_routes::edge_kind
                    .eq(edge_kind)
                    .and(web_graph_edge_routes::source_id.eq(source))
                    .and(web_graph_edge_routes::target_id.gt(target))),
        );
    }
    let mut items = query
        .order((
            web_graph_edge_routes::edge_kind.asc(),
            web_graph_edge_routes::source_id.asc(),
            web_graph_edge_routes::target_id.asc(),
        ))
        .limit(i64::try_from(limit.get() + 1).expect("bounded read limit fits in SQLite INTEGER"))
        .select(RouteRow::as_select())
        .load::<RouteRow>(connection)
        .await?
        .into_iter()
        .map(stored_route)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let has_more = items.len() > limit.get();
    items.truncate(limit.get());
    let next_cursor = has_more.then(|| PageCursor {
        revision,
        layout: kind,
        scope,
        after: items
            .last()
            .expect("a non-empty page has a final edge")
            .edge
            .clone(),
    });
    Ok(GraphPage { items, next_cursor })
}

fn stored_placement(row: PlacementRow) -> std::result::Result<NodePlacement, TransactionError> {
    Ok(NodePlacement {
        node: stored_node_id(row.node_id)?,
        point: Point { x: row.x, y: row.y },
    })
}

fn validate_read_batch_size(actual: usize) -> Result<()> {
    if actual <= MAX_READ_BATCH_SIZE {
        Ok(())
    } else {
        Err(Error::ReadBatchTooLarge {
            actual,
            maximum: MAX_READ_BATCH_SIZE,
        })
    }
}

fn validate_cursor<T>(
    cursor: Option<&PageCursor<T>>,
    current: Revision,
    kind: LayoutKind,
    scope: &CursorScope,
) -> std::result::Result<(), TransactionError> {
    if let Some(cursor) = cursor {
        if cursor.revision != current {
            return Err(TransactionError::Operation(Error::StaleCursor {
                cursor: cursor.revision,
                current,
            }));
        }
        if cursor.layout != kind || &cursor.scope != scope {
            return Err(TransactionError::Operation(Error::CursorQueryMismatch));
        }
    }
    Ok(())
}

fn stored_node_id(value: String) -> std::result::Result<NodeId, TransactionError> {
    NodeId::new(value).map_err(|source| TransactionError::Operation(Error::InvalidGraph { source }))
}

fn stored_edge(row: EdgeRow) -> std::result::Result<EdgeId, TransactionError> {
    Ok(EdgeId::new(
        stored_edge_kind(&row.edge_kind)?,
        stored_node_id(row.source_id)?,
        stored_node_id(row.target_id)?,
    ))
}

fn stored_route(row: RouteRow) -> std::result::Result<RoutedEdge, TransactionError> {
    Ok(RoutedEdge {
        edge: EdgeId::new(
            stored_edge_kind(&row.edge_kind)?,
            stored_node_id(row.source_id)?,
            stored_node_id(row.target_id)?,
        ),
        route: BezierRoute {
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
        },
    })
}

fn stored_edge_kind(value: &str) -> std::result::Result<EdgeKind, TransactionError> {
    match value {
        "primary_parent" => Ok(EdgeKind::Primary),
        "merge_parent" => Ok(EdgeKind::Merge),
        "shadow_parent" => Ok(EdgeKind::Shadow),
        _ => Err(invalid_value("edge_kind", value)),
    }
}

fn stored_u32(value: i32, column: &'static str) -> std::result::Result<u32, TransactionError> {
    u32::try_from(value).map_err(|_| invalid_value(column, value))
}

fn stored_u64(value: i64, column: &'static str) -> std::result::Result<u64, TransactionError> {
    u64::try_from(value).map_err(|_| invalid_value(column, value))
}

fn invalid_value(column: &'static str, value: impl ToString) -> TransactionError {
    TransactionError::Operation(Error::InvalidValue {
        column,
        value: value.to_string(),
    })
}

async fn replace_snapshot(
    connection: &mut AsyncSqliteConnection,
    snapshot: &Snapshot,
) -> std::result::Result<(), TransactionError> {
    use diesel_async::RunQueryDsl;

    let rows = snapshot_rows(snapshot)?;
    spatial::clear(connection).await?;
    diesel::delete(web_graph_edge_routes::table)
        .execute(connection)
        .await?;
    diesel::delete(web_graph_node_placements::table)
        .execute(connection)
        .await?;
    diesel::delete(web_graph_layouts::table)
        .execute(connection)
        .await?;
    diesel::delete(web_graph_edges::table)
        .execute(connection)
        .await?;
    diesel::delete(web_graph_nodes::table)
        .execute(connection)
        .await?;
    diesel::delete(web_graph_state::table)
        .execute(connection)
        .await?;

    connection
        .spawn_blocking(move |connection| {
            for chunk in rows.nodes.chunks(WRITE_BATCH_SIZE) {
                diesel::RunQueryDsl::execute(
                    diesel::insert_into(web_graph_nodes::table).values(chunk),
                    connection,
                )?;
            }
            for chunk in rows.edges.chunks(WRITE_BATCH_SIZE) {
                diesel::RunQueryDsl::execute(
                    diesel::insert_into(web_graph_edges::table).values(chunk),
                    connection,
                )?;
            }
            for chunk in rows.layouts.chunks(WRITE_BATCH_SIZE) {
                diesel::RunQueryDsl::execute(
                    diesel::insert_into(web_graph_layouts::table).values(chunk),
                    connection,
                )?;
            }
            for chunk in rows.placements.chunks(WRITE_BATCH_SIZE) {
                diesel::RunQueryDsl::execute(
                    diesel::insert_into(web_graph_node_placements::table).values(chunk),
                    connection,
                )?;
            }
            for chunk in rows.routes.chunks(WRITE_BATCH_SIZE) {
                diesel::RunQueryDsl::execute(
                    diesel::insert_into(web_graph_edge_routes::table).values(chunk),
                    connection,
                )?;
            }
            spatial::insert_snapshot(connection, &rows.spatial)?;
            diesel::RunQueryDsl::execute(
                diesel::insert_into(web_graph_state::table).values(&rows.state),
                connection,
            )?;
            Ok(())
        })
        .await?;
    Ok(())
}

async fn validate_patch_candidate(
    connection: &mut AsyncSqliteConnection,
    patch: &Patch,
) -> std::result::Result<(), TransactionError> {
    validate_patch_item_existence(connection, patch).await?;
    validate_explicit_cleanup(connection, patch).await?;
    validate_final_references(connection, patch).await
}

async fn validate_patch_item_existence(
    connection: &mut AsyncSqliteConnection,
    patch: &Patch,
) -> std::result::Result<(), TransactionError> {
    for node in &patch.topology.remove_nodes {
        require_patch_item(
            topology_node_exists(connection, node).await?,
            false,
            "topology.remove_nodes",
            node,
        )?;
    }
    for node in &patch.topology.add_nodes {
        require_patch_item(
            topology_node_exists(connection, node).await?,
            true,
            "topology.add_nodes",
            node,
        )?;
    }
    for edge in &patch.topology.remove_edges {
        require_patch_item(
            topology_edge_exists(connection, edge).await?,
            false,
            "topology.remove_edges",
            edge,
        )?;
    }
    for edge in &patch.topology.add_edges {
        require_patch_item(
            topology_edge_exists(connection, edge).await?,
            true,
            "topology.add_edges",
            edge,
        )?;
    }
    for kind in [LayoutKind::Anchors, LayoutKind::All] {
        let layout_patch = patch_layout(patch, kind);
        for node in &layout_patch.remove_nodes {
            require_patch_item(
                layout_node_exists(connection, kind, node).await?,
                false,
                layout_patch_collection(kind, "remove_nodes"),
                node,
            )?;
        }
        for edge in &layout_patch.remove_edges {
            require_patch_item(
                layout_edge_exists(connection, kind, edge).await?,
                false,
                layout_patch_collection(kind, "remove_edges"),
                edge,
            )?;
        }
    }
    Ok(())
}

async fn validate_explicit_cleanup(
    connection: &mut AsyncSqliteConnection,
    patch: &Patch,
) -> std::result::Result<(), TransactionError> {
    for node in &patch.topology.remove_nodes {
        for edge in load_incident_topology_edges(connection, node).await? {
            require_explicit_removal(
                patch.topology.remove_edges.contains(&edge),
                "topology.remove_edges",
                &edge,
            )?;
        }
        for kind in [LayoutKind::Anchors, LayoutKind::All] {
            if layout_node_exists(connection, kind, node).await? {
                require_explicit_removal(
                    patch_layout(patch, kind).remove_nodes.contains(node),
                    layout_patch_collection(kind, "remove_nodes"),
                    node,
                )?;
            }
        }
    }
    for edge in &patch.topology.remove_edges {
        for kind in [LayoutKind::Anchors, LayoutKind::All] {
            if layout_edge_exists(connection, kind, edge).await? {
                require_explicit_removal(
                    patch_layout(patch, kind).remove_edges.contains(edge),
                    layout_patch_collection(kind, "remove_edges"),
                    edge,
                )?;
            }
        }
    }
    for kind in [LayoutKind::Anchors, LayoutKind::All] {
        let layout_patch = patch_layout(patch, kind);
        for node in &layout_patch.remove_nodes {
            for edge in load_incident_layout_edges(connection, kind, node).await? {
                require_explicit_removal(
                    layout_patch.remove_edges.contains(&edge),
                    layout_patch_collection(kind, "remove_edges"),
                    &edge,
                )?;
            }
        }
    }
    Ok(())
}

async fn validate_final_references(
    connection: &mut AsyncSqliteConnection,
    patch: &Patch,
) -> std::result::Result<(), TransactionError> {
    let affected_nodes = affected_nodes(patch);
    let affected_edges = affected_edges(patch);

    for edge in &affected_edges {
        if final_topology_edge_exists(connection, patch, edge).await? {
            for node in [&edge.source, &edge.target] {
                if !final_topology_node_exists(connection, patch, node).await? {
                    return Err(invalid_graph(
                        crate::web_graph::Error::MissingEdgeEndpoint {
                            edge: edge.clone(),
                            node: node.clone(),
                        },
                    ));
                }
            }
        }
    }
    for kind in [LayoutKind::Anchors, LayoutKind::All] {
        for node in &affected_nodes {
            if final_layout_node_exists(connection, patch, kind, node).await?
                && !final_topology_node_exists(connection, patch, node).await?
            {
                return Err(invalid_graph(
                    crate::web_graph::Error::LayoutNodeMissingFromTopology {
                        layout: kind,
                        node: node.clone(),
                    },
                ));
            }
        }
        for edge in &affected_edges {
            if !final_layout_edge_exists(connection, patch, kind, edge).await? {
                continue;
            }
            if !final_topology_edge_exists(connection, patch, edge).await? {
                return Err(invalid_graph(
                    crate::web_graph::Error::LayoutEdgeMissingFromTopology {
                        layout: kind,
                        edge: edge.clone(),
                    },
                ));
            }
            for node in [&edge.source, &edge.target] {
                if !final_layout_node_exists(connection, patch, kind, node).await? {
                    return Err(invalid_graph(
                        crate::web_graph::Error::LayoutEdgeEndpointMissing {
                            layout: kind,
                            edge: edge.clone(),
                            node: node.clone(),
                        },
                    ));
                }
            }
        }
    }
    for node in &affected_nodes {
        let all = final_layout_node_exists(connection, patch, LayoutKind::All, node).await?;
        if final_layout_node_exists(connection, patch, LayoutKind::Anchors, node).await? && !all {
            return Err(invalid_graph(
                crate::web_graph::Error::AnchorNodeMissingFromAll { node: node.clone() },
            ));
        }
        if final_topology_node_exists(connection, patch, node).await? && !all {
            return Err(invalid_graph(
                crate::web_graph::Error::TopologyNodeMissingFromAll { node: node.clone() },
            ));
        }
    }
    for edge in &affected_edges {
        if final_topology_edge_exists(connection, patch, edge).await?
            && !final_layout_edge_exists(connection, patch, LayoutKind::Anchors, edge).await?
            && !final_layout_edge_exists(connection, patch, LayoutKind::All, edge).await?
        {
            return Err(invalid_graph(crate::web_graph::Error::UnusedTopologyEdge {
                edge: edge.clone(),
            }));
        }
    }
    Ok(())
}

async fn validate_new_cycles(
    connection: &mut AsyncSqliteConnection,
    patch: &Patch,
) -> std::result::Result<(), TransactionError> {
    for kind in [LayoutKind::Anchors, LayoutKind::All] {
        for routed in &patch_layout(patch, kind).upsert_edges {
            if layout_path_exists(connection, kind, &routed.edge.target, &routed.edge.source)
                .await?
            {
                return Err(invalid_graph(crate::web_graph::Error::Cycle {
                    layout: kind,
                    node: routed.edge.source.clone(),
                }));
            }
        }
    }
    Ok(())
}

async fn layout_path_exists(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    start: &NodeId,
    target: &NodeId,
) -> std::result::Result<bool, TransactionError> {
    use diesel_async::RunQueryDsl;

    let mut visited = BTreeSet::from([start.clone()]);
    let mut pending = VecDeque::from([start.clone()]);
    while !pending.is_empty() {
        let mut sources = Vec::with_capacity(MAX_READ_BATCH_SIZE);
        while sources.len() < MAX_READ_BATCH_SIZE {
            let Some(source) = pending.pop_front() else {
                break;
            };
            sources.push(source.as_str().to_owned());
        }
        let targets = web_graph_edge_routes::table
            .filter(web_graph_edge_routes::layout_kind.eq(layout_kind_value(kind)))
            .filter(web_graph_edge_routes::source_id.eq_any(&sources))
            .select(web_graph_edge_routes::target_id)
            .load::<String>(connection)
            .await?;
        for target_id in targets {
            let node = stored_node_id(target_id)?;
            if &node == target {
                return Ok(true);
            }
            if visited.insert(node.clone()) {
                pending.push_back(node);
            }
        }
    }
    Ok(false)
}

fn affected_nodes(patch: &Patch) -> BTreeSet<NodeId> {
    let mut nodes = BTreeSet::new();
    nodes.extend(patch.topology.add_nodes.iter().cloned());
    nodes.extend(patch.topology.remove_nodes.iter().cloned());
    for kind in [LayoutKind::Anchors, LayoutKind::All] {
        let layout = patch_layout(patch, kind);
        nodes.extend(
            layout
                .upsert_nodes
                .iter()
                .map(|placement| placement.node.clone()),
        );
        nodes.extend(layout.remove_nodes.iter().cloned());
    }
    nodes
}

fn affected_edges(patch: &Patch) -> BTreeSet<EdgeId> {
    let mut edges = BTreeSet::new();
    edges.extend(patch.topology.add_edges.iter().cloned());
    edges.extend(patch.topology.remove_edges.iter().cloned());
    for kind in [LayoutKind::Anchors, LayoutKind::All] {
        let layout = patch_layout(patch, kind);
        edges.extend(layout.upsert_edges.iter().map(|routed| routed.edge.clone()));
        edges.extend(layout.remove_edges.iter().cloned());
    }
    edges
}

async fn topology_node_exists(
    connection: &mut AsyncSqliteConnection,
    node: &NodeId,
) -> std::result::Result<bool, TransactionError> {
    use diesel_async::RunQueryDsl;

    Ok(web_graph_nodes::table
        .filter(web_graph_nodes::node_id.eq(node.as_str()))
        .select(web_graph_nodes::node_id)
        .first::<String>(connection)
        .await
        .optional()?
        .is_some())
}

async fn topology_edge_exists(
    connection: &mut AsyncSqliteConnection,
    edge: &EdgeId,
) -> std::result::Result<bool, TransactionError> {
    use diesel_async::RunQueryDsl;

    Ok(web_graph_edges::table
        .filter(web_graph_edges::edge_kind.eq(edge_kind_value(edge.kind)))
        .filter(web_graph_edges::source_id.eq(edge.source.as_str()))
        .filter(web_graph_edges::target_id.eq(edge.target.as_str()))
        .select(web_graph_edges::target_id)
        .first::<String>(connection)
        .await
        .optional()?
        .is_some())
}

async fn layout_node_exists(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    node: &NodeId,
) -> std::result::Result<bool, TransactionError> {
    use diesel_async::RunQueryDsl;

    Ok(web_graph_node_placements::table
        .filter(web_graph_node_placements::layout_kind.eq(layout_kind_value(kind)))
        .filter(web_graph_node_placements::node_id.eq(node.as_str()))
        .select(web_graph_node_placements::node_id)
        .first::<String>(connection)
        .await
        .optional()?
        .is_some())
}

async fn layout_edge_exists(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    edge: &EdgeId,
) -> std::result::Result<bool, TransactionError> {
    use diesel_async::RunQueryDsl;

    Ok(web_graph_edge_routes::table
        .filter(web_graph_edge_routes::layout_kind.eq(layout_kind_value(kind)))
        .filter(web_graph_edge_routes::edge_kind.eq(edge_kind_value(edge.kind)))
        .filter(web_graph_edge_routes::source_id.eq(edge.source.as_str()))
        .filter(web_graph_edge_routes::target_id.eq(edge.target.as_str()))
        .select(web_graph_edge_routes::target_id)
        .first::<String>(connection)
        .await
        .optional()?
        .is_some())
}

async fn load_incident_topology_edges(
    connection: &mut AsyncSqliteConnection,
    node: &NodeId,
) -> std::result::Result<Vec<EdgeId>, TransactionError> {
    use diesel_async::RunQueryDsl;

    web_graph_edges::table
        .filter(
            web_graph_edges::source_id
                .eq(node.as_str())
                .or(web_graph_edges::target_id.eq(node.as_str())),
        )
        .select(EdgeRow::as_select())
        .load::<EdgeRow>(connection)
        .await?
        .into_iter()
        .map(stored_edge)
        .collect()
}

async fn load_incident_layout_edges(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    node: &NodeId,
) -> std::result::Result<Vec<EdgeId>, TransactionError> {
    use diesel_async::RunQueryDsl;

    web_graph_edge_routes::table
        .filter(web_graph_edge_routes::layout_kind.eq(layout_kind_value(kind)))
        .filter(
            web_graph_edge_routes::source_id
                .eq(node.as_str())
                .or(web_graph_edge_routes::target_id.eq(node.as_str())),
        )
        .select((
            web_graph_edge_routes::edge_kind,
            web_graph_edge_routes::source_id,
            web_graph_edge_routes::target_id,
        ))
        .load::<(String, String, String)>(connection)
        .await?
        .into_iter()
        .map(|(edge_kind, source_id, target_id)| {
            stored_edge(EdgeRow {
                edge_kind,
                source_id,
                target_id,
            })
        })
        .collect()
}

async fn final_topology_node_exists(
    connection: &mut AsyncSqliteConnection,
    patch: &Patch,
    node: &NodeId,
) -> std::result::Result<bool, TransactionError> {
    if patch.topology.remove_nodes.contains(node) {
        Ok(false)
    } else if patch.topology.add_nodes.contains(node) {
        Ok(true)
    } else {
        topology_node_exists(connection, node).await
    }
}

async fn final_topology_edge_exists(
    connection: &mut AsyncSqliteConnection,
    patch: &Patch,
    edge: &EdgeId,
) -> std::result::Result<bool, TransactionError> {
    if patch.topology.remove_edges.contains(edge) {
        Ok(false)
    } else if patch.topology.add_edges.contains(edge) {
        Ok(true)
    } else {
        topology_edge_exists(connection, edge).await
    }
}

async fn final_layout_node_exists(
    connection: &mut AsyncSqliteConnection,
    patch: &Patch,
    kind: LayoutKind,
    node: &NodeId,
) -> std::result::Result<bool, TransactionError> {
    let layout = patch_layout(patch, kind);
    if layout.remove_nodes.contains(node) {
        Ok(false)
    } else if layout
        .upsert_nodes
        .iter()
        .any(|placement| &placement.node == node)
    {
        Ok(true)
    } else {
        layout_node_exists(connection, kind, node).await
    }
}

async fn final_layout_edge_exists(
    connection: &mut AsyncSqliteConnection,
    patch: &Patch,
    kind: LayoutKind,
    edge: &EdgeId,
) -> std::result::Result<bool, TransactionError> {
    let layout = patch_layout(patch, kind);
    if layout.remove_edges.contains(edge) {
        Ok(false)
    } else if layout
        .upsert_edges
        .iter()
        .any(|routed| &routed.edge == edge)
    {
        Ok(true)
    } else {
        layout_edge_exists(connection, kind, edge).await
    }
}

fn require_patch_item(
    exists: bool,
    adding: bool,
    collection: &'static str,
    key: &impl std::fmt::Display,
) -> std::result::Result<(), TransactionError> {
    match (adding, exists) {
        (true, true) => Err(invalid_graph(crate::web_graph::Error::ExistingPatchItem {
            collection,
            key: key.to_string(),
        })),
        (false, false) => Err(invalid_graph(crate::web_graph::Error::MissingPatchItem {
            collection,
            key: key.to_string(),
        })),
        _ => Ok(()),
    }
}

fn require_explicit_removal(
    removed: bool,
    collection: &'static str,
    key: &impl std::fmt::Display,
) -> std::result::Result<(), TransactionError> {
    if removed {
        Ok(())
    } else {
        Err(invalid_graph(crate::web_graph::Error::MissingPatchItem {
            collection,
            key: key.to_string(),
        }))
    }
}

fn invalid_graph(source: crate::web_graph::Error) -> TransactionError {
    TransactionError::Operation(Error::InvalidGraph { source })
}

fn patch_layout(patch: &Patch, kind: LayoutKind) -> &LayoutPatch {
    match kind {
        LayoutKind::Anchors => &patch.layouts.anchors,
        LayoutKind::All => &patch.layouts.all,
    }
}

fn layout_patch_collection(kind: LayoutKind, operation: &'static str) -> &'static str {
    match (kind, operation) {
        (LayoutKind::Anchors, "remove_nodes") => "layouts.anchors.remove_nodes",
        (LayoutKind::Anchors, "remove_edges") => "layouts.anchors.remove_edges",
        (LayoutKind::All, "remove_nodes") => "layouts.all.remove_nodes",
        (LayoutKind::All, "remove_edges") => "layouts.all.remove_edges",
        _ => "layouts",
    }
}

async fn apply_patch_rows(
    connection: &mut AsyncSqliteConnection,
    patch: &Patch,
) -> std::result::Result<(), TransactionError> {
    use diesel_async::RunQueryDsl;

    let base_revision = sqlite_integer(patch.base_revision.get(), "base_revision")?;
    let revision = sqlite_integer(patch.revision.get(), "revision")?;
    let source_version = sqlite_integer(patch.source_version.get(), "source_version")?;

    remove_layout_rows(connection, LayoutKind::Anchors, &patch.layouts.anchors).await?;
    remove_layout_rows(connection, LayoutKind::All, &patch.layouts.all).await?;
    remove_topology_rows(connection, &patch.topology).await?;
    add_topology_rows(connection, &patch.topology).await?;
    upsert_layout_rows(connection, LayoutKind::Anchors, &patch.layouts.anchors).await?;
    upsert_layout_rows(connection, LayoutKind::All, &patch.layouts.all).await?;

    let changed = diesel::update(
        web_graph_state::table
            .filter(web_graph_state::id.eq(1))
            .filter(web_graph_state::revision.eq(base_revision)),
    )
    .set((
        web_graph_state::revision.eq(revision),
        web_graph_state::source_version.eq(source_version),
    ))
    .execute(connection)
    .await?;
    expect_row_count("advance_revision", 1, changed)
}

async fn remove_layout_rows(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    patch: &LayoutPatch,
) -> std::result::Result<(), TransactionError> {
    use diesel_async::RunQueryDsl;

    let layout = layout_kind_value(kind);
    for edge in &patch.remove_edges {
        spatial::remove_route(connection, kind, edge).await?;
        let changed = diesel::delete(
            web_graph_edge_routes::table
                .filter(web_graph_edge_routes::layout_kind.eq(layout))
                .filter(web_graph_edge_routes::edge_kind.eq(edge_kind_value(edge.kind)))
                .filter(web_graph_edge_routes::source_id.eq(edge.source.as_str()))
                .filter(web_graph_edge_routes::target_id.eq(edge.target.as_str())),
        )
        .execute(connection)
        .await?;
        expect_row_count("remove_layout_edge", 1, changed)?;
    }
    for node in &patch.remove_nodes {
        spatial::remove_node(connection, kind, node).await?;
        let changed = diesel::delete(
            web_graph_node_placements::table
                .filter(web_graph_node_placements::layout_kind.eq(layout))
                .filter(web_graph_node_placements::node_id.eq(node.as_str())),
        )
        .execute(connection)
        .await?;
        expect_row_count("remove_layout_node", 1, changed)?;
    }
    Ok(())
}

async fn remove_topology_rows(
    connection: &mut AsyncSqliteConnection,
    patch: &TopologyPatch,
) -> std::result::Result<(), TransactionError> {
    use diesel_async::RunQueryDsl;

    for edge in &patch.remove_edges {
        let changed = diesel::delete(
            web_graph_edges::table
                .filter(web_graph_edges::edge_kind.eq(edge_kind_value(edge.kind)))
                .filter(web_graph_edges::source_id.eq(edge.source.as_str()))
                .filter(web_graph_edges::target_id.eq(edge.target.as_str())),
        )
        .execute(connection)
        .await?;
        expect_row_count("remove_topology_edge", 1, changed)?;
    }
    for node in &patch.remove_nodes {
        let changed = diesel::delete(
            web_graph_nodes::table.filter(web_graph_nodes::node_id.eq(node.as_str())),
        )
        .execute(connection)
        .await?;
        expect_row_count("remove_topology_node", 1, changed)?;
    }
    Ok(())
}

async fn add_topology_rows(
    connection: &mut AsyncSqliteConnection,
    patch: &TopologyPatch,
) -> std::result::Result<(), TransactionError> {
    let nodes = patch
        .add_nodes
        .iter()
        .map(|node| NodeRow {
            node_id: node.as_str().to_owned(),
        })
        .collect::<Vec<_>>();
    let edges = patch.add_edges.iter().map(edge_row).collect::<Vec<_>>();
    connection
        .spawn_blocking(move |connection| {
            for chunk in nodes.chunks(WRITE_BATCH_SIZE) {
                diesel::RunQueryDsl::execute(
                    diesel::insert_into(web_graph_nodes::table).values(chunk),
                    connection,
                )?;
            }
            for chunk in edges.chunks(WRITE_BATCH_SIZE) {
                diesel::RunQueryDsl::execute(
                    diesel::insert_into(web_graph_edges::table).values(chunk),
                    connection,
                )?;
            }
            Ok(())
        })
        .await?;
    Ok(())
}

async fn upsert_layout_rows(
    connection: &mut AsyncSqliteConnection,
    kind: LayoutKind,
    patch: &LayoutPatch,
) -> std::result::Result<(), TransactionError> {
    use diesel::upsert::excluded;
    use diesel_async::RunQueryDsl;

    if let Some(canvas) = patch.canvas {
        let changed = diesel::update(
            web_graph_layouts::table
                .filter(web_graph_layouts::layout_kind.eq(layout_kind_value(kind))),
        )
        .set((
            web_graph_layouts::canvas_width.eq(canvas.width),
            web_graph_layouts::canvas_height.eq(canvas.height),
        ))
        .execute(connection)
        .await?;
        expect_row_count("update_layout_canvas", 1, changed)?;
    }
    for placement in &patch.upsert_nodes {
        let row = placement_row(kind, placement);
        let changed = diesel::insert_into(web_graph_node_placements::table)
            .values(&row)
            .on_conflict((
                web_graph_node_placements::layout_kind,
                web_graph_node_placements::node_id,
            ))
            .do_update()
            .set((
                web_graph_node_placements::x.eq(excluded(web_graph_node_placements::x)),
                web_graph_node_placements::y.eq(excluded(web_graph_node_placements::y)),
            ))
            .execute(connection)
            .await?;
        expect_row_count("upsert_layout_node", 1, changed)?;
        spatial::upsert_node(connection, kind, placement).await?;
    }
    for routed in &patch.upsert_edges {
        let row = route_row(kind, routed);
        let changed = diesel::insert_into(web_graph_edge_routes::table)
            .values(&row)
            .on_conflict((
                web_graph_edge_routes::layout_kind,
                web_graph_edge_routes::edge_kind,
                web_graph_edge_routes::source_id,
                web_graph_edge_routes::target_id,
            ))
            .do_update()
            .set((
                web_graph_edge_routes::source_x.eq(excluded(web_graph_edge_routes::source_x)),
                web_graph_edge_routes::source_y.eq(excluded(web_graph_edge_routes::source_y)),
                web_graph_edge_routes::control_1_x.eq(excluded(web_graph_edge_routes::control_1_x)),
                web_graph_edge_routes::control_1_y.eq(excluded(web_graph_edge_routes::control_1_y)),
                web_graph_edge_routes::control_2_x.eq(excluded(web_graph_edge_routes::control_2_x)),
                web_graph_edge_routes::control_2_y.eq(excluded(web_graph_edge_routes::control_2_y)),
                web_graph_edge_routes::target_x.eq(excluded(web_graph_edge_routes::target_x)),
                web_graph_edge_routes::target_y.eq(excluded(web_graph_edge_routes::target_y)),
            ))
            .execute(connection)
            .await?;
        expect_row_count("upsert_layout_edge", 1, changed)?;
        spatial::upsert_route(connection, kind, routed).await?;
    }
    Ok(())
}

fn expect_row_count(
    operation: &'static str,
    expected: usize,
    actual: usize,
) -> std::result::Result<(), TransactionError> {
    if actual == expected {
        Ok(())
    } else {
        Err(TransactionError::Operation(Error::UnexpectedRowCount {
            operation,
            expected,
            actual,
        }))
    }
}

fn snapshot_rows(snapshot: &Snapshot) -> std::result::Result<SnapshotRows, TransactionError> {
    let mut layouts = Vec::with_capacity(2);
    let mut placements =
        Vec::with_capacity(snapshot.layouts.anchors.nodes.len() + snapshot.layouts.all.nodes.len());
    let mut routes =
        Vec::with_capacity(snapshot.layouts.anchors.edges.len() + snapshot.layouts.all.edges.len());
    append_layout_rows(
        LayoutKind::Anchors,
        &snapshot.layouts.anchors,
        &mut layouts,
        &mut placements,
        &mut routes,
    );
    append_layout_rows(
        LayoutKind::All,
        &snapshot.layouts.all,
        &mut layouts,
        &mut placements,
        &mut routes,
    );
    let spatial = spatial::snapshot_rows(&placements, &routes)?;
    Ok(SnapshotRows {
        state: StateRow {
            id: 1,
            format_version: i32::try_from(snapshot.format_version).map_err(|_| {
                TransactionError::Operation(Error::IntegerOutOfRange {
                    column: "format_version",
                    value: u64::from(snapshot.format_version),
                })
            })?,
            revision: sqlite_integer(snapshot.revision.get(), "revision")?,
            source_version: sqlite_integer(snapshot.source_version.get(), "source_version")?,
        },
        nodes: snapshot
            .topology
            .nodes
            .iter()
            .map(|node| NodeRow {
                node_id: node.as_str().to_owned(),
            })
            .collect(),
        edges: snapshot.topology.edges.iter().map(edge_row).collect(),
        layouts,
        placements,
        routes,
        spatial,
    })
}

fn append_layout_rows(
    kind: LayoutKind,
    layout: &LayoutSnapshot,
    layouts: &mut Vec<LayoutRow>,
    placements: &mut Vec<PlacementRow>,
    routes: &mut Vec<RouteRow>,
) {
    layouts.push(LayoutRow {
        layout_kind: layout_kind_value(kind).to_owned(),
        canvas_width: layout.canvas.width,
        canvas_height: layout.canvas.height,
    });
    placements.extend(
        layout
            .nodes
            .iter()
            .map(|placement| placement_row(kind, placement)),
    );
    routes.extend(layout.edges.iter().map(|routed| route_row(kind, routed)));
}

fn edge_row(edge: &EdgeId) -> EdgeRow {
    EdgeRow {
        edge_kind: edge_kind_value(edge.kind).to_owned(),
        source_id: edge.source.as_str().to_owned(),
        target_id: edge.target.as_str().to_owned(),
    }
}

fn placement_row(kind: LayoutKind, placement: &NodePlacement) -> PlacementRow {
    PlacementRow {
        layout_kind: layout_kind_value(kind).to_owned(),
        node_id: placement.node.as_str().to_owned(),
        x: placement.point.x,
        y: placement.point.y,
    }
}

fn route_row(kind: LayoutKind, routed: &RoutedEdge) -> RouteRow {
    RouteRow {
        layout_kind: layout_kind_value(kind).to_owned(),
        edge_kind: edge_kind_value(routed.edge.kind).to_owned(),
        source_id: routed.edge.source.as_str().to_owned(),
        target_id: routed.edge.target.as_str().to_owned(),
        source_x: routed.route.source.x,
        source_y: routed.route.source.y,
        control_1_x: routed.route.control_1.x,
        control_1_y: routed.route.control_1.y,
        control_2_x: routed.route.control_2.x,
        control_2_y: routed.route.control_2.y,
        target_x: routed.route.target.x,
        target_y: routed.route.target.y,
    }
}

fn edge_kind_value(kind: EdgeKind) -> &'static str {
    match kind {
        EdgeKind::Primary => "primary_parent",
        EdgeKind::Merge => "merge_parent",
        EdgeKind::Shadow => "shadow_parent",
    }
}

fn layout_kind_value(kind: LayoutKind) -> &'static str {
    match kind {
        LayoutKind::Anchors => "anchors",
        LayoutKind::All => "all",
    }
}

fn sqlite_integer(value: u64, column: &'static str) -> std::result::Result<i64, TransactionError> {
    i64::try_from(value)
        .map_err(|_| TransactionError::Operation(Error::IntegerOutOfRange { column, value }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use diesel::sqlite::SqliteConnection;
    use diesel::{Connection, RunQueryDsl};
    use diesel_migrations::MigrationHarness;
    use tokio::sync::Barrier;

    use super::*;
    use crate::web_graph::{
        Error as GraphError, FORMAT_VERSION, LayoutPatches, LayoutSnapshots, TopologySnapshot,
    };

    static TEST_NONCE: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new() -> Self {
            let nonce = TEST_NONCE.fetch_add(1, Ordering::Relaxed);
            Self {
                path: std::env::temp_dir().join(format!(
                    "coco-web-graph-store-{}-{nonce}",
                    std::process::id()
                )),
            }
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            if self.path.exists() {
                std::fs::remove_dir_all(&self.path).unwrap();
            }
        }
    }

    fn node(value: &str) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn edge(kind: EdgeKind, source: &str, target: &str) -> EdgeId {
        EdgeId::new(kind, node(source), node(target))
    }

    fn point(x: i32, y: i32) -> Point {
        Point { x, y }
    }

    fn route(offset: i32) -> BezierRoute {
        BezierRoute {
            source: point(offset, offset + 1),
            control_1: point(offset + 2, offset + 3),
            control_2: point(offset + 4, offset + 5),
            target: point(offset + 6, offset + 7),
        }
    }

    fn placement(node_id: &str, x: i32, y: i32) -> NodePlacement {
        NodePlacement {
            node: node(node_id),
            point: point(x, y),
        }
    }

    fn routed(edge: EdgeId, offset: i32) -> RoutedEdge {
        RoutedEdge {
            edge,
            route: route(offset),
        }
    }

    fn graph() -> Graph {
        let primary_ab = edge(EdgeKind::Primary, "a", "b");
        let primary_bc = edge(EdgeKind::Primary, "b", "c");
        let shadow_ac = edge(EdgeKind::Shadow, "a", "c");
        Graph::from_snapshot(Snapshot {
            format_version: FORMAT_VERSION,
            revision: Revision::new(1),
            source_version: SourceVersion::new(10),
            topology: TopologySnapshot {
                nodes: vec![node("c"), node("a"), node("b")],
                edges: vec![shadow_ac.clone(), primary_bc.clone(), primary_ab.clone()],
            },
            layouts: LayoutSnapshots {
                anchors: LayoutSnapshot {
                    canvas: Canvas {
                        width: 480,
                        height: 240,
                    },
                    nodes: vec![placement("c", 320, 80), placement("a", 80, 80)],
                    edges: vec![routed(shadow_ac, 200)],
                },
                all: LayoutSnapshot {
                    canvas: Canvas {
                        width: 640,
                        height: 360,
                    },
                    nodes: vec![
                        placement("c", 360, 120),
                        placement("a", 80, 120),
                        placement("b", 220, 120),
                    ],
                    edges: vec![routed(primary_bc, 100), routed(primary_ab, 0)],
                },
            },
        })
        .unwrap()
    }

    fn empty_graph(revision: u64, source_version: u64) -> Graph {
        Graph::from_snapshot(Snapshot {
            format_version: FORMAT_VERSION,
            revision: Revision::new(revision),
            source_version: SourceVersion::new(source_version),
            topology: TopologySnapshot {
                nodes: Vec::new(),
                edges: Vec::new(),
            },
            layouts: LayoutSnapshots {
                anchors: empty_layout(),
                all: empty_layout(),
            },
        })
        .unwrap()
    }

    fn empty_layout() -> LayoutSnapshot {
        LayoutSnapshot {
            canvas: Canvas {
                width: 1,
                height: 1,
            },
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    fn graph_patch() -> Patch {
        let primary_ab = edge(EdgeKind::Primary, "a", "b");
        let primary_bc = edge(EdgeKind::Primary, "b", "c");
        let primary_cd = edge(EdgeKind::Primary, "c", "d");
        Patch {
            format_version: FORMAT_VERSION,
            base_revision: Revision::new(1),
            revision: Revision::new(2),
            source_version: SourceVersion::new(11),
            topology: TopologyPatch {
                add_nodes: vec![node("d")],
                remove_nodes: vec![node("b")],
                add_edges: vec![primary_cd.clone()],
                remove_edges: vec![primary_ab.clone(), primary_bc.clone()],
            },
            layouts: LayoutPatches {
                anchors: LayoutPatch::default(),
                all: LayoutPatch {
                    canvas: Some(Canvas {
                        width: 720,
                        height: 400,
                    }),
                    upsert_nodes: vec![placement("c", 340, 140), placement("d", 500, 140)],
                    remove_nodes: vec![node("b")],
                    upsert_edges: vec![routed(primary_cd, 300)],
                    remove_edges: vec![primary_ab, primary_bc],
                },
            },
        }
    }

    fn relayout_patch() -> Patch {
        let primary_bc = edge(EdgeKind::Primary, "b", "c");
        Patch {
            format_version: FORMAT_VERSION,
            base_revision: Revision::new(1),
            revision: Revision::new(2),
            source_version: SourceVersion::new(10),
            topology: TopologyPatch::default(),
            layouts: LayoutPatches {
                anchors: LayoutPatch::default(),
                all: LayoutPatch {
                    canvas: None,
                    upsert_nodes: vec![placement("c", 400, 180)],
                    remove_nodes: Vec::new(),
                    upsert_edges: vec![routed(primary_bc, 500)],
                    remove_edges: Vec::new(),
                },
            },
        }
    }

    async fn load_layout_for_test(store: &WebGraphStore, kind: LayoutKind) -> LayoutSnapshot {
        let canvas = store.canvas(kind).await.unwrap().unwrap().value;
        let limit = NonZeroUsize::new(2).unwrap();
        let mut nodes = Vec::new();
        let mut node_cursor = None;
        loop {
            let page = store
                .node_placements_page(kind, node_cursor.as_ref(), limit)
                .await
                .unwrap()
                .unwrap()
                .value;
            nodes.extend(page.items);
            let Some(cursor) = page.next_cursor else {
                break;
            };
            node_cursor = Some(cursor);
        }
        let mut edges = Vec::new();
        let mut edge_cursor = None;
        loop {
            let page = store
                .edge_routes_page(kind, edge_cursor.as_ref(), limit)
                .await
                .unwrap()
                .unwrap()
                .value;
            edges.extend(page.items);
            let Some(cursor) = page.next_cursor else {
                break;
            };
            edge_cursor = Some(cursor);
        }
        LayoutSnapshot {
            canvas,
            nodes,
            edges,
        }
    }

    async fn load_graph_for_test(store: &WebGraphStore) -> Option<Graph> {
        let state = store.state().await.unwrap()?;
        let anchors = load_layout_for_test(store, LayoutKind::Anchors).await;
        let all = load_layout_for_test(store, LayoutKind::All).await;
        let nodes = all
            .nodes
            .iter()
            .map(|placement| placement.node.clone())
            .collect();
        let edges = anchors
            .edges
            .iter()
            .chain(&all.edges)
            .map(|routed| routed.edge.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Some(
            Graph::from_snapshot(Snapshot {
                format_version: state.format_version,
                revision: state.revision,
                source_version: state.source_version,
                topology: TopologySnapshot { nodes, edges },
                layouts: LayoutSnapshots { anchors, all },
            })
            .unwrap(),
        )
    }

    fn create_v1_store(path: &Path, graph: &Graph) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut connection = SqliteConnection::establish(path.to_str().unwrap()).unwrap();
        connection.run_pending_migrations(MIGRATIONS).unwrap();
        connection.revert_last_migration(MIGRATIONS).unwrap();
        let rows = snapshot_rows(&graph.snapshot()).unwrap();
        connection
            .transaction::<_, diesel::result::Error, _>(|connection| {
                diesel::insert_into(web_graph_nodes::table)
                    .values(&rows.nodes)
                    .execute(connection)?;
                diesel::insert_into(web_graph_edges::table)
                    .values(&rows.edges)
                    .execute(connection)?;
                diesel::insert_into(web_graph_layouts::table)
                    .values(&rows.layouts)
                    .execute(connection)?;
                diesel::insert_into(web_graph_node_placements::table)
                    .values(&rows.placements)
                    .execute(connection)?;
                diesel::insert_into(web_graph_edge_routes::table)
                    .values(&rows.routes)
                    .execute(connection)?;
                diesel::insert_into(web_graph_state::table)
                    .values(&rows.state)
                    .execute(connection)?;
                Ok(())
            })
            .unwrap();
    }

    fn viewport(x: i32, y: i32, width: i32, height: i32) -> Viewport {
        Viewport {
            x,
            y,
            width,
            height,
            overscan: 0,
        }
    }

    async fn load_viewport_for_test(
        store: &WebGraphStore,
        kind: LayoutKind,
        viewport: Viewport,
        limit: usize,
    ) -> (Vec<NodePlacement>, Vec<RoutedEdge>) {
        let limit = NonZeroUsize::new(limit).unwrap();
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut cursor = None;
        loop {
            let page = store
                .viewport_page(kind, viewport, cursor.as_ref(), limit)
                .await
                .unwrap()
                .unwrap()
                .value;
            nodes.extend(page.nodes);
            edges.extend(page.edges);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
        }
        (nodes, edges)
    }

    #[tokio::test]
    async fn new_store_is_empty() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();

        assert_eq!(store.path(), directory.path.join(DATABASE_FILE_NAME));
        assert_eq!(store.state().await.unwrap(), None);
    }

    #[tokio::test]
    async fn reopened_stores_share_the_database_pool() {
        let directory = TestDirectory::new();
        let first = WebGraphStore::open(&directory.path).await.unwrap();
        let second = WebGraphStore::open(directory.path.join(".")).await.unwrap();

        assert!(std::ptr::eq(
            first.database.shared_pool(),
            second.database.shared_pool()
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_first_opens_share_one_initialized_database_pool() {
        let directory = TestDirectory::new();
        let barrier = Arc::new(Barrier::new(8));
        let handles = (0..8)
            .map(|_| {
                let barrier = barrier.clone();
                let path = directory.path.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    WebGraphStore::open(path).await.unwrap()
                })
            })
            .collect::<Vec<_>>();
        let mut stores = Vec::with_capacity(handles.len());
        for handle in handles {
            stores.push(handle.await.unwrap());
        }

        assert!(stores.iter().all(|store| std::ptr::eq(
            stores[0].database.shared_pool(),
            store.database.shared_pool()
        )));
        assert_eq!(stores[0].state().await.unwrap(), None);
    }

    #[tokio::test]
    async fn pool_keeps_a_connection_available_while_three_are_held() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();
        let expected = graph();
        store.replace(&expected).await.unwrap();
        let mut held_connections = Vec::new();
        for _ in 0..3 {
            held_connections.push(store.database.acquire().await.unwrap());
        }

        let actual = tokio::time::timeout(Duration::from_secs(1), store.state())
            .await
            .expect("state query should acquire the fourth pool connection")
            .unwrap();

        assert_eq!(
            actual,
            Some(StoredGraphState {
                format_version: FORMAT_VERSION,
                revision: expected.revision(),
                source_version: expected.source_version(),
            })
        );
    }

    #[tokio::test]
    async fn replace_round_trips_across_reopen_and_removes_stale_rows() {
        let directory = TestDirectory::new();
        let expected = graph();
        {
            let store = WebGraphStore::open(&directory.path).await.unwrap();
            store.replace(&expected).await.unwrap();
            assert_eq!(load_graph_for_test(&store).await, Some(expected.clone()));
        }
        {
            let store = WebGraphStore::open(&directory.path).await.unwrap();
            assert_eq!(load_graph_for_test(&store).await, Some(expected));

            let empty = empty_graph(2, 11);
            store.replace(&empty).await.unwrap();
            assert_eq!(load_graph_for_test(&store).await, Some(empty));
        }
    }

    #[tokio::test]
    async fn migration_backfills_spatial_indexes_for_a_v1_store() {
        let directory = TestDirectory::new();
        let mut snapshot = graph().snapshot();
        snapshot.topology.nodes.push(node("minimum"));
        snapshot
            .layouts
            .all
            .nodes
            .push(placement("minimum", i32::MIN, i32::MIN));
        let expected = Graph::from_snapshot(snapshot).unwrap();
        create_v1_store(&directory.path.join(DATABASE_FILE_NAME), &expected);

        let store = WebGraphStore::open(&directory.path).await.unwrap();
        let (nodes, edges) =
            load_viewport_for_test(&store, LayoutKind::All, viewport(0, 0, 1_000, 1_000), 2).await;

        assert_eq!(nodes.len(), 3);
        assert_eq!(edges.len(), 2);
        let (minimum, _) = load_viewport_for_test(
            &store,
            LayoutKind::All,
            viewport(i32::MIN, i32::MIN, 1, 1),
            2,
        )
        .await;
        assert_eq!(minimum[0].node, node("minimum"));
        assert_eq!(load_graph_for_test(&store).await, Some(expected));
    }

    #[tokio::test]
    async fn filtered_queries_return_only_requested_layout_data() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();
        store.replace(&graph()).await.unwrap();

        let placements = store
            .node_placements(LayoutKind::All, &[node("c"), node("missing"), node("a")])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(placements.state.revision, Revision::new(1));
        assert_eq!(
            placements
                .value
                .iter()
                .map(|placement| placement.node.clone())
                .collect::<Vec<_>>(),
            vec![node("a"), node("c")]
        );

        let routes = store
            .incident_edge_routes_page(
                LayoutKind::All,
                &[node("b")],
                None,
                NonZeroUsize::new(10).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            routes
                .value
                .items
                .iter()
                .map(|routed| routed.edge.clone())
                .collect::<Vec<_>>(),
            vec![
                edge(EdgeKind::Primary, "a", "b"),
                edge(EdgeKind::Primary, "b", "c")
            ]
        );
        let first_incident_page = store
            .incident_edge_routes_page(
                LayoutKind::All,
                &[node("b")],
                None,
                NonZeroUsize::new(1).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        let incident_cursor = first_incident_page.value.next_cursor.unwrap();
        let error = store
            .incident_edge_routes_page(
                LayoutKind::All,
                &[node("a")],
                Some(&incident_cursor),
                NonZeroUsize::new(1).unwrap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::CursorQueryMismatch));
        assert!(
            store
                .incident_edge_routes_page(
                    LayoutKind::Anchors,
                    &[node("b")],
                    None,
                    NonZeroUsize::new(10).unwrap(),
                )
                .await
                .unwrap()
                .unwrap()
                .value
                .items
                .is_empty()
        );
    }

    #[tokio::test]
    async fn viewport_query_uses_node_hitboxes_and_route_bounds() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();
        store.replace(&graph()).await.unwrap();

        let (nodes, edges) =
            load_viewport_for_test(&store, LayoutKind::All, viewport(150, 100, 10, 10), 10).await;
        assert_eq!(
            nodes
                .iter()
                .map(|placement| placement.node.clone())
                .collect::<Vec<_>>(),
            vec![node("b")]
        );
        assert!(edges.is_empty());
        let (anchor_nodes, anchor_edges) =
            load_viewport_for_test(&store, LayoutKind::Anchors, viewport(150, 100, 10, 10), 10)
                .await;
        assert!(anchor_nodes.is_empty());
        assert!(anchor_edges.is_empty());

        let (nodes, edges) =
            load_viewport_for_test(&store, LayoutKind::All, viewport(100, 101, 6, 6), 10).await;
        assert_eq!(
            nodes
                .iter()
                .map(|placement| placement.node.clone())
                .collect::<Vec<_>>(),
            vec![node("a")]
        );
        assert_eq!(
            edges
                .iter()
                .map(|routed| routed.edge.clone())
                .collect::<Vec<_>>(),
            vec![edge(EdgeKind::Primary, "b", "c")]
        );

        let (nodes, _) = load_viewport_for_test(
            &store,
            LayoutKind::All,
            Viewport {
                overscan: 11,
                ..viewport(145, 100, 1, 1)
            },
            10,
        )
        .await;
        assert_eq!(
            nodes
                .iter()
                .map(|placement| placement.node.clone())
                .collect::<Vec<_>>(),
            vec![node("a"), node("b")]
        );
    }

    #[tokio::test]
    async fn viewport_pagination_is_bounded_and_query_scoped() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();
        store.replace(&graph()).await.unwrap();
        let viewport = viewport(0, 0, 1_000, 1_000);
        let limit = NonZeroUsize::new(1).unwrap();

        let first = store
            .viewport_page(LayoutKind::All, viewport, None, limit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.value.nodes.len(), 1);
        assert_eq!(first.value.edges.len(), 1);
        assert_eq!(first.value.canvas, graph().layout(LayoutKind::All).canvas());
        let cursor = first.value.next_cursor.unwrap();
        assert_eq!(cursor.revision(), Revision::new(1));

        let error = store
            .viewport_page(LayoutKind::Anchors, viewport, Some(&cursor), limit)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::CursorQueryMismatch));
        let error = store
            .viewport_page(
                LayoutKind::All,
                Viewport { x: 1, ..viewport },
                Some(&cursor),
                limit,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::CursorQueryMismatch));

        let (nodes, edges) = load_viewport_for_test(&store, LayoutKind::All, viewport, 1).await;
        assert_eq!(nodes.len(), 3);
        assert_eq!(edges.len(), 2);
    }

    #[tokio::test]
    async fn patch_atomically_updates_viewport_spatial_indexes() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();
        store.replace(&graph()).await.unwrap();
        let old_node_viewport = viewport(350, 110, 1, 1);
        let old_route_viewport = viewport(100, 101, 6, 6);
        let first = store
            .viewport_page(
                LayoutKind::All,
                viewport(0, 0, 1_000, 1_000),
                None,
                NonZeroUsize::new(1).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        let stale_cursor = first.value.next_cursor.unwrap();

        store.apply_patch(relayout_patch()).await.unwrap();

        let error = store
            .viewport_page(
                LayoutKind::All,
                viewport(0, 0, 1_000, 1_000),
                Some(&stale_cursor),
                NonZeroUsize::new(1).unwrap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::StaleCursor { .. }));
        let (nodes, _) =
            load_viewport_for_test(&store, LayoutKind::All, old_node_viewport, 10).await;
        assert!(!nodes.iter().any(|placement| placement.node == node("c")));
        let (nodes, _) =
            load_viewport_for_test(&store, LayoutKind::All, viewport(400, 180, 1, 1), 10).await;
        assert!(nodes.iter().any(|placement| placement.node == node("c")));
        let (_, edges) =
            load_viewport_for_test(&store, LayoutKind::All, old_route_viewport, 10).await;
        assert!(
            !edges
                .iter()
                .any(|routed| routed.edge == edge(EdgeKind::Primary, "b", "c"))
        );
        let (_, edges) =
            load_viewport_for_test(&store, LayoutKind::All, viewport(500, 501, 6, 6), 10).await;
        assert!(
            edges
                .iter()
                .any(|routed| routed.edge == edge(EdgeKind::Primary, "b", "c"))
        );

        store.replace(&empty_graph(3, 11)).await.unwrap();
        let (nodes, edges) =
            load_viewport_for_test(&store, LayoutKind::All, viewport(0, 0, 1_000, 1_000), 10).await;
        assert!(nodes.is_empty());
        assert!(edges.is_empty());
    }

    #[tokio::test]
    async fn viewport_query_rejects_invalid_geometry() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();

        let error = store
            .viewport_page(
                LayoutKind::All,
                Viewport {
                    x: 0,
                    y: 0,
                    width: 0,
                    height: 100,
                    overscan: 0,
                },
                None,
                NonZeroUsize::new(1).unwrap(),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, Error::InvalidViewport { width: 0, .. }));
    }

    #[tokio::test]
    async fn pagination_is_stable_and_rejects_a_cursor_after_revision_changes() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();
        store.replace(&graph()).await.unwrap();
        let limit = NonZeroUsize::new(1).unwrap();

        let first = store
            .node_placements_page(LayoutKind::All, None, limit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.value.items[0].node, node("a"));
        let cursor = first.value.next_cursor.unwrap();
        assert_eq!(cursor.revision(), Revision::new(1));
        let error = store
            .node_placements_page(LayoutKind::Anchors, Some(&cursor), limit)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::CursorQueryMismatch));
        let second = store
            .node_placements_page(LayoutKind::All, Some(&cursor), limit)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.value.items[0].node, node("b"));

        store.apply_patch(relayout_patch()).await.unwrap();
        let error = store
            .node_placements_page(LayoutKind::All, Some(&cursor), limit)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::StaleCursor {
                cursor,
                current
            } if cursor == Revision::new(1) && current == Revision::new(2)
        ));
    }

    #[tokio::test]
    async fn read_queries_enforce_the_sqlite_parameter_budget() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();
        store.replace(&graph()).await.unwrap();
        let nodes = (0..=MAX_READ_BATCH_SIZE)
            .map(|index| node(&format!("node-{index}")))
            .collect::<Vec<_>>();

        let error = store
            .node_placements(LayoutKind::All, &nodes)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::ReadBatchTooLarge {
                actual,
                maximum: MAX_READ_BATCH_SIZE
            } if actual == MAX_READ_BATCH_SIZE + 1
        ));
        let error = store
            .edge_routes_page(
                LayoutKind::All,
                None,
                NonZeroUsize::new(MAX_READ_BATCH_SIZE + 1).unwrap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::ReadBatchTooLarge { .. }));
    }

    #[tokio::test]
    async fn patch_persists_incremental_topology_and_layout_changes() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();
        let mut expected = graph();
        store.replace(&expected).await.unwrap();
        let patch = graph_patch();
        expected.apply_patch(patch.clone()).unwrap();

        let actual = store.apply_patch(patch).await.unwrap();

        assert_eq!(actual.revision, expected.revision());
        assert_eq!(actual.source_version, expected.source_version());
        assert_eq!(load_graph_for_test(&store).await, Some(expected));
        let (removed, _) =
            load_viewport_for_test(&store, LayoutKind::All, viewport(220, 120, 1, 1), 10).await;
        assert!(removed.is_empty());
        let (added, _) =
            load_viewport_for_test(&store, LayoutKind::All, viewport(500, 140, 1, 1), 10).await;
        assert_eq!(added[0].node, node("d"));
        let (_, added_routes) =
            load_viewport_for_test(&store, LayoutKind::All, viewport(300, 301, 6, 6), 10).await;
        assert_eq!(added_routes[0].edge, edge(EdgeKind::Primary, "c", "d"));
    }

    #[tokio::test]
    async fn invalid_patch_rolls_back_every_sqlite_change() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();
        let original = graph();
        store.replace(&original).await.unwrap();
        let mut invalid = relayout_patch();
        invalid.topology.remove_nodes = vec![node("b")];

        let error = store.apply_patch(invalid).await.unwrap_err();

        assert!(matches!(error, Error::InvalidGraph { .. }));
        assert_eq!(load_graph_for_test(&store).await, Some(original));
    }

    #[tokio::test]
    async fn cyclic_patch_rolls_back_incremental_rows_and_revision() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();
        let original = graph();
        store.replace(&original).await.unwrap();
        let cycle = edge(EdgeKind::Primary, "c", "a");
        let patch = Patch {
            format_version: FORMAT_VERSION,
            base_revision: Revision::new(1),
            revision: Revision::new(2),
            source_version: SourceVersion::new(10),
            topology: TopologyPatch {
                add_nodes: Vec::new(),
                remove_nodes: Vec::new(),
                add_edges: vec![cycle.clone()],
                remove_edges: Vec::new(),
            },
            layouts: LayoutPatches {
                anchors: LayoutPatch::default(),
                all: LayoutPatch {
                    upsert_edges: vec![routed(cycle.clone(), 600)],
                    ..LayoutPatch::default()
                },
            },
        };

        let error = store.apply_patch(patch).await.unwrap_err();

        assert!(matches!(
            error,
            Error::InvalidGraph {
                source: GraphError::Cycle {
                    layout: LayoutKind::All,
                    ..
                }
            }
        ));
        assert_eq!(load_graph_for_test(&store).await, Some(original));
        let (_, edges) =
            load_viewport_for_test(&store, LayoutKind::All, viewport(600, 601, 6, 6), 10).await;
        assert!(!edges.iter().any(|routed| routed.edge == cycle));
    }

    #[tokio::test]
    async fn concurrent_patches_allow_one_writer_for_each_base_revision() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();
        store.replace(&graph()).await.unwrap();
        let left_store = store.clone();
        let right_store = store.clone();
        let patch = relayout_patch();

        let (left, right) = tokio::join!(
            left_store.apply_patch(patch.clone()),
            right_store.apply_patch(patch)
        );

        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        let failure = left.err().or_else(|| right.err()).unwrap();
        assert!(matches!(
            failure,
            Error::InvalidGraph {
                source: GraphError::RevisionMismatch { .. }
            }
        ));
        assert_eq!(
            store.state().await.unwrap().unwrap().revision,
            Revision::new(2)
        );
    }

    #[tokio::test]
    async fn patch_requires_an_initialized_store() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();

        let error = store.apply_patch(relayout_patch()).await.unwrap_err();

        assert!(matches!(error, Error::NotInitialized { .. }));
        assert_eq!(store.state().await.unwrap(), None);
    }

    #[tokio::test]
    async fn out_of_range_replace_preserves_previous_graph() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();
        let original = graph();
        store.replace(&original).await.unwrap();
        let too_large = empty_graph(i64::MAX as u64 + 1, 11);

        let error = store.replace(&too_large).await.unwrap_err();

        assert!(matches!(
            error,
            Error::IntegerOutOfRange {
                column: "revision",
                ..
            }
        ));
        assert_eq!(load_graph_for_test(&store).await, Some(original));
    }
}
