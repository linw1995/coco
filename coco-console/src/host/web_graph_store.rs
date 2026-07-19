use std::path::{Path, PathBuf};

use diesel::{
    ExpressionMethods, Insertable, OptionalExtension, QueryDsl, Queryable, Selectable,
    SelectableHelper,
};
use diesel_async::AsyncConnection;
use diesel_migrations::{EmbeddedMigrations, embed_migrations};
use snafu::prelude::*;

use crate::web_graph::{
    BezierRoute, Canvas, EdgeId, EdgeKind, Graph, LayoutKind, LayoutPatch, LayoutSnapshot,
    LayoutSnapshots, NodeId, NodePlacement, Patch, Point, Revision, RoutedEdge, Snapshot,
    SourceVersion, TopologyPatch, TopologySnapshot,
};

const DATABASE_FILE_NAME: &str = "web-graph.sqlite3";
const WRITE_BATCH_SIZE: usize = 64;
const MIGRATIONS: EmbeddedMigrations = embed_migrations!("web-graph-migrations");

mod database;
mod schema;

use database::{AsyncSqliteConnection, Database};
use schema::{
    web_graph_edge_routes, web_graph_edges, web_graph_layouts, web_graph_node_placements,
    web_graph_nodes, web_graph_state,
};

#[derive(Clone)]
pub struct WebGraphStore {
    path: PathBuf,
    database: Database,
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

    pub async fn load(&self) -> Result<Option<Graph>> {
        let path = self.path.clone();
        let mut connection = self.database.acquire().await?;
        connection
            .transaction::<_, TransactionError, _>(async |connection| load_graph(connection).await)
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

    pub async fn apply_patch(&self, patch: Patch) -> Result<Graph> {
        let path = self.path.clone();
        let mut connection = self.database.acquire().await?;
        connection
            .immediate_transaction::<_, TransactionError, _>(async |connection| {
                let mut graph = load_graph(connection).await?.ok_or_else(|| {
                    TransactionError::Operation(Error::NotInitialized { path: path.clone() })
                })?;
                graph.apply_patch(patch.clone()).map_err(|source| {
                    TransactionError::Operation(Error::InvalidGraph { source })
                })?;
                apply_patch_rows(connection, &patch).await?;
                Ok(graph)
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

#[derive(Default)]
struct LoadedLayout {
    canvas: Option<Canvas>,
    nodes: Vec<NodePlacement>,
    edges: Vec<RoutedEdge>,
}

struct SnapshotRows {
    state: StateRow,
    nodes: Vec<NodeRow>,
    edges: Vec<EdgeRow>,
    layouts: Vec<LayoutRow>,
    placements: Vec<PlacementRow>,
    routes: Vec<RouteRow>,
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

async fn load_graph(
    connection: &mut AsyncSqliteConnection,
) -> std::result::Result<Option<Graph>, TransactionError> {
    use diesel_async::RunQueryDsl;

    let Some(state) = web_graph_state::table
        .filter(web_graph_state::id.eq(1))
        .select(StateRow::as_select())
        .first::<StateRow>(connection)
        .await
        .optional()?
    else {
        return Ok(None);
    };

    let nodes = web_graph_nodes::table
        .order(web_graph_nodes::node_id.asc())
        .select(NodeRow::as_select())
        .load::<NodeRow>(connection)
        .await?
        .into_iter()
        .map(|row| stored_node_id(row.node_id))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let edges = web_graph_edges::table
        .order((
            web_graph_edges::edge_kind.asc(),
            web_graph_edges::source_id.asc(),
            web_graph_edges::target_id.asc(),
        ))
        .select(EdgeRow::as_select())
        .load::<EdgeRow>(connection)
        .await?
        .into_iter()
        .map(stored_edge)
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let mut anchors = LoadedLayout::default();
    let mut all = LoadedLayout::default();
    for row in web_graph_layouts::table
        .order(web_graph_layouts::layout_kind.asc())
        .select(LayoutRow::as_select())
        .load::<LayoutRow>(connection)
        .await?
    {
        let layout = loaded_layout_mut(
            stored_layout_kind(&row.layout_kind)?,
            &mut anchors,
            &mut all,
        );
        if layout
            .canvas
            .replace(Canvas {
                width: row.canvas_width,
                height: row.canvas_height,
            })
            .is_some()
        {
            return Err(invalid_value(
                "web_graph_layouts.layout_kind",
                row.layout_kind,
            ));
        }
    }
    for row in web_graph_node_placements::table
        .order((
            web_graph_node_placements::layout_kind.asc(),
            web_graph_node_placements::node_id.asc(),
        ))
        .select(PlacementRow::as_select())
        .load::<PlacementRow>(connection)
        .await?
    {
        let kind = stored_layout_kind(&row.layout_kind)?;
        loaded_layout_mut(kind, &mut anchors, &mut all)
            .nodes
            .push(NodePlacement {
                node: stored_node_id(row.node_id)?,
                point: Point { x: row.x, y: row.y },
            });
    }
    for row in web_graph_edge_routes::table
        .order((
            web_graph_edge_routes::layout_kind.asc(),
            web_graph_edge_routes::edge_kind.asc(),
            web_graph_edge_routes::source_id.asc(),
            web_graph_edge_routes::target_id.asc(),
        ))
        .select(RouteRow::as_select())
        .load::<RouteRow>(connection)
        .await?
    {
        let kind = stored_layout_kind(&row.layout_kind)?;
        loaded_layout_mut(kind, &mut anchors, &mut all)
            .edges
            .push(stored_route(row)?);
    }

    let snapshot = Snapshot {
        format_version: stored_u32(state.format_version, "web_graph_state.format_version")?,
        revision: Revision::new(stored_u64(state.revision, "web_graph_state.revision")?),
        source_version: SourceVersion::new(stored_u64(
            state.source_version,
            "web_graph_state.source_version",
        )?),
        topology: TopologySnapshot { nodes, edges },
        layouts: LayoutSnapshots {
            anchors: finish_loaded_layout(LayoutKind::Anchors, anchors)?,
            all: finish_loaded_layout(LayoutKind::All, all)?,
        },
    };
    Graph::from_snapshot(snapshot)
        .map(Some)
        .map_err(|source| TransactionError::Operation(Error::InvalidGraph { source }))
}

fn loaded_layout_mut<'a>(
    kind: LayoutKind,
    anchors: &'a mut LoadedLayout,
    all: &'a mut LoadedLayout,
) -> &'a mut LoadedLayout {
    match kind {
        LayoutKind::Anchors => anchors,
        LayoutKind::All => all,
    }
}

fn finish_loaded_layout(
    kind: LayoutKind,
    layout: LoadedLayout,
) -> std::result::Result<LayoutSnapshot, TransactionError> {
    let canvas = layout.canvas.ok_or_else(|| {
        invalid_value(
            "web_graph_layouts.layout_kind",
            format!("missing {} layout", layout_kind_value(kind)),
        )
    })?;
    Ok(LayoutSnapshot {
        canvas,
        nodes: layout.nodes,
        edges: layout.edges,
    })
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

fn stored_layout_kind(value: &str) -> std::result::Result<LayoutKind, TransactionError> {
    match value {
        "anchors" => Ok(LayoutKind::Anchors),
        "all" => Ok(LayoutKind::All),
        _ => Err(invalid_value("layout_kind", value)),
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
            diesel::RunQueryDsl::execute(
                diesel::insert_into(web_graph_state::table).values(&rows.state),
                connection,
            )?;
            Ok(())
        })
        .await?;
    Ok(())
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

    use tokio::sync::Barrier;

    use super::*;
    use crate::web_graph::{Error as GraphError, FORMAT_VERSION, LayoutPatches};

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

    #[tokio::test]
    async fn new_store_is_empty() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();

        assert_eq!(store.path(), directory.path.join(DATABASE_FILE_NAME));
        assert_eq!(store.load().await.unwrap(), None);
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
        assert_eq!(stores[0].load().await.unwrap(), None);
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

        let actual = tokio::time::timeout(Duration::from_secs(1), store.load())
            .await
            .expect("graph load should acquire the fourth pool connection")
            .unwrap();

        assert_eq!(actual, Some(expected));
    }

    #[tokio::test]
    async fn replace_round_trips_across_reopen_and_removes_stale_rows() {
        let directory = TestDirectory::new();
        let expected = graph();
        {
            let store = WebGraphStore::open(&directory.path).await.unwrap();
            store.replace(&expected).await.unwrap();
            assert_eq!(store.load().await.unwrap(), Some(expected.clone()));
        }
        {
            let store = WebGraphStore::open(&directory.path).await.unwrap();
            assert_eq!(store.load().await.unwrap(), Some(expected));

            let empty = empty_graph(2, 11);
            store.replace(&empty).await.unwrap();
            assert_eq!(store.load().await.unwrap(), Some(empty));
        }
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

        assert_eq!(actual, expected);
        assert_eq!(store.load().await.unwrap(), Some(expected));
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
        assert_eq!(store.load().await.unwrap(), Some(original));
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
            store.load().await.unwrap().unwrap().revision(),
            Revision::new(2)
        );
    }

    #[tokio::test]
    async fn patch_requires_an_initialized_store() {
        let directory = TestDirectory::new();
        let store = WebGraphStore::open(&directory.path).await.unwrap();

        let error = store.apply_patch(relayout_patch()).await.unwrap_err();

        assert!(matches!(error, Error::NotInitialized { .. }));
        assert_eq!(store.load().await.unwrap(), None);
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
        assert_eq!(store.load().await.unwrap(), Some(original));
    }
}
