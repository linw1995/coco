use super::*;

#[derive(Clone, Queryable, QueryableByName)]
pub struct MaterializationRow {
    #[diesel(sql_type = BigInt)]
    pub source_version: i64,
    #[diesel(sql_type = Integer)]
    pub world_min_x: i32,
    #[diesel(sql_type = Integer)]
    pub world_min_y: i32,
    #[diesel(sql_type = Integer)]
    pub world_max_x: i32,
    #[diesel(sql_type = Integer)]
    pub world_max_y: i32,
}

#[derive(Clone, Queryable, QueryableByName)]
pub struct LaneRow {
    #[diesel(sql_type = Text)]
    pub lane_key: String,
    #[diesel(sql_type = Text)]
    pub lane_label: String,
    #[diesel(sql_type = Integer)]
    pub lane_y: i32,
}

#[derive(Queryable, QueryableByName)]
pub struct NodeLocationRow {
    #[diesel(sql_type = Text)]
    pub node_key: String,
    #[diesel(sql_type = Text)]
    pub node_id: String,
    #[diesel(sql_type = Text)]
    pub node_target: String,
    #[diesel(sql_type = Text)]
    pub short_id: String,
    #[diesel(sql_type = Text)]
    pub node_kind: String,
    #[diesel(sql_type = Text)]
    pub summary: String,
    #[diesel(sql_type = Text)]
    pub labels_json: String,
    #[diesel(sql_type = Integer)]
    pub x: i32,
    #[diesel(sql_type = Integer)]
    pub y: i32,
}

#[derive(Clone, Queryable, QueryableByName)]
pub struct EdgeRouteRow {
    #[diesel(sql_type = Text)]
    pub edge_key: String,
    #[diesel(sql_type = Text)]
    pub edge_kind: String,
    #[diesel(sql_type = Text)]
    pub source_id: String,
    #[diesel(sql_type = Text)]
    pub target_id: String,
    #[diesel(sql_type = Integer)]
    pub source_x: i32,
    #[diesel(sql_type = Integer)]
    pub source_y: i32,
    #[diesel(sql_type = Integer)]
    pub target_x: i32,
    #[diesel(sql_type = Integer)]
    pub target_y: i32,
    #[diesel(sql_type = Integer)]
    pub route_slot: i32,
    #[diesel(sql_type = Double)]
    pub target_port_offset: f64,
}

#[derive(Clone, Queryable, QueryableByName)]
pub struct MaterializedTailNodeRow {
    #[diesel(sql_type = Text)]
    pub node_key: String,
    #[diesel(sql_type = Text)]
    pub node_id: String,
    #[diesel(sql_type = Text)]
    pub lane_key: String,
    #[diesel(sql_type = Text)]
    pub lane_label: String,
    #[diesel(sql_type = Integer)]
    pub lane_y: i32,
    #[diesel(sql_type = Integer)]
    pub x: i32,
    #[diesel(sql_type = Integer)]
    pub y: i32,
}

#[derive(Queryable, QueryableByName)]
pub struct MaterializedNodePointRow {
    #[diesel(sql_type = Integer)]
    pub x: i32,
    #[diesel(sql_type = Integer)]
    pub y: i32,
}

#[derive(Clone, Debug)]
pub struct MaterializedGraphShellFacts {
    pub version: u64,
    pub lanes: Vec<GraphViewportLane>,
    pub nodes: Vec<MaterializedGraphShellNode>,
    pub edge_count: usize,
}

#[derive(Clone, Debug)]
pub struct MaterializedGraphShellNode {
    pub node_id: String,
    pub point: Point,
}

pub struct NodeLocationInsert<'a> {
    pub mode: GraphMode,
    pub node: &'a GraphViewportNode,
    pub lane: &'a GraphViewportLane,
    pub bounds: ItemBounds,
}

#[derive(Insertable)]
#[diesel(table_name = console_graph_materializations)]
pub struct MaterializationInsert<'a> {
    pub mode: &'a str,
    pub source_version: i64,
    pub coordinate_space: &'a str,
    pub world_min_x: i32,
    pub world_min_y: i32,
    pub world_max_x: i32,
    pub world_max_y: i32,
}

pub struct MaterializedNodeReference {
    pub node_id: String,
    pub labels: Vec<String>,
}

pub struct EdgeRouteInsert<'a> {
    pub mode: GraphMode,
    pub edge: &'a GraphViewportEdge,
    pub bounds: ItemBounds,
}

pub struct AppendLinearBranchInput<'a> {
    pub mode: GraphMode,
    pub branch: &'a str,
    pub state: &'a SessionState,
    pub head_id: &'a str,
}

pub struct MergeColumnConstraintInput<'a> {
    pub mode: GraphMode,
    pub node: &'a Node,
    pub primary_parent_id: &'a str,
    pub point: Point,
    pub event_order: &'a BTreeMap<String, usize>,
    pub reserved_lane_y: Option<i32>,
    pub context_start_id: Option<&'a str>,
}

pub struct NodeMergeEdgesInput<'a> {
    pub mode: GraphMode,
    pub node: &'a Node,
    pub primary_parent_id: &'a str,
    pub target: Point,
    pub context_start_id: Option<&'a str>,
}

pub struct AnchorBranchLaneInsert {
    pub lane_y: i32,
    pub nodes: Vec<Node>,
    pub previous: Option<(String, Point)>,
    pub context_start_id: Option<String>,
}

pub struct VisibleMergeParentPoint {
    pub node_id: String,
    pub point: Point,
}

pub enum MergeParentPoint {
    Visible(VisibleMergeParentPoint),
    Skipped,
    Unsupported,
}

pub struct OrphanMergeParentLane {
    pub source_id: String,
    pub lane: GraphViewportLane,
    pub nodes: Vec<Node>,
    pub fork_source: Option<(String, Point)>,
    pub context_start_id: Option<String>,
}

pub struct OrphanMergeParentNodeEdgeInput<'a> {
    pub mode: GraphMode,
    pub node: &'a Node,
    pub point: Point,
    pub previous: Option<&'a (String, Point)>,
    pub first_node: bool,
    pub force_fork: bool,
    pub context_start_id: Option<&'a str>,
}

pub struct OrphanMergeParentLaneInput<'a> {
    pub mode: GraphMode,
    pub ancestry: &'a [Node],
    pub source_index: usize,
    pub reserved_lane_y: Option<i32>,
    pub context_start_id: Option<&'a str>,
}

pub enum SkillSubtreeAppend {
    Absent,
    Applied,
    Unsupported,
}

pub struct MaterializationMetaInput {
    pub source_version: u64,
    pub mode: GraphMode,
    pub world_min_x: i32,
    pub world_min_y: i32,
    pub world_max_x: i32,
    pub world_max_y: i32,
}
