use super::*;

pub fn full_layout_materialization_lane(
    lane: &GraphViewportLane,
    nodes_by_y: &BTreeMap<i32, Vec<&GraphViewportNode>>,
    branch_labels: &BTreeSet<String>,
) -> GraphViewportLane {
    if branch_labels.contains(&lane.label) {
        return lane.clone();
    }
    let derived_prefix = if lane.label.starts_with("orphan ") {
        Some(DERIVED_ORPHAN_LANE_KEY_PREFIX)
    } else if lane.label.starts_with("skill ") {
        Some(DERIVED_SKILL_LANE_KEY_PREFIX)
    } else {
        None
    };
    let Some(prefix) = derived_prefix else {
        return lane.clone();
    };
    let Some(source_id) = nodes_by_y
        .get(&lane.y)
        .and_then(|nodes| nodes.iter().max_by_key(|node| node.x))
        .map(|node| node.id.as_str())
    else {
        return lane.clone();
    };
    GraphViewportLane {
        key: format!("{prefix}{source_id}"),
        label: lane.label.clone(),
        y: lane.y,
    }
}

#[derive(Clone, Copy)]
pub struct ItemBounds {
    pub left: i32,
    pub top: i32,
    pub right: i32,
    pub bottom: i32,
}

#[derive(Clone, Copy)]
pub struct ViewportItemBounds {
    pub left: i32,
    pub top: i32,
    pub right: i32,
    pub bottom: i32,
}

impl ViewportItemBounds {
    pub fn from_request(request: GraphViewportRequest) -> Self {
        Self {
            left: request.x.saturating_sub(request.overscan),
            top: request.y.saturating_sub(request.overscan),
            right: request
                .x
                .saturating_add(request.width)
                .saturating_add(request.overscan),
            bottom: request
                .y
                .saturating_add(request.height)
                .saturating_add(request.overscan),
        }
    }
}

pub fn node_bounds(node: &GraphViewportNode) -> ItemBounds {
    ItemBounds {
        left: node.x - NODE_RADIUS,
        top: node.y - NODE_RADIUS,
        right: node.x + NODE_RADIUS,
        bottom: node.y + NODE_RADIUS,
    }
}

pub fn edge_bounds(edge: &GraphViewportEdge) -> ItemBounds {
    let padding = NODE_RADIUS + EDGE_TARGET_APPROACH;
    let mut left = edge.source.x.min(edge.target.x) - padding;
    let mut top = edge.source.y.min(edge.target.y) - padding;
    let mut right = edge.source.x.max(edge.target.x) + padding;
    let mut bottom = edge.source.y.max(edge.target.y) + padding;
    if edge.kind != GraphViewportEdgeKind::PrimaryParent {
        let corridor_y = edge_corridor_y(edge.source.y, edge.target.y, edge.route_slot);
        top = top.min(corridor_y - padding);
        bottom = bottom.max(corridor_y + padding);
        right = right.max(edge.source.x.max(edge.target.x) + EDGE_TARGET_APPROACH);
        left = left.min(edge.source.x.min(edge.target.x) - EDGE_TARGET_APPROACH);
    }

    ItemBounds {
        left,
        top,
        right,
        bottom,
    }
}

pub fn graph_viewport_node_from_node(
    node: &Node,
    point: Point,
    labels: Vec<String>,
) -> GraphViewportNode {
    GraphViewportNode {
        key: node_key(&node.id, point),
        id: node.id.clone(),
        node_target: node_target_id(&node.id),
        short_id: shorten_id(&node.id),
        kind: graph_kind_name(node).to_owned(),
        summary: summarize_node(node),
        labels,
        x: point.x,
        y: point.y,
    }
}

pub fn primary_parent_edge(
    source_id: &str,
    source: Point,
    target_id: &str,
    target: Point,
) -> GraphViewportEdge {
    GraphViewportEdge {
        key: primary_parent_edge_key(source_id, source, target_id, target),
        kind: GraphViewportEdgeKind::PrimaryParent,
        source_id: source_id.to_owned(),
        target_id: target_id.to_owned(),
        source,
        target,
        route_slot: 0,
        target_port_offset: 0.0,
    }
}

pub fn routed_edge(
    kind: GraphViewportEdgeKind,
    source_id: &str,
    source: Point,
    target_id: &str,
    target: Point,
    route_slot: i32,
) -> GraphViewportEdge {
    GraphViewportEdge {
        key: edge_key(kind, source_id, source, target_id, target),
        kind,
        source_id: source_id.to_owned(),
        target_id: target_id.to_owned(),
        source,
        target,
        route_slot,
        target_port_offset: 0.0,
    }
}

pub fn node_key(node_id: &str, point: Point) -> String {
    format!("node:{node_id}:{}:{}", point.x, point.y)
}

pub fn primary_parent_edge_key(
    source_id: &str,
    source: Point,
    target_id: &str,
    target: Point,
) -> String {
    edge_key(
        GraphViewportEdgeKind::PrimaryParent,
        source_id,
        source,
        target_id,
        target,
    )
}

pub fn edge_key(
    kind: GraphViewportEdgeKind,
    source_id: &str,
    source: Point,
    target_id: &str,
    target: Point,
) -> String {
    format!(
        "edge:{}:{source_id}:{}:{}:{target_id}:{}:{}",
        edge_kind_query_value(kind),
        source.x,
        source.y,
        target.x,
        target.y
    )
}

pub fn is_linear_primary_chain(chain: &[Node]) -> bool {
    chain.windows(2).all(|nodes| nodes[1].parent == nodes[0].id)
}

pub fn node_anchor_merge_parents(node: &Node) -> &[MergeParent] {
    match &node.kind {
        Kind::Anchor(anchor) => anchor.merge_parents(),
        _ => &[],
    }
}

pub fn is_anchor_node(node: &Node) -> bool {
    matches!(&node.kind, Kind::Anchor(_))
}

pub fn is_visible_mode_node(mode: GraphMode, node: &Node) -> bool {
    !node.is_root() && (mode == GraphMode::All || is_anchor_node(node))
}

pub fn merge_parent_context_start_id(mode: GraphMode, ancestry: &[Node]) -> Option<String> {
    (mode == GraphMode::Anchors)
        .then(|| {
            context_start_id_from_scoped_ancestry(&provider_context_ancestry_nodes(
                ancestry.to_vec(),
            ))
        })
        .flatten()
}

pub fn context_start_id_from_scoped_ancestry(scoped_ancestry: &[Node]) -> Option<String> {
    scoped_ancestry.last().map(|node| node.id.clone())
}

pub fn visible_scoped_merge_parent_source_index(
    mode: GraphMode,
    ancestry: &[Node],
    context_start_id: Option<&str>,
) -> Option<usize> {
    let end_index = scoped_merge_parent_end_index(ancestry, context_start_id)?;
    ancestry[..=end_index]
        .iter()
        .position(|node| is_visible_mode_node(mode, node))
}

pub fn scoped_merge_parent_end_index(
    ancestry: &[Node],
    context_start_id: Option<&str>,
) -> Option<usize> {
    match context_start_id {
        Some(context_start_id) => ancestry
            .iter()
            .position(|node| !node.is_root() && node.id == context_start_id),
        None => ancestry.iter().position(|node| node.is_root()),
    }
}

pub fn is_orphan_lane_key(key: &str) -> bool {
    key.starts_with(DERIVED_ORPHAN_LANE_KEY_PREFIX)
}

pub fn is_skill_invocation_lane_key(key: &str) -> bool {
    key.starts_with(DERIVED_SKILL_LANE_KEY_PREFIX)
}

pub fn is_derived_lane_key(key: &str) -> bool {
    is_orphan_lane_key(key) || is_skill_invocation_lane_key(key)
}

pub fn orphan_merge_parent_lane(source_id: &str, y: i32) -> GraphViewportLane {
    let label = format!("orphan {}", shorten_id(source_id));
    GraphViewportLane {
        key: format!("{DERIVED_ORPHAN_LANE_KEY_PREFIX}{source_id}"),
        label,
        y,
    }
}

pub fn skill_invocation_subtree_lane(source_id: &str, y: i32) -> GraphViewportLane {
    let label = format!("skill {}", shorten_id(source_id));
    GraphViewportLane {
        key: format!("{DERIVED_SKILL_LANE_KEY_PREFIX}{source_id}"),
        label,
        y,
    }
}

pub fn visible_orphan_merge_parent_nodes(
    mode: GraphMode,
    ancestry: &[Node],
    end_index: usize,
) -> Vec<Node> {
    ancestry[..end_index]
        .iter()
        .filter(|node| is_visible_mode_node(mode, node))
        .cloned()
        .rev()
        .collect()
}

pub fn initial_visible_lane_is_linear(mode: GraphMode, nodes: &[Node]) -> bool {
    mode == GraphMode::Anchors || nodes.windows(2).all(|nodes| nodes[1].parent == nodes[0].id)
}

pub fn is_linear_new_nodes(source_id: &str, nodes: &[Node]) -> bool {
    nodes.first().is_some_and(|node| node.parent == source_id)
        && nodes.windows(2).all(|nodes| nodes[1].parent == nodes[0].id)
}

pub fn visible_skill_invocation_linear_subtrees(
    source_id: &str,
    nodes: Vec<Node>,
) -> Option<Vec<Vec<Node>>> {
    let mut nodes_by_id = BTreeMap::<String, Node>::new();
    let mut child_ids_by_parent = BTreeMap::<String, Vec<String>>::new();
    for node in nodes {
        child_ids_by_parent
            .entry(node.parent.clone())
            .or_default()
            .push(node.id.clone());
        nodes_by_id.insert(node.id.clone(), node);
    }

    let roots = child_ids_by_parent
        .get(source_id)
        .cloned()
        .unwrap_or_default();
    let mut subtrees = Vec::<Vec<Node>>::new();
    for root_id in roots {
        push_visible_skill_invocation_paths(
            source_id,
            &root_id,
            &nodes_by_id,
            &child_ids_by_parent,
            &mut subtrees,
        )?;
    }
    Some(subtrees)
}

pub fn push_visible_skill_invocation_paths(
    source_id: &str,
    node_id: &str,
    nodes_by_id: &BTreeMap<String, Node>,
    child_ids_by_parent: &BTreeMap<String, Vec<String>>,
    subtrees: &mut Vec<Vec<Node>>,
) -> Option<()> {
    let mut pending = vec![node_id.to_owned()];
    let mut visited = BTreeSet::new();

    while let Some(node_id) = pending.pop() {
        if !visited.insert(node_id.clone()) {
            return None;
        }
        nodes_by_id.get(&node_id)?;
        let Some(child_ids) = child_ids_by_parent.get(&node_id) else {
            subtrees.push(visible_skill_invocation_path(
                source_id,
                &node_id,
                nodes_by_id,
            )?);
            continue;
        };
        if child_ids.is_empty() {
            subtrees.push(visible_skill_invocation_path(
                source_id,
                &node_id,
                nodes_by_id,
            )?);
            continue;
        }
        for child_id in child_ids.iter().rev() {
            pending.push(child_id.clone());
        }
    }
    Some(())
}

pub fn visible_skill_invocation_path(
    source_id: &str,
    leaf_id: &str,
    nodes_by_id: &BTreeMap<String, Node>,
) -> Option<Vec<Node>> {
    let mut path = Vec::new();
    let mut node_id = leaf_id;
    let mut visited = BTreeSet::new();
    loop {
        let node = nodes_by_id.get(node_id)?;
        if !visited.insert(node.id.clone()) {
            return None;
        }
        path.push(node.clone());
        if node.parent == source_id {
            break;
        }
        node_id = &node.parent;
    }
    path.reverse();
    Some(path)
}

pub fn required_column_gap(
    source_id: &str,
    target_id: &str,
    event_order_by_node: &BTreeMap<String, usize>,
) -> i32 {
    event_order_by_node
        .get(target_id)
        .zip(event_order_by_node.get(source_id))
        .and_then(|(target_order, source_order)| target_order.checked_sub(*source_order))
        .map(|gap| gap.clamp(1, MAX_EDGE_COLUMN_GAP) as i32)
        .unwrap_or(1)
}

pub fn branch_label(branch: &str, state: &SessionState) -> String {
    format!("{branch}{}", session_state_suffix(state))
}

pub fn session_state_suffix(state: &SessionState) -> String {
    match state {
        SessionState::Active => String::new(),
        SessionState::Attached { target_branch, .. } => format!("@Attached({target_branch})"),
        SessionState::Paused {
            target_branch,
            reason,
        } => match reason {
            PauseReason::Merged { .. } => format!("@Paused({target_branch},merged)"),
            PauseReason::Closed => format!("@Paused({target_branch},closed)"),
        },
    }
}

pub fn branch_lane_priority(branch: &str) -> (u8, &str) {
    if branch == "main" {
        (0, branch)
    } else {
        (1, branch)
    }
}

pub fn existing_branch_lanes_preserve_order(
    session_states: &[(String, SessionState)],
    materialized_lanes: &[LaneRow],
    materialized_lane_labels: &BTreeSet<String>,
) -> bool {
    let expected_existing_lanes = session_states
        .iter()
        .filter(|(branch, _)| materialized_lane_labels.contains(branch))
        .map(|(branch, _)| branch.as_str())
        .collect::<Vec<_>>();
    let current_existing_lanes = materialized_lanes
        .iter()
        .filter(|lane| {
            !is_derived_lane_key(&lane.lane_key)
                && materialized_lane_labels.contains(&lane.lane_label)
        })
        .map(|lane| lane.lane_label.as_str())
        .collect::<Vec<_>>();
    expected_existing_lanes == current_existing_lanes
}

pub fn removed_lanes_in_order(
    materialized_lanes: &[LaneRow],
    branch_names: &BTreeSet<String>,
) -> Vec<LaneRow> {
    materialized_lanes
        .iter()
        .filter(|lane| {
            !is_derived_lane_key(&lane.lane_key) && !branch_names.contains(&lane.lane_label)
        })
        .cloned()
        .collect()
}

pub fn removed_lane_count_before(removed_lanes: &[LaneRow], lane_y: i32) -> i32 {
    removed_lanes
        .iter()
        .filter(|removed| removed.lane_y < lane_y)
        .count() as i32
}

pub fn edge_corridor_y(source_y: i32, target_y: i32, route_slot: i32) -> i32 {
    let base_y = match target_y.cmp(&source_y) {
        std::cmp::Ordering::Less => source_y - GRAPH_LANE_HEIGHT / 2,
        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => source_y + GRAPH_LANE_HEIGHT / 2,
    };
    let offset = route_slot_offset(route_slot);
    (base_y + offset).max(16)
}

pub fn route_slot_offset(route_slot: i32) -> i32 {
    let magnitude = (route_slot + 1) / 2;
    let direction = if route_slot % 2 == 0 { 1 } else { -1 };

    magnitude.min(4) * EDGE_ROUTE_STEP * direction
}

pub fn primary_incoming_port_offset(count: usize, index: usize) -> f64 {
    (index as f64 - (count as f64 - 1.0) / 2.0) * EDGE_TARGET_PORT_STEP
}

pub fn secondary_incoming_port_offset(index: usize) -> f64 {
    let distance = index / 2 + 1;
    let direction = if index.is_multiple_of(2) { 1.0 } else { -1.0 };
    distance as f64 * EDGE_TARGET_PORT_STEP * direction
}

pub fn edge_kind_query_value(kind: GraphViewportEdgeKind) -> &'static str {
    match kind {
        GraphViewportEdgeKind::PrimaryParent => "primary_parent",
        GraphViewportEdgeKind::Fork => "fork",
        GraphViewportEdgeKind::MergeParent => "merge_parent",
    }
}

pub fn target_port_rebalance_order(kind: &str) -> i32 {
    match kind {
        "primary_parent" => 0,
        "fork" => 1,
        _ => 2,
    }
}

pub fn routed_edge_kind_order(kind: &str) -> i32 {
    match kind {
        "fork" => 0,
        _ => 1,
    }
}

pub fn is_derived_orphan_or_skill_lane(lane_key: &str) -> bool {
    lane_key.starts_with(DERIVED_ORPHAN_LANE_KEY_PREFIX)
        || lane_key.starts_with(DERIVED_SKILL_LANE_KEY_PREFIX)
}

pub fn node_point_on_lane(
    nodes: &[MaterializedTailNodeRow],
    lane_key: &str,
    node_id: &str,
    point: Point,
) -> bool {
    nodes.iter().any(|node| {
        node.lane_key == lane_key
            && node.node_id == node_id
            && node.x == point.x
            && node.y == point.y
    })
}

pub fn node_point_on_lane_suffix(
    nodes: &[MaterializedTailNodeRow],
    lane_key: &str,
    min_x: i32,
    node_id: &str,
    point: Point,
) -> bool {
    nodes.iter().any(|node| {
        node.lane_key == lane_key
            && node.x > min_x
            && node.node_id == node_id
            && node.x == point.x
            && node.y == point.y
    })
}

pub fn node_point_on_derived_lane(
    nodes: &[MaterializedTailNodeRow],
    node_id: &str,
    point: Point,
) -> bool {
    nodes.iter().any(|node| {
        node.lane_key.starts_with("derived:")
            && node.node_id == node_id
            && node.x == point.x
            && node.y == point.y
    })
}

pub fn parse_edge_kind(value: &str) -> crate::Result<GraphViewportEdgeKind> {
    let json = format!("\"{value}\"");
    serde_json::from_str(&json).context(ParseGraphSnapshotStoreValueSnafu {
        column: "console_graph_edge_routes.edge_kind",
    })
}
