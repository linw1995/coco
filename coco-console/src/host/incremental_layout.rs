use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayoutPoint {
    pub x: i32,
    pub y: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CubicRoute {
    pub source: LayoutPoint,
    pub control_1: LayoutPoint,
    pub control_2: LayoutPoint,
    pub target: LayoutPoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StableEdgeKind {
    Primary,
    Merge,
    Shadow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StableLayoutConfig {
    pub padding: i32,
    pub rank_step: i32,
    pub row_step: i32,
    pub node_radius: i32,
    pub edge_source_padding: i32,
    pub edge_target_padding: i32,
    pub edge_port_step: i32,
    pub edge_port_range: i32,
    pub edge_control_ratio_percent: i32,
    pub edge_min_control_distance: i32,
}

impl Default for StableLayoutConfig {
    fn default() -> Self {
        Self {
            padding: 56,
            rank_step: 112,
            row_step: 72,
            node_radius: 18,
            edge_source_padding: 2,
            edge_target_padding: 6,
            edge_port_step: 4,
            edge_port_range: 12,
            edge_control_ratio_percent: 45,
            edge_min_control_distance: 24,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyNode {
    pub node_id: String,
    pub parent_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePlacement {
    pub node_id: String,
    pub rank: usize,
    pub row: usize,
    pub point: LayoutPoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableEdge {
    pub edge_key: String,
    pub kind: StableEdgeKind,
    pub source_id: String,
    pub target_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndpointPortSlots {
    pub source: usize,
    pub target: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortAssignment {
    pub edge: StableEdge,
    pub slots: EndpointPortSlots,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StableLayoutError {
    DuplicateNode {
        node_id: String,
    },
    MissingNode {
        node_id: String,
    },
    MissingParent {
        node_id: String,
        parent_id: String,
    },
    SlotOccupied {
        rank: usize,
        row: usize,
        owner: String,
    },
    ExistingRankTooLow {
        node_id: String,
        current_rank: usize,
        required_rank: usize,
    },
    DuplicateEdge {
        edge_key: String,
    },
    PortSlotOccupied {
        node_id: String,
        slot: usize,
        owner: String,
    },
    Cycle {
        node_ids: Vec<String>,
    },
}

impl fmt::Display for StableLayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateNode { node_id } => write!(formatter, "duplicate node {node_id}"),
            Self::MissingNode { node_id } => write!(formatter, "missing node {node_id}"),
            Self::MissingParent { node_id, parent_id } => {
                write!(
                    formatter,
                    "node {node_id} references missing parent {parent_id}"
                )
            }
            Self::SlotOccupied { rank, row, owner } => {
                write!(formatter, "rank {rank} row {row} is occupied by {owner}")
            }
            Self::ExistingRankTooLow {
                node_id,
                current_rank,
                required_rank,
            } => write!(
                formatter,
                "node {node_id} has rank {current_rank}, but its parents require rank {required_rank}",
            ),
            Self::DuplicateEdge { edge_key } => write!(formatter, "duplicate edge {edge_key}"),
            Self::PortSlotOccupied {
                node_id,
                slot,
                owner,
            } => write!(
                formatter,
                "endpoint {node_id} port slot {slot} is occupied by {owner}",
            ),
            Self::Cycle { node_ids } => {
                write!(
                    formatter,
                    "rank propagation found a cycle involving {node_ids:?}"
                )
            }
        }
    }
}

impl Error for StableLayoutError {}

#[derive(Debug, Clone)]
pub struct StableDagLayout {
    config: StableLayoutConfig,
    active: BTreeMap<String, NodePlacement>,
    tombstones: BTreeMap<String, NodePlacement>,
    occupied: BTreeMap<usize, BTreeMap<usize, String>>,
}

impl StableDagLayout {
    pub fn new(config: StableLayoutConfig) -> Self {
        Self {
            config,
            active: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            occupied: BTreeMap::new(),
        }
    }

    pub fn config(&self) -> StableLayoutConfig {
        self.config
    }

    pub fn restore_active(&mut self, placement: NodePlacement) -> Result<(), StableLayoutError> {
        self.restore_placement(placement, false)
    }

    pub fn restore_tombstone(&mut self, placement: NodePlacement) -> Result<(), StableLayoutError> {
        self.restore_placement(placement, true)
    }

    pub fn placement(&self, node_id: &str) -> Option<&NodePlacement> {
        self.active
            .get(node_id)
            .or_else(|| self.tombstones.get(node_id))
    }

    pub fn active_placement(&self, node_id: &str) -> Option<&NodePlacement> {
        self.active.get(node_id)
    }

    pub fn is_tombstone(&self, node_id: &str) -> bool {
        self.tombstones.contains_key(node_id)
    }

    pub fn place_ready(&mut self, node: ReadyNode) -> Result<NodePlacement, StableLayoutError> {
        let parent_ids = node.parent_ids.into_iter().collect::<BTreeSet<_>>();
        let mut parent_placements = Vec::with_capacity(parent_ids.len());
        for parent_id in &parent_ids {
            let parent =
                self.placement(parent_id)
                    .ok_or_else(|| StableLayoutError::MissingParent {
                        node_id: node.node_id.clone(),
                        parent_id: parent_id.clone(),
                    })?;
            parent_placements.push(parent);
        }
        let required_rank = parent_placements
            .iter()
            .map(|parent| parent.rank.saturating_add(1))
            .max()
            .unwrap_or(0);

        if let Some(existing) = self.placement(&node.node_id).cloned() {
            if existing.rank < required_rank {
                return Err(StableLayoutError::ExistingRankTooLow {
                    node_id: node.node_id,
                    current_rank: existing.rank,
                    required_rank,
                });
            }
            if self.tombstones.remove(&existing.node_id).is_some() {
                self.active
                    .insert(existing.node_id.clone(), existing.clone());
            }
            return Ok(existing);
        }

        let desired_row = median_row(&parent_placements);
        let row = self.nearest_free_row(required_rank, desired_row);
        let placement = NodePlacement {
            node_id: node.node_id,
            rank: required_rank,
            row,
            point: self.point(required_rank, row),
        };
        self.occupy(&placement)?;
        self.active
            .insert(placement.node_id.clone(), placement.clone());
        Ok(placement)
    }

    pub fn place_ready_batch(
        &mut self,
        nodes: impl IntoIterator<Item = ReadyNode>,
    ) -> Result<Vec<NodePlacement>, StableLayoutError> {
        nodes
            .into_iter()
            .map(|node| self.place_ready(node))
            .collect()
    }

    pub fn retire(&mut self, node_id: &str) -> Option<NodePlacement> {
        let placement = self.active.remove(node_id)?;
        self.tombstones
            .insert(node_id.to_owned(), placement.clone());
        Some(placement)
    }

    pub fn active_nodes(&self) -> impl Iterator<Item = &NodePlacement> {
        self.active.values()
    }

    pub fn tombstones(&self) -> impl Iterator<Item = &NodePlacement> {
        self.tombstones.values()
    }

    fn restore_placement(
        &mut self,
        placement: NodePlacement,
        tombstone: bool,
    ) -> Result<(), StableLayoutError> {
        if let Some(existing) = self.placement(&placement.node_id) {
            if existing == &placement && self.is_tombstone(&placement.node_id) == tombstone {
                return Ok(());
            }
            return Err(StableLayoutError::DuplicateNode {
                node_id: placement.node_id,
            });
        }
        self.occupy(&placement)?;
        let placements = if tombstone {
            &mut self.tombstones
        } else {
            &mut self.active
        };
        placements.insert(placement.node_id.clone(), placement);
        Ok(())
    }

    fn occupy(&mut self, placement: &NodePlacement) -> Result<(), StableLayoutError> {
        let rows = self.occupied.entry(placement.rank).or_default();
        if let Some(owner) = rows.get(&placement.row) {
            return Err(StableLayoutError::SlotOccupied {
                rank: placement.rank,
                row: placement.row,
                owner: owner.clone(),
            });
        }
        rows.insert(placement.row, placement.node_id.clone());
        Ok(())
    }

    fn nearest_free_row(&self, rank: usize, desired: usize) -> usize {
        let Some(occupied) = self.occupied.get(&rank) else {
            return desired;
        };
        for distance in 0..=occupied.len().saturating_add(1) {
            if let Some(lower) = desired.checked_sub(distance)
                && !occupied.contains_key(&lower)
            {
                return lower;
            }
            if distance > 0
                && let Some(upper) = desired.checked_add(distance)
                && !occupied.contains_key(&upper)
            {
                return upper;
            }
        }
        unreachable!("a finite occupied row set must have a nearest free row")
    }

    fn point(&self, rank: usize, row: usize) -> LayoutPoint {
        LayoutPoint {
            x: coordinate(self.config.padding, rank, self.config.rank_step),
            y: coordinate(self.config.padding, row, self.config.row_step),
        }
    }
}

impl Default for StableDagLayout {
    fn default() -> Self {
        Self::new(StableLayoutConfig::default())
    }
}

#[derive(Debug, Clone, Default)]
pub struct StablePortAllocator {
    assignments: BTreeMap<String, PortAssignment>,
    outgoing: BTreeMap<String, BTreeMap<usize, String>>,
    incoming: BTreeMap<String, BTreeMap<usize, String>>,
}

impl StablePortAllocator {
    pub fn restore(&mut self, assignment: PortAssignment) -> Result<(), StableLayoutError> {
        if let Some(existing) = self.assignments.get(&assignment.edge.edge_key) {
            if existing == &assignment {
                return Ok(());
            }
            return Err(StableLayoutError::DuplicateEdge {
                edge_key: assignment.edge.edge_key,
            });
        }
        ensure_port_available(
            &self.outgoing,
            &assignment.edge.source_id,
            assignment.slots.source,
        )?;
        ensure_port_available(
            &self.incoming,
            &assignment.edge.target_id,
            assignment.slots.target,
        )?;
        reserve_port(
            &mut self.outgoing,
            &assignment.edge.source_id,
            assignment.slots.source,
            &assignment.edge.edge_key,
        );
        reserve_port(
            &mut self.incoming,
            &assignment.edge.target_id,
            assignment.slots.target,
            &assignment.edge.edge_key,
        );
        self.assignments
            .insert(assignment.edge.edge_key.clone(), assignment);
        Ok(())
    }

    pub fn assign(&mut self, edge: StableEdge) -> Result<PortAssignment, StableLayoutError> {
        if let Some(existing) = self.assignments.get(&edge.edge_key) {
            if existing.edge == edge {
                return Ok(existing.clone());
            }
            return Err(StableLayoutError::DuplicateEdge {
                edge_key: edge.edge_key,
            });
        }
        let slots = EndpointPortSlots {
            source: first_free_port(self.outgoing.get(&edge.source_id)),
            target: first_free_port(self.incoming.get(&edge.target_id)),
        };
        let assignment = PortAssignment { edge, slots };
        self.restore(assignment.clone())?;
        Ok(assignment)
    }

    pub fn assignment(&self, edge_key: &str) -> Option<&PortAssignment> {
        self.assignments.get(edge_key)
    }
}

pub fn route_edge(
    source_center: LayoutPoint,
    target_center: LayoutPoint,
    slots: EndpointPortSlots,
    config: StableLayoutConfig,
) -> CubicRoute {
    let source = LayoutPoint {
        x: source_center
            .x
            .saturating_add(config.node_radius)
            .saturating_add(config.edge_source_padding),
        y: source_center
            .y
            .saturating_add(port_offset(slots.source, config)),
    };
    let target = LayoutPoint {
        x: target_center
            .x
            .saturating_sub(config.node_radius)
            .saturating_sub(config.edge_target_padding),
        y: target_center
            .y
            .saturating_add(port_offset(slots.target, config)),
    };
    let horizontal_distance = target
        .x
        .saturating_sub(source.x)
        .max(config.edge_min_control_distance);
    let scaled_distance = i64::from(horizontal_distance)
        .saturating_mul(i64::from(config.edge_control_ratio_percent))
        / 100;
    let control_distance = i32::try_from(scaled_distance)
        .unwrap_or(i32::MAX)
        .max(config.edge_min_control_distance);
    let control_1 = LayoutPoint {
        x: source
            .x
            .saturating_add(control_distance)
            .min(target.x.saturating_sub(8)),
        y: source.y,
    };
    let control_2 = LayoutPoint {
        x: target
            .x
            .saturating_sub(control_distance)
            .max(source.x.saturating_add(8)),
        y: target.y,
    };
    CubicRoute {
        source,
        control_1,
        control_2,
        target,
    }
}

pub fn propagate_rank_increases(
    ranks: &mut BTreeMap<String, usize>,
    incoming: &BTreeMap<String, Vec<String>>,
    outgoing: &BTreeMap<String, Vec<String>>,
    seeds: &[String],
) -> Result<BTreeSet<String>, StableLayoutError> {
    let mut affected = BTreeSet::new();
    let mut pending = VecDeque::new();
    for seed in seeds {
        if !ranks.contains_key(seed) {
            return Err(StableLayoutError::MissingNode {
                node_id: seed.clone(),
            });
        }
        if affected.insert(seed.clone()) {
            pending.push_back(seed.clone());
        }
    }
    while let Some(node_id) = pending.pop_front() {
        for child_id in unique_neighbors(outgoing.get(&node_id)) {
            if !ranks.contains_key(child_id) {
                return Err(StableLayoutError::MissingNode {
                    node_id: child_id.clone(),
                });
            }
            if affected.insert(child_id.clone()) {
                pending.push_back(child_id.clone());
            }
        }
    }

    let mut indegree = affected
        .iter()
        .map(|node_id| (node_id.clone(), 0_usize))
        .collect::<BTreeMap<_, _>>();
    for node_id in &affected {
        for parent_id in unique_neighbors(incoming.get(node_id)) {
            if !ranks.contains_key(parent_id) {
                return Err(StableLayoutError::MissingNode {
                    node_id: parent_id.clone(),
                });
            }
            if affected.contains(parent_id) {
                *indegree
                    .get_mut(node_id)
                    .expect("affected nodes must have indegree") += 1;
            }
        }
    }

    let mut ready = indegree
        .iter()
        .filter_map(|(node_id, degree)| (*degree == 0).then_some(node_id.clone()))
        .collect::<BTreeSet<_>>();
    let mut changed = BTreeSet::new();
    let mut visited = 0_usize;
    while let Some(node_id) = ready.pop_first() {
        visited += 1;
        let required_rank = unique_neighbors(incoming.get(&node_id))
            .map(|parent_id| {
                ranks
                    .get(parent_id)
                    .copied()
                    .expect("validated parents must have ranks")
                    .saturating_add(1)
            })
            .max()
            .unwrap_or(0);
        let rank = ranks
            .get_mut(&node_id)
            .expect("affected nodes must have ranks");
        if *rank < required_rank {
            *rank = required_rank;
            changed.insert(node_id.clone());
        }
        for child_id in unique_neighbors(outgoing.get(&node_id)) {
            if !affected.contains(child_id) {
                continue;
            }
            let child_indegree = indegree
                .get_mut(child_id)
                .expect("affected children must have indegree");
            *child_indegree -= 1;
            if *child_indegree == 0 {
                ready.insert(child_id.clone());
            }
        }
    }
    if visited != affected.len() {
        return Err(StableLayoutError::Cycle {
            node_ids: indegree
                .into_iter()
                .filter_map(|(node_id, degree)| (degree > 0).then_some(node_id))
                .collect(),
        });
    }
    Ok(changed)
}

fn median_row(parents: &[&NodePlacement]) -> usize {
    if parents.is_empty() {
        return 0;
    }
    let mut rows = parents.iter().map(|parent| parent.row).collect::<Vec<_>>();
    rows.sort_unstable();
    rows[rows.len() / 2]
}

fn coordinate(padding: i32, index: usize, step: i32) -> i32 {
    let index = i32::try_from(index).unwrap_or(i32::MAX);
    padding.saturating_add(index.saturating_mul(step))
}

fn ensure_port_available(
    ports: &BTreeMap<String, BTreeMap<usize, String>>,
    node_id: &str,
    slot: usize,
) -> Result<(), StableLayoutError> {
    if let Some(owner) = ports.get(node_id).and_then(|endpoint| endpoint.get(&slot)) {
        return Err(StableLayoutError::PortSlotOccupied {
            node_id: node_id.to_owned(),
            slot,
            owner: owner.clone(),
        });
    }
    Ok(())
}

fn reserve_port(
    ports: &mut BTreeMap<String, BTreeMap<usize, String>>,
    node_id: &str,
    slot: usize,
    edge_key: &str,
) {
    ports
        .entry(node_id.to_owned())
        .or_default()
        .insert(slot, edge_key.to_owned());
}

fn first_free_port(ports: Option<&BTreeMap<usize, String>>) -> usize {
    let Some(ports) = ports else {
        return 0;
    };
    (0..=ports.len())
        .find(|slot| !ports.contains_key(slot))
        .expect("a finite port set must have a free slot")
}

fn port_offset(slot: usize, config: StableLayoutConfig) -> i32 {
    if slot == 0 {
        return 0;
    }
    let magnitude = i32::try_from(slot.div_ceil(2))
        .unwrap_or(i32::MAX)
        .saturating_mul(config.edge_port_step.max(0));
    let signed = if slot % 2 == 1 {
        magnitude.saturating_neg()
    } else {
        magnitude
    };
    let range = config.edge_port_range.max(0);
    signed.clamp(range.saturating_neg(), range)
}

fn unique_neighbors(neighbors: Option<&Vec<String>>) -> impl Iterator<Item = &String> {
    neighbors
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>()
        .into_iter()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready(node_id: &str, parent_ids: &[&str]) -> ReadyNode {
        ReadyNode {
            node_id: node_id.to_owned(),
            parent_ids: parent_ids
                .iter()
                .map(|parent| (*parent).to_owned())
                .collect(),
        }
    }

    fn edge(edge_key: &str, source_id: &str, target_id: &str) -> StableEdge {
        StableEdge {
            edge_key: edge_key.to_owned(),
            kind: StableEdgeKind::Primary,
            source_id: source_id.to_owned(),
            target_id: target_id.to_owned(),
        }
    }

    #[test]
    fn stable_layout_errors_have_actionable_messages() {
        let cases = [
            (
                StableLayoutError::DuplicateNode {
                    node_id: "duplicate".to_owned(),
                },
                "duplicate node duplicate",
            ),
            (
                StableLayoutError::MissingNode {
                    node_id: "missing".to_owned(),
                },
                "missing node missing",
            ),
            (
                StableLayoutError::MissingParent {
                    node_id: "child".to_owned(),
                    parent_id: "parent".to_owned(),
                },
                "node child references missing parent parent",
            ),
            (
                StableLayoutError::SlotOccupied {
                    rank: 2,
                    row: 3,
                    owner: "owner".to_owned(),
                },
                "rank 2 row 3 is occupied by owner",
            ),
            (
                StableLayoutError::ExistingRankTooLow {
                    node_id: "child".to_owned(),
                    current_rank: 1,
                    required_rank: 4,
                },
                "node child has rank 1, but its parents require rank 4",
            ),
            (
                StableLayoutError::DuplicateEdge {
                    edge_key: "edge".to_owned(),
                },
                "duplicate edge edge",
            ),
            (
                StableLayoutError::PortSlotOccupied {
                    node_id: "endpoint".to_owned(),
                    slot: 5,
                    owner: "edge".to_owned(),
                },
                "endpoint endpoint port slot 5 is occupied by edge",
            ),
            (
                StableLayoutError::Cycle {
                    node_ids: vec!["a".to_owned(), "b".to_owned()],
                },
                "rank propagation found a cycle involving [\"a\", \"b\"]",
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn ready_nodes_use_longest_parent_rank_and_nearest_free_median_row() {
        let mut layout = StableDagLayout::default();
        let left = layout.place_ready(ready("left", &[])).unwrap();
        let right = layout.place_ready(ready("right", &[])).unwrap();
        let left_child = layout.place_ready(ready("left-child", &["left"])).unwrap();
        let merge = layout
            .place_ready(ready("merge", &["left", "right"]))
            .unwrap();

        assert_eq!((left.rank, left.row), (0, 0));
        assert_eq!((right.rank, right.row), (0, 1));
        assert_eq!((left_child.rank, left_child.row), (1, 0));
        assert_eq!((merge.rank, merge.row), (1, 1));
        assert_eq!(merge.point, LayoutPoint { x: 168, y: 128 });
    }

    #[test]
    fn restored_nodes_and_tombstones_keep_their_slots() {
        let mut layout = StableDagLayout::default();
        let hint = NodePlacement {
            node_id: "hint".to_owned(),
            rank: 0,
            row: 0,
            point: LayoutPoint { x: 70, y: 80 },
        };
        let tombstone = NodePlacement {
            node_id: "old-child".to_owned(),
            rank: 1,
            row: 0,
            point: LayoutPoint { x: 182, y: 80 },
        };
        layout.restore_active(hint.clone()).unwrap();
        layout.restore_tombstone(tombstone.clone()).unwrap();

        assert_eq!(layout.place_ready(ready("hint", &[])).unwrap(), hint);
        let child = layout.place_ready(ready("new-child", &["hint"])).unwrap();
        assert_eq!((child.rank, child.row), (1, 1));

        let restored = layout.place_ready(ready("old-child", &["hint"])).unwrap();
        assert_eq!(restored, tombstone);
        assert!(!layout.is_tombstone("old-child"));
    }

    #[test]
    fn retiring_a_node_keeps_its_slot_reserved() {
        let mut layout = StableDagLayout::default();
        layout.place_ready(ready("root", &[])).unwrap();
        let first = layout.place_ready(ready("first", &["root"])).unwrap();
        layout.retire("first").unwrap();
        let second = layout.place_ready(ready("second", &["root"])).unwrap();

        assert_eq!((first.rank, first.row), (1, 0));
        assert_eq!((second.rank, second.row), (1, 1));
    }

    #[test]
    fn existing_hints_reject_new_rank_constraints_without_moving() {
        let mut layout = StableDagLayout::default();
        layout
            .restore_active(NodePlacement {
                node_id: "parent".to_owned(),
                rank: 3,
                row: 0,
                point: LayoutPoint { x: 392, y: 56 },
            })
            .unwrap();
        layout
            .restore_active(NodePlacement {
                node_id: "child".to_owned(),
                rank: 1,
                row: 0,
                point: LayoutPoint { x: 168, y: 56 },
            })
            .unwrap();

        assert!(matches!(
            layout.place_ready(ready("child", &["parent"])),
            Err(StableLayoutError::ExistingRankTooLow {
                current_rank: 1,
                required_rank: 4,
                ..
            })
        ));
        assert_eq!(layout.active_placement("child").unwrap().rank, 1);
    }

    #[test]
    fn rank_increases_propagate_only_through_descendants() {
        let mut ranks = BTreeMap::from([
            ("a".to_owned(), 5),
            ("b".to_owned(), 1),
            ("c".to_owned(), 2),
            ("other".to_owned(), 9),
        ]);
        let incoming = BTreeMap::from([
            ("a".to_owned(), Vec::new()),
            ("b".to_owned(), vec!["a".to_owned()]),
            ("c".to_owned(), vec!["b".to_owned()]),
            ("other".to_owned(), Vec::new()),
        ]);
        let outgoing = BTreeMap::from([
            ("a".to_owned(), vec!["b".to_owned()]),
            ("b".to_owned(), vec!["c".to_owned()]),
            ("c".to_owned(), Vec::new()),
            ("other".to_owned(), Vec::new()),
        ]);

        let changed =
            propagate_rank_increases(&mut ranks, &incoming, &outgoing, &["b".to_owned()]).unwrap();

        assert_eq!(changed, BTreeSet::from(["b".to_owned(), "c".to_owned()]));
        assert_eq!(ranks["b"], 6);
        assert_eq!(ranks["c"], 7);
        assert_eq!(ranks["other"], 9);
    }

    #[test]
    fn rank_propagation_rejects_cycles() {
        let mut ranks = BTreeMap::from([("a".to_owned(), 0), ("b".to_owned(), 0)]);
        let incoming = BTreeMap::from([
            ("a".to_owned(), vec!["b".to_owned()]),
            ("b".to_owned(), vec!["a".to_owned()]),
        ]);
        let outgoing = BTreeMap::from([
            ("a".to_owned(), vec!["b".to_owned()]),
            ("b".to_owned(), vec!["a".to_owned()]),
        ]);

        assert!(matches!(
            propagate_rank_increases(&mut ranks, &incoming, &outgoing, &["a".to_owned()]),
            Err(StableLayoutError::Cycle { .. })
        ));
    }

    #[test]
    fn endpoint_port_slots_never_rebalance_existing_edges() {
        let mut ports = StablePortAllocator::default();
        let first = ports.assign(edge("first", "source", "target-a")).unwrap();
        let second = ports.assign(edge("second", "source", "target-b")).unwrap();
        let incoming = ports.assign(edge("incoming", "other", "target-a")).unwrap();

        assert_eq!(
            first.slots,
            EndpointPortSlots {
                source: 0,
                target: 0
            }
        );
        assert_eq!(second.slots.source, 1);
        assert_eq!(incoming.slots.target, 1);
        assert_eq!(ports.assignment("first").unwrap(), &first);
    }

    #[test]
    fn cubic_routes_use_stable_endpoint_slots() {
        let config = StableLayoutConfig::default();
        let route = route_edge(
            LayoutPoint { x: 56, y: 56 },
            LayoutPoint { x: 168, y: 128 },
            EndpointPortSlots {
                source: 1,
                target: 2,
            },
            config,
        );

        assert_eq!(route.source, LayoutPoint { x: 76, y: 52 });
        assert_eq!(route.target, LayoutPoint { x: 144, y: 132 });
        assert!(route.source.x < route.control_1.x);
        assert!(route.control_1.x < route.control_2.x);
        assert!(route.control_2.x < route.target.x);
    }
}
