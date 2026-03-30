use std::collections::{HashMap, HashSet};

use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::StoreResult as Result;
use crate::error::{
    BranchExistsSnafu, BranchHeadMovedSnafu, BranchNotFoundSnafu, DuplicateMergeParentSnafu,
    InvalidAnchorSnafu, MergeParentMatchesParentSnafu, MissingSessionAnchorSnafu, NotFoundSnafu,
    ParentNotFoundSnafu, RefsNotConnectedSnafu, SessionStateMovedSnafu,
};
use crate::{
    Anchor, AnchorPayload, Kind, NewNode, Node, PauseReason, Role, SessionAnchorPatch, SessionState,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoreState {
    pub nodes: HashMap<String, Node>,
    pub children: HashMap<String, HashSet<String>>,
    pub root: String,
    pub branches: HashMap<String, String>,
    pub sessions: HashMap<String, SessionState>,
}

#[derive(Debug, Clone)]
pub(crate) struct RebasePlan {
    pub branch: String,
    pub expected_old_head: String,
    pub new_head: String,
    pub nodes: Vec<Node>,
}

#[derive(Debug, Clone)]
pub(crate) struct ForkPlan {
    pub head_id: String,
}

impl Default for StoreState {
    fn default() -> Self {
        Self::new()
    }
}

impl StoreState {
    pub fn new() -> Self {
        let root = Node::new(
            String::new(),
            Role::System,
            None,
            Kind::Text("The Big Bang".to_owned()),
            "1970-01-01T00:00:00Z"
                .parse()
                .expect("root timestamp should parse"),
        );
        let root_id = root.id.clone();

        let mut nodes = HashMap::new();
        nodes.insert(root_id.clone(), root);

        Self {
            nodes,
            children: HashMap::new(),
            root: root_id,
            branches: HashMap::new(),
            sessions: HashMap::new(),
        }
    }

    pub fn from_root(root: Node) -> Self {
        let root_id = root.id.clone();
        let mut nodes = HashMap::new();
        nodes.insert(root_id.clone(), root);

        Self {
            nodes,
            children: HashMap::new(),
            root: root_id,
            branches: HashMap::new(),
            sessions: HashMap::new(),
        }
    }

    pub fn root_id(&self) -> &str {
        &self.root
    }

    pub fn root_node(&self) -> &Node {
        self.nodes
            .get(&self.root)
            .expect("root node should always exist")
    }

    pub fn plan_append_node(&self, node: NewNode) -> Result<Node> {
        let node = Node::new(
            node.parent,
            node.role,
            node.metadata,
            node.kind,
            Timestamp::now(),
        );
        self.validate_new_node(&node)?;
        Ok(node)
    }

    pub fn insert_existing_node(&mut self, node: Node) -> Result<String> {
        self.validate_new_node(&node)?;
        self.insert_existing_node_unchecked(node)
    }

    pub fn plan_fork(&self, name: &str, from_ref: &str) -> Result<ForkPlan> {
        ensure!(
            !self.branches.contains_key(name),
            BranchExistsSnafu {
                name: name.to_owned(),
            }
        );
        Ok(ForkPlan {
            head_id: self.resolve_ref_id(from_ref)?.to_owned(),
        })
    }

    pub fn apply_fork(&mut self, name: String, head_id: String) -> Result<()> {
        ensure!(
            !self.branches.contains_key(&name),
            BranchExistsSnafu { name: name.clone() }
        );
        ensure!(
            self.nodes.contains_key(&head_id),
            NotFoundSnafu {
                id: head_id.clone(),
            }
        );
        self.branches.insert(name.clone(), head_id);
        self.sessions.insert(name, SessionState::Active);
        Ok(())
    }

    pub fn apply_set_branch_head(
        &mut self,
        name: String,
        expected_old_head: &str,
        new_head: String,
    ) -> Result<()> {
        let actual = self.get_branch_head(&name)?.to_owned();
        ensure!(
            actual == expected_old_head,
            BranchHeadMovedSnafu {
                name: name.clone(),
                expected: expected_old_head.to_owned(),
                actual,
            }
        );
        ensure!(
            self.nodes.contains_key(&new_head),
            NotFoundSnafu {
                id: new_head.clone(),
            }
        );
        self.branches.insert(name, new_head);
        Ok(())
    }

    pub fn get_branch_head(&self, name: &str) -> Result<&str> {
        self.branches
            .get(name)
            .map(String::as_str)
            .context(BranchNotFoundSnafu {
                name: name.to_owned(),
            })
    }

    pub fn delete_branch(&mut self, name: &str) -> Result<()> {
        self.branches.remove(name).context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })?;
        self.sessions.remove(name).context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })?;
        Ok(())
    }

    pub fn ancestry(&self, head_ref: &str) -> Result<Vec<&Node>> {
        let mut node = self.resolve_ref(head_ref)?;
        let mut ancestry = vec![];

        loop {
            ancestry.push(node);
            if node.is_root() {
                break;
            }

            node = self.nodes.get(&node.parent).context(ParentNotFoundSnafu {
                id: node.parent.clone(),
            })?;
        }

        Ok(ancestry)
    }

    pub fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<&Node>> {
        let base_id = self.resolve_ref_id(base_ref)?;
        let mut node = self.resolve_ref(head_ref)?;

        let mut ans = vec![];
        loop {
            ans.push(node);
            if node.id == base_id {
                break;
            }

            let parent = &node.parent;
            ensure!(
                !parent.is_empty(),
                RefsNotConnectedSnafu {
                    base_ref: base_ref.to_owned(),
                    head_ref: head_ref.to_owned(),
                }
            );

            node = self.nodes.get(parent).context(ParentNotFoundSnafu {
                id: parent.to_owned(),
            })?;
        }

        Ok(ans)
    }

    pub fn get_node(&self, id: &str) -> Result<Node> {
        self.nodes
            .get(id)
            .cloned()
            .context(NotFoundSnafu { id: id.to_owned() })
    }

    pub fn list_session_states(&self) -> HashMap<String, SessionState> {
        self.sessions.clone()
    }

    pub fn get_session_state(&self, name: &str) -> Result<SessionState> {
        self.sessions
            .get(name)
            .cloned()
            .context(BranchNotFoundSnafu {
                name: name.to_owned(),
            })
    }

    pub fn set_session_state(
        &mut self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> Result<SessionState> {
        let state = self.sessions.get(name).context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })?;
        if let Some(expected) = expected {
            ensure!(
                state == expected,
                SessionStateMovedSnafu {
                    name: name.to_owned(),
                    expected: format!("{expected:?}"),
                    actual: format!("{state:?}"),
                }
            );
        }
        self.validate_session_state(&next)?;
        let state = self.sessions.get_mut(name).context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })?;
        *state = next;
        Ok(state.clone())
    }

    pub fn plan_rebase_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
    ) -> Result<RebasePlan> {
        let branch = name.to_owned();
        let expected_old_head = self.get_branch_head(name)?.to_owned();
        let chain_ids = self
            .session_chain_ids(name)?
            .into_iter()
            .rev()
            .collect::<Vec<_>>();
        let session_node = self
            .nodes
            .get(
                chain_ids
                    .first()
                    .expect("session chain should not be empty"),
            )
            .expect("session chain node should exist");
        let session_anchor = match &session_node.kind {
            Kind::Anchor(anchor) => anchor
                .as_session()
                .expect("session chain should start with session anchor"),
            _ => unreachable!("session chain should start with anchor"),
        }
        .clone();

        let mut temp = self.clone();
        let mut previous_new_id = None;
        let mut new_head = String::new();
        let mut nodes = Vec::with_capacity(chain_ids.len());

        for (index, node_id) in chain_ids.into_iter().enumerate() {
            let node = self
                .nodes
                .get(&node_id)
                .cloned()
                .context(NotFoundSnafu { id: node_id })?;
            let parent = previous_new_id
                .clone()
                .unwrap_or_else(|| node.parent.clone());
            let kind = if index == 0 {
                let Kind::Anchor(anchor) = &node.kind else {
                    unreachable!("session chain should start with anchor");
                };
                Kind::Anchor(Anchor::session(
                    anchor.merge_parents().to_vec(),
                    session_anchor.apply_patch(patch),
                ))
            } else {
                node.kind.clone()
            };
            let new_node = Node::new(parent, node.role, node.metadata, kind, node.created_at);
            temp.insert_existing_node(new_node.clone())?;
            previous_new_id = Some(new_node.id.clone());
            new_head = new_node.id.clone();
            nodes.push(new_node);
        }

        temp.apply_set_branch_head(branch.clone(), &expected_old_head, new_head.clone())?;

        Ok(RebasePlan {
            branch,
            expected_old_head,
            new_head,
            nodes,
        })
    }

    fn session_chain_ids(&self, reference: &str) -> Result<Vec<String>> {
        let branch = reference.to_owned();
        let mut node = self.resolve_ref(reference)?;
        let mut chain_ids = vec![];

        loop {
            chain_ids.push(node.id.clone());
            if matches!(
                node.kind,
                Kind::Anchor(Anchor {
                    payload: AnchorPayload::Session(_),
                    ..
                })
            ) {
                break;
            }

            ensure!(
                !node.is_root(),
                MissingSessionAnchorSnafu {
                    branch: branch.clone(),
                }
            );

            node = self.nodes.get(&node.parent).context(ParentNotFoundSnafu {
                id: node.parent.clone(),
            })?;
        }

        Ok(chain_ids)
    }

    fn validate_new_node(&self, node: &Node) -> Result<()> {
        ensure!(
            self.nodes.contains_key(&node.parent),
            ParentNotFoundSnafu {
                id: node.parent.clone()
            }
        );
        self.validate_anchor_merge_parents(&node.parent, &node.kind)?;
        Ok(())
    }

    pub fn validate_session_records(&self) -> Result<()> {
        for (branch, state) in &self.sessions {
            self.get_branch_head(branch)?;
            self.validate_session_state(state)?;
        }

        for branch in self.branches.keys() {
            ensure!(
                self.sessions.contains_key(branch),
                BranchNotFoundSnafu {
                    name: branch.clone(),
                }
            );
        }

        Ok(())
    }

    fn insert_existing_node_unchecked(&mut self, node: Node) -> Result<String> {
        let id = node.id.clone();

        for parent in parent_ids(&node) {
            self.children
                .entry(parent.to_owned())
                .or_default()
                .insert(id.clone());
        }
        self.nodes.insert(id.clone(), node);

        Ok(id)
    }

    fn resolve_ref_id<'a>(&'a self, reference: &str) -> Result<&'a str> {
        Ok(&self.resolve_ref(reference)?.id)
    }

    fn resolve_ref<'a>(&'a self, reference: &str) -> Result<&'a Node> {
        if let Some(node) = self.nodes.get(reference) {
            return Ok(node);
        }

        if let Some(head_id) = self.branches.get(reference) {
            return self.nodes.get(head_id).context(NotFoundSnafu {
                id: head_id.clone(),
            });
        }

        ensure!(
            is_node_id(reference),
            BranchNotFoundSnafu {
                name: reference.to_owned(),
            }
        );

        NotFoundSnafu {
            id: reference.to_owned(),
        }
        .fail()
    }

    fn validate_anchor_merge_parents(&self, parent: &str, kind: &Kind) -> Result<()> {
        let Kind::Anchor(anchor) = kind else {
            return Ok(());
        };

        let mut seen = HashSet::new();
        for merge_parent in anchor.merge_parents() {
            ensure!(
                merge_parent != parent,
                MergeParentMatchesParentSnafu {
                    id: merge_parent.clone(),
                }
            );
            ensure!(
                seen.insert(merge_parent.as_str()),
                DuplicateMergeParentSnafu {
                    id: merge_parent.clone(),
                }
            );
            ensure!(
                self.nodes.contains_key(merge_parent),
                ParentNotFoundSnafu {
                    id: merge_parent.clone(),
                }
            );
        }

        Ok(())
    }

    fn validate_session_state(&self, state: &SessionState) -> Result<()> {
        match state {
            SessionState::Active => Ok(()),
            SessionState::Attached {
                target_branch,
                base_head_id,
            } => self.validate_ref_on_branch(target_branch, base_head_id),
            SessionState::Paused {
                target_branch,
                reason,
            } => match reason {
                PauseReason::Merged { merged_anchor_id } => {
                    self.validate_anchor_on_branch(target_branch, merged_anchor_id)
                }
                PauseReason::Closed => {
                    if target_branch.is_empty() {
                        return Ok(());
                    }
                    self.get_branch_head(target_branch).map(|_| ())
                }
            },
        }
    }

    fn validate_ref_on_branch(&self, branch: &str, node_id: &str) -> Result<()> {
        self.get_branch_head(branch)?;
        self.nodes.get(node_id).context(NotFoundSnafu {
            id: node_id.to_owned(),
        })?;
        let visible = self
            .ancestry(branch)?
            .into_iter()
            .any(|node| node.id == node_id);
        ensure!(
            visible,
            RefsNotConnectedSnafu {
                base_ref: node_id.to_owned(),
                head_ref: branch.to_owned(),
            }
        );
        Ok(())
    }

    fn validate_anchor_on_branch(&self, branch: &str, node_id: &str) -> Result<()> {
        let node = self.nodes.get(node_id).context(NotFoundSnafu {
            id: node_id.to_owned(),
        })?;
        ensure!(
            matches!(node.kind, Kind::Anchor(_)),
            InvalidAnchorSnafu {
                id: node_id.to_owned()
            }
        );
        self.validate_ref_on_branch(branch, node_id)
    }
}

fn is_node_id(reference: &str) -> bool {
    reference.len() == 64 && reference.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn parent_ids(node: &Node) -> Vec<&str> {
    let mut parents = vec![node.parent.as_str()];
    if let Kind::Anchor(anchor) = &node.kind {
        parents.extend(anchor.merge_parents().iter().map(String::as_str));
    }
    parents
}
