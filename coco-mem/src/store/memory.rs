use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use jiff::Timestamp;
use snafu::prelude::*;

use crate::{Kind, NewNode, Node, Role};

#[derive(Debug, Clone)]
pub struct Store {
    pub nodes: HashMap<String, Node>,
    pub children: HashMap<String, HashSet<String>>,

    pub root: String,
    pub branches: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct SharedStore {
    inner: Arc<RwLock<Store>>,
}

#[derive(Snafu, Debug, PartialEq, Eq)]
pub enum Error {
    #[snafu(display("Parent ID {id:?} not found"))]
    ParentNotFound { id: String },

    #[snafu(display("Merge parent ID {id:?} is duplicated"))]
    DuplicateMergeParent { id: String },

    #[snafu(display("Merge parent ID {id:?} matches the primary parent"))]
    MergeParentMatchesParent { id: String },

    #[snafu(display("ID {id:?} not found"))]
    NotFound { id: String },

    #[snafu(display("Branch {name:?} not found"))]
    BranchNotFound { name: String },

    #[snafu(display("Branch {name:?} already exists"))]
    BranchExists { name: String },

    #[snafu(display("Branch {name:?} moved from {expected:?} to {actual:?}"))]
    BranchHeadMoved {
        name: String,
        expected: String,
        actual: String,
    },

    #[snafu(display("Ref {base_ref:?} is not an ancestor of {head_ref:?}"))]
    RefsNotConnected { base_ref: String, head_ref: String },
}

type Result<T> = std::result::Result<T, Error>;

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

impl Store {
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
        }
    }

    pub fn root_id(&self) -> &str {
        &self.root
    }

    pub fn append(&mut self, node: NewNode) -> Result<String> {
        ensure!(
            self.nodes.contains_key(&node.parent),
            ParentNotFoundSnafu {
                id: node.parent.clone()
            }
        );
        self.validate_anchor_merge_parents(&node.parent, &node.kind)?;

        let node = Node::new(
            node.parent,
            node.role,
            node.turn,
            node.kind,
            Timestamp::now(),
        );
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

    pub fn fork(&mut self, name: impl Into<String>, from_ref: &str) -> Result<String> {
        let name = name.into();
        ensure!(
            !self.branches.contains_key(&name),
            BranchExistsSnafu { name: name.clone() }
        );
        let head_id = self.resolve_ref_id(from_ref)?.to_owned();
        self.branches.insert(name, head_id.clone());
        Ok(head_id)
    }

    pub fn get_branch_head(&self, name: &str) -> Result<&str> {
        self.branches
            .get(name)
            .map(String::as_str)
            .context(BranchNotFoundSnafu {
                name: name.to_owned(),
            })
    }

    pub fn set_branch_head(
        &mut self,
        name: &str,
        expected_old_head: &str,
        new_head: &str,
    ) -> Result<()> {
        let actual = self.get_branch_head(name)?.to_owned();
        ensure!(
            actual == expected_old_head,
            BranchHeadMovedSnafu {
                name: name.to_owned(),
                expected: expected_old_head.to_owned(),
                actual,
            }
        );
        ensure!(
            self.nodes.contains_key(new_head),
            NotFoundSnafu {
                id: new_head.to_owned(),
            }
        );
        self.branches.insert(name.to_owned(), new_head.to_owned());
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
}

impl Default for SharedStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedStore {
    pub fn new() -> Self {
        Self::from_store(Store::new())
    }

    pub fn from_store(store: Store) -> Self {
        Self {
            inner: Arc::new(RwLock::new(store)),
        }
    }

    pub fn root_id(&self) -> String {
        self.inner
            .read()
            .expect("store lock poisoned")
            .root_id()
            .to_owned()
    }

    pub fn append(&self, node: NewNode) -> Result<String> {
        self.inner
            .write()
            .expect("store lock poisoned")
            .append(node)
    }

    pub fn fork(&self, name: impl Into<String>, from_ref: &str) -> Result<String> {
        self.inner
            .write()
            .expect("store lock poisoned")
            .fork(name, from_ref)
    }

    pub fn get_branch_head(&self, name: &str) -> Result<String> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_branch_head(name)
            .map(str::to_owned)
    }

    pub fn set_branch_head(
        &self,
        name: &str,
        expected_old_head: &str,
        new_head: &str,
    ) -> Result<()> {
        self.inner
            .write()
            .expect("store lock poisoned")
            .set_branch_head(name, expected_old_head, new_head)
    }

    pub fn ancestry(&self, head_ref: &str) -> Result<Vec<Node>> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .ancestry(head_ref)
            .map(|nodes| nodes.into_iter().cloned().collect())
    }

    pub fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .log(base_ref, head_ref)
            .map(|nodes| nodes.into_iter().cloned().collect())
    }

    pub fn get_node(&self, id: &str) -> Result<Node> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .nodes
            .get(id)
            .cloned()
            .context(NotFoundSnafu { id: id.to_owned() })
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

#[cfg(test)]
mod tests {
    use super::{Error, SharedStore, Store};
    use crate::{Anchor, Kind, NewNode, PromptAnchor, Role, SessionAnchor};
    use serde_json::json;

    fn make_text_node(parent: &str, text: &str) -> NewNode {
        NewNode {
            parent: parent.to_owned(),
            role: Role::User,
            turn: None,
            kind: Kind::Text(text.to_owned()),
        }
    }

    fn make_session_anchor_node(parent: &str) -> NewNode {
        NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            turn: None,
            kind: Kind::Anchor(Anchor::session(
                vec![],
                SessionAnchor {
                    provider: "openai".to_owned(),
                    model: "gpt-5.4".to_owned(),
                    tools: vec![],
                    system_prompt: "system".to_owned(),
                    prompt: "prompt".to_owned(),
                    temperature: Some(0.1),
                    max_tokens: Some(64),
                    additional_params: Some(json!({"reasoning_effort": "low"})),
                },
            )),
        }
    }

    fn make_prompt_anchor_node(parent: &str, merge_parents: &[&str]) -> NewNode {
        NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            turn: None,
            kind: Kind::Anchor(Anchor::prompt(
                merge_parents.iter().map(|id| (*id).to_owned()).collect(),
                PromptAnchor {
                    prompt: "merge prompt".to_owned(),
                },
            )),
        }
    }

    fn make_store_with_root() -> (Store, String) {
        let store = Store::new();
        let root_id = store.root_id().to_owned();

        (store, root_id)
    }

    #[test]
    fn new_store_exposes_root_text_node() {
        let store = Store::new();
        let root = store.nodes.get(store.root_id()).unwrap();

        let Kind::Text(text) = &root.kind else {
            panic!("expected text root node");
        };
        assert_eq!(text, "The Big Bang");
        assert!(root.is_root());
    }

    #[test]
    fn append_inserts_node_and_updates_children_index() {
        let (mut store, root_id) = make_store_with_root();
        let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();

        let child_id = store.append(make_text_node(&session_id, "child")).unwrap();

        let stored = store.nodes.get(&child_id).unwrap();
        assert_eq!(stored.parent, session_id);
        assert!(
            store
                .children
                .get(&stored.parent)
                .unwrap()
                .contains(&child_id)
        );
    }

    #[test]
    fn append_rejects_missing_parent() {
        let (mut store, _) = make_store_with_root();
        let node = make_text_node("missing", "child");

        let err = store.append(node).unwrap_err();

        assert!(matches!(err, Error::ParentNotFound { id } if id == "missing"));
    }

    #[test]
    fn append_prompt_anchor_indexes_merge_parents_as_children() {
        let (mut store, root_id) = make_store_with_root();
        let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
        let merge_parent_id = store
            .append(make_text_node(&session_id, "merge-parent"))
            .unwrap();

        let anchor_id = store
            .append(make_prompt_anchor_node(&session_id, &[&merge_parent_id]))
            .unwrap();

        assert!(
            store
                .children
                .get(&session_id)
                .unwrap()
                .contains(&anchor_id)
        );
        assert!(
            store
                .children
                .get(&merge_parent_id)
                .unwrap()
                .contains(&anchor_id)
        );
    }

    #[test]
    fn append_session_anchor_indexes_merge_parents_as_children() {
        let (mut store, root_id) = make_store_with_root();
        let merge_parent_id = store
            .append(make_text_node(&root_id, "merge-parent"))
            .unwrap();

        let anchor_id = store
            .append(NewNode {
                parent: root_id,
                role: Role::System,
                turn: None,
                kind: Kind::Anchor(Anchor::session(
                    vec![merge_parent_id.clone()],
                    SessionAnchor {
                        provider: "openai".to_owned(),
                        model: "gpt-5.4".to_owned(),
                        tools: vec![],
                        system_prompt: "system".to_owned(),
                        prompt: "prompt".to_owned(),
                        temperature: Some(0.1),
                        max_tokens: Some(64),
                        additional_params: Some(json!({"reasoning_effort": "low"})),
                    },
                )),
            })
            .unwrap();

        assert!(
            store
                .children
                .get(&merge_parent_id)
                .unwrap()
                .contains(&anchor_id)
        );
    }

    #[test]
    fn append_prompt_anchor_rejects_missing_merge_parent() {
        let (mut store, root_id) = make_store_with_root();
        let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
        let node = make_prompt_anchor_node(&session_id, &["missing"]);

        let err = store.append(node).unwrap_err();

        assert!(matches!(err, Error::ParentNotFound { id } if id == "missing"));
    }

    #[test]
    fn append_prompt_anchor_rejects_duplicate_merge_parents() {
        let (mut store, root_id) = make_store_with_root();
        let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
        let merge_parent_id = store
            .append(make_text_node(&session_id, "merge-parent"))
            .unwrap();
        let node = make_prompt_anchor_node(&session_id, &[&merge_parent_id, &merge_parent_id]);

        let err = store.append(node).unwrap_err();

        assert!(matches!(
            err,
            Error::DuplicateMergeParent { id } if id == merge_parent_id
        ));
    }

    #[test]
    fn append_prompt_anchor_rejects_merge_parent_matching_primary_parent() {
        let (mut store, root_id) = make_store_with_root();
        let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
        let node = make_prompt_anchor_node(&session_id, &[&session_id]);

        let err = store.append(node).unwrap_err();

        assert!(matches!(
            err,
            Error::MergeParentMatchesParent { id } if id == session_id
        ));
    }

    #[test]
    fn append_prompt_anchor_allows_merge_parent_from_other_session_root() {
        let (mut store, root_id) = make_store_with_root();
        let left_root = store.append(make_session_anchor_node(&root_id)).unwrap();
        let right_root = store.append(make_session_anchor_node(&root_id)).unwrap();
        let left_leaf = store.append(make_text_node(&left_root, "left")).unwrap();
        let right_leaf = store.append(make_text_node(&right_root, "right")).unwrap();

        let merge_id = store
            .append(make_prompt_anchor_node(&left_leaf, &[&right_leaf]))
            .unwrap();

        assert!(
            store
                .children
                .get(&right_leaf)
                .is_some_and(|children| children.contains(&merge_id))
        );
    }

    #[test]
    fn ancestry_returns_nodes_back_to_root() {
        let (mut store, root_id) = make_store_with_root();
        let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
        let a_id = store.append(make_text_node(&session_id, "a")).unwrap();
        let b_id = store.append(make_text_node(&a_id, "b")).unwrap();

        let ancestry = store.ancestry(&b_id).unwrap();
        let ids: Vec<_> = ancestry.into_iter().map(|node| node.id.as_str()).collect();

        assert_eq!(
            ids,
            vec![
                b_id.as_str(),
                a_id.as_str(),
                session_id.as_str(),
                root_id.as_str()
            ]
        );
    }

    #[test]
    fn log_returns_nodes_from_head_back_to_base_inclusive() {
        let (mut store, root_id) = make_store_with_root();
        let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
        let a_id = store.append(make_text_node(&session_id, "a")).unwrap();
        let b_id = store.append(make_text_node(&a_id, "b")).unwrap();
        let c_id = store.append(make_text_node(&b_id, "c")).unwrap();

        let log = store.log(&a_id, &c_id).unwrap();
        let ids: Vec<_> = log.into_iter().map(|node| node.id.as_str()).collect();

        assert_eq!(ids, vec![c_id.as_str(), b_id.as_str(), a_id.as_str()]);
    }

    #[test]
    fn log_returns_single_node_when_base_equals_head() {
        let (store, root_id) = make_store_with_root();

        let log = store.log(&root_id, &root_id).unwrap();
        let ids: Vec<_> = log.into_iter().map(|node| node.id.as_str()).collect();

        assert_eq!(ids, vec![root_id.as_str()]);
    }

    #[test]
    fn log_returns_not_found_when_head_is_missing() {
        let (store, root_id) = make_store_with_root();
        let missing_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

        let err = store.log(&root_id, missing_id).unwrap_err();

        assert!(matches!(err, Error::NotFound { id } if id == missing_id));
    }

    #[test]
    fn log_returns_parent_not_found_when_chain_is_broken() {
        let (mut store, root_id) = make_store_with_root();
        let mut broken = store
            .append(make_session_anchor_node(&root_id))
            .map(|_| {
                crate::Node::new(
                    "missing".to_owned(),
                    Role::User,
                    None,
                    Kind::Text("broken".to_owned()),
                    "2026-03-25T09:10:11Z".parse().unwrap(),
                )
            })
            .unwrap();
        let broken_id = broken.id.clone();
        store.nodes.insert(broken.id.clone(), broken.clone());

        let err = store.log(&root_id, &broken_id).unwrap_err();

        assert!(matches!(err, Error::ParentNotFound { id } if id == "missing"));
        broken.parent = root_id;
    }

    #[test]
    fn log_returns_refs_not_connected_when_base_is_not_an_ancestor() {
        let (mut store, root_id) = make_store_with_root();
        let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
        let a_id = store.append(make_text_node(&session_id, "a")).unwrap();
        let b_id = store.append(make_text_node(&a_id, "b")).unwrap();
        let sibling_id = store
            .append(make_text_node(&session_id, "sibling"))
            .unwrap();

        let err = store.log(&sibling_id, &b_id).unwrap_err();

        assert!(matches!(
            err,
            Error::RefsNotConnected {
                base_ref,
                head_ref,
            } if base_ref == sibling_id && head_ref == b_id
        ));
    }

    #[test]
    fn log_ignores_prompt_anchor_parents() {
        let (mut store, root_id) = make_store_with_root();
        let session_id = store.append(make_session_anchor_node(&root_id)).unwrap();
        let merge_parent_id = store
            .append(make_text_node(&session_id, "merge-parent"))
            .unwrap();
        let anchor_id = store
            .append(make_prompt_anchor_node(&session_id, &[&merge_parent_id]))
            .unwrap();

        let err = store.log(&merge_parent_id, &anchor_id).unwrap_err();

        assert!(matches!(
            err,
            Error::RefsNotConnected {
                base_ref,
                head_ref,
            } if base_ref == merge_parent_id && head_ref == anchor_id
        ));
    }

    #[test]
    fn branch_creation_resolves_refs() {
        let (mut store, root_id) = make_store_with_root();

        let head_id = store.fork("main", &root_id).unwrap();

        assert_eq!(head_id, root_id);
        assert_eq!(store.get_branch_head("main").unwrap(), root_id);
    }

    #[test]
    fn fork_rejects_duplicates() {
        let (mut store, root_id) = make_store_with_root();
        store.fork("main", &root_id).unwrap();

        let err = store.fork("main", &root_id).unwrap_err();

        assert!(matches!(err, Error::BranchExists { name } if name == "main"));
    }

    #[test]
    fn set_branch_head_requires_matching_expected_head() {
        let (mut store, root_id) = make_store_with_root();
        let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
        let next_id = store.append(make_text_node(&child_id, "next")).unwrap();
        store.fork("main", &child_id).unwrap();

        let err = store
            .set_branch_head("main", &root_id, &next_id)
            .unwrap_err();

        assert!(matches!(
            err,
            Error::BranchHeadMoved {
                name,
                expected,
                actual,
            } if name == "main" && expected == root_id && actual == child_id
        ));
    }

    #[test]
    fn log_supports_branch_name_on_head_ref() {
        let (mut store, root_id) = make_store_with_root();
        let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
        store.branches.insert("main".to_owned(), child_id.clone());

        let log = store.log(&root_id, "main").unwrap();
        let ids: Vec<_> = log.into_iter().map(|node| node.id.as_str()).collect();

        assert_eq!(ids, vec![child_id.as_str(), root_id.as_str()]);
    }

    #[test]
    fn log_supports_branch_name_on_base_ref() {
        let (mut store, root_id) = make_store_with_root();
        let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
        let leaf_id = store.append(make_text_node(&child_id, "leaf")).unwrap();
        store.branches.insert("base".to_owned(), child_id.clone());

        let log = store.log("base", &leaf_id).unwrap();
        let ids: Vec<_> = log.into_iter().map(|node| node.id.as_str()).collect();

        assert_eq!(ids, vec![leaf_id.as_str(), child_id.as_str()]);
    }

    #[test]
    fn log_supports_branch_name_on_both_sides() {
        let (mut store, root_id) = make_store_with_root();
        let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
        let leaf_id = store.append(make_text_node(&child_id, "leaf")).unwrap();
        store.branches.insert("base".to_owned(), child_id.clone());
        store.branches.insert("main".to_owned(), leaf_id.clone());

        let log = store.log("base", "main").unwrap();
        let ids: Vec<_> = log.into_iter().map(|node| node.id.as_str()).collect();

        assert_eq!(ids, vec![leaf_id.as_str(), child_id.as_str()]);
    }

    #[test]
    fn log_returns_branch_not_found_when_branch_is_missing() {
        let (store, root_id) = make_store_with_root();

        let err = store.log(&root_id, "main").unwrap_err();

        assert!(matches!(err, Error::BranchNotFound { name } if name == "main"));
    }

    #[test]
    fn log_returns_not_found_when_branch_head_is_missing() {
        let (mut store, root_id) = make_store_with_root();
        let missing_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        store
            .branches
            .insert("main".to_owned(), missing_id.to_owned());

        let err = store.log(&root_id, "main").unwrap_err();

        assert!(matches!(err, Error::NotFound { id } if id == missing_id));
    }

    #[test]
    fn shared_store_supports_branch_queries() {
        let (mut store, root_id) = make_store_with_root();
        let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
        store.fork("main", &child_id).unwrap();
        let shared = SharedStore::from_store(store);

        assert_eq!(shared.get_branch_head("main").unwrap(), child_id);
        assert_eq!(shared.log(&root_id, "main").unwrap().len(), 2);
    }
}
