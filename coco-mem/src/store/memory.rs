use std::collections::{HashMap, HashSet};

use jiff::Timestamp;
use snafu::prelude::*;

use crate::{Kind, NewNode, Node};

pub struct Store {
    pub nodes: HashMap<String, Node>,
    pub children: HashMap<String, HashSet<String>>,

    pub current: String,
    pub branches: HashMap<String, String>,
}

#[derive(Snafu, Debug)]
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

    #[snafu(display("Ref {base_ref:?} is not an ancestor of {head_ref:?}"))]
    RefsNotConnected { base_ref: String, head_ref: String },
}

type Result<T> = std::result::Result<T, Error>;

impl Store {
    pub fn append(&mut self, node: NewNode) -> Result<String> {
        ensure!(
            self.nodes.contains_key(&node.parent),
            ParentNotFoundSnafu {
                id: node.parent.clone()
            }
        );
        self.validate_anchor_merge_parents(&node.parent, &node.kind)?;

        let node = Node::new(node.parent, node.role, node.kind, Timestamp::now());
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
        for merge_parent in &anchor.merge_parents {
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

fn is_node_id(reference: &str) -> bool {
    reference.len() == 64 && reference.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn parent_ids(node: &Node) -> Vec<&str> {
    let mut parents = vec![node.parent.as_str()];
    if let Kind::Anchor(anchor) = &node.kind {
        parents.extend(anchor.merge_parents.iter().map(String::as_str));
    }
    parents
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{Error, Store};
    use crate::{Anchor, Kind, NewNode, Node, Role};
    use jiff::Timestamp;

    fn fixed_timestamp() -> Timestamp {
        "2026-03-25T09:10:11Z".parse().unwrap()
    }

    fn make_text_node(parent: &str, text: &str) -> NewNode {
        NewNode {
            parent: parent.to_owned(),
            role: Role::User,
            kind: Kind::Text(text.to_owned()),
        }
    }

    fn make_anchor_node(parent: &str, merge_parents: &[&str]) -> NewNode {
        NewNode {
            parent: parent.to_owned(),
            role: Role::System,
            kind: Kind::Anchor(Anchor {
                model: "gpt-5.4".to_owned(),
                tools: vec![],
                system_prompt: "system".to_owned(),
                prompt: "prompt".to_owned(),
                merge_parents: merge_parents.iter().map(|id| (*id).to_owned()).collect(),
            }),
        }
    }

    fn make_persisted_text_node(parent: &str, text: &str) -> Node {
        Node::new(
            parent.to_owned(),
            Role::User,
            Kind::Text(text.to_owned()),
            fixed_timestamp(),
        )
    }

    fn make_store_with_root() -> (Store, String) {
        let root = make_persisted_text_node("", "root");
        let root_id = root.id.clone();
        let mut nodes = HashMap::new();
        nodes.insert(root.id.clone(), root);

        (
            Store {
                nodes,
                children: HashMap::new(),
                current: root_id.clone(),
                branches: HashMap::new(),
            },
            root_id,
        )
    }

    #[test]
    fn append_inserts_node_and_updates_children_index() {
        let (mut store, root_id) = make_store_with_root();
        let before = Timestamp::now();

        let child_id = store.append(make_text_node(&root_id, "child")).unwrap();
        let after = Timestamp::now();

        let stored = store.nodes.get(&child_id).unwrap();
        assert_eq!(stored.parent, root_id);
        assert!(stored.created_at >= before);
        assert!(stored.created_at <= after);
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
    fn append_anchor_indexes_merge_parents_as_children() {
        let (mut store, root_id) = make_store_with_root();
        let merge_parent_id = store
            .append(make_text_node(&root_id, "merge-parent"))
            .unwrap();

        let anchor_id = store
            .append(make_anchor_node(&root_id, &[&merge_parent_id]))
            .unwrap();

        assert!(store.children.get(&root_id).unwrap().contains(&anchor_id));
        assert!(
            store
                .children
                .get(&merge_parent_id)
                .unwrap()
                .contains(&anchor_id)
        );
    }

    #[test]
    fn append_anchor_rejects_missing_merge_parent() {
        let (mut store, root_id) = make_store_with_root();
        let node = make_anchor_node(&root_id, &["missing"]);

        let err = store.append(node).unwrap_err();

        assert!(matches!(err, Error::ParentNotFound { id } if id == "missing"));
    }

    #[test]
    fn append_anchor_rejects_duplicate_merge_parents() {
        let (mut store, root_id) = make_store_with_root();
        let merge_parent_id = store
            .append(make_text_node(&root_id, "merge-parent"))
            .unwrap();
        let node = make_anchor_node(&root_id, &[&merge_parent_id, &merge_parent_id]);

        let err = store.append(node).unwrap_err();

        assert!(matches!(
            err,
            Error::DuplicateMergeParent { id } if id == merge_parent_id
        ));
    }

    #[test]
    fn append_anchor_rejects_merge_parent_matching_primary_parent() {
        let (mut store, root_id) = make_store_with_root();
        let node = make_anchor_node(&root_id, &[&root_id]);

        let err = store.append(node).unwrap_err();

        assert!(matches!(
            err,
            Error::MergeParentMatchesParent { id } if id == root_id
        ));
    }

    #[test]
    fn log_returns_nodes_from_head_back_to_base_inclusive() {
        let (mut store, root_id) = make_store_with_root();
        let a_id = store.append(make_text_node(&root_id, "a")).unwrap();
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
        let broken = make_persisted_text_node("missing", "broken");
        let broken_id = broken.id.clone();
        store.nodes.insert(broken.id.clone(), broken);

        let err = store.log(&root_id, &broken_id).unwrap_err();

        assert!(matches!(err, Error::ParentNotFound { id } if id == "missing"));
    }

    #[test]
    fn log_returns_refs_not_connected_when_base_is_not_an_ancestor() {
        let (mut store, root_id) = make_store_with_root();
        let a_id = store.append(make_text_node(&root_id, "a")).unwrap();
        let b_id = store.append(make_text_node(&a_id, "b")).unwrap();
        let sibling_id = store.append(make_text_node(&root_id, "sibling")).unwrap();

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
    fn log_ignores_anchor_merge_parents() {
        let (mut store, root_id) = make_store_with_root();
        let merge_parent_id = store
            .append(make_text_node(&root_id, "merge-parent"))
            .unwrap();
        let anchor_id = store
            .append(make_anchor_node(&root_id, &[&merge_parent_id]))
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
}
