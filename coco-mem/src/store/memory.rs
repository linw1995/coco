use std::collections::{HashMap, HashSet};

use jiff::Timestamp;
use snafu::prelude::*;

use crate::{NewNode, Node};

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

    #[snafu(display("ID {id:?} not found"))]
    NotFound { id: String },

    #[snafu(display("Branch {name:?} not found"))]
    BranchNotFound { name: String },
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

        let node = Node::new(node.parent, node.role, node.kind, Timestamp::now());
        let parent = node.parent.clone();
        let id = node.id.clone();

        self.children.entry(parent).or_default().insert(id.clone());
        self.nodes.insert(id.clone(), node);

        Ok(id)
    }

    pub fn log(&self, left: &str, right: &str) -> Result<Vec<&Node>> {
        let mut node = self.nodes.get(right).context(NotFoundSnafu {
            id: right.to_owned(),
        })?;

        let mut ans = vec![];
        loop {
            ans.push(node);
            if node.id == left {
                break;
            }

            let parent = &node.parent;
            node = self.nodes.get(parent).context(ParentNotFoundSnafu {
                id: parent.to_owned(),
            })?;
        }

        Ok(ans)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{Error, Store};
    use crate::{Kind, NewNode, Node, Role};
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
    fn log_returns_nodes_from_right_back_to_left_inclusive() {
        let (mut store, root_id) = make_store_with_root();
        let a_id = store.append(make_text_node(&root_id, "a")).unwrap();
        let b_id = store.append(make_text_node(&a_id, "b")).unwrap();
        let c_id = store.append(make_text_node(&b_id, "c")).unwrap();

        let log = store.log(&a_id, &c_id).unwrap();
        let ids: Vec<_> = log.into_iter().map(|node| node.id.as_str()).collect();

        assert_eq!(ids, vec![c_id.as_str(), b_id.as_str(), a_id.as_str()]);
    }

    #[test]
    fn log_returns_single_node_when_left_equals_right() {
        let (store, root_id) = make_store_with_root();

        let log = store.log(&root_id, &root_id).unwrap();
        let ids: Vec<_> = log.into_iter().map(|node| node.id.as_str()).collect();

        assert_eq!(ids, vec![root_id.as_str()]);
    }

    #[test]
    fn log_returns_not_found_when_right_is_missing() {
        let (store, root_id) = make_store_with_root();

        let err = store.log(&root_id, "missing").unwrap_err();

        assert!(matches!(err, Error::NotFound { id } if id == "missing"));
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
    fn log_returns_parent_not_found_when_left_is_not_an_ancestor() {
        let (mut store, root_id) = make_store_with_root();
        let a_id = store.append(make_text_node(&root_id, "a")).unwrap();
        let b_id = store.append(make_text_node(&a_id, "b")).unwrap();
        let sibling_id = store.append(make_text_node(&root_id, "sibling")).unwrap();

        let err = store.log(&sibling_id, &b_id).unwrap_err();

        assert!(matches!(err, Error::ParentNotFound { id } if id.is_empty()));
    }
}
