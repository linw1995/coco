use super::*;

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use coco_mem::{NewNode, NodeStore, PersistentStore, Role, SqliteStore};
use tokio::sync::oneshot;

async fn test_store() -> SqliteStore {
    SqliteStore::open_temporary()
        .await
        .expect("temporary SQLite store should open")
}

struct BranchAdvanceDuringWalkStore {
    root: Node,
    old_head: Node,
    new_head_id: String,
    branch_head: Mutex<String>,
    advanced: AtomicBool,
}

impl BranchAdvanceDuringWalkStore {
    async fn new() -> Self {
        let memory = test_store().await;
        let root = memory.get_node(&memory.root_id()).await.unwrap();
        let old_head = Node::new(
            root.id.clone(),
            Role::User,
            None,
            Kind::Text("old head".to_owned()),
            "1970-01-01T00:00:01Z".parse().unwrap(),
        );
        let new_head = Node::new(
            old_head.id.clone(),
            Role::User,
            None,
            Kind::Text("new head".to_owned()),
            "1970-01-01T00:00:02Z".parse().unwrap(),
        );
        Self {
            root,
            branch_head: Mutex::new(old_head.id.clone()),
            old_head,
            new_head_id: new_head.id,
            advanced: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl NodeStore for BranchAdvanceDuringWalkStore {
    fn root_id(&self) -> String {
        self.root.id.clone()
    }

    async fn append(&self, _node: NewNode) -> coco_mem::StoreResult<String> {
        Err(coco_mem::StoreError::StoreReadOnly {
            path: PathBuf::from("branch advance test store"),
        })
    }

    async fn ancestry(&self, head_ref: &str) -> coco_mem::StoreResult<Vec<Node>> {
        match head_ref {
            id if id == self.old_head.id => Ok(vec![self.old_head.clone(), self.root.clone()]),
            id if id == self.root.id => Ok(vec![self.root.clone()]),
            id => Err(coco_mem::StoreError::NotFound { id: id.to_owned() }),
        }
    }

    async fn log(&self, _base_ref: &str, head_ref: &str) -> coco_mem::StoreResult<Vec<Node>> {
        match head_ref {
            id if id == self.old_head.id => Ok(vec![self.old_head.clone(), self.root.clone()]),
            id if id == self.root.id => Ok(vec![self.root.clone()]),
            id => Err(coco_mem::StoreError::NotFound { id: id.to_owned() }),
        }
    }

    async fn get_node(&self, id: &str) -> coco_mem::StoreResult<Node> {
        match id {
            id if id == self.root.id => Ok(self.root.clone()),
            id if id == self.old_head.id => Ok(self.old_head.clone()),
            id => Err(coco_mem::StoreError::NotFound { id: id.to_owned() }),
        }
    }

    async fn list_children(&self, node_id: &str) -> coco_mem::StoreResult<Vec<Node>> {
        if node_id == self.root.id {
            if !self.advanced.swap(true, Ordering::SeqCst) {
                *self.branch_head.lock().unwrap() = self.new_head_id.clone();
            }
            Ok(vec![self.old_head.clone()])
        } else if node_id == self.old_head.id {
            Ok(Vec::new())
        } else {
            Err(coco_mem::StoreError::NotFound {
                id: node_id.to_owned(),
            })
        }
    }
}

#[async_trait]
impl BranchStore for BranchAdvanceDuringWalkStore {
    async fn fork(&self, _name: &str, _from_ref: &str) -> coco_mem::StoreResult<String> {
        Err(coco_mem::StoreError::StoreReadOnly {
            path: PathBuf::from("branch advance test store"),
        })
    }

    async fn get_branch_head(&self, name: &str) -> coco_mem::StoreResult<String> {
        if name == "main" {
            Ok(self.branch_head.lock().unwrap().clone())
        } else {
            Err(coco_mem::StoreError::BranchNotFound {
                name: name.to_owned(),
            })
        }
    }

    async fn delete_branch(&self, _name: &str) -> coco_mem::StoreResult<()> {
        Err(coco_mem::StoreError::StoreReadOnly {
            path: PathBuf::from("branch advance test store"),
        })
    }

    async fn set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _new_head: &str,
    ) -> coco_mem::StoreResult<()> {
        Err(coco_mem::StoreError::StoreReadOnly {
            path: PathBuf::from("branch advance test store"),
        })
    }

    async fn append_nodes_and_set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _nodes: Vec<coco_mem::NewNodeContent>,
    ) -> coco_mem::StoreResult<String> {
        Err(coco_mem::StoreError::StoreReadOnly {
            path: PathBuf::from("branch advance test store"),
        })
    }

    async fn append_nodes_and_set_branch_head_to(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _new_head: &str,
        _nodes: Vec<coco_mem::NewNodeContent>,
    ) -> coco_mem::StoreResult<String> {
        Err(coco_mem::StoreError::StoreReadOnly {
            path: PathBuf::from("branch advance test store"),
        })
    }

    async fn append_nodes_and_set_branch_head_with_session_state(
        &self,
        _update: coco_mem::BranchAppendSessionState,
    ) -> coco_mem::StoreResult<String> {
        Err(coco_mem::StoreError::StoreReadOnly {
            path: PathBuf::from("branch advance test store"),
        })
    }
}

#[async_trait]
impl SessionStore for BranchAdvanceDuringWalkStore {
    async fn list_session_states(&self) -> coco_mem::StoreResult<HashMap<String, SessionState>> {
        Ok(HashMap::from([("main".to_owned(), SessionState::Active)]))
    }

    async fn get_session_state(&self, name: &str) -> coco_mem::StoreResult<SessionState> {
        if name == "main" {
            Ok(SessionState::Active)
        } else {
            Err(coco_mem::StoreError::BranchNotFound {
                name: name.to_owned(),
            })
        }
    }

    async fn set_session_state(
        &self,
        _name: &str,
        _expected: Option<&SessionState>,
        _next: SessionState,
    ) -> coco_mem::StoreResult<SessionState> {
        Err(coco_mem::StoreError::StoreReadOnly {
            path: PathBuf::from("branch advance test store"),
        })
    }

    async fn rebase_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
    ) -> coco_mem::StoreResult<String> {
        Err(coco_mem::StoreError::StoreReadOnly {
            path: PathBuf::from("branch advance test store"),
        })
    }

    async fn handoff_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
        _prompt: &str,
    ) -> coco_mem::StoreResult<String> {
        Err(coco_mem::StoreError::StoreReadOnly {
            path: PathBuf::from("branch advance test store"),
        })
    }
}

fn temp_store_path() -> PathBuf {
    static TEMP_STORE_COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let process_id = std::process::id();
    let counter = TEMP_STORE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "coco-console-snapshot-{process_id}-{nanos}-{counter}"
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[tokio::test]
async fn materialization_source_snapshot_captures_branch_heads_before_node_walk() {
    let store = BranchAdvanceDuringWalkStore::new().await;

    let snapshot = MaterializationSourceSnapshot::from_store(
        &store,
        &[("main".to_owned(), SessionState::Active)],
    )
    .await
    .unwrap();

    assert_eq!(
        store.get_branch_head("main").await.unwrap(),
        store.new_head_id
    );
    assert_eq!(
        snapshot.get_branch_head("main").await.unwrap(),
        store.old_head.id
    );
    assert_eq!(
        snapshot.ancestry("main").await.unwrap()[0].id,
        store.old_head.id
    );
}

#[tokio::test]
async fn failed_batch_seed_clears_committed_facts_and_restores_empty_meta() {
    let path = temp_store_path();
    let _writer = PersistentStore::open(&path).await.unwrap();
    let snapshots = ConsoleGraphSnapshotStore::open(&path).await.unwrap();

    let store = snapshots.clone();
    let (restored_version, row_count) = snapshots
        .with_connection_for_tests(move |connection| {
            store.run_bool_write_transaction(connection, |this, connection| {
                this.put_empty_materialization_in_transaction(connection, 7, GraphMode::All)
            })?;
            let previous_meta =
                store.latest_materialization_row_in_connection(connection, GraphMode::All)?;
            store.run_write_transaction(connection, |this, connection| {
                this.delete_materialization_meta(connection, GraphMode::All)?;
                let lane = GraphViewportLane {
                    key: "main".to_owned(),
                    label: "main".to_owned(),
                    y: crate::layout::GRAPH_TOP_Y,
                };
                let node = GraphViewportNode {
                    key: "node:test:0:0".to_owned(),
                    id: "test".to_owned(),
                    node_target: "test".to_owned(),
                    short_id: "test".to_owned(),
                    kind: "text".to_owned(),
                    summary: "test".to_owned(),
                    labels: Vec::new(),
                    x: GRAPH_LEFT_X,
                    y: lane.y,
                };
                this.insert_node_location(
                    connection,
                    NodeLocationInsert {
                        mode: GraphMode::All,
                        node: &node,
                        lane: &lane,
                        bounds: node_bounds(&node),
                    },
                )?;
                Ok(())
            })?;

            store.restore_empty_materialization_after_failed_batch_seed(
                connection,
                GraphMode::All,
                previous_meta,
            )?;
            let restored = store
                .latest_materialization_row_in_connection(connection, GraphMode::All)?
                .expect("empty materialization meta should be restored");
            let rows = store.materialized_node_rows_in_connection(connection, GraphMode::All)?;
            Ok((restored.source_version, rows.len()))
        })
        .await
        .unwrap();

    assert_eq!(restored_version, 7);
    assert_eq!(row_count, 0);

    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn direct_materialized_node_lookups_ignore_rows_without_meta() {
    let path = temp_store_path();
    let _writer = PersistentStore::open(&path).await.unwrap();
    let snapshots = ConsoleGraphSnapshotStore::open(&path).await.unwrap();

    let store = snapshots.clone();
    snapshots
        .with_connection_for_tests(move |connection| {
            store.run_write_transaction(connection, |this, connection| {
                this.delete_materialization_meta(connection, GraphMode::All)?;
                let lane = GraphViewportLane {
                    key: "main".to_owned(),
                    label: "main".to_owned(),
                    y: crate::layout::GRAPH_TOP_Y,
                };
                let node = GraphViewportNode {
                    key: "node:test:0:0".to_owned(),
                    id: "test".to_owned(),
                    node_target: "test".to_owned(),
                    short_id: "test".to_owned(),
                    kind: "text".to_owned(),
                    summary: "test".to_owned(),
                    labels: vec!["main".to_owned()],
                    x: GRAPH_LEFT_X,
                    y: lane.y,
                };
                this.insert_node_location(
                    connection,
                    NodeLocationInsert {
                        mode: GraphMode::All,
                        node: &node,
                        lane: &lane,
                        bounds: node_bounds(&node),
                    },
                )?;
                Ok(())
            })
        })
        .await
        .unwrap();

    let reference = snapshots
        .materialized_node_reference(GraphMode::All, "test")
        .await
        .unwrap();
    let points = snapshots
        .materialized_node_points(GraphMode::All, &BTreeSet::from(["test".to_owned()]))
        .await
        .unwrap();
    assert!(reference.is_none());
    assert!(points.is_empty());

    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn latest_viewport_reads_meta_and_rows_from_one_snapshot() {
    let path = temp_store_path();
    let _writer = PersistentStore::open(&path).await.unwrap();
    let snapshots = ConsoleGraphSnapshotStore::open(&path).await.unwrap();

    let store = snapshots.clone();
    snapshots
        .with_connection_for_tests(move |connection| {
            store.run_bool_write_transaction(connection, |this, connection| {
                this.put_empty_materialization_in_transaction(connection, 7, GraphMode::All)
            })
        })
        .await
        .unwrap();

    let (read_started_tx, read_started_rx) = oneshot::channel();
    let (write_finished_tx, write_finished_rx) = oneshot::channel();
    let reader = snapshots.clone();
    let read = tokio::spawn(async move {
        let reader_store = reader.clone();
        reader
            .with_connection_for_tests(move |connection| {
                reader_store.run_read_transaction(connection, |this, connection| {
                    let meta = this
                        .latest_materialization_row_in_connection(connection, GraphMode::All)?
                        .expect("empty materialization meta should exist");
                    read_started_tx.send(()).unwrap();
                    write_finished_rx.blocking_recv().unwrap();
                    this.viewport_from_row(
                        connection,
                        GraphMode::All,
                        meta,
                        GraphViewportRequest::default(),
                    )
                })
            })
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), read_started_rx)
        .await
        .expect("snapshot read transaction should start")
        .unwrap();

    let writer = snapshots.clone();
    let writer_store = writer.clone();
    writer
        .with_connection_for_tests(move |writer_connection| {
            writer_store.run_write_transaction(writer_connection, |this, writer_connection| {
                this.delete_materialization_meta(writer_connection, GraphMode::All)?;
                let lane = GraphViewportLane {
                    key: "main".to_owned(),
                    label: "main".to_owned(),
                    y: crate::layout::GRAPH_TOP_Y,
                };
                let node = GraphViewportNode {
                    key: "node:test:0:0".to_owned(),
                    id: "test".to_owned(),
                    node_target: "test".to_owned(),
                    short_id: "test".to_owned(),
                    kind: "text".to_owned(),
                    summary: "test".to_owned(),
                    labels: vec!["main".to_owned()],
                    x: GRAPH_LEFT_X,
                    y: lane.y,
                };
                this.insert_node_location(
                    writer_connection,
                    NodeLocationInsert {
                        mode: GraphMode::All,
                        node: &node,
                        lane: &lane,
                        bounds: node_bounds(&node),
                    },
                )?;
                Ok(())
            })
        })
        .await
        .unwrap();
    write_finished_tx.send(()).unwrap();
    let response = read
        .await
        .unwrap()
        .unwrap()
        .expect("viewport should be available");

    assert_eq!(response.version, 7);
    assert!(response.nodes.is_empty());

    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn meta_gated_materialized_node_lookups_read_from_one_snapshot() {
    let path = temp_store_path();
    let _writer = PersistentStore::open(&path).await.unwrap();
    let snapshots = ConsoleGraphSnapshotStore::open(&path).await.unwrap();

    let store = snapshots.clone();
    snapshots
        .with_connection_for_tests(move |connection| {
            store.run_bool_write_transaction(connection, |this, connection| {
                this.put_empty_materialization_in_transaction(connection, 7, GraphMode::All)
            })
        })
        .await
        .unwrap();

    let (read_started_tx, read_started_rx) = oneshot::channel();
    let (write_finished_tx, write_finished_rx) = oneshot::channel();
    let reader = snapshots.clone();
    let read = tokio::spawn(async move {
        let reader_store = reader.clone();
        reader
            .with_connection_for_tests(move |connection| {
                reader_store.run_read_transaction(connection, |this, connection| {
                    assert!(
                        this.latest_materialization_row_in_connection(connection, GraphMode::All,)?
                            .is_some()
                    );
                    read_started_tx.send(()).unwrap();
                    write_finished_rx.blocking_recv().unwrap();
                    let reference = this.materialized_node_reference_in_connection(
                        connection,
                        GraphMode::All,
                        "test",
                    )?;
                    let points = this.materialized_node_points_in_connection(
                        connection,
                        GraphMode::All,
                        &BTreeSet::from(["test".to_owned()]),
                    )?;
                    Ok((reference, points))
                })
            })
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), read_started_rx)
        .await
        .expect("snapshot lookup transaction should start")
        .unwrap();

    let writer = snapshots.clone();
    let writer_store = writer.clone();
    writer
        .with_connection_for_tests(move |writer_connection| {
            writer_store.run_write_transaction(writer_connection, |this, writer_connection| {
                this.delete_materialization_meta(writer_connection, GraphMode::All)?;
                let lane = GraphViewportLane {
                    key: "main".to_owned(),
                    label: "main".to_owned(),
                    y: crate::layout::GRAPH_TOP_Y,
                };
                let node = GraphViewportNode {
                    key: "node:test:0:0".to_owned(),
                    id: "test".to_owned(),
                    node_target: "test".to_owned(),
                    short_id: "test".to_owned(),
                    kind: "text".to_owned(),
                    summary: "test".to_owned(),
                    labels: vec!["main".to_owned()],
                    x: GRAPH_LEFT_X,
                    y: lane.y,
                };
                this.insert_node_location(
                    writer_connection,
                    NodeLocationInsert {
                        mode: GraphMode::All,
                        node: &node,
                        lane: &lane,
                        bounds: node_bounds(&node),
                    },
                )?;
                Ok(())
            })
        })
        .await
        .unwrap();
    write_finished_tx.send(()).unwrap();
    let (reference, points) = read.await.unwrap().unwrap();

    assert!(reference.is_none());
    assert!(points.is_empty());

    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn visible_skill_invocation_linear_subtrees_handles_deep_chain() {
    let store = test_store().await;
    let source_id = store.root_id();
    let depth = 20_000;
    let mut node_ids = Vec::with_capacity(depth);
    let mut parent = source_id.clone();
    for index in 0..depth {
        parent = store
            .append(NewNode {
                parent,
                role: Role::User,
                metadata: None,
                kind: Kind::Text(format!("node {index}")),
            })
            .await
            .unwrap();
        node_ids.push(parent.clone());
    }
    let mut nodes = Vec::new();
    for node_id in &node_ids {
        nodes.push(store.get_node(node_id).await.unwrap());
    }

    let subtrees = visible_skill_invocation_linear_subtrees(&source_id, nodes).unwrap();
    let expected_last = node_ids.last().unwrap();

    assert_eq!(subtrees.len(), 1);
    assert_eq!(subtrees[0].len(), depth);
    assert_eq!(
        subtrees[0].first().map(|node| node.id.as_str()),
        node_ids.first().map(String::as_str)
    );
    assert_eq!(
        subtrees[0].last().map(|node| node.id.as_str()),
        Some(expected_last.as_str())
    );
}
