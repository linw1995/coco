use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use super::Store;
use super::state::StoreState;
use crate::error::{
    CorruptedStoreSnafu, ParseStoreLogSnafu, ParseStoreMetaSnafu, SerializeStoreRecordSnafu,
    StorePathIsNotDirectorySnafu, WriteStoreDirectorySnafu, WriteStoreLogSnafu,
    WriteStoreMetaSnafu,
};
use crate::{NewNode, Node, SessionAnchorPatch, SessionState, StoreError, StoreResult as Result};

const STORE_FORMAT_VERSION: u64 = 4;
const META_FILE_NAME: &str = "meta.json";
const NODES_FILE_NAME: &str = "nodes.jsonl";
const SESSIONS_FILE_NAME: &str = "sessions.json";
const BRANCHES_DIR_NAME: &str = "branches";

#[derive(Clone, Debug)]
pub struct FsStore {
    inner: Arc<RwLock<StoreState>>,
    persistence: Arc<Persistence>,
}

#[derive(Debug, Clone)]
pub(crate) struct Persistence {
    dir: PathBuf,
    meta_path: PathBuf,
    nodes_path: PathBuf,
    sessions_path: PathBuf,
    branches_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Meta {
    pub version: u64,
    pub root_id: String,
}

impl Persistence {
    pub fn open(path: impl AsRef<Path>) -> Result<(Self, StoreState)> {
        let persistence = Self::new(path.as_ref());
        if !persistence.dir.exists() {
            return persistence.initialize();
        }

        let metadata = fs::metadata(&persistence.dir).context(WriteStoreDirectorySnafu {
            path: persistence.dir.clone(),
        })?;
        ensure!(
            metadata.is_dir(),
            StorePathIsNotDirectorySnafu {
                path: persistence.dir.clone(),
            }
        );

        persistence.load()
    }

    pub fn append_node(&self, node: &Node) -> Result<()> {
        append_jsonl_record(&self.nodes_path, node)
    }

    pub fn persist_fork(&self, branch: &str, head_id: &str, state: &StoreState) -> Result<()> {
        let branch_path = self.branch_path(branch);
        ensure!(
            !branch_path.exists(),
            CorruptedStoreSnafu {
                path: branch_path.clone(),
                message: "branch view already exists".to_owned(),
            }
        );
        let nodes = branch_view_nodes(head_id, state)?;
        write_jsonl_file_create_new(&branch_path, &nodes)?;
        self.persist_sessions(state)?;
        Ok(())
    }

    pub fn persist_branch_head_update(
        &self,
        branch: &str,
        previous_head: &str,
        new_head: &str,
        state: &StoreState,
    ) -> Result<()> {
        let branch_path = self.branch_path(branch);
        ensure!(
            branch_path.is_file(),
            CorruptedStoreSnafu {
                path: branch_path.clone(),
                message: "missing branch view file".to_owned(),
            }
        );

        match state.log(previous_head, new_head) {
            Ok(path) => {
                let nodes = path.into_iter().rev().skip(1).cloned().collect::<Vec<_>>();
                if nodes.is_empty() {
                    self.persist_sessions(state)?;
                    return Ok(());
                }
                append_jsonl_records(&branch_path, &nodes)?;
                self.persist_sessions(state)
            }
            Err(StoreError::RefsNotConnected { .. }) => {
                self.rewrite_branch_view(branch, new_head, state)?;
                self.persist_sessions(state)
            }
            Err(source) => Err(source),
        }
    }

    pub fn rewrite_branch_view(
        &self,
        branch: &str,
        head_id: &str,
        state: &StoreState,
    ) -> Result<()> {
        let branch_path = self.branch_path(branch);
        ensure!(
            branch_path.is_file(),
            CorruptedStoreSnafu {
                path: branch_path.clone(),
                message: "missing branch view file".to_owned(),
            }
        );
        let nodes = branch_view_nodes(head_id, state)?;
        write_jsonl_file(&branch_path, &nodes)
    }

    fn new(path: &Path) -> Self {
        Self {
            dir: path.to_owned(),
            meta_path: path.join(META_FILE_NAME),
            nodes_path: path.join(NODES_FILE_NAME),
            sessions_path: path.join(SESSIONS_FILE_NAME),
            branches_dir: path.join(BRANCHES_DIR_NAME),
        }
    }

    fn branch_path(&self, branch: &str) -> PathBuf {
        self.branches_dir
            .join(format!("{}.jsonl", encode_branch_name(branch)))
    }

    fn initialize(&self) -> Result<(Self, StoreState)> {
        fs::create_dir_all(&self.dir).context(WriteStoreDirectorySnafu {
            path: self.dir.clone(),
        })?;
        fs::create_dir_all(&self.branches_dir).context(WriteStoreDirectorySnafu {
            path: self.branches_dir.clone(),
        })?;

        let store = StoreState::new();
        let root = store.root_node().clone();
        let meta = Meta {
            version: STORE_FORMAT_VERSION,
            root_id: root.id.clone(),
        };

        write_json_file(&self.meta_path, &meta)?;
        write_jsonl_file(&self.nodes_path, &[root])?;
        write_json_file(&self.sessions_path, &HashMap::<String, SessionState>::new())?;

        Ok((self.clone(), store))
    }

    fn load(&self) -> Result<(Self, StoreState)> {
        ensure!(
            self.meta_path.is_file(),
            CorruptedStoreSnafu {
                path: self.meta_path.clone(),
                message: "missing meta.json".to_owned(),
            }
        );
        ensure!(
            self.nodes_path.is_file(),
            CorruptedStoreSnafu {
                path: self.nodes_path.clone(),
                message: "missing nodes.jsonl".to_owned(),
            }
        );
        ensure!(
            self.branches_dir.is_dir(),
            CorruptedStoreSnafu {
                path: self.branches_dir.clone(),
                message: "missing branches directory".to_owned(),
            }
        );

        let meta = read_json_file::<Meta>(&self.meta_path)?;
        ensure!(
            meta.version == STORE_FORMAT_VERSION,
            CorruptedStoreSnafu {
                path: self.meta_path.clone(),
                message: format!(
                    "unsupported store format version {}, expected {}",
                    meta.version, STORE_FORMAT_VERSION
                ),
            }
        );

        let nodes = read_jsonl_file::<Node>(&self.nodes_path)?;
        let mut node_iter = nodes.into_iter();
        let root = node_iter.next().context(CorruptedStoreSnafu {
            path: self.nodes_path.clone(),
            message: "nodes.jsonl is empty".to_owned(),
        })?;
        ensure!(
            root.is_root(),
            CorruptedStoreSnafu {
                path: self.nodes_path.clone(),
                message: "first node must be the root node".to_owned(),
            }
        );
        ensure!(
            root.id == meta.root_id,
            CorruptedStoreSnafu {
                path: self.meta_path.clone(),
                message: format!(
                    "root id mismatch: meta has {:?}, nodes has {:?}",
                    meta.root_id, root.id
                ),
            }
        );

        let mut store = StoreState::from_root(root);
        for node in node_iter {
            store.insert_existing_node(node)?;
        }

        let mut seen_branches = HashSet::new();
        let entries = fs::read_dir(&self.branches_dir).context(WriteStoreDirectorySnafu {
            path: self.branches_dir.clone(),
        })?;
        let mut branch_paths = Vec::new();
        for entry in entries {
            let entry = entry.context(WriteStoreDirectorySnafu {
                path: self.branches_dir.clone(),
            })?;
            branch_paths.push(entry.path());
        }
        branch_paths.sort();

        for path in branch_paths {
            ensure!(
                path.is_file(),
                CorruptedStoreSnafu {
                    path: path.clone(),
                    message: "branch view entry must be a file".to_owned(),
                }
            );
            let branch = decode_branch_path(&path)?;
            ensure!(
                seen_branches.insert(branch.clone()),
                CorruptedStoreSnafu {
                    path: path.clone(),
                    message: format!("duplicate branch mapping for {:?}", branch),
                }
            );
            let nodes = read_jsonl_file::<Node>(&path)?;
            let head_id = validate_branch_view(&path, &branch, &nodes, &store)?;
            store.apply_fork(branch, head_id)?;
        }

        ensure!(
            self.sessions_path.is_file(),
            CorruptedStoreSnafu {
                path: self.sessions_path.clone(),
                message: "missing sessions metadata file".to_owned(),
            }
        );
        store.sessions = read_json_file::<HashMap<String, SessionState>>(&self.sessions_path)?;
        map_session_validation_error(&self.sessions_path, store.validate_session_records())?;

        Ok((self.clone(), store))
    }

    pub fn persist_sessions(&self, state: &StoreState) -> Result<()> {
        write_json_file(&self.sessions_path, &state.list_session_states())
    }
}

impl FsStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let (persistence, state) = Persistence::open(path)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(state)),
            persistence: Arc::new(persistence),
        })
    }

    pub fn path(&self) -> &Path {
        &self.persistence.dir
    }

    #[cfg(test)]
    pub(crate) fn snapshot_state(&self) -> StoreState {
        self.inner.read().expect("store lock poisoned").clone()
    }
}

impl Store for FsStore {
    fn root_id(&self) -> String {
        self.inner
            .read()
            .expect("store lock poisoned")
            .root_id()
            .to_owned()
    }

    fn append(&self, node: NewNode) -> Result<String> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let node = state.plan_append_node(node)?;
        self.persistence.append_node(&node)?;
        state.insert_existing_node(node)
    }

    fn fork(&self, name: &str, from_ref: &str) -> Result<String> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let plan = state.plan_fork(name, from_ref)?;
        let mut temp = state.clone();
        temp.apply_fork(name.to_owned(), plan.head_id.clone())?;
        self.persistence.persist_fork(name, &plan.head_id, &temp)?;
        state.apply_fork(name.to_owned(), plan.head_id.clone())?;
        Ok(plan.head_id)
    }

    fn get_branch_head(&self, name: &str) -> Result<String> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_branch_head(name)
            .map(str::to_owned)
    }

    fn set_branch_head(&self, name: &str, expected_old_head: &str, new_head: &str) -> Result<()> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        temp.apply_set_branch_head(name.to_owned(), expected_old_head, new_head.to_owned())?;
        self.persistence
            .persist_branch_head_update(name, expected_old_head, new_head, &temp)?;
        state.apply_set_branch_head(name.to_owned(), expected_old_head, new_head.to_owned())
    }

    fn ancestry(&self, head_ref: &str) -> Result<Vec<Node>> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .ancestry(head_ref)
            .map(|nodes| nodes.into_iter().cloned().collect())
    }

    fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<Node>> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .log(base_ref, head_ref)
            .map(|nodes| nodes.into_iter().cloned().collect())
    }

    fn get_node(&self, id: &str) -> Result<Node> {
        self.inner.read().expect("store lock poisoned").get_node(id)
    }

    fn list_session_states(&self) -> Result<HashMap<String, SessionState>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_session_states())
    }

    fn get_session_state(&self, name: &str) -> Result<SessionState> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_session_state(name)
    }

    fn set_session_state(
        &self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> Result<SessionState> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.set_session_state(name, expected, next)?;
        self.persistence.persist_sessions(&temp)?;
        state.set_session_state(name, expected, updated)
    }

    fn rebase_session(&self, name: &str, patch: &SessionAnchorPatch) -> Result<String> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let plan = state.plan_rebase_session(name, patch)?;
        let mut persisted_state = state.clone();
        for node in &plan.nodes {
            persisted_state.insert_existing_node(node.clone())?;
        }
        persisted_state.apply_set_branch_head(
            plan.branch.clone(),
            &plan.expected_old_head,
            plan.new_head.clone(),
        )?;

        for node in &plan.nodes {
            self.persistence.append_node(node)?;
        }
        self.persistence
            .rewrite_branch_view(&plan.branch, &plan.new_head, &persisted_state)?;
        self.persistence.persist_sessions(&persisted_state)?;

        for node in plan.nodes {
            state.insert_existing_node(node)?;
        }
        state.apply_set_branch_head(plan.branch, &plan.expected_old_head, plan.new_head.clone())?;
        Ok(plan.new_head)
    }

    fn runtime_store_path(&self) -> Option<PathBuf> {
        Some(self.path().to_path_buf())
    }
}

fn branch_view_nodes(head_id: &str, state: &StoreState) -> Result<Vec<Node>> {
    Ok(state
        .ancestry(head_id)?
        .into_iter()
        .rev()
        .cloned()
        .collect())
}

fn validate_branch_view(
    path: &Path,
    branch: &str,
    nodes: &[Node],
    store: &StoreState,
) -> Result<String> {
    ensure!(
        !nodes.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("branch view for {:?} is empty", branch),
        }
    );

    ensure!(
        nodes.first().is_some_and(|node| node.id == store.root),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("branch view for {:?} must start from root", branch),
        }
    );

    for (index, node) in nodes.iter().enumerate() {
        let stored = store.nodes.get(&node.id).context(CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "branch view for {:?} references unknown node {:?}",
                branch, node.id
            ),
        })?;
        ensure!(
            *stored == *node,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "branch view for {:?} node {:?} mismatches global node log",
                    branch, node.id
                ),
            }
        );
        if index == 0 {
            continue;
        }
        let previous = &nodes[index - 1];
        ensure!(
            node.parent == previous.id,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("branch view for {:?} is not a continuous chain", branch),
            }
        );
    }

    Ok(nodes.last().expect("nodes should not be empty").id.clone())
}

fn map_session_validation_error<T>(path: &Path, result: Result<T>) -> Result<T> {
    result.map_err(|source| match source {
        StoreError::CorruptedStore { .. } => source,
        _ => StoreError::CorruptedStore {
            path: path.to_owned(),
            message: source.to_string(),
        },
    })
}

fn encode_branch_name(branch: &str) -> String {
    let mut encoded = String::new();
    for byte in branch.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => {
                encoded.push(char::from(*byte));
            }
            _ => {
                encoded.push('%');
                encoded.push(hex_digit(byte >> 4));
                encoded.push(hex_digit(byte & 0x0F));
            }
        }
    }
    encoded
}

fn decode_branch_path(path: &Path) -> Result<String> {
    let file_name =
        path.file_name()
            .and_then(|name| name.to_str())
            .context(CorruptedStoreSnafu {
                path: path.to_owned(),
                message: "branch view file name is not valid UTF-8".to_owned(),
            })?;
    let encoded = file_name
        .strip_suffix(".jsonl")
        .context(CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "branch view file must have .jsonl extension".to_owned(),
        })?;
    decode_branch_name(encoded).context(CorruptedStoreSnafu {
        path: path.to_owned(),
        message: "branch view file name cannot be decoded".to_owned(),
    })
}

fn decode_branch_name(encoded: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(encoded.len());
    let mut chars = encoded.chars();

    while let Some(ch) = chars.next() {
        if ch != '%' {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                bytes.push(ch as u8);
                continue;
            }
            return None;
        }

        let hi = chars.next()?;
        let lo = chars.next()?;
        let hi = from_hex(hi)?;
        let lo = from_hex(lo)?;
        bytes.push((hi << 4) | lo);
    }

    String::from_utf8(bytes).ok()
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'A' + (value - 10)),
        _ => unreachable!("hex digit should be in range 0..=15"),
    }
}

fn from_hex(ch: char) -> Option<u8> {
    match ch {
        '0'..='9' => Some((ch as u8) - b'0'),
        'A'..='F' => Some((ch as u8) - b'A' + 10),
        'a'..='f' => Some((ch as u8) - b'a' + 10),
        _ => None,
    }
}

fn append_jsonl_record<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize,
{
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .context(WriteStoreLogSnafu {
            path: path.to_owned(),
        })?;
    serde_json::to_writer(&mut file, value).context(SerializeStoreRecordSnafu {
        path: path.to_owned(),
    })?;
    file.write_all(b"\n").context(WriteStoreLogSnafu {
        path: path.to_owned(),
    })?;
    file.flush().context(WriteStoreLogSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

fn append_jsonl_records<T>(path: &Path, values: &[T]) -> Result<()>
where
    T: Serialize,
{
    let mut file = OpenOptions::new()
        .append(true)
        .open(path)
        .context(WriteStoreLogSnafu {
            path: path.to_owned(),
        })?;

    for value in values {
        serde_json::to_writer(&mut file, value).context(SerializeStoreRecordSnafu {
            path: path.to_owned(),
        })?;
        file.write_all(b"\n").context(WriteStoreLogSnafu {
            path: path.to_owned(),
        })?;
    }

    file.flush().context(WriteStoreLogSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

fn write_json_file<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize,
{
    let data = serde_json::to_vec_pretty(value).context(SerializeStoreRecordSnafu {
        path: path.to_owned(),
    })?;
    fs::write(path, data).context(WriteStoreMetaSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

fn write_jsonl_file<T>(path: &Path, values: &[T]) -> Result<()>
where
    T: Serialize,
{
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .context(WriteStoreLogSnafu {
            path: path.to_owned(),
        })?;

    for value in values {
        serde_json::to_writer(&mut file, value).context(SerializeStoreRecordSnafu {
            path: path.to_owned(),
        })?;
        file.write_all(b"\n").context(WriteStoreLogSnafu {
            path: path.to_owned(),
        })?;
    }

    file.flush().context(WriteStoreLogSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

fn write_jsonl_file_create_new<T>(path: &Path, values: &[T]) -> Result<()>
where
    T: Serialize,
{
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .context(WriteStoreLogSnafu {
            path: path.to_owned(),
        })?;

    for value in values {
        serde_json::to_writer(&mut file, value).context(SerializeStoreRecordSnafu {
            path: path.to_owned(),
        })?;
        file.write_all(b"\n").context(WriteStoreLogSnafu {
            path: path.to_owned(),
        })?;
    }

    file.flush().context(WriteStoreLogSnafu {
        path: path.to_owned(),
    })?;
    Ok(())
}

fn read_json_file<T>(path: &Path) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let data = fs::read_to_string(path).context(WriteStoreMetaSnafu {
        path: path.to_owned(),
    })?;
    serde_json::from_str(&data).context(ParseStoreMetaSnafu {
        path: path.to_owned(),
    })
}

fn read_jsonl_file<T>(path: &Path) -> Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .context(WriteStoreLogSnafu {
            path: path.to_owned(),
        })?;
    let reader = BufReader::new(file);
    let mut values = Vec::new();

    for (index, line) in reader.lines().enumerate() {
        let line = line.context(WriteStoreLogSnafu {
            path: path.to_owned(),
        })?;
        if line.trim().is_empty() {
            continue;
        }

        let value = serde_json::from_str(&line).context(ParseStoreLogSnafu {
            path: path.to_owned(),
            line: index + 1,
        })?;
        values.push(value);
    }

    Ok(values)
}
