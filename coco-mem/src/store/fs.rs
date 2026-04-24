use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

#[cfg(test)]
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use super::Store;
use super::state::StoreState;
use crate::error::{
    CorruptedStoreSnafu, ParseStoreLogSnafu, ParseStoreMetaSnafu, SerializeStoreRecordSnafu,
    StorePathIsNotDirectorySnafu, WriteStoreDirectorySnafu, WriteStoreLogSnafu,
    WriteStoreMetaSnafu,
};
use crate::{
    BranchConfig, Job, JobStatus, NewNode, Node, SessionAnchorPatch, SessionRole, SessionState,
    SkillGroups, SkillRecord, SkillUpdatePatch, SkillVersion, SkillVersionSpec, StoreError,
    StoreResult as Result, default_skill_groups,
};

const STORE_FORMAT_VERSION: u64 = 6;
const META_FILE_NAME: &str = "meta.json";
const NODES_FILE_NAME: &str = "nodes.jsonl";
const SESSIONS_FILE_NAME: &str = "sessions.json";
const BRANCH_CONFIGS_FILE_NAME: &str = "branch-configs.json";
const JOBS_FILE_NAME: &str = "jobs.json";
const SKILLS_FILE_NAME: &str = "skills.json";
const SKILL_HISTORY_DIR_NAME: &str = "skill-history";
const SHARED_SKILL_HISTORY_DIR_NAME: &str = "shared";
const ORCHESTRATOR_SKILL_HISTORY_DIR_NAME: &str = "orchestrator";
const RUNNER_SKILL_HISTORY_DIR_NAME: &str = "runner";
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
    branch_configs_path: PathBuf,
    jobs_path: PathBuf,
    skills_path: PathBuf,
    skill_history_dir: PathBuf,
    branches_dir: PathBuf,
    #[cfg(test)]
    failpoints: Arc<Mutex<SkillPersistenceFailpoints>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Meta {
    pub version: u64,
    pub root_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedSkillRecord {
    name: String,
    current_version: u64,
    created_at: jiff::Timestamp,
    description: String,
    body: String,
    #[serde(default)]
    enable_coco_shim: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedSkillGroups {
    #[serde(default)]
    orchestrator: BTreeMap<String, PersistedSkillRecord>,
    #[serde(default)]
    runner: BTreeMap<String, PersistedSkillRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SkillHistoryEntry {
    role: SessionRole,
    name: String,
    version: u64,
    created_at: jiff::Timestamp,
    description: String,
    body: String,
    #[serde(default)]
    enable_coco_shim: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkillHistoryScope {
    Shared,
    Role(SessionRole),
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkillPersistenceFailpoint {
    NextHistoryAppend,
    NextSnapshotWrite,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct SkillPersistenceFailpoints {
    next: Option<SkillPersistenceFailpoint>,
}

impl PersistedSkillRecord {
    fn from_record(record: &SkillRecord) -> Self {
        let current = record
            .current()
            .expect("skill record should always have a current version");
        Self {
            name: record.name.clone(),
            current_version: record.current_version,
            created_at: current.created_at,
            description: current.description.clone(),
            body: current.body.clone(),
            enable_coco_shim: current.enable_coco_shim,
        }
    }
}

impl PersistedSkillGroups {
    fn from_groups(groups: &SkillGroups) -> Self {
        Self {
            orchestrator: groups
                .orchestrator
                .iter()
                .map(|(name, record)| (name.clone(), PersistedSkillRecord::from_record(record)))
                .collect(),
            runner: groups
                .runner
                .iter()
                .map(|(name, record)| (name.clone(), PersistedSkillRecord::from_record(record)))
                .collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.orchestrator.is_empty() && self.runner.is_empty()
    }
}

impl SkillHistoryEntry {
    fn from_version(role: SessionRole, name: &str, version: &SkillVersion) -> Self {
        Self {
            role,
            name: name.to_owned(),
            version: version.version,
            created_at: version.created_at,
            description: version.description.clone(),
            body: version.body.clone(),
            enable_coco_shim: version.enable_coco_shim,
        }
    }

    fn into_version(self) -> SkillVersion {
        SkillVersion {
            version: self.version,
            created_at: self.created_at,
            description: self.description,
            body: self.body,
            enable_coco_shim: self.enable_coco_shim,
        }
    }
}

impl SkillHistoryScope {
    fn dir_name(self) -> &'static str {
        match self {
            Self::Shared => SHARED_SKILL_HISTORY_DIR_NAME,
            Self::Role(SessionRole::Orchestrator) => ORCHESTRATOR_SKILL_HISTORY_DIR_NAME,
            Self::Role(SessionRole::Runner) => RUNNER_SKILL_HISTORY_DIR_NAME,
        }
    }
}

impl Persistence {
    #[cfg(test)]
    fn arm_failpoint(&self, failpoint: SkillPersistenceFailpoint) {
        self.failpoints
            .lock()
            .expect("failpoint lock poisoned")
            .next = Some(failpoint);
    }

    #[cfg(test)]
    fn maybe_fail_skill_history_append(&self, path: &Path) -> Result<()> {
        let mut failpoints = self.failpoints.lock().expect("failpoint lock poisoned");
        if failpoints.next == Some(SkillPersistenceFailpoint::NextHistoryAppend) {
            failpoints.next = None;
            return Err(StoreError::WriteStoreLog {
                path: path.to_owned(),
                source: std::io::Error::other("injected skill history append failure"),
            });
        }
        Ok(())
    }

    #[cfg(test)]
    fn maybe_fail_skill_snapshot_write(&self) -> Result<()> {
        let mut failpoints = self.failpoints.lock().expect("failpoint lock poisoned");
        if failpoints.next == Some(SkillPersistenceFailpoint::NextSnapshotWrite) {
            failpoints.next = None;
            return Err(StoreError::WriteStoreMeta {
                path: self.skills_path.clone(),
                source: std::io::Error::other("injected skill snapshot write failure"),
            });
        }
        Ok(())
    }

    #[cfg(not(test))]
    fn maybe_fail_skill_history_append(&self, _path: &Path) -> Result<()> {
        Ok(())
    }

    #[cfg(not(test))]
    fn maybe_fail_skill_snapshot_write(&self) -> Result<()> {
        Ok(())
    }

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

    pub fn persist_branch_deletion(&self, branch: &str, state: &StoreState) -> Result<()> {
        let branch_path = self.branch_path(branch);
        ensure!(
            branch_path.is_file(),
            CorruptedStoreSnafu {
                path: branch_path.clone(),
                message: "missing branch view file".to_owned(),
            }
        );
        fs::remove_file(&branch_path).context(WriteStoreDirectorySnafu { path: branch_path })?;
        self.persist_sessions(state)
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

    pub fn persist_skill_groups(&self, state: &StoreState) -> Result<()> {
        self.maybe_fail_skill_snapshot_write()?;
        write_json_file(
            &self.skills_path,
            &PersistedSkillGroups::from_groups(&state.skill_groups),
        )
    }

    pub fn append_skill_history_entry(
        &self,
        groups: &SkillGroups,
        role: SessionRole,
        name: &str,
        version: &SkillVersion,
    ) -> Result<()> {
        self.create_skill_history_directories()?;
        let target_path = self.consolidate_skill_history_paths(groups, name)?;
        self.maybe_fail_skill_history_append(&target_path)?;
        append_jsonl_record_create(
            &target_path,
            &SkillHistoryEntry::from_version(role, name, version),
        )
    }

    pub fn rewrite_skill_history(&self, groups: &SkillGroups) -> Result<()> {
        self.create_skill_history_directories()?;

        let mut history_by_name = BTreeMap::<String, Vec<SkillHistoryEntry>>::new();
        for (role, records) in [
            (SessionRole::Orchestrator, &groups.orchestrator),
            (SessionRole::Runner, &groups.runner),
        ] {
            for (name, record) in records {
                let entries = history_by_name.entry(name.clone()).or_default();
                entries.extend(
                    record
                        .versions
                        .values()
                        .map(|version| SkillHistoryEntry::from_version(role, name, version)),
                );
            }
        }

        for (name, mut entries) in history_by_name {
            entries.sort_by(|left, right| {
                left.version
                    .cmp(&right.version)
                    .then_with(|| left.role.as_str().cmp(right.role.as_str()))
                    .then_with(|| left.created_at.cmp(&right.created_at))
            });
            let path = self.skill_history_path(
                &name,
                if skill_name_is_shared(groups, &name) {
                    None
                } else {
                    find_skill_role(groups, &name)
                },
            );
            write_jsonl_file(&path, &entries)?;
        }
        Ok(())
    }

    fn new(path: &Path) -> Self {
        Self {
            dir: path.to_owned(),
            meta_path: path.join(META_FILE_NAME),
            nodes_path: path.join(NODES_FILE_NAME),
            sessions_path: path.join(SESSIONS_FILE_NAME),
            branch_configs_path: path.join(BRANCH_CONFIGS_FILE_NAME),
            jobs_path: path.join(JOBS_FILE_NAME),
            skills_path: path.join(SKILLS_FILE_NAME),
            skill_history_dir: path.join(SKILL_HISTORY_DIR_NAME),
            branches_dir: path.join(BRANCHES_DIR_NAME),
            #[cfg(test)]
            failpoints: Arc::new(Mutex::new(SkillPersistenceFailpoints::default())),
        }
    }

    fn branch_path(&self, branch: &str) -> PathBuf {
        self.branches_dir
            .join(format!("{}.jsonl", encode_branch_name(branch)))
    }

    fn create_skill_history_directories(&self) -> Result<()> {
        fs::create_dir_all(&self.skill_history_dir).context(WriteStoreDirectorySnafu {
            path: self.skill_history_dir.clone(),
        })?;
        for scope in [
            SkillHistoryScope::Shared,
            SkillHistoryScope::Role(SessionRole::Orchestrator),
            SkillHistoryScope::Role(SessionRole::Runner),
        ] {
            let path = self.skill_history_scope_dir(scope);
            fs::create_dir_all(&path).context(WriteStoreDirectorySnafu { path })?;
        }
        Ok(())
    }

    fn skill_history_scope_dir(&self, scope: SkillHistoryScope) -> PathBuf {
        self.skill_history_dir.join(scope.dir_name())
    }

    fn skill_history_path(&self, skill_name: &str, role: Option<SessionRole>) -> PathBuf {
        let scope = match role {
            Some(role) => SkillHistoryScope::Role(role),
            None => SkillHistoryScope::Shared,
        };
        self.skill_history_scope_dir(scope)
            .join(format!("{}.jsonl", encode_branch_name(skill_name)))
    }

    fn skill_history_candidate_paths(&self, skill_name: &str) -> Vec<PathBuf> {
        vec![
            self.skill_history_path(skill_name, None),
            self.skill_history_path(skill_name, Some(SessionRole::Orchestrator)),
            self.skill_history_path(skill_name, Some(SessionRole::Runner)),
        ]
    }

    fn list_skill_history_paths(&self) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for scope in [
            SkillHistoryScope::Shared,
            SkillHistoryScope::Role(SessionRole::Orchestrator),
            SkillHistoryScope::Role(SessionRole::Runner),
        ] {
            let dir = self.skill_history_scope_dir(scope);
            let entries =
                fs::read_dir(&dir).context(WriteStoreDirectorySnafu { path: dir.clone() })?;
            for entry in entries {
                let entry = entry.context(WriteStoreDirectorySnafu { path: dir.clone() })?;
                let path = entry.path();
                ensure!(
                    path.is_file(),
                    CorruptedStoreSnafu {
                        path: path.clone(),
                        message: "skill history entry must be a file".to_owned(),
                    }
                );
                paths.push(path);
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn consolidate_skill_history_paths(&self, groups: &SkillGroups, name: &str) -> Result<PathBuf> {
        let target_path = self.skill_history_path(
            name,
            if skill_name_is_shared(groups, name) {
                None
            } else {
                find_skill_role(groups, name)
            },
        );
        let existing_paths = self
            .skill_history_candidate_paths(name)
            .into_iter()
            .filter(|path| path.exists())
            .collect::<Vec<_>>();
        let requires_consolidation = existing_paths.iter().any(|path| *path != target_path);
        if !requires_consolidation {
            return Ok(target_path);
        }

        let entries = read_skill_history_entries(&existing_paths)?;
        write_jsonl_file(&target_path, &entries)?;

        for path in existing_paths {
            if path != target_path {
                fs::remove_file(&path).context(WriteStoreDirectorySnafu { path })?;
            }
        }

        Ok(target_path)
    }

    fn initialize(&self) -> Result<(Self, StoreState)> {
        fs::create_dir_all(&self.dir).context(WriteStoreDirectorySnafu {
            path: self.dir.clone(),
        })?;
        fs::create_dir_all(&self.branches_dir).context(WriteStoreDirectorySnafu {
            path: self.branches_dir.clone(),
        })?;
        self.create_skill_history_directories()?;

        let store = StoreState::new();
        let root = store.root_node().clone();
        let skill_groups = store.skill_groups.clone();
        let meta = Meta {
            version: STORE_FORMAT_VERSION,
            root_id: root.id.clone(),
        };

        write_json_file(&self.meta_path, &meta)?;
        write_jsonl_file(&self.nodes_path, &[root])?;
        write_json_file(&self.sessions_path, &HashMap::<String, SessionState>::new())?;
        write_json_file(
            &self.branch_configs_path,
            &HashMap::<String, BranchConfig>::new(),
        )?;
        write_json_file(&self.jobs_path, &HashMap::<String, Job>::new())?;
        write_json_file(
            &self.skills_path,
            &PersistedSkillGroups::from_groups(&skill_groups),
        )?;
        self.rewrite_skill_history(&skill_groups)?;

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
        ensure!(
            self.jobs_path.is_file(),
            CorruptedStoreSnafu {
                path: self.jobs_path.clone(),
                message: "missing jobs metadata file".to_owned(),
            }
        );
        ensure!(
            self.skills_path.is_file(),
            CorruptedStoreSnafu {
                path: self.skills_path.clone(),
                message: "missing skills metadata file".to_owned(),
            }
        );
        ensure!(
            self.skill_history_dir.is_dir(),
            CorruptedStoreSnafu {
                path: self.skill_history_dir.clone(),
                message: "missing skill history directory".to_owned(),
            }
        );
        self.create_skill_history_directories()?;
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
        store.branch_configs = if self.branch_configs_path.exists() {
            ensure!(
                self.branch_configs_path.is_file(),
                CorruptedStoreSnafu {
                    path: self.branch_configs_path.clone(),
                    message: "branch preset config metadata entry must be a file".to_owned(),
                }
            );
            read_json_file::<HashMap<String, BranchConfig>>(&self.branch_configs_path)?
        } else {
            let configs = HashMap::<String, BranchConfig>::new();
            write_json_file(&self.branch_configs_path, &configs)?;
            configs
        };
        store.jobs = read_json_file::<HashMap<String, Job>>(&self.jobs_path)?;
        let skill_groups = read_json_file::<PersistedSkillGroups>(&self.skills_path)?;
        if skill_groups.is_empty() {
            store.skill_groups = default_skill_groups();
            write_json_file(
                &self.skills_path,
                &PersistedSkillGroups::from_groups(&store.skill_groups),
            )?;
            self.rewrite_skill_history(&store.skill_groups)?;
        } else {
            store.skill_groups = load_skill_groups_from_history(self, &skill_groups)?;
            let recovered = PersistedSkillGroups::from_groups(&store.skill_groups);
            if recovered != skill_groups {
                let _ = write_json_file(&self.skills_path, &recovered);
            }
        }

        Ok((self.clone(), store))
    }

    pub fn persist_sessions(&self, state: &StoreState) -> Result<()> {
        write_json_file(&self.sessions_path, &state.list_session_states())
    }

    pub fn persist_branch_configs(&self, state: &StoreState) -> Result<()> {
        write_json_file(&self.branch_configs_path, &state.list_branch_configs())
    }

    pub fn persist_jobs(&self, state: &StoreState) -> Result<()> {
        write_json_file(&self.jobs_path, &state.jobs)
    }
}

fn load_skill_groups_from_history(
    persistence: &Persistence,
    current: &PersistedSkillGroups,
) -> Result<SkillGroups> {
    let history = load_skill_history_records(persistence)?;
    validate_skill_snapshots(current, &history)?;
    Ok(history)
}

fn load_skill_history_records(persistence: &Persistence) -> Result<SkillGroups> {
    let mut orchestrator = BTreeMap::<String, Vec<(PathBuf, SkillHistoryEntry)>>::new();
    let mut runner = BTreeMap::<String, Vec<(PathBuf, SkillHistoryEntry)>>::new();
    for path in persistence.list_skill_history_paths()? {
        let entries = read_jsonl_file::<SkillHistoryEntry>(&path)?;
        for entry in entries {
            let records = match entry.role {
                SessionRole::Orchestrator => &mut orchestrator,
                SessionRole::Runner => &mut runner,
            };
            records
                .entry(entry.name.clone())
                .or_default()
                .push((path.clone(), entry));
        }
    }

    Ok(SkillGroups {
        orchestrator: build_skill_records_for_role(SessionRole::Orchestrator, orchestrator)?,
        runner: build_skill_records_for_role(SessionRole::Runner, runner)?,
    })
}

fn validate_skill_snapshots(current: &PersistedSkillGroups, history: &SkillGroups) -> Result<()> {
    for (role, records) in [
        (SessionRole::Orchestrator, &current.orchestrator),
        (SessionRole::Runner, &current.runner),
    ] {
        for (name, snapshot) in records {
            let record = history
                .for_role(role)
                .get(name)
                .context(CorruptedStoreSnafu {
                    path: PathBuf::from(SKILL_HISTORY_DIR_NAME),
                    message: format!("missing skill history for {:?} role {:?}", name, role),
                })?;
            let persisted_current =
                record
                    .versions
                    .get(&snapshot.current_version)
                    .context(CorruptedStoreSnafu {
                        path: PathBuf::from(SKILL_HISTORY_DIR_NAME),
                        message: format!(
                            "missing current skill version {} for {:?} role {:?}",
                            snapshot.current_version, snapshot.name, role
                        ),
                    })?;
            ensure!(
                persisted_current.created_at == snapshot.created_at
                    && persisted_current.description == snapshot.description
                    && persisted_current.body == snapshot.body
                    && persisted_current.enable_coco_shim == snapshot.enable_coco_shim,
                CorruptedStoreSnafu {
                    path: PathBuf::from(SKILL_HISTORY_DIR_NAME),
                    message: format!(
                        "current skill snapshot mismatch for {:?} role {:?}",
                        snapshot.name, role
                    ),
                }
            );
        }
    }
    Ok(())
}

fn build_skill_records_for_role(
    role: SessionRole,
    grouped: BTreeMap<String, Vec<(PathBuf, SkillHistoryEntry)>>,
) -> Result<BTreeMap<String, SkillRecord>> {
    grouped
        .iter()
        .map(|(name, entries)| {
            let record = skill_record_from_history(role, name, entries)?;
            Ok((name.clone(), record))
        })
        .collect()
}

fn skill_record_from_history(
    role: SessionRole,
    name: &str,
    entries: &[(PathBuf, SkillHistoryEntry)],
) -> Result<SkillRecord> {
    let mut versions = BTreeMap::new();
    let mut source_path = None;
    for (path, entry) in entries {
        source_path.get_or_insert_with(|| path.clone());
        ensure!(
            versions
                .insert(entry.version, entry.clone().into_version())
                .is_none(),
            CorruptedStoreSnafu {
                path: path.clone(),
                message: format!(
                    "duplicate history version {} for skill {:?} role {:?}",
                    entry.version, name, role
                ),
            }
        );
    }
    let path = source_path.unwrap_or_else(|| PathBuf::from(SKILL_HISTORY_DIR_NAME));
    ensure!(
        !versions.is_empty(),
        CorruptedStoreSnafu {
            path: path.clone(),
            message: format!("empty skill history for {:?} role {:?}", name, role),
        }
    );
    let mut expected_version = 1;
    for version in versions.keys().copied() {
        ensure!(
            version == expected_version,
            CorruptedStoreSnafu {
                path: path.clone(),
                message: format!(
                    "non-contiguous history version {} for skill {:?} role {:?}, expected {}",
                    version, name, role, expected_version
                ),
            }
        );
        expected_version += 1;
    }
    let current_version = versions
        .keys()
        .next_back()
        .copied()
        .expect("versions checked non-empty");

    Ok(SkillRecord {
        name: name.to_owned(),
        current_version,
        versions,
    })
}

fn read_skill_history_entries(paths: &[PathBuf]) -> Result<Vec<SkillHistoryEntry>> {
    let mut entries = Vec::new();
    let mut seen = BTreeMap::<String, PathBuf>::new();
    for path in paths {
        for entry in read_jsonl_file::<SkillHistoryEntry>(path)? {
            let key = format!("{}:{}:{}", entry.role.as_str(), entry.name, entry.version);
            if let Some(previous_path) = seen.insert(key.clone(), path.clone()) {
                return CorruptedStoreSnafu {
                    path: path.clone(),
                    message: format!(
                        "duplicate history entry {key:?} across {:?} and {:?}",
                        previous_path, path
                    ),
                }
                .fail();
            }
            entries.push(entry);
        }
    }
    entries.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.role.as_str().cmp(right.role.as_str()))
            .then_with(|| left.version.cmp(&right.version))
            .then_with(|| left.created_at.cmp(&right.created_at))
    });
    Ok(entries)
}

fn skill_name_is_shared(groups: &SkillGroups, name: &str) -> bool {
    groups.orchestrator.contains_key(name) && groups.runner.contains_key(name)
}

fn find_skill_role(groups: &SkillGroups, name: &str) -> Option<SessionRole> {
    if groups.orchestrator.contains_key(name) {
        Some(SessionRole::Orchestrator)
    } else if groups.runner.contains_key(name) {
        Some(SessionRole::Runner)
    } else {
        None
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

    #[cfg(test)]
    pub(crate) fn fail_next_skill_history_append(&self) {
        self.persistence
            .arm_failpoint(SkillPersistenceFailpoint::NextHistoryAppend);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_skill_snapshot_write(&self) {
        self.persistence
            .arm_failpoint(SkillPersistenceFailpoint::NextSnapshotWrite);
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

    fn delete_branch(&self, name: &str) -> Result<()> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        temp.delete_branch(name)?;
        self.persistence.persist_branch_deletion(name, &temp)?;
        state.delete_branch(name)
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

    fn list_children(&self, node_id: &str) -> Result<Vec<Node>> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .list_children(node_id)
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

    fn list_branch_configs(&self) -> Result<HashMap<String, BranchConfig>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_branch_configs())
    }

    fn get_branch_config(&self, name: &str) -> Result<BranchConfig> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_branch_config(name)
    }

    fn set_branch_config(&self, name: &str, config: BranchConfig) -> Result<BranchConfig> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.set_branch_config(name, config);
        self.persistence.persist_branch_configs(&temp)?;
        state.branch_configs = temp.branch_configs;
        Ok(updated)
    }

    fn delete_branch_config(&self, name: &str) -> Result<()> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        temp.delete_branch_config(name)?;
        self.persistence.persist_branch_configs(&temp)?;
        state.branch_configs = temp.branch_configs;
        Ok(())
    }

    fn skill_groups(&self) -> Result<SkillGroups> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .skill_groups())
    }

    fn list_skills(&self, role: SessionRole) -> Result<Vec<SkillRecord>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_skills(role))
    }

    fn get_skill(&self, role: SessionRole, name: &str) -> Result<SkillRecord> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_skill(role, name)
    }

    fn add_skill(
        &self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> Result<SkillRecord> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let created = temp.add_skill(role, name, spec)?;
        let version = created
            .current()
            .expect("created skill should have a current version")
            .clone();
        self.persistence.append_skill_history_entry(
            &temp.skill_groups,
            role,
            &created.name,
            &version,
        )?;
        let _ = self.persistence.persist_skill_groups(&temp);
        state.skill_groups = temp.skill_groups;
        Ok(created)
    }

    fn update_skill(
        &self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> Result<SkillRecord> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.update_skill(role, name, patch)?;
        let version = updated
            .current()
            .expect("updated skill should have a current version")
            .clone();
        self.persistence.append_skill_history_entry(
            &temp.skill_groups,
            role,
            &updated.name,
            &version,
        )?;
        let _ = self.persistence.persist_skill_groups(&temp);
        state.skill_groups = temp.skill_groups;
        Ok(updated)
    }

    fn rollback_skill(
        &self,
        role: SessionRole,
        name: &str,
        target_version: u64,
    ) -> Result<SkillRecord> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.rollback_skill(role, name, target_version)?;
        let version = updated
            .current()
            .expect("rolled back skill should have a current version")
            .clone();
        self.persistence.append_skill_history_entry(
            &temp.skill_groups,
            role,
            &updated.name,
            &version,
        )?;
        let _ = self.persistence.persist_skill_groups(&temp);
        state.skill_groups = temp.skill_groups;
        Ok(updated)
    }

    fn submit_job(&self, branch: &str, base: &str) -> Result<Job> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let created = temp.submit_job(branch, base)?;
        self.persistence.persist_jobs(&temp)?;
        state.jobs = temp.jobs;
        Ok(created)
    }

    fn get_job(&self, job_id: &str) -> Result<Job> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_job(job_id)
    }

    fn list_jobs(&self) -> Result<HashMap<String, Job>> {
        Ok(self.inner.read().expect("store lock poisoned").list_jobs())
    }

    fn set_job_status(&self, job_id: &str, expected: JobStatus, next: JobStatus) -> Result<Job> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.set_job_status(job_id, expected, next)?;
        self.persistence.persist_jobs(&temp)?;
        state.jobs = temp.jobs;
        Ok(updated)
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

fn append_jsonl_record_create<T>(path: &Path, value: &T) -> Result<()>
where
    T: Serialize,
{
    let mut file = OpenOptions::new()
        .create(true)
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
