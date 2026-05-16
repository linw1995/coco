use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::prelude::*;

use super::projection::ProjectionContext;
use super::state::StoreState;
use super::{
    BranchStore, JobStore, MessageQueueStore, NodeStore, PresetStore, ProcessShareableStore,
    SessionStore, SkillStore,
};
use crate::error::{
    CorruptedStoreSnafu, ParseStoreLogSnafu, ParseStoreMetaSnafu, SerializeStoreRecordSnafu,
    StoreLockedSnafu, StorePathIsNotDirectorySnafu, StoreReadOnlySnafu, WriteStoreDirectorySnafu,
    WriteStoreLogSnafu, WriteStoreMetaSnafu,
};
use crate::{
    Job, JobStatus, MessageQueueItem, NewNode, Node, Preset, PresetRecord, PresetVersion,
    SessionAnchorPatch, SessionRole, SessionState, SkillGroups, SkillRecord, SkillScript,
    SkillUpdatePatch, SkillVersion, SkillVersionSpec, StoreError, StoreResult as Result,
    default_skill_groups,
};

type VersionMap<V> = BTreeMap<u64, V>;
type VersionReplay<V> = (u64, VersionMap<V>);

struct VersionedRecordRef<'a, V> {
    key: &'a str,
    current_version: u64,
    versions: &'a VersionMap<V>,
}

const STORE_FORMAT_VERSION: u64 = 9;
const LEGACY_STORE_FORMAT_VERSION: u64 = 6;
const META_FILE_NAME: &str = "meta.json";
const NODES_FILE_NAME: &str = "nodes.jsonl";
const SESSIONS_FILE_NAME: &str = "sessions.json";
const PRESETS_FILE_NAME: &str = "branch-configs.json";
const JOBS_FILE_NAME: &str = "jobs.json";
const QUEUES_FILE_NAME: &str = "queues.jsonl";
const SKILLS_FILE_NAME: &str = "skills.json";
const PRESET_HISTORY_DIR_NAME: &str = "branch-config-history";
const SKILL_HISTORY_DIR_NAME: &str = "skill-history";
const SHARED_SKILL_HISTORY_DIR_NAME: &str = "shared";
const ORCHESTRATOR_SKILL_HISTORY_DIR_NAME: &str = "orchestrator";
const RUNNER_SKILL_HISTORY_DIR_NAME: &str = "runner";
const BRANCHES_DIR_NAME: &str = "branches";
const LOCK_FILE_NAME: &str = "store.lock";
const MESSAGE_QUEUE_COMPACT_MIN_LOG_ENTRIES: usize = 64;

#[derive(Clone, Debug)]
pub struct FsStore {
    inner: Arc<RwLock<StoreState>>,
    persistence: Arc<Persistence>,
}

#[derive(Debug, Clone)]
pub struct Persistence {
    dir: PathBuf,
    _lock_file: Option<Arc<File>>,
    access: StoreAccess,
    meta_path: PathBuf,
    nodes_path: PathBuf,
    sessions_path: PathBuf,
    presets_path: PathBuf,
    jobs_path: PathBuf,
    queues_path: PathBuf,
    skills_path: PathBuf,
    preset_history_dir: PathBuf,
    skill_history_dir: PathBuf,
    branches_dir: PathBuf,
    #[cfg(test)]
    failpoints: Arc<Mutex<SkillPersistenceFailpoints>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreAccess {
    ReadWrite,
    ReadOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub version: u64,
    pub root_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct PersistedPresetRecord {
    name: String,
    current_version: u64,
    created_at: jiff::Timestamp,
    #[serde(flatten)]
    config: Preset,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct PresetHistoryEntry {
    name: String,
    version: u64,
    created_at: jiff::Timestamp,
    #[serde(flatten)]
    config: Preset,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
enum MessageQueueHistoryEntry {
    Enqueued { item: MessageQueueItem },
    Dequeued { queue: String, message_id: String },
}

#[derive(Debug, Default)]
struct PresetSnapshotRead {
    current: HashMap<String, PersistedPresetRecord>,
    history_seeds: HashMap<String, PresetRecord>,
    migrated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedSkillRecord {
    name: String,
    current_version: u64,
    created_at: jiff::Timestamp,
    description: String,
    body: String,
    #[serde(default)]
    scripts: Vec<SkillScript>,
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
    scripts: Vec<SkillScript>,
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

impl PersistedPresetRecord {
    fn from_record(record: &PresetRecord) -> Self {
        let current = record
            .current()
            .expect("preset record should always have a current version");
        Self::from_version(&record.name, record.current_version, current)
    }

    fn from_version(name: &str, current_version: u64, version: &PresetVersion) -> Self {
        Self {
            name: name.to_owned(),
            current_version,
            created_at: version.created_at,
            config: version.to_preset(),
        }
    }
}

impl PresetHistoryEntry {
    fn from_version(name: &str, version: &PresetVersion) -> Self {
        Self {
            name: name.to_owned(),
            version: version.version,
            created_at: version.created_at,
            config: version.to_preset(),
        }
    }

    fn into_version(self) -> PresetVersion {
        PresetVersion {
            version: self.version,
            created_at: self.created_at,
            config: self.config,
        }
    }
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
            scripts: current.scripts.clone(),
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
            scripts: version.scripts.clone(),
            enable_coco_shim: version.enable_coco_shim,
        }
    }

    fn into_version(self) -> SkillVersion {
        SkillVersion {
            version: self.version,
            created_at: self.created_at,
            description: self.description,
            body: self.body,
            scripts: self.scripts,
            enable_coco_shim: self.enable_coco_shim,
        }
    }
}

fn preset_history_key(entry: &PresetHistoryEntry) -> &str {
    &entry.name
}

fn preset_history_entry_version(entry: &PresetHistoryEntry) -> u64 {
    entry.version
}

fn skill_history_key(entry: &SkillHistoryEntry) -> &str {
    &entry.name
}

fn skill_history_entry_version(entry: &SkillHistoryEntry) -> u64 {
    entry.version
}

fn validate_versioned_record<V>(
    context: &ProjectionContext,
    path: &Path,
    record: VersionedRecordRef<'_, V>,
    version_of: impl Fn(&V) -> u64,
) -> Result<()> {
    ensure!(
        !record.versions.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("{} {:?} has no versions", context.entity(), record.key),
        }
    );
    ensure!(
        record.versions.contains_key(&record.current_version),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "{} {:?} missing current version {}",
                context.entity(),
                record.key,
                record.current_version
            ),
        }
    );
    validate_versions(context, path, record.key, record.versions, version_of)
}

fn versions_from_history<E, V>(
    context: &ProjectionContext,
    fallback_path: &Path,
    entries: &[(PathBuf, E)],
    key_of: impl for<'a> Fn(&'a E) -> &'a str,
    entry_version_of: impl Fn(&E) -> u64,
    into_version: impl Fn(E) -> V,
    version_of: impl Fn(&V) -> u64,
) -> Result<VersionReplay<V>>
where
    E: Clone,
{
    let mut versions = BTreeMap::new();
    let mut source_path = None;
    let mut key = None;

    for (path, entry) in entries {
        source_path.get_or_insert_with(|| path.clone());
        key.get_or_insert_with(|| key_of(entry).to_owned());
        let entry_version = entry_version_of(entry);
        let version = into_version(entry.clone());
        let version_number = version_of(&version);
        ensure!(
            version_number == entry_version,
            CorruptedStoreSnafu {
                path: path.clone(),
                message: format!(
                    "{} {:?} stores version {} in history entry {}",
                    context.entity(),
                    key_of(entry),
                    version_number,
                    entry_version
                ),
            }
        );
        ensure!(
            versions.insert(entry_version, version).is_none(),
            CorruptedStoreSnafu {
                path: path.clone(),
                message: format!(
                    "duplicate history version {} for {} {:?}",
                    entry_version,
                    context.entity(),
                    key_of(entry)
                ),
            }
        );
    }

    let path = source_path.unwrap_or_else(|| fallback_path.to_owned());
    let key = key.unwrap_or_else(|| "<unknown>".to_owned());
    ensure!(
        !versions.is_empty(),
        CorruptedStoreSnafu {
            path: path.clone(),
            message: format!("empty {} history for {:?}", context.entity(), key),
        }
    );
    validate_versions(context, &path, &key, &versions, &version_of)?;
    let current_version = versions
        .keys()
        .next_back()
        .copied()
        .expect("versions checked non-empty");

    Ok((current_version, versions))
}

fn validate_snapshot<V>(
    context: &ProjectionContext,
    snapshot_key: &str,
    current_version: u64,
    versions: &VersionMap<V>,
    matches_version: impl FnOnce(&V) -> bool,
) -> Result<()> {
    let persisted_current = versions
        .get(&current_version)
        .context(CorruptedStoreSnafu {
            path: context.history_path().to_owned(),
            message: format!(
                "missing current {} version {} for {:?}",
                context.entity(),
                current_version,
                snapshot_key
            ),
        })?;
    ensure!(
        matches_version(persisted_current),
        CorruptedStoreSnafu {
            path: context.history_path().to_owned(),
            message: format!(
                "current {} snapshot mismatch for {:?}",
                context.entity(),
                snapshot_key
            ),
        }
    );
    Ok(())
}

fn validate_versions<V>(
    context: &ProjectionContext,
    path: &Path,
    key: &str,
    versions: &VersionMap<V>,
    version_of: impl Fn(&V) -> u64,
) -> Result<()> {
    let mut expected_version = 1;
    for (version, entry) in versions {
        ensure!(
            *version == expected_version,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "non-contiguous history version {} for {} {:?}, expected {}",
                    version,
                    context.entity(),
                    key,
                    expected_version
                ),
            }
        );
        let entry_version = version_of(entry);
        ensure!(
            entry_version == *version,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "{} {:?} stores version {} under key {}",
                    context.entity(),
                    key,
                    entry_version,
                    version
                ),
            }
        );
        expected_version += 1;
    }
    Ok(())
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
        let path = path.as_ref();
        let existed = path.exists();
        let persistence = Self::new_locked(path)?;
        if !existed {
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

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<(Self, StoreState)> {
        let persistence = Self::new_read_only(path.as_ref())?;
        persistence.load()
    }

    pub fn append_node(&self, node: &Node) -> Result<()> {
        self.ensure_writable()?;
        append_jsonl_record(&self.nodes_path, node)
    }

    pub fn persist_fork(&self, branch: &str, head_id: &str, state: &StoreState) -> Result<()> {
        self.ensure_writable()?;
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
        self.ensure_writable()?;
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
        self.ensure_writable()?;
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
        self.ensure_writable()?;
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
        self.ensure_writable()?;
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
        self.ensure_writable()?;
        self.create_skill_history_directories()?;
        let target_path = self.consolidate_skill_history_paths(groups, name)?;
        self.maybe_fail_skill_history_append(&target_path)?;
        append_jsonl_record_create(
            &target_path,
            &SkillHistoryEntry::from_version(role, name, version),
        )
    }

    pub fn append_preset_history_entry(&self, name: &str, version: &PresetVersion) -> Result<()> {
        self.ensure_writable()?;
        self.create_preset_history_directory()?;
        append_jsonl_record_create(
            &self.preset_history_path(name),
            &PresetHistoryEntry::from_version(name, version),
        )
    }

    pub fn rewrite_preset_history(&self, records: &HashMap<String, PresetRecord>) -> Result<()> {
        self.ensure_writable()?;
        self.create_preset_history_directory()?;
        for (name, record) in records {
            let entries = record
                .versions
                .values()
                .map(|version| PresetHistoryEntry::from_version(name, version))
                .collect::<Vec<_>>();
            write_jsonl_file(&self.preset_history_path(name), &entries)?;
        }
        Ok(())
    }

    pub fn delete_preset_history(&self, name: &str) -> Result<()> {
        self.ensure_writable()?;
        let path = self.preset_history_path(name);
        if path.exists() {
            fs::remove_file(&path).context(WriteStoreDirectorySnafu { path })?;
        }
        Ok(())
    }

    pub fn rewrite_skill_history(&self, groups: &SkillGroups) -> Result<()> {
        self.ensure_writable()?;
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

    fn new_locked(path: &Path) -> Result<Self> {
        if path.exists() {
            let metadata = fs::metadata(path).context(WriteStoreDirectorySnafu {
                path: path.to_owned(),
            })?;
            ensure!(
                metadata.is_dir(),
                StorePathIsNotDirectorySnafu {
                    path: path.to_owned(),
                }
            );
        } else {
            fs::create_dir_all(path).context(WriteStoreDirectorySnafu {
                path: path.to_owned(),
            })?;
        }

        let lock_file = open_store_lock(path)?;

        Ok(Self {
            dir: path.to_owned(),
            _lock_file: Some(lock_file),
            access: StoreAccess::ReadWrite,
            meta_path: path.join(META_FILE_NAME),
            nodes_path: path.join(NODES_FILE_NAME),
            sessions_path: path.join(SESSIONS_FILE_NAME),
            presets_path: path.join(PRESETS_FILE_NAME),
            jobs_path: path.join(JOBS_FILE_NAME),
            queues_path: path.join(QUEUES_FILE_NAME),
            skills_path: path.join(SKILLS_FILE_NAME),
            preset_history_dir: path.join(PRESET_HISTORY_DIR_NAME),
            skill_history_dir: path.join(SKILL_HISTORY_DIR_NAME),
            branches_dir: path.join(BRANCHES_DIR_NAME),
            #[cfg(test)]
            failpoints: Arc::new(Mutex::new(SkillPersistenceFailpoints::default())),
        })
    }

    fn new_read_only(path: &Path) -> Result<Self> {
        let metadata = fs::metadata(path).context(WriteStoreDirectorySnafu {
            path: path.to_owned(),
        })?;
        ensure!(
            metadata.is_dir(),
            StorePathIsNotDirectorySnafu {
                path: path.to_owned(),
            }
        );

        Ok(Self {
            dir: path.to_owned(),
            _lock_file: None,
            access: StoreAccess::ReadOnly,
            meta_path: path.join(META_FILE_NAME),
            nodes_path: path.join(NODES_FILE_NAME),
            sessions_path: path.join(SESSIONS_FILE_NAME),
            presets_path: path.join(PRESETS_FILE_NAME),
            jobs_path: path.join(JOBS_FILE_NAME),
            queues_path: path.join(QUEUES_FILE_NAME),
            skills_path: path.join(SKILLS_FILE_NAME),
            preset_history_dir: path.join(PRESET_HISTORY_DIR_NAME),
            skill_history_dir: path.join(SKILL_HISTORY_DIR_NAME),
            branches_dir: path.join(BRANCHES_DIR_NAME),
            #[cfg(test)]
            failpoints: Arc::new(Mutex::new(SkillPersistenceFailpoints::default())),
        })
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.access == StoreAccess::ReadWrite {
            return Ok(());
        }

        StoreReadOnlySnafu {
            path: self.dir.clone(),
        }
        .fail()
    }

    fn branch_path(&self, branch: &str) -> PathBuf {
        self.branches_dir
            .join(format!("{}.jsonl", encode_branch_name(branch)))
    }

    fn create_preset_history_directory(&self) -> Result<()> {
        fs::create_dir_all(&self.preset_history_dir).context(WriteStoreDirectorySnafu {
            path: self.preset_history_dir.clone(),
        })
    }

    fn preset_history_path(&self, name: &str) -> PathBuf {
        self.preset_history_dir
            .join(format!("{}.jsonl", encode_branch_name(name)))
    }

    fn list_preset_history_paths(&self) -> Result<Vec<PathBuf>> {
        if !self.preset_history_dir.exists() {
            if self.access == StoreAccess::ReadOnly {
                return Ok(Vec::new());
            }
            self.create_preset_history_directory()?;
        }
        ensure!(
            self.preset_history_dir.is_dir(),
            CorruptedStoreSnafu {
                path: self.preset_history_dir.clone(),
                message: "preset history entry must be a directory".to_owned(),
            }
        );
        let entries = fs::read_dir(&self.preset_history_dir).context(WriteStoreDirectorySnafu {
            path: self.preset_history_dir.clone(),
        })?;
        let mut paths = Vec::new();
        for entry in entries {
            let entry = entry.context(WriteStoreDirectorySnafu {
                path: self.preset_history_dir.clone(),
            })?;
            let path = entry.path();
            ensure!(
                path.is_file(),
                CorruptedStoreSnafu {
                    path: path.clone(),
                    message: "preset history entry must be a file".to_owned(),
                }
            );
            paths.push(path);
        }
        paths.sort();
        Ok(paths)
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
        if self.access == StoreAccess::ReadWrite {
            self.create_skill_history_directories()?;
        }
        let mut paths = Vec::new();
        for scope in [
            SkillHistoryScope::Shared,
            SkillHistoryScope::Role(SessionRole::Orchestrator),
            SkillHistoryScope::Role(SessionRole::Runner),
        ] {
            let dir = self.skill_history_scope_dir(scope);
            if !dir.exists() {
                if self.access == StoreAccess::ReadOnly {
                    continue;
                }
                fs::create_dir_all(&dir).context(WriteStoreDirectorySnafu { path: dir.clone() })?;
            }
            ensure!(
                dir.is_dir(),
                CorruptedStoreSnafu {
                    path: dir.clone(),
                    message: "skill history scope entry must be a directory".to_owned(),
                }
            );
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
        self.create_preset_history_directory()?;
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
            &self.presets_path,
            &HashMap::<String, PersistedPresetRecord>::new(),
        )?;
        write_json_file(&self.jobs_path, &HashMap::<String, Job>::new())?;
        write_jsonl_file(&self.queues_path, &[] as &[MessageQueueHistoryEntry])?;
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
        if self.access == StoreAccess::ReadWrite {
            self.create_skill_history_directories()?;
        } else if self.skill_history_dir.exists() {
            ensure!(
                self.skill_history_dir.is_dir(),
                CorruptedStoreSnafu {
                    path: self.skill_history_dir.clone(),
                    message: "skill history entry must be a directory".to_owned(),
                }
            );
        }
        let meta = read_json_file::<Meta>(&self.meta_path)?;
        ensure!(
            matches!(
                meta.version,
                LEGACY_STORE_FORMAT_VERSION..=STORE_FORMAT_VERSION
            ),
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
        if self.access == StoreAccess::ReadWrite {
            self.create_preset_history_directory()?;
        } else if self.preset_history_dir.exists() {
            ensure!(
                self.preset_history_dir.is_dir(),
                CorruptedStoreSnafu {
                    path: self.preset_history_dir.clone(),
                    message: "preset history entry must be a directory".to_owned(),
                }
            );
        }
        let preset_snapshots = if self.presets_path.exists() {
            ensure!(
                self.presets_path.is_file(),
                CorruptedStoreSnafu {
                    path: self.presets_path.clone(),
                    message: "preset metadata entry must be a file".to_owned(),
                }
            );
            read_preset_snapshots(&self.presets_path)?
        } else {
            let snapshots = PresetSnapshotRead::default();
            if self.access == StoreAccess::ReadWrite {
                write_json_file(&self.presets_path, &snapshots.current)?;
            }
            snapshots
        };
        let mut presets = load_preset_history_records(self)?;
        let mut preset_history_repaired = false;
        for (name, record) in preset_snapshots.history_seeds {
            if let std::collections::hash_map::Entry::Vacant(entry) = presets.entry(name) {
                entry.insert(record);
                preset_history_repaired = true;
            }
        }
        validate_preset_snapshots(&preset_snapshots.current, &presets)?;
        store.presets = presets;
        store.jobs = read_json_file::<HashMap<String, Job>>(&self.jobs_path)?;
        let (message_queues, message_queue_log_len) = self.load_message_queues()?;
        store.message_queues = message_queues;
        if self.access == StoreAccess::ReadWrite
            && message_queue_log_should_compact(&store.message_queues, message_queue_log_len)
        {
            self.rewrite_message_queue_history(&store)?;
        }
        let skill_groups = read_json_file::<PersistedSkillGroups>(&self.skills_path)?;
        if skill_groups.is_empty() {
            store.skill_groups = default_skill_groups();
            if self.access == StoreAccess::ReadWrite {
                write_json_file(
                    &self.skills_path,
                    &PersistedSkillGroups::from_groups(&store.skill_groups),
                )?;
                self.rewrite_skill_history(&store.skill_groups)?;
            }
        } else {
            store.skill_groups = load_skill_groups_from_history(self, &skill_groups)?;
            let recovered = PersistedSkillGroups::from_groups(&store.skill_groups);
            if recovered != skill_groups && self.access == StoreAccess::ReadWrite {
                let _ = write_json_file(&self.skills_path, &recovered);
            }
        }
        let recovered_preset_snapshots = persisted_preset_records(&store.presets);
        if self.access == StoreAccess::ReadWrite
            && (preset_snapshots.migrated || recovered_preset_snapshots != preset_snapshots.current)
        {
            self.persist_presets(&store)?;
        }
        if preset_history_repaired && self.access == StoreAccess::ReadWrite {
            self.rewrite_preset_history(&store.presets)?;
        }
        if meta.version != STORE_FORMAT_VERSION && self.access == StoreAccess::ReadWrite {
            write_json_file(
                &self.meta_path,
                &Meta {
                    version: STORE_FORMAT_VERSION,
                    root_id: meta.root_id,
                },
            )?;
        }

        Ok((self.clone(), store))
    }

    pub fn persist_sessions(&self, state: &StoreState) -> Result<()> {
        self.ensure_writable()?;
        write_json_file(&self.sessions_path, &state.list_session_states())
    }

    pub fn persist_presets(&self, state: &StoreState) -> Result<()> {
        self.ensure_writable()?;
        write_json_file(
            &self.presets_path,
            &persisted_preset_records(&state.presets),
        )
    }

    pub fn persist_jobs(&self, state: &StoreState) -> Result<()> {
        self.ensure_writable()?;
        write_json_file(&self.jobs_path, &state.jobs)
    }

    fn load_message_queues(&self) -> Result<(HashMap<String, Vec<MessageQueueItem>>, usize)> {
        if self.queues_path.exists() {
            ensure!(
                self.queues_path.is_file(),
                CorruptedStoreSnafu {
                    path: self.queues_path.clone(),
                    message: "message queue WAL entry must be a file".to_owned(),
                }
            );
            let entries = read_jsonl_file::<MessageQueueHistoryEntry>(&self.queues_path)?;
            let len = entries.len();
            return Ok((message_queues_from_history(entries), len));
        }

        if self.access == StoreAccess::ReadWrite {
            write_jsonl_file(&self.queues_path, &[] as &[MessageQueueHistoryEntry])?;
        }
        Ok((HashMap::new(), 0))
    }

    fn append_message_queue_history_entry(&self, entry: &MessageQueueHistoryEntry) -> Result<()> {
        self.ensure_writable()?;
        append_jsonl_record_create(&self.queues_path, entry)
    }

    fn rewrite_message_queue_history(&self, state: &StoreState) -> Result<()> {
        self.ensure_writable()?;
        self.rewrite_message_queue_history_from_map(&state.message_queues)
    }

    fn rewrite_message_queue_history_from_map(
        &self,
        queues: &HashMap<String, Vec<MessageQueueItem>>,
    ) -> Result<()> {
        write_jsonl_file(&self.queues_path, &message_queue_history_entries(queues))
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

fn persisted_preset_records(
    records: &HashMap<String, PresetRecord>,
) -> HashMap<String, PersistedPresetRecord> {
    records
        .iter()
        .map(|(name, record)| (name.clone(), PersistedPresetRecord::from_record(record)))
        .collect()
}

fn message_queues_from_history(
    entries: Vec<MessageQueueHistoryEntry>,
) -> HashMap<String, Vec<MessageQueueItem>> {
    let mut queues = HashMap::<String, Vec<MessageQueueItem>>::new();
    for entry in entries {
        match entry {
            MessageQueueHistoryEntry::Enqueued { item } => {
                queues.entry(item.queue.clone()).or_default().push(item);
            }
            MessageQueueHistoryEntry::Dequeued { queue, message_id } => {
                if let Some(items) = queues.get_mut(&queue) {
                    if let Some(index) = items.iter().position(|item| item.message_id == message_id)
                    {
                        items.remove(index);
                    }
                    if items.is_empty() {
                        queues.remove(&queue);
                    }
                }
            }
        }
    }
    queues
}

fn message_queue_history_entries(
    queues: &HashMap<String, Vec<MessageQueueItem>>,
) -> Vec<MessageQueueHistoryEntry> {
    let mut entries = Vec::new();
    let mut queue_names = queues.keys().collect::<Vec<_>>();
    queue_names.sort();
    for queue in queue_names {
        if let Some(items) = queues.get(queue) {
            entries.extend(
                items
                    .iter()
                    .cloned()
                    .map(|item| MessageQueueHistoryEntry::Enqueued { item }),
            );
        }
    }
    entries
}

fn message_queue_log_should_compact(
    queues: &HashMap<String, Vec<MessageQueueItem>>,
    log_len: usize,
) -> bool {
    let live_len = queues.values().map(Vec::len).sum::<usize>();
    if live_len == 0 {
        return log_len > 0;
    }

    log_len >= MESSAGE_QUEUE_COMPACT_MIN_LOG_ENTRIES && log_len > live_len.saturating_mul(2)
}

fn open_store_lock(store_dir: &Path) -> Result<Arc<File>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<File>>>> = OnceLock::new();

    let key = store_lock_key(store_dir);
    let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks.lock().expect("store lock registry poisoned");
    if let Some(lock_file) = locks.get(&key).and_then(Weak::upgrade) {
        return Ok(lock_file);
    }

    let lock_path = store_dir.join(LOCK_FILE_NAME);
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .context(WriteStoreMetaSnafu {
            path: lock_path.clone(),
        })?;

    let result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        let lock_file = Arc::new(lock_file);
        locks.insert(key, Arc::downgrade(&lock_file));
        return Ok(lock_file);
    }

    let source = std::io::Error::last_os_error();
    if source.kind() == std::io::ErrorKind::WouldBlock {
        return StoreLockedSnafu {
            path: store_dir.to_owned(),
        }
        .fail();
    }

    Err(StoreError::WriteStoreMeta {
        path: lock_path,
        source,
    })
}

fn store_lock_key(store_dir: &Path) -> PathBuf {
    fs::canonicalize(store_dir).unwrap_or_else(|_| {
        if store_dir.is_absolute() {
            store_dir.to_owned()
        } else {
            std::env::current_dir()
                .map(|current_dir| current_dir.join(store_dir))
                .unwrap_or_else(|_| store_dir.to_owned())
        }
    })
}

fn read_preset_snapshots(path: &Path) -> Result<PresetSnapshotRead> {
    let values = read_json_file::<HashMap<String, Value>>(path)?;
    let mut read = PresetSnapshotRead::default();

    for (name, value) in values {
        if is_embedded_preset_record_value(&value) {
            let record =
                serde_json::from_value::<PresetRecord>(value).context(ParseStoreMetaSnafu {
                    path: path.to_owned(),
                })?;
            validate_preset_record(path, &name, &record)?;
            read.current
                .insert(name.clone(), PersistedPresetRecord::from_record(&record));
            read.history_seeds.insert(name, record);
            read.migrated = true;
        } else if is_preset_snapshot_value(&value) {
            let snapshot = serde_json::from_value::<PersistedPresetRecord>(value).context(
                ParseStoreMetaSnafu {
                    path: path.to_owned(),
                },
            )?;
            validate_preset_snapshot(path, &name, &snapshot)?;
            read.current.insert(name, snapshot);
        } else {
            let config = serde_json::from_value::<Preset>(value).context(ParseStoreMetaSnafu {
                path: path.to_owned(),
            })?;
            let record = PresetRecord::new(name.clone(), config);
            read.current
                .insert(name.clone(), PersistedPresetRecord::from_record(&record));
            read.history_seeds.insert(name, record);
            read.migrated = true;
        }
    }

    Ok(read)
}

fn is_embedded_preset_record_value(value: &Value) -> bool {
    value.get("versions").is_some()
}

fn is_preset_snapshot_value(value: &Value) -> bool {
    value.get("name").is_some()
        || value.get("current_version").is_some()
        || value.get("created_at").is_some()
}

fn validate_preset_record(path: &Path, name: &str, record: &PresetRecord) -> Result<()> {
    ensure!(
        record.name == name,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "preset record key {:?} mismatches record name {:?}",
                name, record.name
            ),
        }
    );
    validate_versioned_record(
        &ProjectionContext::new("preset", PRESET_HISTORY_DIR_NAME),
        path,
        VersionedRecordRef {
            key: &record.name,
            current_version: record.current_version,
            versions: &record.versions,
        },
        |version| version.version,
    )
}

fn validate_preset_snapshot(
    path: &Path,
    name: &str,
    snapshot: &PersistedPresetRecord,
) -> Result<()> {
    ensure!(
        snapshot.name == name,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "preset snapshot key {:?} mismatches record name {:?}",
                name, snapshot.name
            ),
        }
    );
    ensure!(
        snapshot.current_version > 0,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "preset snapshot {:?} has invalid current version {}",
                name, snapshot.current_version
            ),
        }
    );
    Ok(())
}

fn load_preset_history_records(persistence: &Persistence) -> Result<HashMap<String, PresetRecord>> {
    let mut grouped = BTreeMap::<String, Vec<(PathBuf, PresetHistoryEntry)>>::new();
    for path in persistence.list_preset_history_paths()? {
        let entries = read_jsonl_file::<PresetHistoryEntry>(&path)?;
        for entry in entries {
            grouped
                .entry(entry.name.clone())
                .or_default()
                .push((path.clone(), entry));
        }
    }

    grouped
        .iter()
        .map(|(name, entries)| {
            let record = preset_record_from_history(name, entries)?;
            Ok((name.clone(), record))
        })
        .collect()
}

fn validate_preset_snapshots(
    current: &HashMap<String, PersistedPresetRecord>,
    history: &HashMap<String, PresetRecord>,
) -> Result<()> {
    let context = ProjectionContext::new("preset", PRESET_HISTORY_DIR_NAME);
    for snapshot in current.values() {
        let record = history.get(&snapshot.name).context(CorruptedStoreSnafu {
            path: context.history_path().to_owned(),
            message: format!(
                "missing {} history for {:?}",
                context.entity(),
                snapshot.name
            ),
        })?;
        validate_snapshot(
            &context,
            &snapshot.name,
            snapshot.current_version,
            &record.versions,
            |version| {
                version.created_at == snapshot.created_at && version.config == snapshot.config
            },
        )?;
    }
    Ok(())
}

fn preset_record_from_history(
    name: &str,
    entries: &[(PathBuf, PresetHistoryEntry)],
) -> Result<PresetRecord> {
    let (current_version, versions) = versions_from_history(
        &ProjectionContext::new("preset", PRESET_HISTORY_DIR_NAME),
        Path::new(PRESET_HISTORY_DIR_NAME),
        entries,
        preset_history_key,
        preset_history_entry_version,
        PresetHistoryEntry::into_version,
        |version| version.version,
    )?;

    Ok(PresetRecord {
        name: name.to_owned(),
        current_version,
        versions,
    })
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
        let context =
            ProjectionContext::new(format!("skill role {:?}", role), SKILL_HISTORY_DIR_NAME);
        let history = history.for_role(role);
        for snapshot in records.values() {
            let record = history.get(&snapshot.name).context(CorruptedStoreSnafu {
                path: context.history_path().to_owned(),
                message: format!(
                    "missing {} history for {:?}",
                    context.entity(),
                    snapshot.name
                ),
            })?;
            validate_snapshot(
                &context,
                &snapshot.name,
                snapshot.current_version,
                &record.versions,
                |version| {
                    version.created_at == snapshot.created_at
                        && version.description == snapshot.description
                        && version.body == snapshot.body
                        && version.scripts == snapshot.scripts
                        && version.enable_coco_shim == snapshot.enable_coco_shim
                },
            )?;
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
    let (current_version, versions) = versions_from_history(
        &ProjectionContext::new(format!("skill role {:?}", role), SKILL_HISTORY_DIR_NAME),
        Path::new(SKILL_HISTORY_DIR_NAME),
        entries,
        skill_history_key,
        skill_history_entry_version,
        SkillHistoryEntry::into_version,
        |version| version.version,
    )?;

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

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let (persistence, state) = Persistence::open_read_only(path)?;
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

impl NodeStore for FsStore {
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
}

impl BranchStore for FsStore {
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
}

impl SessionStore for FsStore {
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
        apply_rebase_plan(&mut state, &self.persistence, plan)
    }
}

fn apply_rebase_plan(
    state: &mut StoreState,
    persistence: &Persistence,
    plan: super::state::RebasePlan,
) -> Result<String> {
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
        persistence.append_node(node)?;
    }
    persistence.rewrite_branch_view(&plan.branch, &plan.new_head, &persisted_state)?;
    persistence.persist_sessions(&persisted_state)?;

    for node in plan.nodes {
        state.insert_existing_node(node)?;
    }
    state.apply_set_branch_head(plan.branch, &plan.expected_old_head, plan.new_head.clone())?;
    Ok(plan.new_head)
}

impl PresetStore for FsStore {
    fn list_preset_records(&self) -> Result<HashMap<String, PresetRecord>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_preset_records())
    }

    fn get_preset_record(&self, name: &str) -> Result<PresetRecord> {
        self.inner
            .read()
            .expect("store lock poisoned")
            .get_preset_record(name)
    }

    fn set_preset(&self, name: &str, config: Preset) -> Result<PresetRecord> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.set_preset(name, config)?;
        let version = updated
            .current()
            .expect("updated preset should have a current version")
            .clone();
        self.persistence
            .append_preset_history_entry(&updated.name, &version)?;
        let _ = self.persistence.persist_presets(&temp);
        state.presets = temp.presets;
        Ok(updated)
    }

    fn rollback_preset(&self, name: &str, target_version: u64) -> Result<PresetRecord> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.rollback_preset(name, target_version)?;
        let version = updated
            .current()
            .expect("rolled back preset should have a current version")
            .clone();
        self.persistence
            .append_preset_history_entry(&updated.name, &version)?;
        let _ = self.persistence.persist_presets(&temp);
        state.presets = temp.presets;
        Ok(updated)
    }

    fn delete_preset(&self, name: &str) -> Result<()> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        temp.delete_preset(name)?;
        self.persistence.delete_preset_history(name)?;
        self.persistence.persist_presets(&temp)?;
        state.presets = temp.presets;
        Ok(())
    }
}

impl SkillStore for FsStore {
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
}

impl JobStore for FsStore {
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
}

impl MessageQueueStore for FsStore {
    fn enqueue_message(&self, queue: &str, payload: serde_json::Value) -> Result<MessageQueueItem> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let item = temp.enqueue_message(queue, payload);
        self.persistence.append_message_queue_history_entry(
            &MessageQueueHistoryEntry::Enqueued { item: item.clone() },
        )?;
        state.message_queues = temp.message_queues;
        Ok(item)
    }

    fn dequeue_message(&self, queue: &str) -> Result<Option<MessageQueueItem>> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let item = temp.dequeue_message(queue);
        let Some(item) = item else {
            return Ok(None);
        };

        self.persistence.append_message_queue_history_entry(
            &MessageQueueHistoryEntry::Dequeued {
                queue: queue.to_owned(),
                message_id: item.message_id.clone(),
            },
        )?;
        state.message_queues = temp.message_queues;
        if state.message_queues.is_empty() {
            let _ = self.persistence.rewrite_message_queue_history(&state);
        }
        Ok(Some(item))
    }

    fn list_queue_messages(&self, queue: &str) -> Result<Vec<MessageQueueItem>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_queue_messages(queue))
    }
}

impl ProcessShareableStore for FsStore {
    fn store_path(&self) -> &Path {
        self.path()
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
