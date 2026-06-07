use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

use serde::{Deserialize, Serialize};
use snafu::prelude::*;

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

const STORE_FORMAT_VERSION: &str = "2026-06-07";
const LEGACY_STORE_FORMAT_VERSION: u64 = 10;
const META_FILE_NAME: &str = "meta.json";
const NODES_FILE_NAME: &str = "nodes.jsonl";
const SESSIONS_FILE_NAME: &str = "sessions.json";
const PRESETS_FILE_NAME: &str = "presets.json";
const JOBS_FILE_NAME: &str = "jobs.json";
const JOB_HISTORY_FILE_NAME: &str = "jobs.jsonl";
const JOB_COMPACTION_FILE_NAME: &str = "jobs.compaction.json";
const QUEUES_FILE_NAME: &str = "queues.jsonl";
const BRANCH_QUEUE_DIR_NAME: &str = "queues";
const PROMPT_JOB_BRANCH_QUEUE_PREFIX: &str = "prompt.job/";
const SKILLS_FILE_NAME: &str = "skills.json";
const PRESET_HISTORY_DIR_NAME: &str = "preset-history";
const SKILL_HISTORY_DIR_NAME: &str = "skill-history";
const SHARED_SKILL_HISTORY_DIR_NAME: &str = "shared";
const ORCHESTRATOR_SKILL_HISTORY_DIR_NAME: &str = "orchestrator";
const RUNNER_SKILL_HISTORY_DIR_NAME: &str = "runner";
const BUILTIN_COCO_ORCHESTRATOR_REVISION_ID: &str =
    "eafe15f4db18391cbc6abee65a874317f6b350bed013272dea152e6285c18952";
const BUILTIN_NEW_SKILL_REVISION_ID: &str =
    "f6ede23518a575c8d87472a189b71dedf4fbc92b26403db2af748a00d481dbad";
const BUILTIN_CRONJOB_REVISION_ID: &str =
    "872b8f90c21af69be61fe7d90085dbd4491ca6dedd0aeae08feeee65db3aae5a";
const BUILTIN_RECOVERY_REVISION_ID: &str =
    "91adf3f8b4e2fb11008b58db4d0c62c21b1b76cbe13b53a58e81fdeca1548b3b";
const BUILTIN_COMPACT_REVISION_ID: &str =
    "6a260a4377c10fe227c4957db8a63ebfb8b6b292a9e3862c21402a1c1b73d14e";
const BUILTIN_COCO_RUNNER_REVISION_ID: &str =
    "dcf88bdb5caaa2c8e4702cd5dfaa3e20919e08ce367ab7965e1f0d62710a60f4";
const BUILTIN_TELEGRAM_REVISION_ID: &str =
    "1b3f4dcf9b56400edb41ba960e6743b2e938ee58800e5dbb7fc02b11a8d432a0";
const BRANCHES_DIR_NAME: &str = "branches";
const LOCK_FILE_NAME: &str = "store.lock";
const JOB_COMPACT_MIN_LOG_ENTRIES: usize = 64;
const MESSAGE_QUEUE_COMPACT_MIN_LOG_ENTRIES: usize = 64;
const BUILTIN_SKILL_MIGRATIONS: &[BuiltinSkillMigration] = &[
    BuiltinSkillMigration {
        role: SessionRole::Orchestrator,
        name: "coco-orchestrator",
        from_revision_ids: &[
            // Before the orchestrator runner prompt included load_image.
            "cbc625296d083943949e2255e848aec2c439d4573a3386cd39a63e71726c2438",
            // Before the prompt command was renamed to job.
            "79a81ed8e48dc4bac77d8d87ad5566d3b25c1aa1c6fd63cf89aec1efbc0ea6b9",
            // Before skill run required an explicit handoff.
            "1df4b89775b27c799b4f6b80b32b75c0cccd837dd574048484b38c13a5aff146",
        ],
        target_revision_id: BUILTIN_COCO_ORCHESTRATOR_REVISION_ID,
    },
    BuiltinSkillMigration {
        role: SessionRole::Orchestrator,
        name: "new-skill",
        from_revision_ids: &[],
        target_revision_id: BUILTIN_NEW_SKILL_REVISION_ID,
    },
    BuiltinSkillMigration {
        role: SessionRole::Orchestrator,
        name: "cronjob",
        from_revision_ids: &[
            // Cronjob default before stale prompt job state was ignored.
            "88035685e93fab0d2a1b297aaf3e34da83e7415415112cc2266f7135ed019b9e",
            // Before the prompt command was renamed to job.
            "f57de170e92e784a37b2debbcf6854c73857235a4bf0e699a1cd67035b24cd92",
        ],
        target_revision_id: BUILTIN_CRONJOB_REVISION_ID,
    },
    BuiltinSkillMigration {
        role: SessionRole::Orchestrator,
        name: "recovery",
        from_revision_ids: &[
            // Before the prompt command was renamed to job.
            "6bf4094ad2dd2f9932cfc8d13a0f4a6b7adc9fe293e1ea6bc9f995d9c880a3f8",
            // Before session handoff required an explicit prompt.
            "dfc5ea6b5ef4c46ffb4c0c7d1fde59f1ebfe782eeb673a0987353047b72c7e3b",
        ],
        target_revision_id: BUILTIN_RECOVERY_REVISION_ID,
    },
    BuiltinSkillMigration {
        role: SessionRole::Orchestrator,
        name: "compact",
        from_revision_ids: &[
            // Before the prompt command was renamed to job.
            "3abb36a6333215088666cb168fef445430d19e19e19232e9e703286e3be3b9c6",
            // Before session handoff required an explicit prompt.
            "d035938926144776ca4341aaa57eaa3ed28a76234222f1ff06fe06cf5d8ab9ff",
        ],
        target_revision_id: BUILTIN_COMPACT_REVISION_ID,
    },
    BuiltinSkillMigration {
        role: SessionRole::Runner,
        name: "coco-runner",
        from_revision_ids: &[
            // Before the prompt command was renamed to job.
            "faa2096bbf0847b8e91247c56caf688e02442bdebde1d6dabae0b830ab373f22",
        ],
        target_revision_id: BUILTIN_COCO_RUNNER_REVISION_ID,
    },
    BuiltinSkillMigration {
        role: SessionRole::Runner,
        name: "telegram",
        from_revision_ids: &[
            // Telegram default before the attachment download script was added.
            "8d8630a19107380d2ba0cc1bcd3bf904f888a68bf535364b12b30340a582265c",
            // Telegram default before downloads were directed into the workspace.
            "fe5361a23cc71e2253b9d7867604cf1994db8fb6273dcae2ba2088a48c827e3c",
            // Telegram default before the download script defaulted into the workspace.
            "a86a9cb4ec5d5b8f6284970aa7c9feb53ddfbe7d1b984e9210dda7d1801edfd1",
        ],
        target_revision_id: BUILTIN_TELEGRAM_REVISION_ID,
    },
];
const STORE_MIGRATIONS: &[StoreMigration] = &[
    StoreMigration {
        name: "main-to-2026-05-17",
        from: StoreFormatVersion::Legacy(LEGACY_STORE_FORMAT_VERSION),
        to: StoreFormatVersion::Chronicle("2026-05-17"),
        run: Persistence::migrate_main_store_to_chronicle_format,
        builtin_skills: BUILTIN_SKILL_MIGRATIONS,
    },
    StoreMigration {
        name: "2026-05-16-to-2026-05-17",
        from: StoreFormatVersion::Chronicle("2026-05-16"),
        to: StoreFormatVersion::Chronicle("2026-05-17"),
        run: Persistence::migrate_store_format_without_structural_changes,
        builtin_skills: BUILTIN_SKILL_MIGRATIONS,
    },
    StoreMigration {
        name: "2026-05-17-to-2026-05-18",
        from: StoreFormatVersion::Chronicle("2026-05-17"),
        to: StoreFormatVersion::Chronicle("2026-05-18"),
        run: Persistence::migrate_jobs_to_wal,
        builtin_skills: &[],
    },
    StoreMigration {
        name: "2026-05-18-to-2026-05-21",
        from: StoreFormatVersion::Chronicle("2026-05-18"),
        // PR #79 first introduced recovery/compact builtin skills at this version.
        to: StoreFormatVersion::Chronicle("2026-05-21"),
        run: Persistence::migrate_store_format_without_structural_changes,
        builtin_skills: BUILTIN_SKILL_MIGRATIONS,
    },
    StoreMigration {
        name: "2026-05-21-to-2026-05-23",
        from: StoreFormatVersion::Chronicle("2026-05-21"),
        // origin/main advanced to this version for console store entity views.
        to: StoreFormatVersion::Chronicle("2026-05-23"),
        run: Persistence::migrate_store_format_without_structural_changes,
        builtin_skills: BUILTIN_SKILL_MIGRATIONS,
    },
    StoreMigration {
        name: "2026-05-23-to-2026-05-24",
        from: StoreFormatVersion::Chronicle("2026-05-23"),
        // origin/main advanced to this version before PR #79 was rebased.
        to: StoreFormatVersion::Chronicle("2026-05-24"),
        run: Persistence::migrate_store_format_without_structural_changes,
        builtin_skills: BUILTIN_SKILL_MIGRATIONS,
    },
    StoreMigration {
        name: "2026-05-24-to-2026-05-25",
        from: StoreFormatVersion::Chronicle("2026-05-24"),
        to: StoreFormatVersion::Chronicle("2026-05-25"),
        run: Persistence::migrate_store_format_without_structural_changes,
        builtin_skills: BUILTIN_SKILL_MIGRATIONS,
    },
    StoreMigration {
        name: "2026-05-25-to-2026-05-28",
        from: StoreFormatVersion::Chronicle("2026-05-25"),
        to: StoreFormatVersion::Chronicle("2026-05-28"),
        run: Persistence::migrate_store_format_without_structural_changes,
        builtin_skills: BUILTIN_SKILL_MIGRATIONS,
    },
    StoreMigration {
        name: "2026-05-28-to-2026-05-29",
        from: StoreFormatVersion::Chronicle("2026-05-28"),
        to: StoreFormatVersion::Chronicle("2026-05-29"),
        run: Persistence::migrate_store_format_without_structural_changes,
        builtin_skills: BUILTIN_SKILL_MIGRATIONS,
    },
    StoreMigration {
        name: "2026-05-29-to-2026-05-30",
        from: StoreFormatVersion::Chronicle("2026-05-29"),
        to: StoreFormatVersion::Chronicle("2026-05-30"),
        run: Persistence::migrate_store_format_without_structural_changes,
        builtin_skills: BUILTIN_SKILL_MIGRATIONS,
    },
    StoreMigration {
        name: "2026-05-30-to-2026-06-07",
        from: StoreFormatVersion::Chronicle("2026-05-30"),
        to: StoreFormatVersion::Chronicle(STORE_FORMAT_VERSION),
        run: Persistence::migrate_store_format_without_structural_changes,
        builtin_skills: BUILTIN_SKILL_MIGRATIONS,
    },
];

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
    job_history_path: PathBuf,
    job_compaction_path: PathBuf,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StoreFormatVersion<S = String> {
    Legacy(u64),
    Chronicle(S),
}

struct StoreMigration {
    name: &'static str,
    from: StoreFormatVersion<&'static str>,
    to: StoreFormatVersion<&'static str>,
    run: fn(&Persistence) -> Result<()>,
    builtin_skills: &'static [BuiltinSkillMigration],
}

struct BuiltinSkillMigration {
    role: SessionRole,
    name: &'static str,
    from_revision_ids: &'static [&'static str],
    target_revision_id: &'static str,
}

#[derive(Debug, Default)]
struct BuiltinSkillMigrationSummary {
    added: usize,
    updated: usize,
    skipped_user_modified: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub version: StoreFormatVersion,
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

impl MessageQueueHistoryEntry {
    fn queue(&self) -> Option<&str> {
        match self {
            Self::Enqueued { item } => Some(&item.queue),
            Self::Dequeued { queue, .. } => Some(queue),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
enum JobHistoryEntry {
    Submitted {
        job: Job,
    },
    StatusChanged {
        job_id: String,
        expected: JobStatus,
        updated: Job,
    },
    WorkBranchChanged {
        job_id: String,
        expected_work_branch: String,
        updated: Job,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedSkillRecord {
    name: String,
    current_version: u64,
    #[serde(default)]
    id: String,
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
    #[serde(default)]
    id: String,
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

impl<S: AsRef<str>> std::fmt::Display for StoreFormatVersion<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy(version) => write!(formatter, "{version}"),
            Self::Chronicle(version) => formatter.write_str(version.as_ref()),
        }
    }
}

impl StoreFormatVersion<String> {
    fn current() -> Self {
        Self::Chronicle(STORE_FORMAT_VERSION.to_owned())
    }
}

impl From<StoreFormatVersion<&'static str>> for StoreFormatVersion<String> {
    fn from(version: StoreFormatVersion<&'static str>) -> Self {
        match version {
            StoreFormatVersion::Legacy(version) => Self::Legacy(version),
            StoreFormatVersion::Chronicle(version) => Self::Chronicle(version.to_owned()),
        }
    }
}

impl BuiltinSkillMigrationSummary {
    fn changed(&self) -> bool {
        self.added > 0 || self.updated > 0
    }
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
            id: current.id.clone(),
            created_at: current.created_at,
            description: current.description.clone(),
            body: current.body.clone(),
            scripts: current.scripts.clone(),
            enable_coco_shim: current.enable_coco_shim,
        }
    }

    fn normalize_id(&mut self) -> bool {
        let previous = self.id.clone();
        let mut version = SkillVersion {
            id: self.id.clone(),
            version: self.current_version,
            created_at: self.created_at,
            description: self.description.clone(),
            body: self.body.clone(),
            scripts: self.scripts.clone(),
            enable_coco_shim: self.enable_coco_shim,
        };
        version.normalize_id();
        self.id = version.id;
        self.id != previous
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

    fn normalize_ids(&mut self) -> bool {
        let mut changed = false;
        for record in self.orchestrator.values_mut() {
            changed |= record.normalize_id();
        }
        for record in self.runner.values_mut() {
            changed |= record.normalize_id();
        }
        changed
    }
}

impl SkillHistoryEntry {
    fn from_version(role: SessionRole, name: &str, version: &SkillVersion) -> Self {
        Self {
            role,
            name: name.to_owned(),
            id: version.id.clone(),
            version: version.version,
            created_at: version.created_at,
            description: version.description.clone(),
            body: version.body.clone(),
            scripts: version.scripts.clone(),
            enable_coco_shim: version.enable_coco_shim,
        }
    }

    fn into_version(self) -> SkillVersion {
        let mut version = SkillVersion {
            id: self.id,
            version: self.version,
            created_at: self.created_at,
            description: self.description,
            body: self.body,
            scripts: self.scripts,
            enable_coco_shim: self.enable_coco_shim,
        };
        version.normalize_id();
        version
    }

    fn normalize_id(&mut self) -> bool {
        let previous = self.id.clone();
        let mut version = self.clone().into_version();
        version.normalize_id();
        self.id = version.id;
        self.id != previous
    }
}

fn validate_snapshot<V>(
    entity: &str,
    history_path: &Path,
    snapshot_key: &str,
    current_version: u64,
    versions: &VersionMap<V>,
    matches_version: impl FnOnce(&V) -> bool,
) -> Result<()> {
    let persisted_current = versions
        .get(&current_version)
        .context(CorruptedStoreSnafu {
            path: history_path.to_owned(),
            message: format!(
                "missing current {entity} version {current_version} for {snapshot_key:?}",
            ),
        })?;
    ensure!(
        matches_version(persisted_current),
        CorruptedStoreSnafu {
            path: history_path.to_owned(),
            message: format!("current {entity} snapshot mismatch for {snapshot_key:?}"),
        }
    );
    Ok(())
}

fn validate_versions<V>(
    entity: &str,
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
                    version, entity, key, expected_version
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
                    entity, key, entry_version, version
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
            job_history_path: path.join(JOB_HISTORY_FILE_NAME),
            job_compaction_path: path.join(JOB_COMPACTION_FILE_NAME),
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
            job_history_path: path.join(JOB_HISTORY_FILE_NAME),
            job_compaction_path: path.join(JOB_COMPACTION_FILE_NAME),
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

    fn migrate_store(&self, from: StoreFormatVersion, root_id: &str) -> Result<()> {
        debug_assert_eq!(self.access, StoreAccess::ReadWrite);

        tracing::info!(
            store_path = %self.dir.display(),
            from_version = %from,
            to_version = STORE_FORMAT_VERSION,
            "starting store format migration"
        );

        let mut current = from;
        while current != StoreFormatVersion::current() {
            let migration = store_migration_from(&current).context(CorruptedStoreSnafu {
                path: self.meta_path.clone(),
                message: format!(
                    "missing store migration from version {} to {}",
                    current, STORE_FORMAT_VERSION
                ),
            })?;
            tracing::info!(
                store_path = %self.dir.display(),
                migration = migration.name,
                from_version = %migration.from,
                to_version = %migration.to,
                "running store migration"
            );
            (migration.run)(self)?;
            self.migrate_builtin_skills_for_store_schema(migration.builtin_skills)?;
            self.persist_store_format_version(migration.to.into(), root_id)?;
            tracing::info!(
                store_path = %self.dir.display(),
                migration = migration.name,
                from_version = %migration.from,
                to_version = %migration.to,
                "completed store migration"
            );
            current = migration.to.into();
        }

        tracing::info!(
            store_path = %self.dir.display(),
            meta_path = %self.meta_path.display(),
            to_version = STORE_FORMAT_VERSION,
            "completed store format migration"
        );
        Ok(())
    }

    fn migrate_main_store_to_chronicle_format(&self) -> Result<()> {
        if !self.queues_path.exists() {
            tracing::info!(
                store_path = %self.dir.display(),
                path = %self.queues_path.display(),
                "creating missing message queue history during store migration"
            );
            write_jsonl_file(&self.queues_path, &[] as &[MessageQueueHistoryEntry])?;
        }
        self.migrate_skill_version_ids()?;
        Ok(())
    }

    fn migrate_store_format_without_structural_changes(&self) -> Result<()> {
        Ok(())
    }

    fn migrate_jobs_to_wal(&self) -> Result<()> {
        if !self.job_history_path.exists() {
            tracing::info!(
                store_path = %self.dir.display(),
                path = %self.job_history_path.display(),
                "creating missing prompt job WAL during store migration"
            );
            write_jsonl_file(&self.job_history_path, &[] as &[JobHistoryEntry])?;
        }
        Ok(())
    }

    fn migrate_skill_version_ids(&self) -> Result<()> {
        if self.skills_path.exists() {
            let mut skills = read_json_file::<PersistedSkillGroups>(&self.skills_path)?;
            if skills.normalize_ids() {
                tracing::info!(
                    store_path = %self.dir.display(),
                    path = %self.skills_path.display(),
                    "adding skill revision ids to current skill snapshots during store migration"
                );
                write_json_file(&self.skills_path, &skills)?;
            }
        }

        for path in self.list_skill_history_paths()? {
            let mut entries = read_jsonl_file::<SkillHistoryEntry>(&path)?;
            let mut changed = false;
            for entry in &mut entries {
                changed |= entry.normalize_id();
            }
            if changed {
                tracing::info!(
                    store_path = %self.dir.display(),
                    path = %path.display(),
                    "adding skill revision ids to skill history during store migration"
                );
                write_jsonl_file(&path, &entries)?;
            }
        }
        Ok(())
    }

    fn migrate_builtin_skills_for_store_schema(
        &self,
        migrations: &[BuiltinSkillMigration],
    ) -> Result<()> {
        if migrations.is_empty() {
            return Ok(());
        }
        let skill_groups = read_json_file::<PersistedSkillGroups>(&self.skills_path)?;
        if skill_groups.is_empty() {
            return Ok(());
        }

        let mut groups = load_skill_groups_from_history(self, &skill_groups)?;
        let builtin_skill_migration = migrate_builtin_skills(&mut groups, migrations);
        if builtin_skill_migration.changed() {
            tracing::info!(
                store_path = %self.dir.display(),
                added = builtin_skill_migration.added,
                updated = builtin_skill_migration.updated,
                skipped_user_modified = builtin_skill_migration.skipped_user_modified,
                "persisting builtin skill migrations during store migration"
            );
            self.rewrite_skill_history(&groups)?;
            write_json_file(
                &self.skills_path,
                &PersistedSkillGroups::from_groups(&groups),
            )?;
        }
        Ok(())
    }

    fn persist_store_format_version(
        &self,
        version: StoreFormatVersion,
        root_id: &str,
    ) -> Result<()> {
        write_json_file(
            &self.meta_path,
            &Meta {
                version,
                root_id: root_id.to_owned(),
            },
        )
    }

    fn branch_path(&self, branch: &str) -> PathBuf {
        self.branches_dir
            .join(format!("{}.jsonl", encode_branch_name(branch)))
    }

    fn branch_message_queue_path(&self, branch: &str) -> PathBuf {
        self.dir
            .join(BRANCH_QUEUE_DIR_NAME)
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
            version: StoreFormatVersion::current(),
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
        write_jsonl_file(&self.job_history_path, &[] as &[JobHistoryEntry])?;
        write_jsonl_file(&self.queues_path, &[] as &[MessageQueueHistoryEntry])?;
        write_json_file(
            &self.skills_path,
            &PersistedSkillGroups::from_groups(&skill_groups),
        )?;
        self.rewrite_skill_history(&skill_groups)?;

        Ok((self.clone(), store))
    }

    fn load(&self) -> Result<(Self, StoreState)> {
        self.ensure_load_layout()?;
        let meta = self.load_current_meta()?;
        let mut store = self.load_nodes(&meta)?;
        self.load_branches(&mut store)?;
        self.load_sessions(&mut store)?;
        let preset_snapshots = self.load_presets(&mut store)?;
        self.load_jobs_and_queues(&mut store)?;
        self.load_skills(&mut store)?;
        self.recover_preset_snapshots(&store, preset_snapshots)?;

        Ok((self.clone(), store))
    }

    fn ensure_store_file(&self, path: &Path, message: &str) -> Result<()> {
        ensure!(
            path.is_file(),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: message.to_owned(),
            }
        );
        Ok(())
    }

    fn ensure_store_directory(&self, path: &Path, message: &str) -> Result<()> {
        ensure!(
            path.is_dir(),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: message.to_owned(),
            }
        );
        Ok(())
    }

    fn ensure_existing_directory_if_present(&self, path: &Path, message: &str) -> Result<()> {
        if !path.exists() {
            return Ok(());
        }
        self.ensure_store_directory(path, message)
    }

    fn ensure_load_layout(&self) -> Result<()> {
        self.ensure_store_file(&self.meta_path, "missing meta.json")?;
        self.ensure_store_file(&self.nodes_path, "missing nodes.jsonl")?;
        self.ensure_store_directory(&self.branches_dir, "missing branches directory")?;
        self.ensure_store_file(&self.jobs_path, "missing jobs snapshot file")?;
        self.ensure_store_file(&self.skills_path, "missing skills metadata file")?;
        if self.access == StoreAccess::ReadWrite {
            self.create_skill_history_directories()?;
        } else {
            self.ensure_existing_directory_if_present(
                &self.skill_history_dir,
                "skill history entry must be a directory",
            )?;
        }
        Ok(())
    }

    fn load_current_meta(&self) -> Result<Meta> {
        let mut meta = read_json_file::<Meta>(&self.meta_path)?;
        if meta.version != StoreFormatVersion::current() {
            self.migrate_store_to_current_format(&meta)?;
            meta = self.read_migrated_meta()?;
        }
        Ok(meta)
    }

    fn migrate_store_to_current_format(&self, meta: &Meta) -> Result<()> {
        ensure!(
            store_migration_from(&meta.version).is_some(),
            CorruptedStoreSnafu {
                path: self.meta_path.clone(),
                message: format!(
                    "unsupported store format version {}, expected {}",
                    meta.version, STORE_FORMAT_VERSION
                ),
            }
        );
        if self.access != StoreAccess::ReadWrite {
            tracing::warn!(store_path = %self.dir.display(), from_version = %meta.version, to_version = STORE_FORMAT_VERSION, "read-only store requires writable format migration");
            return CorruptedStoreSnafu {
                path: self.meta_path.clone(),
                message: format!(
                    "store format version {} requires writable migration to {}",
                    meta.version, STORE_FORMAT_VERSION
                ),
            }
            .fail();
        }
        self.migrate_store(meta.version.clone(), &meta.root_id)
    }

    fn read_migrated_meta(&self) -> Result<Meta> {
        let meta = read_json_file::<Meta>(&self.meta_path)?;
        ensure!(
            meta.version == StoreFormatVersion::current(),
            CorruptedStoreSnafu {
                path: self.meta_path.clone(),
                message: format!(
                    "store migration did not upgrade format version to {}",
                    STORE_FORMAT_VERSION
                ),
            }
        );
        Ok(meta)
    }

    fn load_nodes(&self, meta: &Meta) -> Result<StoreState> {
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
        Ok(store)
    }

    fn load_branches(&self, store: &mut StoreState) -> Result<()> {
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
            let head_id = validate_branch_view(&path, &branch, &nodes, store)?;
            store.apply_fork(branch, head_id)?;
        }
        Ok(())
    }

    fn load_sessions(&self, store: &mut StoreState) -> Result<()> {
        self.ensure_store_file(&self.sessions_path, "missing sessions metadata file")?;
        store.sessions = read_json_file::<HashMap<String, SessionState>>(&self.sessions_path)?;
        map_session_validation_error(&self.sessions_path, store.validate_session_records())
    }

    fn load_presets(
        &self,
        store: &mut StoreState,
    ) -> Result<HashMap<String, PersistedPresetRecord>> {
        if self.access == StoreAccess::ReadWrite {
            self.create_preset_history_directory()?;
        } else {
            self.ensure_existing_directory_if_present(
                &self.preset_history_dir,
                "preset history entry must be a directory",
            )?;
        }
        self.ensure_store_file(&self.presets_path, "missing presets metadata file")?;
        let preset_snapshots = read_preset_snapshots(&self.presets_path)?;
        let presets = load_preset_history_records(self)?;
        validate_preset_snapshots(&preset_snapshots, &presets)?;
        store.presets = presets;
        Ok(preset_snapshots)
    }

    fn load_jobs_and_queues(&self, store: &mut StoreState) -> Result<()> {
        self.recover_job_compaction()?;
        let job_log_len = self.load_jobs(store)?;
        if self.access == StoreAccess::ReadWrite && job_log_should_compact(job_log_len) {
            self.rewrite_jobs(store)?;
        }
        let (message_queues, message_queue_log_len) = self.load_message_queues()?;
        store.message_queues = message_queues;
        if self.access == StoreAccess::ReadWrite
            && message_queue_log_should_compact(&store.message_queues, message_queue_log_len)
        {
            self.rewrite_message_queue_history(store)?;
        }
        Ok(())
    }

    fn load_skills(&self, store: &mut StoreState) -> Result<()> {
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
        Ok(())
    }

    fn recover_preset_snapshots(
        &self,
        store: &StoreState,
        preset_snapshots: HashMap<String, PersistedPresetRecord>,
    ) -> Result<()> {
        let recovered_preset_snapshots = persisted_preset_records(&store.presets);
        if self.access == StoreAccess::ReadWrite && recovered_preset_snapshots != preset_snapshots {
            self.persist_presets(store)?;
        }
        Ok(())
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

    fn append_job_history_entry(&self, entry: &JobHistoryEntry) -> Result<()> {
        self.ensure_writable()?;
        append_jsonl_record_create(&self.job_history_path, entry)
    }

    fn load_jobs(&self, state: &mut StoreState) -> Result<usize> {
        let snapshot = read_job_snapshots(&self.jobs_path)?;
        validate_job_snapshots(&self.jobs_path, state, &snapshot)?;
        state.jobs = snapshot;
        let entries = read_jsonl_file::<JobHistoryEntry>(&self.job_history_path)?;
        let len = entries.len();
        for entry in entries {
            apply_job_history_entry(&self.job_history_path, state, entry)?;
        }
        Ok(len)
    }

    fn rewrite_jobs(&self, state: &StoreState) -> Result<()> {
        self.ensure_writable()?;
        write_json_file(&self.job_compaction_path, &state.jobs)?;
        write_json_file(&self.jobs_path, &state.jobs)?;
        write_jsonl_file(&self.job_history_path, &[] as &[JobHistoryEntry])?;
        fs::remove_file(&self.job_compaction_path).context(WriteStoreDirectorySnafu {
            path: self.job_compaction_path.clone(),
        })?;
        Ok(())
    }

    fn recover_job_compaction(&self) -> Result<()> {
        if !self.job_compaction_path.exists() {
            return Ok(());
        }
        ensure!(
            self.access == StoreAccess::ReadWrite,
            CorruptedStoreSnafu {
                path: self.job_compaction_path.clone(),
                message: "job compaction requires writable recovery".to_owned(),
            }
        );
        let jobs = read_job_snapshots(&self.job_compaction_path)?;
        write_json_file(&self.jobs_path, &jobs)?;
        write_jsonl_file(&self.job_history_path, &[] as &[JobHistoryEntry])?;
        fs::remove_file(&self.job_compaction_path).context(WriteStoreDirectorySnafu {
            path: self.job_compaction_path.clone(),
        })?;
        Ok(())
    }

    fn load_message_queues(&self) -> Result<(HashMap<String, Vec<MessageQueueItem>>, usize)> {
        let mut queues = HashMap::new();
        let mut len = 0;
        if self.queues_path.exists() {
            ensure!(
                self.queues_path.is_file(),
                CorruptedStoreSnafu {
                    path: self.queues_path.clone(),
                    message: "message queue WAL entry must be a file".to_owned(),
                }
            );
            let entries = read_jsonl_file::<MessageQueueHistoryEntry>(&self.queues_path)?;
            len += entries.len();
            queues.extend(message_queues_from_history(entries));
        } else if self.access == StoreAccess::ReadWrite {
            write_jsonl_file(&self.queues_path, &[] as &[MessageQueueHistoryEntry])?;
        }

        let (branch_queues, branch_len) = self.load_branch_message_queues()?;
        len += branch_len;
        queues.extend(branch_queues);
        Ok((queues, len))
    }

    fn append_message_queue_history_entry(&self, entry: &MessageQueueHistoryEntry) -> Result<()> {
        self.ensure_writable()?;
        match entry.queue() {
            Some(queue) => {
                let path = self.message_queue_history_path(queue)?;
                append_jsonl_record_create(&path, entry)
            }
            None => append_jsonl_record_create(&self.queues_path, entry),
        }
    }

    fn rewrite_message_queue_history(&self, state: &StoreState) -> Result<()> {
        self.ensure_writable()?;
        self.rewrite_message_queue_history_from_map(&state.message_queues)
    }

    fn rewrite_message_queue_history_from_map(
        &self,
        queues: &HashMap<String, Vec<MessageQueueItem>>,
    ) -> Result<()> {
        let mut root_queues = HashMap::new();
        let mut branch_queues = HashMap::new();
        for (queue, items) in queues {
            if prompt_job_branch_from_queue(queue).is_some() {
                branch_queues.insert(queue.clone(), items.clone());
            } else {
                root_queues.insert(queue.clone(), items.clone());
            }
        }

        write_jsonl_file(
            &self.queues_path,
            &message_queue_history_entries(&root_queues),
        )?;
        self.rewrite_branch_message_queue_histories(&branch_queues)
    }

    fn message_queue_history_path(&self, queue: &str) -> Result<PathBuf> {
        let Some(branch) = prompt_job_branch_from_queue(queue) else {
            return Ok(self.queues_path.clone());
        };
        let dir = self.dir.join(BRANCH_QUEUE_DIR_NAME);
        fs::create_dir_all(&dir).context(WriteStoreDirectorySnafu { path: dir.clone() })?;
        Ok(self.branch_message_queue_path(branch))
    }

    fn load_branch_message_queues(
        &self,
    ) -> Result<(HashMap<String, Vec<MessageQueueItem>>, usize)> {
        let mut queues = HashMap::new();
        let mut len = 0;
        let branch_queues_dir = self.dir.join(BRANCH_QUEUE_DIR_NAME);
        if !branch_queues_dir.exists() {
            return Ok((queues, len));
        }

        for entry in fs::read_dir(&branch_queues_dir).context(WriteStoreDirectorySnafu {
            path: branch_queues_dir.clone(),
        })? {
            let entry = entry.context(WriteStoreDirectorySnafu {
                path: branch_queues_dir.clone(),
            })?;
            let queue_path = entry.path();
            if queue_path.is_dir() {
                continue;
            }
            ensure!(
                queue_path.is_file(),
                CorruptedStoreSnafu {
                    path: queue_path.clone(),
                    message: "branch message queue WAL entry must be a file".to_owned(),
                }
            );
            let encoded_branch = queue_path
                .file_name()
                .and_then(|name| name.to_str())
                .context(CorruptedStoreSnafu {
                    path: queue_path.clone(),
                    message: "branch message queue file name is not valid UTF-8".to_owned(),
                })?
                .strip_suffix(".jsonl")
                .context(CorruptedStoreSnafu {
                    path: queue_path.clone(),
                    message: "branch message queue file must have .jsonl extension".to_owned(),
                })?;
            let branch = decode_branch_name(encoded_branch).context(CorruptedStoreSnafu {
                path: queue_path.clone(),
                message: "branch message queue file name cannot be decoded".to_owned(),
            })?;
            let queue = prompt_job_branch_queue_name(&branch);
            let entries = read_jsonl_file::<MessageQueueHistoryEntry>(&queue_path)?;
            len += entries.len();
            queues.extend(
                message_queues_from_history(entries)
                    .into_iter()
                    .filter(|(loaded_queue, _)| loaded_queue == &queue),
            );
        }
        Ok((queues, len))
    }

    fn rewrite_branch_message_queue_histories(
        &self,
        queues: &HashMap<String, Vec<MessageQueueItem>>,
    ) -> Result<()> {
        let branch_queues_dir = self.dir.join(BRANCH_QUEUE_DIR_NAME);
        if !branch_queues_dir.exists() {
            return self.write_live_branch_message_queue_histories(queues);
        }

        for entry in fs::read_dir(&branch_queues_dir).context(WriteStoreDirectorySnafu {
            path: branch_queues_dir.clone(),
        })? {
            let entry = entry.context(WriteStoreDirectorySnafu {
                path: branch_queues_dir.clone(),
            })?;
            let queue_path = entry.path();
            if queue_path.is_dir() {
                continue;
            }
            write_jsonl_file(&queue_path, &[] as &[MessageQueueHistoryEntry])?;
        }

        self.write_live_branch_message_queue_histories(queues)
    }

    fn write_live_branch_message_queue_histories(
        &self,
        queues: &HashMap<String, Vec<MessageQueueItem>>,
    ) -> Result<()> {
        for (queue, items) in queues {
            let Some(branch) = prompt_job_branch_from_queue(queue) else {
                continue;
            };
            let queue_path = self.branch_message_queue_path(branch);
            if let Some(parent) = queue_path.parent() {
                fs::create_dir_all(parent).context(WriteStoreDirectorySnafu {
                    path: parent.to_path_buf(),
                })?;
            }
            let mut single = HashMap::new();
            single.insert(queue.clone(), items.clone());
            write_jsonl_file(&queue_path, &message_queue_history_entries(&single))?;
        }
        Ok(())
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

fn migrate_builtin_skills(
    groups: &mut SkillGroups,
    migrations: &[BuiltinSkillMigration],
) -> BuiltinSkillMigrationSummary {
    let defaults = default_skill_groups();
    let mut summary = BuiltinSkillMigrationSummary::default();

    for migration in migrations {
        let Some(default_record) = defaults
            .for_role(migration.role)
            .get(migration.name)
            .cloned()
        else {
            continue;
        };
        let Some(default_version) = default_record.current().cloned() else {
            continue;
        };
        if default_version.id != migration.target_revision_id {
            tracing::warn!(
                role = migration.role.as_str(),
                skill = migration.name,
                expected_revision = migration.target_revision_id,
                actual_revision = %default_version.id,
                "builtin skill migration target revision does not match compiled default skill"
            );
        }
        let records = groups.for_role_mut(migration.role);
        let Some(record) = records.get_mut(migration.name) else {
            tracing::info!(
                role = migration.role.as_str(),
                skill = migration.name,
                "adding missing builtin skill during store migration"
            );
            records.insert(migration.name.to_owned(), default_record);
            summary.added += 1;
            continue;
        };

        match builtin_skill_migration_action(migration, record, &default_version) {
            BuiltinSkillMigrationAction::Update => {
                tracing::info!(
                    role = migration.role.as_str(),
                    skill = migration.name,
                    from_version = record.current_version,
                    from_revision = %record.current().map(|version| version.id.as_str()).unwrap_or(""),
                    to_revision = %default_version.id,
                    "updating builtin skill during store migration"
                );
                let patch = SkillUpdatePatch {
                    description: Some(default_version.description),
                    body: Some(default_version.body),
                    scripts: Some(default_version.scripts),
                    enable_coco_shim: Some(default_version.enable_coco_shim),
                };
                if record.update(&patch).is_some() {
                    summary.updated += 1;
                }
            }
            BuiltinSkillMigrationAction::SkipUserModified => {
                tracing::debug!(
                    role = migration.role.as_str(),
                    skill = migration.name,
                    current_version = record.current_version,
                    "skipping user-modified builtin skill during store migration"
                );
                summary.skipped_user_modified += 1;
            }
            BuiltinSkillMigrationAction::Unchanged => {}
        }
    }

    if summary.changed() {
        tracing::info!(
            added = summary.added,
            updated = summary.updated,
            skipped_user_modified = summary.skipped_user_modified,
            "completed builtin skill migration"
        );
    }

    summary
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuiltinSkillMigrationAction {
    Update,
    SkipUserModified,
    Unchanged,
}

fn builtin_skill_migration_action(
    migration: &BuiltinSkillMigration,
    record: &SkillRecord,
    target: &SkillVersion,
) -> BuiltinSkillMigrationAction {
    let Some(current) = record.current() else {
        return BuiltinSkillMigrationAction::Unchanged;
    };
    if current.id == target.id {
        return BuiltinSkillMigrationAction::Unchanged;
    }
    if migration.from_revision_ids.contains(&current.id.as_str()) {
        BuiltinSkillMigrationAction::Update
    } else {
        BuiltinSkillMigrationAction::SkipUserModified
    }
}

fn persisted_preset_records(
    records: &HashMap<String, PresetRecord>,
) -> HashMap<String, PersistedPresetRecord> {
    records
        .iter()
        .map(|(name, record)| (name.clone(), PersistedPresetRecord::from_record(record)))
        .collect()
}

fn read_job_snapshots(path: &Path) -> Result<HashMap<String, Job>> {
    let mut snapshots = read_json_file::<HashMap<String, Job>>(path)?;
    for (job_id, job) in &mut snapshots {
        job.normalize_work_branch();
        ensure!(
            job.job_id == *job_id,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "job snapshot key {:?} mismatches record id {:?}",
                    job_id, job.job_id
                ),
            }
        );
    }
    Ok(snapshots)
}

fn validate_job_snapshots(
    path: &Path,
    state: &StoreState,
    jobs: &HashMap<String, Job>,
) -> Result<()> {
    let mut active_by_branch = HashMap::<String, String>::new();
    for raw_job in jobs.values() {
        let mut job = raw_job.clone();
        job.normalize_work_branch();
        validate_job_refs(path, state, &job)?;
        if matches!(job.status, JobStatus::Finished) {
            continue;
        }
        for branch in active_job_branches(&job) {
            if let Some(existing_job_id) =
                active_by_branch.insert(branch.to_owned(), job.job_id.clone())
            {
                return CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "branch {:?} has multiple active prompt jobs {:?} and {:?}",
                        branch, existing_job_id, job.job_id
                    ),
                }
                .fail();
            }
        }
    }
    Ok(())
}

fn apply_job_history_entry(
    path: &Path,
    state: &mut StoreState,
    entry: JobHistoryEntry,
) -> Result<()> {
    match entry {
        JobHistoryEntry::Submitted { job } => apply_submitted_job(path, state, job),
        JobHistoryEntry::StatusChanged {
            job_id,
            expected,
            updated,
        } => apply_job_status_changed(path, state, job_id, expected, updated),
        JobHistoryEntry::WorkBranchChanged {
            job_id,
            expected_work_branch,
            updated,
        } => apply_job_work_branch_changed(path, state, job_id, expected_work_branch, updated),
    }
}

fn apply_submitted_job(path: &Path, state: &mut StoreState, mut job: Job) -> Result<()> {
    job.normalize_work_branch();
    ensure!(
        matches!(job.status, JobStatus::Queued),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "submitted job {:?} has invalid status {:?}",
                job.job_id, job.status
            ),
        }
    );
    ensure!(
        job.finished_at.is_none(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("submitted job {:?} has finished_at", job.job_id),
        }
    );
    validate_job_refs(path, state, &job)?;
    ensure!(
        !state.jobs.contains_key(&job.job_id),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("duplicate job record for {:?}", job.job_id),
        }
    );
    ensure!(
        state
            .jobs
            .values()
            .all(|existing| !jobs_share_active_branch(existing, &job)),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("branch {:?} already has an active prompt job", job.branch),
        }
    );
    state.jobs.insert(job.job_id.clone(), job);
    Ok(())
}

fn apply_job_status_changed(
    path: &Path,
    state: &mut StoreState,
    job_id: String,
    expected: JobStatus,
    mut updated: Job,
) -> Result<()> {
    updated.normalize_work_branch();
    ensure!(
        updated.job_id == job_id,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "status change key {:?} mismatches updated job id {:?}",
                job_id, updated.job_id
            ),
        }
    );
    let current = state
        .jobs
        .get(&job_id)
        .cloned()
        .context(CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("status change references missing job {job_id:?}"),
        })?;
    ensure!(
        current.status == expected,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "job {:?} status change expected {:?}, found {:?}",
                job_id, expected, current.status
            ),
        }
    );
    ensure!(
        job_identity_matches(&current, &updated),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("job {:?} status change modifies immutable fields", job_id),
        }
    );
    ensure!(
        expected.can_transition_to(updated.status),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "invalid job {:?} status transition {:?} -> {:?}",
                job_id, expected, updated.status
            ),
        }
    );
    ensure!(
        matches!(updated.status, JobStatus::Finished) || updated.finished_at.is_none(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "unfinished job {:?} status change includes finished_at",
                job_id
            ),
        }
    );
    state.jobs.insert(job_id, updated);
    Ok(())
}

fn apply_job_work_branch_changed(
    path: &Path,
    state: &mut StoreState,
    job_id: String,
    expected_work_branch: String,
    mut updated: Job,
) -> Result<()> {
    updated.normalize_work_branch();
    ensure!(
        updated.job_id == job_id,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "work branch change key {:?} mismatches updated job id {:?}",
                job_id, updated.job_id
            ),
        }
    );
    let mut current = state
        .jobs
        .get(&job_id)
        .cloned()
        .context(CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("work branch change references missing job {job_id:?}"),
        })?;
    current.normalize_work_branch();
    ensure!(
        current.work_branch == expected_work_branch,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "job {:?} work branch change expected {:?}, found {:?}",
                job_id, expected_work_branch, current.work_branch
            ),
        }
    );
    ensure!(
        job_identity_matches(&current, &updated),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "job {:?} work branch change modifies immutable fields",
                job_id
            ),
        }
    );
    ensure!(
        current.status == updated.status && current.finished_at == updated.finished_at,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "job {:?} work branch change modifies lifecycle fields",
                job_id
            ),
        }
    );
    ensure!(
        !matches!(updated.status, JobStatus::Finished),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!("finished job {:?} changes work branch", job_id),
        }
    );
    validate_job_refs(path, state, &updated)?;
    ensure!(
        state
            .jobs
            .values()
            .filter(|existing| existing.job_id != job_id)
            .all(|existing| !jobs_share_active_branch(existing, &updated)),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "branch {:?} already has an active prompt job",
                updated.work_branch
            ),
        }
    );
    state.jobs.insert(job_id, updated);
    Ok(())
}

fn validate_job_refs(path: &Path, state: &StoreState, job: &Job) -> Result<()> {
    map_job_validation_error(path, state.get_branch_head(&job.branch))?;
    map_job_validation_error(path, state.get_branch_head(&job.work_branch))?;
    map_job_validation_error(path, state.get_node(&job.base))?;
    Ok(())
}

fn map_job_validation_error<T>(path: &Path, result: Result<T>) -> Result<T> {
    result.map_err(|source| match source {
        StoreError::CorruptedStore { .. } => source,
        _ => StoreError::CorruptedStore {
            path: path.to_owned(),
            message: source.to_string(),
        },
    })
}

fn job_identity_matches(left: &Job, right: &Job) -> bool {
    left.job_id == right.job_id
        && left.created_at == right.created_at
        && left.branch == right.branch
        && left.base == right.base
}

fn jobs_share_active_branch(left: &Job, right: &Job) -> bool {
    if matches!(left.status, JobStatus::Finished) || matches!(right.status, JobStatus::Finished) {
        return false;
    }
    let right_branches = active_job_branches(right);
    active_job_branches(left)
        .into_iter()
        .any(|left_branch| right_branches.contains(&left_branch))
}

fn active_job_branches(job: &Job) -> Vec<&str> {
    let work_branch = if job.work_branch.is_empty() {
        job.branch.as_str()
    } else {
        job.work_branch.as_str()
    };
    if job.branch == work_branch {
        vec![job.branch.as_str()]
    } else {
        vec![job.branch.as_str(), work_branch]
    }
}

fn job_log_should_compact(log_len: usize) -> bool {
    log_len >= JOB_COMPACT_MIN_LOG_ENTRIES
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

fn prompt_job_branch_from_queue(queue: &str) -> Option<&str> {
    queue.strip_prefix(PROMPT_JOB_BRANCH_QUEUE_PREFIX)
}

fn prompt_job_branch_queue_name(branch: &str) -> String {
    format!("{PROMPT_JOB_BRANCH_QUEUE_PREFIX}{branch}")
}

fn store_migration_from(from: &StoreFormatVersion) -> Option<&'static StoreMigration> {
    STORE_MIGRATIONS
        .iter()
        .find(|migration| store_format_versions_match(&migration.from, from))
}

fn store_format_versions_match(
    expected: &StoreFormatVersion<&'static str>,
    actual: &StoreFormatVersion,
) -> bool {
    match (expected, actual) {
        (StoreFormatVersion::Legacy(expected), StoreFormatVersion::Legacy(actual)) => {
            actual == expected
        }
        (StoreFormatVersion::Chronicle(expected), StoreFormatVersion::Chronicle(actual)) => {
            actual == expected
        }
        _ => false,
    }
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

fn read_preset_snapshots(path: &Path) -> Result<HashMap<String, PersistedPresetRecord>> {
    let snapshots = read_json_file::<HashMap<String, PersistedPresetRecord>>(path)?;
    for (name, snapshot) in &snapshots {
        validate_preset_snapshot(path, name, snapshot)?;
    }
    Ok(snapshots)
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
    let history_path = Path::new(PRESET_HISTORY_DIR_NAME);
    for snapshot in current.values() {
        let record = history.get(&snapshot.name).context(CorruptedStoreSnafu {
            path: history_path.to_owned(),
            message: format!("missing preset history for {:?}", snapshot.name),
        })?;
        validate_snapshot(
            "preset",
            history_path,
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
    let mut versions = BTreeMap::new();
    let mut source_path = None;

    for (path, entry) in entries {
        source_path.get_or_insert_with(|| path.clone());
        ensure!(
            entry.name == name,
            CorruptedStoreSnafu {
                path: path.clone(),
                message: format!(
                    "preset history entry name {:?} mismatches file name {:?}",
                    entry.name, name
                ),
            }
        );
        ensure!(
            versions
                .insert(entry.version, entry.clone().into_version())
                .is_none(),
            CorruptedStoreSnafu {
                path: path.clone(),
                message: format!(
                    "duplicate history version {} for preset {:?}",
                    entry.version, name
                ),
            }
        );
    }

    let path = source_path.unwrap_or_else(|| PathBuf::from(PRESET_HISTORY_DIR_NAME));
    ensure!(
        !versions.is_empty(),
        CorruptedStoreSnafu {
            path: path.clone(),
            message: format!("empty preset history for {:?}", name),
        }
    );
    validate_versions("preset", &path, name, &versions, |version| version.version)?;
    let current_version = versions
        .keys()
        .next_back()
        .copied()
        .expect("versions checked non-empty");

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
        let entity = format!("skill role {:?}", role);
        let history_path = Path::new(SKILL_HISTORY_DIR_NAME);
        let history = history.for_role(role);
        for snapshot in records.values() {
            let expected_snapshot_id = SkillVersion {
                id: String::new(),
                version: snapshot.current_version,
                created_at: snapshot.created_at,
                description: snapshot.description.clone(),
                body: snapshot.body.clone(),
                scripts: snapshot.scripts.clone(),
                enable_coco_shim: snapshot.enable_coco_shim,
            }
            .expected_id();
            ensure!(
                snapshot.id.is_empty() || snapshot.id == expected_snapshot_id,
                CorruptedStoreSnafu {
                    path: history_path.to_owned(),
                    message: format!(
                        "current {entity} snapshot for {:?} has invalid id",
                        snapshot.name
                    ),
                }
            );
            let record = history.get(&snapshot.name).context(CorruptedStoreSnafu {
                path: history_path.to_owned(),
                message: format!("missing {entity} history for {:?}", snapshot.name),
            })?;
            validate_snapshot(
                &entity,
                history_path,
                &snapshot.name,
                snapshot.current_version,
                &record.versions,
                |version| {
                    version.id == expected_snapshot_id
                        && version.created_at == snapshot.created_at
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
    let entity = format!("skill role {:?}", role);
    let mut versions = BTreeMap::new();
    let mut source_path = None;

    for (path, entry) in entries {
        source_path.get_or_insert_with(|| path.clone());
        ensure!(
            entry.role == role,
            CorruptedStoreSnafu {
                path: path.clone(),
                message: format!(
                    "skill history entry role {:?} mismatches expected role {:?}",
                    entry.role, role
                ),
            }
        );
        ensure!(
            entry.name == name,
            CorruptedStoreSnafu {
                path: path.clone(),
                message: format!(
                    "skill history entry name {:?} mismatches file name {:?}",
                    entry.name, name
                ),
            }
        );
        let version = entry.clone().into_version();
        ensure!(
            version.id_matches_content(),
            CorruptedStoreSnafu {
                path: path.clone(),
                message: format!(
                    "skill history version {} for role {:?} {:?} has invalid id",
                    entry.version, role, name
                ),
            }
        );
        ensure!(
            versions.insert(entry.version, version).is_none(),
            CorruptedStoreSnafu {
                path: path.clone(),
                message: format!(
                    "duplicate history version {} for skill role {:?} {:?}",
                    entry.version, role, name
                ),
            }
        );
    }

    let path = source_path.unwrap_or_else(|| PathBuf::from(SKILL_HISTORY_DIR_NAME));
    ensure!(
        !versions.is_empty(),
        CorruptedStoreSnafu {
            path: path.clone(),
            message: format!("empty skill role {:?} history for {:?}", role, name),
        }
    );
    validate_versions(&entity, &path, name, &versions, |version| version.version)?;
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
    pub fn snapshot_state(&self) -> StoreState {
        self.inner.read().expect("store lock poisoned").clone()
    }

    #[cfg(test)]
    pub fn fail_next_skill_history_append(&self) {
        self.persistence
            .arm_failpoint(SkillPersistenceFailpoint::NextHistoryAppend);
    }

    #[cfg(test)]
    pub fn fail_next_skill_snapshot_write(&self) {
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

    fn handoff_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        prompt: &str,
    ) -> Result<String> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let plan = state.plan_handoff_session(name, patch, prompt)?;
        apply_handoff_plan(&mut state, &self.persistence, plan)
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

fn apply_handoff_plan(
    state: &mut StoreState,
    persistence: &Persistence,
    plan: super::state::HandoffPlan,
) -> Result<String> {
    let mut persisted_state = state.clone();
    persisted_state.insert_existing_node(plan.node.clone())?;
    persisted_state.apply_set_branch_head(
        plan.branch.clone(),
        &plan.expected_old_head,
        plan.new_head.clone(),
    )?;

    persistence.append_node(&plan.node)?;
    persistence.rewrite_branch_view(&plan.branch, &plan.new_head, &persisted_state)?;
    persistence.persist_sessions(&persisted_state)?;

    state.insert_existing_node(plan.node)?;
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
        self.persistence
            .append_job_history_entry(&JobHistoryEntry::Submitted {
                job: created.clone(),
            })?;
        state.jobs = temp.jobs;
        Ok(created)
    }

    fn submit_job_with_id(&self, job_id: &str, branch: &str, base: &str) -> Result<Job> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let created = temp.submit_job_with_id(job_id, branch, base)?;
        self.persistence
            .append_job_history_entry(&JobHistoryEntry::Submitted {
                job: created.clone(),
            })?;
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
        self.persistence
            .append_job_history_entry(&JobHistoryEntry::StatusChanged {
                job_id: job_id.to_owned(),
                expected,
                updated: updated.clone(),
            })?;
        state.jobs = temp.jobs;
        Ok(updated)
    }

    fn set_job_work_branch(
        &self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> Result<Job> {
        let mut state = self.inner.write().expect("store lock poisoned");
        let mut temp = state.clone();
        let updated = temp.set_job_work_branch(job_id, expected_work_branch, next_work_branch)?;
        self.persistence
            .append_job_history_entry(&JobHistoryEntry::WorkBranchChanged {
                job_id: job_id.to_owned(),
                expected_work_branch: expected_work_branch.to_owned(),
                updated: updated.clone(),
            })?;
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

    fn peek_message(&self, queue: &str) -> Result<Option<MessageQueueItem>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .peek_message(queue))
    }

    fn list_queue_messages(&self, queue: &str) -> Result<Vec<MessageQueueItem>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_queue_messages(queue))
    }

    fn list_message_queues(&self) -> Result<Vec<String>> {
        Ok(self
            .inner
            .read()
            .expect("store lock poisoned")
            .list_message_queues())
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

#[cfg(test)]
mod load_tests {
    use super::*;

    #[test]
    fn load_rejects_missing_branches_directory() {
        let tempdir = tempfile::tempdir().expect("temporary directory should be created");
        let path = tempdir.path().join("store");
        let (persistence, _) = Persistence::open(&path).expect("store should initialize");
        drop(persistence);
        fs::remove_dir_all(path.join(BRANCHES_DIR_NAME)).expect("branches directory should remove");

        let err = Persistence::open(&path).expect_err("store should reject missing branches");

        assert!(matches!(err, StoreError::CorruptedStore { .. }));
    }

    #[test]
    fn load_read_only_rejects_skill_history_file() {
        let tempdir = tempfile::tempdir().expect("temporary directory should be created");
        let path = tempdir.path().join("store");
        let (persistence, _) = Persistence::open(&path).expect("store should initialize");
        drop(persistence);
        fs::remove_dir_all(path.join(SKILL_HISTORY_DIR_NAME))
            .expect("skill history directory should remove");
        fs::write(path.join(SKILL_HISTORY_DIR_NAME), b"not a directory")
            .expect("skill history file should write");

        let err = Persistence::open_read_only(&path)
            .expect_err("read-only store should reject skill history file");

        assert!(matches!(err, StoreError::CorruptedStore { .. }));
    }

    #[test]
    fn load_read_only_rejects_preset_history_file() {
        let tempdir = tempfile::tempdir().expect("temporary directory should be created");
        let path = tempdir.path().join("store");
        let (persistence, _) = Persistence::open(&path).expect("store should initialize");
        drop(persistence);
        fs::remove_dir_all(path.join(PRESET_HISTORY_DIR_NAME))
            .expect("preset history directory should remove");
        fs::write(path.join(PRESET_HISTORY_DIR_NAME), b"not a directory")
            .expect("preset history file should write");

        let err = Persistence::open_read_only(&path)
            .expect_err("read-only store should reject preset history file");

        assert!(matches!(err, StoreError::CorruptedStore { .. }));
    }

    #[test]
    fn read_migrated_meta_rejects_non_current_version() {
        let tempdir = tempfile::tempdir().expect("temporary directory should be created");
        let path = tempdir.path().join("store");
        let (persistence, state) = Persistence::open(&path).expect("store should initialize");
        write_json_file(
            &persistence.meta_path,
            &Meta {
                version: StoreFormatVersion::Chronicle("2026-05-29".to_owned()),
                root_id: state.root_id().to_owned(),
            },
        )
        .expect("stale meta should write");

        let err = persistence
            .read_migrated_meta()
            .expect_err("stale migrated meta should fail");

        assert!(matches!(err, StoreError::CorruptedStore { .. }));
    }

    #[test]
    fn recover_preset_snapshots_persists_recovered_records() {
        let tempdir = tempfile::tempdir().expect("temporary directory should be created");
        let path = tempdir.path().join("store");
        let (persistence, mut state) = Persistence::open(&path).expect("store should initialize");
        state
            .set_preset(
                "default",
                Preset {
                    role: SessionRole::Orchestrator,
                    provider_profile: "openai".to_owned(),
                    model: "gpt-5.4".to_owned(),
                    tools: vec![],
                    system_prompt: "system".to_owned(),
                    prompt: "prompt".to_owned(),
                    temperature: Some(0.2),
                    max_tokens: Some(256),
                    additional_params: None,
                    enable_coco_shim: false,
                },
            )
            .expect("preset should be inserted");

        persistence
            .recover_preset_snapshots(&state, HashMap::new())
            .expect("preset snapshots should recover");

        let snapshots =
            read_preset_snapshots(&persistence.presets_path).expect("snapshots should read");
        assert!(snapshots.contains_key("default"));
    }
}

#[cfg(test)]
mod builtin_skill_migration_tests {
    use super::{
        BUILTIN_SKILL_MIGRATIONS, BuiltinSkillMigration, BuiltinSkillMigrationAction,
        STORE_FORMAT_VERSION, STORE_MIGRATIONS, SessionRole, SkillVersion, SkillVersionSpec,
        StoreFormatVersion, builtin_skill_migration_action, default_skill_groups,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct BuiltinSkillRevision {
        role: SessionRole,
        name: &'static str,
        revision_id: String,
    }

    struct HistoricalBuiltinSkillRevision {
        role: SessionRole,
        name: &'static str,
        revision_id: &'static str,
    }

    const PREVIOUS_BUILTIN_SKILL_STORE_FORMAT_VERSION: &str = "2026-05-30";
    const PREVIOUS_BUILTIN_SKILL_REVISIONS: &[HistoricalBuiltinSkillRevision] = &[
        HistoricalBuiltinSkillRevision {
            role: SessionRole::Orchestrator,
            name: "coco-orchestrator",
            revision_id: "1df4b89775b27c799b4f6b80b32b75c0cccd837dd574048484b38c13a5aff146",
        },
        HistoricalBuiltinSkillRevision {
            role: SessionRole::Orchestrator,
            name: "new-skill",
            revision_id: "f6ede23518a575c8d87472a189b71dedf4fbc92b26403db2af748a00d481dbad",
        },
        HistoricalBuiltinSkillRevision {
            role: SessionRole::Orchestrator,
            name: "cronjob",
            revision_id: "872b8f90c21af69be61fe7d90085dbd4491ca6dedd0aeae08feeee65db3aae5a",
        },
        HistoricalBuiltinSkillRevision {
            role: SessionRole::Orchestrator,
            name: "recovery",
            revision_id: "91adf3f8b4e2fb11008b58db4d0c62c21b1b76cbe13b53a58e81fdeca1548b3b",
        },
        HistoricalBuiltinSkillRevision {
            role: SessionRole::Orchestrator,
            name: "compact",
            revision_id: "6a260a4377c10fe227c4957db8a63ebfb8b6b292a9e3862c21402a1c1b73d14e",
        },
        HistoricalBuiltinSkillRevision {
            role: SessionRole::Runner,
            name: "coco-runner",
            revision_id: "dcf88bdb5caaa2c8e4702cd5dfaa3e20919e08ce367ab7965e1f0d62710a60f4",
        },
        HistoricalBuiltinSkillRevision {
            role: SessionRole::Runner,
            name: "telegram",
            revision_id: "1b3f4dcf9b56400edb41ba960e6743b2e938ee58800e5dbb7fc02b11a8d432a0",
        },
    ];

    #[test]
    fn store_migration_builtin_targets_match_current_defaults() {
        let defaults = default_skill_groups();

        for store_migration in STORE_MIGRATIONS {
            for builtin_migration in store_migration.builtin_skills {
                let default_record = defaults
                    .for_role(builtin_migration.role)
                    .get(builtin_migration.name)
                    .expect("builtin migration should point at a default skill");
                let default_version = default_record
                    .current()
                    .expect("default skill should have a current version");

                assert_eq!(
                    builtin_migration.target_revision_id, default_version.id,
                    "builtin skill changes must update the store migration version and target revision for {}",
                    builtin_migration.name
                );
            }
        }
    }

    #[test]
    fn builtin_skill_revision_changes_require_store_format_migration() {
        let current_revisions = current_builtin_skill_revisions();
        let changed_revisions = PREVIOUS_BUILTIN_SKILL_REVISIONS
            .iter()
            .filter_map(|previous| {
                let current = current_revisions
                    .iter()
                    .find(|revision| {
                        revision.role == previous.role && revision.name == previous.name
                    })
                    .unwrap_or_else(|| {
                        panic!(
                            "missing current builtin skill revision for {:?} {}",
                            previous.role, previous.name
                        )
                    });
                (current.revision_id != previous.revision_id).then_some((previous, current))
            })
            .collect::<Vec<_>>();

        if changed_revisions.is_empty() {
            assert_eq!(
                STORE_FORMAT_VERSION, PREVIOUS_BUILTIN_SKILL_STORE_FORMAT_VERSION,
                "unchanged builtin skill revisions should not require a store format bump"
            );
            return;
        }

        assert_ne!(
            STORE_FORMAT_VERSION, PREVIOUS_BUILTIN_SKILL_STORE_FORMAT_VERSION,
            "builtin skill revision changes must bump STORE_FORMAT_VERSION"
        );
        let migration = STORE_MIGRATIONS
            .iter()
            .find(|migration| {
                matches!(
                    migration.from,
                    StoreFormatVersion::Chronicle(PREVIOUS_BUILTIN_SKILL_STORE_FORMAT_VERSION)
                ) && matches!(
                    migration.to,
                    StoreFormatVersion::Chronicle(STORE_FORMAT_VERSION)
                )
            })
            .expect("builtin skill revision changes must add a store migration");

        for (previous, current) in changed_revisions {
            let builtin_migration = migration
                .builtin_skills
                .iter()
                .find(|migration| {
                    migration.role == previous.role && migration.name == previous.name
                })
                .unwrap_or_else(|| {
                    panic!(
                        "missing builtin migration for changed skill {:?} {}",
                        previous.role, previous.name
                    )
                });
            assert!(
                builtin_migration
                    .from_revision_ids
                    .contains(&previous.revision_id),
                "builtin migration for {:?} {} must include previous revision {}",
                previous.role,
                previous.name,
                previous.revision_id
            );
            assert_eq!(
                builtin_migration.target_revision_id, current.revision_id,
                "builtin migration for {:?} {} must target the computed current revision",
                previous.role, previous.name
            );
        }
    }

    #[test]
    fn builtin_skill_migrations_keep_known_source_revisions() {
        let orchestrator = builtin_migration(SessionRole::Orchestrator, "coco-orchestrator");
        assert!(
            orchestrator
                .from_revision_ids
                .contains(&"cbc625296d083943949e2255e848aec2c439d4573a3386cd39a63e71726c2438")
        );
        assert!(
            orchestrator
                .from_revision_ids
                .contains(&"1df4b89775b27c799b4f6b80b32b75c0cccd837dd574048484b38c13a5aff146")
        );

        let cronjob = builtin_migration(SessionRole::Orchestrator, "cronjob");
        assert!(
            cronjob
                .from_revision_ids
                .contains(&"88035685e93fab0d2a1b297aaf3e34da83e7415415112cc2266f7135ed019b9e")
        );

        let recovery = builtin_migration(SessionRole::Orchestrator, "recovery");
        assert!(
            recovery
                .from_revision_ids
                .contains(&"6bf4094ad2dd2f9932cfc8d13a0f4a6b7adc9fe293e1ea6bc9f995d9c880a3f8")
        );

        let compact = builtin_migration(SessionRole::Orchestrator, "compact");
        assert!(
            compact
                .from_revision_ids
                .contains(&"3abb36a6333215088666cb168fef445430d19e19e19232e9e703286e3be3b9c6")
        );

        let telegram = builtin_migration(SessionRole::Runner, "telegram");
        assert!(
            telegram
                .from_revision_ids
                .contains(&"8d8630a19107380d2ba0cc1bcd3bf904f888a68bf535364b12b30340a582265c")
        );
        assert!(
            telegram
                .from_revision_ids
                .contains(&"fe5361a23cc71e2253b9d7867604cf1994db8fb6273dcae2ba2088a48c827e3c")
        );
        assert!(
            telegram
                .from_revision_ids
                .contains(&"a86a9cb4ec5d5b8f6284970aa7c9feb53ddfbe7d1b984e9210dda7d1801edfd1")
        );
    }

    #[test]
    fn builtin_skill_migration_sources_exclude_current_targets() {
        for migration in BUILTIN_SKILL_MIGRATIONS {
            assert!(
                !migration
                    .from_revision_ids
                    .contains(&migration.target_revision_id),
                "builtin migration sources should only list old default revisions for {}",
                migration.name
            );
        }
    }

    #[test]
    fn updates_known_builtin_revision_to_new_target() {
        let defaults = default_skill_groups();
        let record = defaults
            .orchestrator
            .get("coco-orchestrator")
            .expect("default skill should exist");
        let target = SkillVersion::new(
            1,
            SkillVersionSpec {
                description: "New default".to_owned(),
                body: "New body".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        );

        assert_ne!(record.current().unwrap().id, target.id);
        let migration = BuiltinSkillMigration {
            role: SessionRole::Orchestrator,
            name: "coco-orchestrator",
            from_revision_ids: &[
                // Current default in this test before the synthetic target update.
                "eafe15f4db18391cbc6abee65a874317f6b350bed013272dea152e6285c18952",
            ],
            target_revision_id: "new-target-revision",
        };
        assert_eq!(
            builtin_skill_migration_action(&migration, record, &target),
            BuiltinSkillMigrationAction::Update
        );
    }

    #[test]
    fn skips_unknown_builtin_revision() {
        let mut record = default_skill_groups()
            .orchestrator
            .remove("coco-orchestrator")
            .expect("default skill should exist");
        record
            .update(&crate::SkillUpdatePatch {
                description: Some("Custom".to_owned()),
                body: Some("Custom body".to_owned()),
                scripts: None,
                enable_coco_shim: None,
            })
            .expect("custom update should create a version");
        let target = SkillVersion::new(
            1,
            SkillVersionSpec {
                description: "New default".to_owned(),
                body: "New body".to_owned(),
                scripts: Vec::new(),
                enable_coco_shim: true,
            },
        );
        let migration = BuiltinSkillMigration {
            role: SessionRole::Orchestrator,
            name: "coco-orchestrator",
            from_revision_ids: &["known-builtin-revision"],
            target_revision_id: "new-target-revision",
        };

        assert_eq!(
            builtin_skill_migration_action(&migration, &record, &target),
            BuiltinSkillMigrationAction::SkipUserModified
        );
    }

    fn builtin_migration(role: SessionRole, name: &str) -> &'static BuiltinSkillMigration {
        BUILTIN_SKILL_MIGRATIONS
            .iter()
            .find(|migration| migration.role == role && migration.name == name)
            .expect("builtin migration should exist")
    }

    fn current_builtin_skill_revisions() -> Vec<BuiltinSkillRevision> {
        let defaults = default_skill_groups();
        let mut revisions = Vec::new();
        for historical in PREVIOUS_BUILTIN_SKILL_REVISIONS {
            let default_record = defaults
                .for_role(historical.role)
                .get(historical.name)
                .unwrap_or_else(|| {
                    panic!(
                        "missing default builtin skill for {:?} {}",
                        historical.role, historical.name
                    )
                });
            revisions.push(BuiltinSkillRevision {
                role: historical.role,
                name: historical.name,
                revision_id: default_record
                    .current()
                    .expect("default builtin skill should have a current version")
                    .id
                    .clone(),
            });
        }
        revisions
    }
}
