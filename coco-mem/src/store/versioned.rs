use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use snafu::prelude::*;

use super::log::LogEntry;
use super::projection::ProjectionContext;
use crate::StoreResult as Result;
use crate::error::CorruptedStoreSnafu;
use crate::{BranchConfigRecord, BranchConfigVersion, SkillRecord, SkillVersion};

pub type VersionMap<V> = BTreeMap<u64, V>;
pub type VersionReplay<V> = (u64, VersionMap<V>);

pub trait VersionedRecord {
    type Version;

    fn record_key(&self) -> &str;
    fn current_version(&self) -> u64;
    fn versions(&self) -> &VersionMap<Self::Version>;
}

pub trait VersionedLogEntry: Clone + LogEntry {
    type Version;

    fn version(&self) -> u64;
    fn into_version(self) -> Self::Version;
}

impl VersionedRecord for BranchConfigRecord {
    type Version = BranchConfigVersion;

    fn record_key(&self) -> &str {
        &self.name
    }

    fn current_version(&self) -> u64 {
        self.current_version
    }

    fn versions(&self) -> &VersionMap<Self::Version> {
        &self.versions
    }
}

impl VersionedRecord for SkillRecord {
    type Version = SkillVersion;

    fn record_key(&self) -> &str {
        &self.name
    }

    fn current_version(&self) -> u64 {
        self.current_version
    }

    fn versions(&self) -> &VersionMap<Self::Version> {
        &self.versions
    }
}

pub(crate) fn validate_record<R, F>(
    context: &ProjectionContext,
    path: &Path,
    record: &R,
    version_of: F,
) -> Result<()>
where
    R: VersionedRecord,
    F: Fn(&R::Version) -> u64,
{
    ensure!(
        !record.versions().is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "{} {:?} has no versions",
                context.entity(),
                record.record_key()
            ),
        }
    );
    ensure!(
        record.versions().contains_key(&record.current_version()),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "{} {:?} missing current version {}",
                context.entity(),
                record.record_key(),
                record.current_version()
            ),
        }
    );
    validate_versions(
        context,
        path,
        record.record_key(),
        record.versions(),
        version_of,
    )
}

pub(crate) fn versions_from_history<E, F>(
    context: &ProjectionContext,
    fallback_path: &Path,
    entries: &[(PathBuf, E)],
    version_of: F,
) -> Result<VersionReplay<E::Version>>
where
    E: VersionedLogEntry,
    F: Fn(&E::Version) -> u64,
{
    let mut versions = BTreeMap::new();
    let mut source_path = None;
    let mut key = None;

    for (path, entry) in entries {
        source_path.get_or_insert_with(|| path.clone());
        key.get_or_insert_with(|| entry.log_key().to_owned());
        let entry_version = entry.version();
        let version = entry.clone().into_version();
        let version_number = version_of(&version);
        ensure!(
            version_number == entry_version,
            CorruptedStoreSnafu {
                path: path.clone(),
                message: format!(
                    "{} {:?} stores version {} in history entry {}",
                    context.entity(),
                    entry.log_key(),
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
                    entry.log_key()
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

pub(crate) fn validate_snapshot<V>(
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

fn validate_versions<V, F>(
    context: &ProjectionContext,
    path: &Path,
    key: &str,
    versions: &VersionMap<V>,
    version_of: F,
) -> Result<()>
where
    F: Fn(&V) -> u64,
{
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
