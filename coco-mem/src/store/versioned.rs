use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use snafu::prelude::*;

use super::log::LogEntry;
use super::projection::ProjectionContext;
use crate::StoreResult as Result;
use crate::error::CorruptedStoreSnafu;
use crate::{BranchConfigRecord, BranchConfigVersion, SkillRecord, SkillVersion};

pub trait VersionId: Copy + Ord + std::fmt::Display {
    fn first() -> Self;
    fn next(self) -> Self;
}

impl VersionId for u64 {
    fn first() -> Self {
        1
    }

    fn next(self) -> Self {
        self + 1
    }
}

pub trait VersionedValue {
    type Id: VersionId;

    fn version(&self) -> Self::Id;
}

pub type VersionMap<V> = BTreeMap<<V as VersionedValue>::Id, V>;
pub type VersionReplay<V> = (<V as VersionedValue>::Id, VersionMap<V>);

pub trait VersionedRecord {
    type Version: VersionedValue;

    fn record_key(&self) -> &str;
    fn current_version(&self) -> <Self::Version as VersionedValue>::Id;
    fn versions(&self) -> &VersionMap<Self::Version>;
}

pub trait VersionedLogEntry: Clone + LogEntry {
    type Version: VersionedValue;

    fn version(&self) -> <Self::Version as VersionedValue>::Id;
    fn into_version(self) -> Self::Version;
}

impl VersionedValue for BranchConfigVersion {
    type Id = u64;

    fn version(&self) -> Self::Id {
        self.version
    }
}

impl VersionedValue for SkillVersion {
    type Id = u64;

    fn version(&self) -> Self::Id {
        self.version
    }
}

impl VersionedRecord for BranchConfigRecord {
    type Version = BranchConfigVersion;

    fn record_key(&self) -> &str {
        &self.name
    }

    fn current_version(&self) -> <Self::Version as VersionedValue>::Id {
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

    fn current_version(&self) -> <Self::Version as VersionedValue>::Id {
        self.current_version
    }

    fn versions(&self) -> &VersionMap<Self::Version> {
        &self.versions
    }
}

pub(crate) fn validate_record<R>(context: &ProjectionContext, path: &Path, record: &R) -> Result<()>
where
    R: VersionedRecord,
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
    validate_versions(context, path, record.record_key(), record.versions())
}

pub(crate) fn versions_from_history<E>(
    context: &ProjectionContext,
    fallback_path: &Path,
    entries: &[(PathBuf, E)],
) -> Result<VersionReplay<E::Version>>
where
    E: VersionedLogEntry,
{
    let mut versions = BTreeMap::new();
    let mut source_path = None;
    let mut key = None;

    for (path, entry) in entries {
        source_path.get_or_insert_with(|| path.clone());
        key.get_or_insert_with(|| entry.log_key().to_owned());
        let entry_version = entry.version();
        let version = entry.clone().into_version();
        ensure!(
            version.version() == entry_version,
            CorruptedStoreSnafu {
                path: path.clone(),
                message: format!(
                    "{} {:?} stores version {} in history entry {}",
                    context.entity(),
                    entry.log_key(),
                    version.version(),
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
    validate_versions(context, &path, &key, &versions)?;
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
    current_version: V::Id,
    versions: &VersionMap<V>,
    matches_version: impl FnOnce(&V) -> bool,
) -> Result<()>
where
    V: VersionedValue,
{
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
) -> Result<()>
where
    V: VersionedValue,
{
    let mut expected_version = V::Id::first();
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
        ensure!(
            entry.version() == *version,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "{} {:?} stores version {} under key {}",
                    context.entity(),
                    key,
                    entry.version(),
                    version
                ),
            }
        );
        expected_version = expected_version.next();
    }
    Ok(())
}
