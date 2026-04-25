use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use snafu::prelude::*;

use super::collection::{Collection, CollectionRecord};
use super::log::LogEntry;
use super::projection::ProjectionContext;
use super::snapshot::Snapshot;
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

pub trait VersionedRecord: CollectionRecord {
    type Version: VersionedValue;

    fn current_version(&self) -> <Self::Version as VersionedValue>::Id;
    fn versions(&self) -> &VersionMap<Self::Version>;
}

pub trait VersionedLogEntry: Clone + LogEntry {
    type Version: VersionedValue;

    fn version(&self) -> <Self::Version as VersionedValue>::Id;
    fn into_version(self) -> Self::Version;
}

pub trait VersionedSnapshot: Snapshot {
    type Version: VersionedValue;

    fn current_version(&self) -> <Self::Version as VersionedValue>::Id;
    fn matches_version(&self, version: &Self::Version) -> bool;
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

    fn current_version(&self) -> <Self::Version as VersionedValue>::Id {
        self.current_version
    }

    fn versions(&self) -> &VersionMap<Self::Version> {
        &self.versions
    }
}

impl VersionedRecord for SkillRecord {
    type Version = SkillVersion;

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
                record.collection_key()
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
                record.collection_key(),
                record.current_version()
            ),
        }
    );
    validate_versions(context, path, record.collection_key(), record.versions())
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

pub(crate) fn validate_snapshot_in_collection<C, S>(
    context: &ProjectionContext,
    snapshot: &S,
    collection: &C,
) -> Result<()>
where
    C: Collection,
    C::Record: VersionedRecord,
    S: VersionedSnapshot<Version = <C::Record as VersionedRecord>::Version>,
{
    let record = collection
        .get_record(snapshot.snapshot_key())
        .context(CorruptedStoreSnafu {
            path: context.history_path().to_owned(),
            message: format!(
                "missing {} history for {:?}",
                context.entity(),
                snapshot.snapshot_key()
            ),
        })?;
    validate_snapshot(context, snapshot, record)
}

fn validate_snapshot<S, R>(context: &ProjectionContext, snapshot: &S, record: &R) -> Result<()>
where
    S: VersionedSnapshot<Version = R::Version>,
    R: VersionedRecord,
{
    let persisted_current =
        record
            .versions()
            .get(&snapshot.current_version())
            .context(CorruptedStoreSnafu {
                path: context.history_path().to_owned(),
                message: format!(
                    "missing current {} version {} for {:?}",
                    context.entity(),
                    snapshot.current_version(),
                    snapshot.snapshot_key()
                ),
            })?;
    ensure!(
        snapshot.matches_version(persisted_current),
        CorruptedStoreSnafu {
            path: context.history_path().to_owned(),
            message: format!(
                "current {} snapshot mismatch for {:?}",
                context.entity(),
                snapshot.snapshot_key()
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
