use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use async_trait::async_trait;
use diesel::prelude::*;
use diesel::result::OptionalExtension;
use diesel_async::RunQueryDsl;
use snafu::prelude::*;

use super::codec::{parse_session_role, parse_u64_column};
use super::{AsyncSqliteConnection, SqliteStore, SqliteTransactionError};
use crate::error::{
    CorruptedStoreSnafu, InvalidSkillNameSnafu, QuerySqliteStoreSnafu, SkillAlreadyExistsSnafu,
    SkillNotFoundSnafu, SkillUpdateEmptySnafu, SkillVersionNotFoundSnafu, StoreError,
};
use crate::schema::{skill_version_scripts, skill_versions, skills};
use crate::store::SkillStore;
use crate::{
    SessionRole, SkillGroups, SkillRecord, SkillScript, SkillUpdatePatch, SkillVersion,
    SkillVersionSpec, StoreResult as Result, default_skill_groups,
};

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct SkillRow {
    role: String,
    name: String,
    current_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct SkillVersionRow {
    role: String,
    skill_name: String,
    version: String,
    id: String,
    created_at: String,
    description: String,
    body: String,
    enable_coco_shim: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct SkillVersionScriptRow {
    role: String,
    skill_name: String,
    version: String,
    ordinal: i32,
    path: String,
    content: String,
}

async fn load_skill_groups(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<SkillGroups> {
    let mut groups = default_skill_groups();
    for (role, record) in query_skill_records(connection, path, None, None).await? {
        groups
            .for_role_mut(role)
            .insert(record.name.clone(), record);
    }
    Ok(groups)
}

async fn query_skill_records(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: Option<SessionRole>,
    name: Option<&str>,
) -> Result<Vec<(SessionRole, SkillRecord)>> {
    let mut skill_query = skills::table.into_boxed();
    if let Some(role) = role {
        skill_query = skill_query.filter(skills::role.eq(role.as_str()));
    }
    if let Some(name) = name {
        skill_query = skill_query.filter(skills::name.eq(name));
    }
    let skill_rows = skill_query
        .select((skills::role, skills::name, skills::current_version))
        .order((skills::role, skills::name))
        .load::<SkillRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    let mut version_query = skill_versions::table.into_boxed();
    if let Some(role) = role {
        version_query = version_query.filter(skill_versions::role.eq(role.as_str()));
    }
    if let Some(name) = name {
        version_query = version_query.filter(skill_versions::skill_name.eq(name));
    }
    let version_rows = version_query
        .select((
            skill_versions::role,
            skill_versions::skill_name,
            skill_versions::version,
            skill_versions::id,
            skill_versions::created_at,
            skill_versions::description,
            skill_versions::body,
            skill_versions::enable_coco_shim,
        ))
        .order((
            skill_versions::role,
            skill_versions::skill_name,
            skill_versions::version,
        ))
        .load::<SkillVersionRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    let mut script_query = skill_version_scripts::table.into_boxed();
    if let Some(role) = role {
        script_query = script_query.filter(skill_version_scripts::role.eq(role.as_str()));
    }
    if let Some(name) = name {
        script_query = script_query.filter(skill_version_scripts::skill_name.eq(name));
    }
    let script_rows = script_query
        .select((
            skill_version_scripts::role,
            skill_version_scripts::skill_name,
            skill_version_scripts::version,
            skill_version_scripts::ordinal,
            skill_version_scripts::path,
            skill_version_scripts::content,
        ))
        .order((
            skill_version_scripts::role,
            skill_version_scripts::skill_name,
            skill_version_scripts::version,
            skill_version_scripts::ordinal,
        ))
        .load::<SkillVersionScriptRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    skill_records_from_rows(path, skill_rows, version_rows, script_rows)
}

async fn load_skill_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: SessionRole,
    name: &str,
) -> Result<SkillRecord> {
    if let Some((_, record)) = query_skill_records(connection, path, Some(role), Some(name))
        .await?
        .pop()
    {
        return Ok(record);
    }
    default_skill_groups()
        .for_role(role)
        .get(name)
        .cloned()
        .context(SkillNotFoundSnafu {
            role: role.as_str().to_owned(),
            name: name.to_owned(),
        })
}

fn skill_records_from_rows(
    path: &Path,
    skill_rows: Vec<SkillRow>,
    version_rows: Vec<SkillVersionRow>,
    script_rows: Vec<SkillVersionScriptRow>,
) -> Result<Vec<(SessionRole, SkillRecord)>> {
    let mut scripts_by_version =
        HashMap::<(String, String, String), Vec<SkillVersionScriptRow>>::new();
    for row in script_rows {
        scripts_by_version
            .entry((
                row.role.clone(),
                row.skill_name.clone(),
                row.version.clone(),
            ))
            .or_default()
            .push(row);
    }

    let mut versions_by_skill = HashMap::<(String, String), BTreeMap<u64, SkillVersion>>::new();
    for row in version_rows {
        let role = row.role.clone();
        let skill_name = row.skill_name.clone();
        let version_text = row.version.clone();
        let scripts = scripts_by_version
            .remove(&(role.clone(), skill_name.clone(), version_text))
            .unwrap_or_default();
        let version = row.into_version(path, scripts)?;
        let version_number = version.version;
        let previous = versions_by_skill
            .entry((role.clone(), skill_name.clone()))
            .or_default()
            .insert(version_number, version);
        ensure!(
            previous.is_none(),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "duplicate SQLite skill version {version_number} for {role:?}/{skill_name:?}"
                ),
            }
        );
    }
    ensure!(
        scripts_by_version.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "SQLite skill script rows have no matching version".to_owned(),
        }
    );

    let mut records = Vec::with_capacity(skill_rows.len());
    for row in skill_rows {
        let role = parse_session_role(&row.role, path)?;
        let current_version =
            parse_u64_column(path, "skills.current_version", &row.current_version)?;
        let versions = versions_by_skill
            .remove(&(row.role.clone(), row.name.clone()))
            .unwrap_or_default();
        ensure!(
            versions.contains_key(&current_version),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "missing current SQLite skill version {current_version} for {:?}/{:?}",
                    row.role, row.name
                ),
            }
        );
        records.push((
            role,
            SkillRecord {
                name: row.name,
                current_version,
                versions,
            },
        ));
    }
    ensure!(
        versions_by_skill.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "SQLite skill versions have no matching skill".to_owned(),
        }
    );
    Ok(records)
}

impl SkillVersionRow {
    fn from_version(role: SessionRole, skill_name: &str, version: &SkillVersion) -> Self {
        Self {
            role: role.as_str().to_owned(),
            skill_name: skill_name.to_owned(),
            version: version.version.to_string(),
            id: version.id.clone(),
            created_at: version.created_at.to_string(),
            description: version.description.clone(),
            body: version.body.clone(),
            enable_coco_shim: version.enable_coco_shim,
        }
    }

    fn into_version(
        self,
        path: &Path,
        script_rows: Vec<SkillVersionScriptRow>,
    ) -> Result<SkillVersion> {
        let version = parse_u64_column(path, "skill_versions.version", &self.version)?;
        let created_at = self
            .created_at
            .parse()
            .map_err(|source| StoreError::CorruptedStore {
                path: path.to_owned(),
                message: format!(
                    "invalid SQLite skill timestamp in skill_versions.created_at: {source}"
                ),
            })?;
        let scripts = skill_scripts_from_rows(
            path,
            &self.role,
            &self.skill_name,
            &self.version,
            script_rows,
        )?;
        Ok(SkillVersion {
            id: self.id,
            version,
            created_at,
            description: self.description,
            body: self.body,
            scripts,
            enable_coco_shim: self.enable_coco_shim,
        })
    }
}

fn skill_version_script_rows(
    role: SessionRole,
    skill_name: &str,
    version: &SkillVersion,
) -> Vec<SkillVersionScriptRow> {
    version
        .scripts
        .iter()
        .enumerate()
        .map(|(ordinal, script)| SkillVersionScriptRow {
            role: role.as_str().to_owned(),
            skill_name: skill_name.to_owned(),
            version: version.version.to_string(),
            ordinal: ordinal as i32,
            path: script.path.clone(),
            content: script.content.clone(),
        })
        .collect()
}

fn skill_scripts_from_rows(
    path: &Path,
    role: &str,
    skill_name: &str,
    version: &str,
    rows: Vec<SkillVersionScriptRow>,
) -> Result<Vec<SkillScript>> {
    rows.into_iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure!(
                row.role == role
                    && row.skill_name == skill_name
                    && row.version == version
                    && row.ordinal == ordinal as i32,
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "invalid SQLite skill script ordinal for {role:?}/{skill_name:?} version {version}"
                    ),
                }
            );
            Ok(SkillScript {
                path: row.path,
                content: row.content,
            })
        })
        .collect()
}

async fn persist_skill(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: SessionRole,
    record: &SkillRecord,
) -> Result<()> {
    let version = record.current().context(CorruptedStoreSnafu {
        path: path.to_owned(),
        message: format!(
            "missing current skill version {} for {:?}/{:?}",
            record.current_version,
            role.as_str(),
            record.name
        ),
    })?;
    ensure!(
        version.version == record.current_version,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "skill version {} does not match current version {} for {:?}/{:?}",
                version.version,
                record.current_version,
                role.as_str(),
                record.name
            ),
        }
    );
    let skill_is_persisted = skills::table
        .filter(skills::role.eq(role.as_str()))
        .filter(skills::name.eq(&record.name))
        .select(skills::name)
        .first::<String>(connection)
        .await
        .optional()
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?
        .is_some();
    // Built-in skills live in memory until their first update, so materialize their history too.
    let versions = if skill_is_persisted {
        vec![(record.current_version, version)]
    } else {
        record
            .versions
            .iter()
            .map(|(version_number, version)| (*version_number, version))
            .collect()
    };
    let mut rows = Vec::with_capacity(versions.len());
    for (version_number, version) in versions {
        ensure!(
            version.version == version_number,
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "skill version {} does not match version key {version_number} for {:?}/{:?}",
                    version.version,
                    role.as_str(),
                    record.name
                ),
            }
        );
        rows.push((
            SkillVersionRow::from_version(role, &record.name, version),
            skill_version_script_rows(role, &record.name, version),
        ));
    }

    diesel::insert_into(skills::table)
        .values((
            skills::role.eq(role.as_str()),
            skills::name.eq(&record.name),
            skills::current_version.eq(record.current_version.to_string()),
        ))
        .on_conflict((skills::role, skills::name))
        .do_update()
        .set(skills::current_version.eq(diesel::upsert::excluded(skills::current_version)))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    for (version_row, script_rows) in rows {
        diesel::insert_into(skill_versions::table)
            .values((
                skill_versions::role.eq(version_row.role),
                skill_versions::skill_name.eq(version_row.skill_name),
                skill_versions::version.eq(version_row.version),
                skill_versions::id.eq(version_row.id),
                skill_versions::created_at.eq(version_row.created_at),
                skill_versions::description.eq(version_row.description),
                skill_versions::body.eq(version_row.body),
                skill_versions::enable_coco_shim.eq(version_row.enable_coco_shim),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;

        for row in script_rows {
            diesel::insert_into(skill_version_scripts::table)
                .values((
                    skill_version_scripts::role.eq(row.role),
                    skill_version_scripts::skill_name.eq(row.skill_name),
                    skill_version_scripts::version.eq(row.version),
                    skill_version_scripts::ordinal.eq(row.ordinal),
                    skill_version_scripts::path.eq(row.path),
                    skill_version_scripts::content.eq(row.content),
                ))
                .execute(connection)
                .await
                .context(QuerySqliteStoreSnafu {
                    path: path.to_owned(),
                })?;
        }
    }
    Ok(())
}

async fn add_skill_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: SessionRole,
    name: &str,
    spec: SkillVersionSpec,
) -> Result<SkillRecord> {
    validate_skill_name(name)?;
    connection
        .immediate_transaction::<SkillRecord, SqliteTransactionError, _>(async |connection| {
            let groups = load_skill_groups(connection, path)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            if groups.for_role(role).contains_key(name) {
                return Err(SqliteTransactionError::Operation(
                    SkillAlreadyExistsSnafu {
                        role: role.as_str().to_owned(),
                        name: name.to_owned(),
                    }
                    .build(),
                ));
            }
            let record = SkillRecord::new(name.to_owned(), spec);
            persist_skill(connection, path, role, &record)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(record)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn update_skill_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: SessionRole,
    name: &str,
    patch: &SkillUpdatePatch,
) -> Result<SkillRecord> {
    ensure!(
        !patch.is_empty(),
        SkillUpdateEmptySnafu {
            role: role.as_str().to_owned(),
            name: name.to_owned(),
        }
    );
    connection
        .immediate_transaction::<SkillRecord, SqliteTransactionError, _>(async |connection| {
            let mut record = load_skill_record(connection, path, role, name)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            let current_version = record.current_version;
            record.update(patch).ok_or_else(|| {
                SqliteTransactionError::Operation(
                    SkillVersionNotFoundSnafu {
                        role: role.as_str().to_owned(),
                        name: name.to_owned(),
                        version: current_version,
                    }
                    .build(),
                )
            })?;
            persist_skill(connection, path, role, &record)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(record)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn rollback_skill_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    role: SessionRole,
    name: &str,
    target_version: u64,
) -> Result<SkillRecord> {
    connection
        .immediate_transaction::<SkillRecord, SqliteTransactionError, _>(async |connection| {
            let mut record = load_skill_record(connection, path, role, name)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            record.rollback(target_version).ok_or_else(|| {
                SqliteTransactionError::Operation(
                    SkillVersionNotFoundSnafu {
                        role: role.as_str().to_owned(),
                        name: name.to_owned(),
                        version: target_version,
                    }
                    .build(),
                )
            })?;
            persist_skill(connection, path, role, &record)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(record)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

fn validate_skill_name(name: &str) -> Result<()> {
    let trimmed = name.trim();
    ensure!(
        !trimmed.is_empty(),
        InvalidSkillNameSnafu {
            name: name.to_owned(),
            message: "name must not be empty".to_owned(),
        }
    );
    ensure!(
        trimmed == name,
        InvalidSkillNameSnafu {
            name: name.to_owned(),
            message: "name must not have leading or trailing whitespace".to_owned(),
        }
    );
    Ok(())
}

#[async_trait]
impl SkillStore for SqliteStore {
    async fn list_skills(&self, role: SessionRole) -> Result<Vec<SkillRecord>> {
        let mut connection = self.connect().await?;
        Ok(load_skill_groups(&mut connection, &self.database_path)
            .await?
            .for_role(role)
            .values()
            .cloned()
            .collect())
    }

    async fn get_skill(&self, role: SessionRole, name: &str) -> Result<SkillRecord> {
        let mut connection = self.connect().await?;
        load_skill_record(&mut connection, &self.database_path, role, name).await
    }

    async fn add_skill(
        &self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> Result<SkillRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        add_skill_record(&mut connection, &self.database_path, role, name, spec).await
    }

    async fn update_skill(
        &self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> Result<SkillRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        update_skill_record(&mut connection, &self.database_path, role, name, patch).await
    }

    async fn rollback_skill(
        &self,
        role: SessionRole,
        name: &str,
        target_version: u64,
    ) -> Result<SkillRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        rollback_skill_record(
            &mut connection,
            &self.database_path,
            role,
            name,
            target_version,
        )
        .await
    }
}
