use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use async_trait::async_trait;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use snafu::prelude::*;

use super::codec::{parse_json_column, parse_session_role, parse_u64_column};
use super::{AsyncSqliteConnection, SqliteStore, SqliteTransactionError};
use crate::error::{
    CorruptedStoreSnafu, ParseSqliteStoreValueSnafu, PresetNotFoundSnafu,
    PresetVersionNotFoundSnafu, QuerySqliteStoreSnafu, StoreError,
};
use crate::schema::{preset_version_tools, preset_versions, presets};
use crate::store::PresetStore;
use crate::{Preset, PresetRecord, PresetVersion, StoreResult as Result, Tool};

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct PresetRow {
    name: String,
    current_version: String,
}

#[derive(Clone, Debug, PartialEq, Queryable)]
struct PresetVersionRow {
    preset_name: String,
    version: String,
    created_at: String,
    role: String,
    provider_profile: String,
    model: String,
    system_prompt: String,
    prompt: String,
    temperature: Option<f64>,
    max_tokens: Option<String>,
    additional_params_json: Option<String>,
    enable_coco_shim: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Queryable)]
struct PresetVersionToolRow {
    preset_name: String,
    version: String,
    ordinal: i32,
    name: String,
    description: String,
    input_schema_json: String,
}

async fn load_preset_records(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
) -> Result<HashMap<String, PresetRecord>> {
    query_preset_records(connection, path, None).await
}

async fn query_preset_records(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: Option<&str>,
) -> Result<HashMap<String, PresetRecord>> {
    let mut preset_query = presets::table.into_boxed();
    if let Some(name) = name {
        preset_query = preset_query.filter(presets::name.eq(name));
    }
    let preset_rows = preset_query
        .select((presets::name, presets::current_version))
        .order(presets::name)
        .load::<PresetRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    let mut version_query = preset_versions::table.into_boxed();
    if let Some(name) = name {
        version_query = version_query.filter(preset_versions::preset_name.eq(name));
    }
    let version_rows = version_query
        .select((
            preset_versions::preset_name,
            preset_versions::version,
            preset_versions::created_at,
            preset_versions::role,
            preset_versions::provider_profile,
            preset_versions::model,
            preset_versions::system_prompt,
            preset_versions::prompt,
            preset_versions::temperature,
            preset_versions::max_tokens,
            preset_versions::additional_params_json,
            preset_versions::enable_coco_shim,
        ))
        .order((preset_versions::preset_name, preset_versions::version))
        .load::<PresetVersionRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    let mut tool_query = preset_version_tools::table.into_boxed();
    if let Some(name) = name {
        tool_query = tool_query.filter(preset_version_tools::preset_name.eq(name));
    }
    let tool_rows = tool_query
        .select((
            preset_version_tools::preset_name,
            preset_version_tools::version,
            preset_version_tools::ordinal,
            preset_version_tools::name,
            preset_version_tools::description,
            preset_version_tools::input_schema_json,
        ))
        .order((
            preset_version_tools::preset_name,
            preset_version_tools::version,
            preset_version_tools::ordinal,
        ))
        .load::<PresetVersionToolRow>(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    preset_records_from_rows(path, preset_rows, version_rows, tool_rows)
}

async fn load_preset_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
) -> Result<PresetRecord> {
    query_preset_records(connection, path, Some(name))
        .await?
        .remove(name)
        .context(PresetNotFoundSnafu {
            name: name.to_owned(),
        })
}

fn preset_records_from_rows(
    path: &Path,
    preset_rows: Vec<PresetRow>,
    version_rows: Vec<PresetVersionRow>,
    tool_rows: Vec<PresetVersionToolRow>,
) -> Result<HashMap<String, PresetRecord>> {
    let mut tools_by_version = HashMap::<(String, String), Vec<_>>::new();
    for row in tool_rows {
        tools_by_version
            .entry((row.preset_name.clone(), row.version.clone()))
            .or_default()
            .push(row);
    }

    let mut versions_by_preset = HashMap::<String, BTreeMap<u64, PresetVersion>>::new();
    for row in version_rows {
        let preset_name = row.preset_name.clone();
        let version_text = row.version.clone();
        let tool_rows = tools_by_version
            .remove(&(preset_name.clone(), version_text))
            .unwrap_or_default();
        let version = row.into_version(path, &tool_rows)?;
        let version_number = version.version;
        let previous = versions_by_preset
            .entry(preset_name.clone())
            .or_default()
            .insert(version_number, version);
        ensure!(
            previous.is_none(),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "duplicate SQLite preset version {version_number} for {preset_name:?}"
                ),
            }
        );
    }
    ensure!(
        tools_by_version.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "SQLite preset tool rows have no matching version".to_owned(),
        }
    );

    let mut records = HashMap::with_capacity(preset_rows.len());
    for row in preset_rows {
        let current_version =
            parse_u64_column(path, "presets.current_version", &row.current_version)?;
        let versions = versions_by_preset.remove(&row.name).unwrap_or_default();
        ensure!(
            versions.contains_key(&current_version),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!(
                    "missing current SQLite preset version {current_version} for {:?}",
                    row.name
                ),
            }
        );
        let name = row.name;
        let previous = records.insert(
            name.clone(),
            PresetRecord {
                name: name.clone(),
                current_version,
                versions,
            },
        );
        ensure!(
            previous.is_none(),
            CorruptedStoreSnafu {
                path: path.to_owned(),
                message: format!("duplicate SQLite preset row for {name:?}"),
            }
        );
    }
    ensure!(
        versions_by_preset.is_empty(),
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: "SQLite preset versions have no matching preset".to_owned(),
        }
    );
    Ok(records)
}

impl PresetVersionRow {
    fn from_version(preset_name: &str, version: &PresetVersion, path: &Path) -> Result<Self> {
        let additional_params_json = version
            .config
            .additional_params
            .as_ref()
            .map(|value| {
                serde_json::to_string(value).context(ParseSqliteStoreValueSnafu {
                    path: path.to_owned(),
                    column: "preset_versions.additional_params_json".to_owned(),
                })
            })
            .transpose()?;
        Ok(Self {
            preset_name: preset_name.to_owned(),
            version: version.version.to_string(),
            created_at: version.created_at.to_string(),
            role: version.config.role.as_str().to_owned(),
            provider_profile: version.config.provider_profile.clone(),
            model: version.config.model.clone(),
            system_prompt: version.config.system_prompt.clone(),
            prompt: version.config.prompt.clone(),
            temperature: version.config.temperature,
            max_tokens: version.config.max_tokens.map(|value| value.to_string()),
            additional_params_json,
            enable_coco_shim: version.config.enable_coco_shim,
        })
    }

    fn into_version(
        self,
        path: &Path,
        tool_rows: &[PresetVersionToolRow],
    ) -> Result<PresetVersion> {
        let version = parse_u64_column(path, "preset_versions.version", &self.version)?;
        let created_at = self
            .created_at
            .parse()
            .map_err(|source| StoreError::CorruptedStore {
                path: path.to_owned(),
                message: format!(
                    "invalid SQLite preset timestamp in preset_versions.created_at: {source}"
                ),
            })?;
        let max_tokens = self
            .max_tokens
            .as_deref()
            .map(|value| parse_u64_column(path, "preset_versions.max_tokens", value))
            .transpose()?;
        let additional_params = self
            .additional_params_json
            .as_deref()
            .map(|value| parse_json_column(path, "preset_versions.additional_params_json", value))
            .transpose()?;
        let tools = preset_tools_from_rows(path, &self.preset_name, &self.version, tool_rows)?;
        Ok(PresetVersion {
            version,
            created_at,
            config: Preset {
                role: parse_session_role(&self.role, path)?,
                provider_profile: self.provider_profile,
                model: self.model,
                tools,
                system_prompt: self.system_prompt,
                prompt: self.prompt,
                temperature: self.temperature,
                max_tokens,
                additional_params,
                enable_coco_shim: self.enable_coco_shim,
            },
        })
    }
}

fn preset_version_tool_rows(
    preset_name: &str,
    version: &PresetVersion,
    path: &Path,
) -> Result<Vec<PresetVersionToolRow>> {
    version
        .config
        .tools
        .iter()
        .enumerate()
        .map(|(ordinal, tool)| {
            Ok(PresetVersionToolRow {
                preset_name: preset_name.to_owned(),
                version: version.version.to_string(),
                ordinal: ordinal as i32,
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema_json: serde_json::to_string(&tool.input_schema).context(
                    ParseSqliteStoreValueSnafu {
                        path: path.to_owned(),
                        column: "preset_version_tools.input_schema_json".to_owned(),
                    },
                )?,
            })
        })
        .collect()
}

fn preset_tools_from_rows(
    path: &Path,
    preset_name: &str,
    version: &str,
    rows: &[PresetVersionToolRow],
) -> Result<Vec<Tool>> {
    rows.iter()
        .enumerate()
        .map(|(ordinal, row)| {
            ensure!(
                row.preset_name == preset_name
                    && row.version == version
                    && row.ordinal == ordinal as i32,
                CorruptedStoreSnafu {
                    path: path.to_owned(),
                    message: format!(
                        "invalid SQLite preset tool ordinal for {preset_name:?} version {version}"
                    ),
                }
            );
            Ok(Tool {
                name: row.name.clone(),
                description: row.description.clone(),
                input_schema: parse_json_column(
                    path,
                    "preset_version_tools.input_schema_json",
                    &row.input_schema_json,
                )?,
            })
        })
        .collect()
}

async fn persist_preset(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    record: &PresetRecord,
) -> Result<()> {
    let version = record.current().context(CorruptedStoreSnafu {
        path: path.to_owned(),
        message: format!(
            "missing current preset version {} for {:?}",
            record.current_version, record.name
        ),
    })?;
    ensure!(
        version.version == record.current_version,
        CorruptedStoreSnafu {
            path: path.to_owned(),
            message: format!(
                "preset version {} does not match current version {} for {:?}",
                version.version, record.current_version, record.name
            ),
        }
    );
    let version_row = PresetVersionRow::from_version(&record.name, version, path)?;
    let tool_rows = preset_version_tool_rows(&record.name, version, path)?;

    diesel::insert_into(presets::table)
        .values((
            presets::name.eq(&record.name),
            presets::current_version.eq(record.current_version.to_string()),
        ))
        .on_conflict(presets::name)
        .do_update()
        .set(presets::current_version.eq(diesel::upsert::excluded(presets::current_version)))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    diesel::insert_into(preset_versions::table)
        .values((
            preset_versions::preset_name.eq(version_row.preset_name),
            preset_versions::version.eq(version_row.version),
            preset_versions::created_at.eq(version_row.created_at),
            preset_versions::role.eq(version_row.role),
            preset_versions::provider_profile.eq(version_row.provider_profile),
            preset_versions::model.eq(version_row.model),
            preset_versions::system_prompt.eq(version_row.system_prompt),
            preset_versions::prompt.eq(version_row.prompt),
            preset_versions::temperature.eq(version_row.temperature),
            preset_versions::max_tokens.eq(version_row.max_tokens),
            preset_versions::additional_params_json.eq(version_row.additional_params_json),
            preset_versions::enable_coco_shim.eq(version_row.enable_coco_shim),
        ))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;

    for row in tool_rows {
        diesel::insert_into(preset_version_tools::table)
            .values((
                preset_version_tools::preset_name.eq(row.preset_name),
                preset_version_tools::version.eq(row.version),
                preset_version_tools::ordinal.eq(row.ordinal),
                preset_version_tools::name.eq(row.name),
                preset_version_tools::description.eq(row.description),
                preset_version_tools::input_schema_json.eq(row.input_schema_json),
            ))
            .execute(connection)
            .await
            .context(QuerySqliteStoreSnafu {
                path: path.to_owned(),
            })?;
    }
    Ok(())
}

async fn set_preset_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
    config: Preset,
) -> Result<PresetRecord> {
    connection
        .immediate_transaction::<PresetRecord, SqliteTransactionError, _>(async |connection| {
            let mut records = query_preset_records(connection, path, Some(name))
                .await
                .map_err(SqliteTransactionError::Operation)?;
            let record = if let Some(mut record) = records.remove(name) {
                let current_version = record.current_version;
                record.update(config).ok_or_else(|| {
                    SqliteTransactionError::Operation(
                        PresetVersionNotFoundSnafu {
                            name: name.to_owned(),
                            version: current_version,
                        }
                        .build(),
                    )
                })?;
                record
            } else {
                PresetRecord::new(name.to_owned(), config)
            };
            persist_preset(connection, path, &record)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(record)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn rollback_preset_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
    target_version: u64,
) -> Result<PresetRecord> {
    connection
        .immediate_transaction::<PresetRecord, SqliteTransactionError, _>(async |connection| {
            let mut record = load_preset_record(connection, path, name)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            record.rollback(target_version).ok_or_else(|| {
                SqliteTransactionError::Operation(
                    PresetVersionNotFoundSnafu {
                        name: name.to_owned(),
                        version: target_version,
                    }
                    .build(),
                )
            })?;
            persist_preset(connection, path, &record)
                .await
                .map_err(SqliteTransactionError::Operation)?;
            Ok(record)
        })
        .await
        .map_err(|error| error.into_store_error(path))
}

async fn delete_preset_record(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
) -> Result<()> {
    diesel::delete(presets::table.filter(presets::name.eq(name)))
        .execute(connection)
        .await
        .context(QuerySqliteStoreSnafu {
            path: path.to_owned(),
        })?;
    Ok(())
}

async fn delete_preset_record_checked(
    connection: &mut AsyncSqliteConnection,
    path: &Path,
    name: &str,
) -> Result<()> {
    load_preset_record(connection, path, name).await?;
    delete_preset_record(connection, path, name).await
}

#[async_trait]
impl PresetStore for SqliteStore {
    async fn list_preset_records(&self) -> Result<std::collections::HashMap<String, PresetRecord>> {
        let mut connection = self.connect().await?;
        load_preset_records(&mut connection, &self.database_path).await
    }

    async fn get_preset_record(&self, name: &str) -> Result<PresetRecord> {
        let mut connection = self.connect().await?;
        load_preset_record(&mut connection, &self.database_path, name).await
    }

    async fn set_preset(&self, name: &str, config: Preset) -> Result<PresetRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        set_preset_record(&mut connection, &self.database_path, name, config).await
    }

    async fn rollback_preset(&self, name: &str, target_version: u64) -> Result<PresetRecord> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        rollback_preset_record(&mut connection, &self.database_path, name, target_version).await
    }

    async fn delete_preset(&self, name: &str) -> Result<()> {
        self.ensure_writable()?;
        let mut connection = self.connect().await?;
        delete_preset_record_checked(&mut connection, &self.database_path, name).await
    }
}
