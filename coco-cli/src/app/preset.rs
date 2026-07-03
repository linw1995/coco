use coco_mem::{Preset, PresetRecord, PresetStore, PresetVersion};
use serde::Serialize;
use serde_json::Value;
use snafu::prelude::*;

use crate::{
    Result,
    app::config::{ProviderProfileLookup, ProviderProfiles},
    cli::{
        CliTool, PresetCommand, PresetNameCommand, PresetRollbackCommand, PresetSetCommand,
        PresetSubcommand,
    },
    error::{ParsePresetAdditionalParamsSnafu, StoreSnafu},
};

#[derive(Debug, Serialize, PartialEq)]
struct PresetSummaryView {
    name: String,
    current_version: u64,
    available_versions: Vec<u64>,
    config: Preset,
}

#[derive(Debug, Serialize, PartialEq)]
struct PresetShowView {
    name: String,
    current_version: u64,
    versions: Vec<PresetVersionView>,
}

#[derive(Debug, Serialize, PartialEq)]
struct PresetVersionView {
    version: u64,
    created_at: String,
    config: Preset,
}

#[derive(Debug, Serialize, PartialEq)]
struct PresetDeleteResult {
    name: String,
}

pub async fn run_preset_command<S>(
    command: PresetCommand,
    store: &S,
    provider_profiles: &ProviderProfiles,
) -> Result<Option<String>>
where
    S: PresetStore,
{
    match command.command {
        PresetSubcommand::Set(command) => {
            let json = command.json;
            let preset = run_preset_set(command, store, provider_profiles).await?;
            Ok(Some(if json {
                render_json(preset)
            } else {
                render_preset_summary_text(&preset)
            }))
        }
        PresetSubcommand::List(command) => {
            let presets = run_preset_list(store).await?;
            Ok(Some(if command.json {
                render_json(presets)
            } else {
                render_preset_list_text(&presets)
            }))
        }
        PresetSubcommand::Show(command) => {
            let json = command.json;
            let preset = run_preset_show(command, store).await?;
            Ok(Some(if json {
                render_json(preset)
            } else {
                render_preset_show_text(&preset)
            }))
        }
        PresetSubcommand::Rollback(command) => {
            let json = command.json;
            let preset = run_preset_rollback(command, store).await?;
            Ok(Some(if json {
                render_json(preset)
            } else {
                render_preset_summary_text(&preset)
            }))
        }
        PresetSubcommand::Delete(command) => {
            let json = command.json;
            let result = run_preset_delete(command, store).await?;
            Ok(Some(if json {
                render_json(result)
            } else {
                render_preset_delete_text(&result)
            }))
        }
    }
}

async fn run_preset_set<S>(
    command: PresetSetCommand,
    store: &S,
    provider_profiles: &ProviderProfiles,
) -> Result<PresetSummaryView>
where
    S: PresetStore,
{
    let name = command.name.clone();
    let config = resolve_preset(command, provider_profiles)?;
    let record = store.set_preset(&name, config).await.context(StoreSnafu)?;
    Ok(preset_summary_view(&record))
}

async fn run_preset_list(store: &impl PresetStore) -> Result<Vec<PresetSummaryView>> {
    let mut records = store
        .list_preset_records()
        .await
        .context(StoreSnafu)?
        .into_values()
        .collect::<Vec<_>>();
    records.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(records.iter().map(preset_summary_view).collect())
}

async fn run_preset_show(
    command: PresetNameCommand,
    store: &impl PresetStore,
) -> Result<PresetShowView> {
    let record = store
        .get_preset_record(&command.name)
        .await
        .context(StoreSnafu)?;
    Ok(PresetShowView {
        name: record.name,
        current_version: record.current_version,
        versions: record.versions.values().map(preset_version_view).collect(),
    })
}

async fn run_preset_rollback(
    command: PresetRollbackCommand,
    store: &impl PresetStore,
) -> Result<PresetSummaryView> {
    let record = store
        .rollback_preset(&command.name, command.to_version)
        .await
        .context(StoreSnafu)?;
    Ok(preset_summary_view(&record))
}

async fn run_preset_delete(
    command: PresetNameCommand,
    store: &impl PresetStore,
) -> Result<PresetDeleteResult> {
    store
        .delete_preset(&command.name)
        .await
        .context(StoreSnafu)?;
    Ok(PresetDeleteResult { name: command.name })
}

fn resolve_preset(command: PresetSetCommand, store: &impl ProviderProfileLookup) -> Result<Preset> {
    let provider_profile = command.provider_profile;
    let profile = store
        .get_provider_profile(&provider_profile)
        .context(StoreSnafu)?;

    Ok(Preset {
        role: command.role.into(),
        provider_profile,
        model: command
            .model
            .or(profile.default_model)
            .context(crate::error::MissingPresetModelSnafu)?,
        tools: if command.enable_all_tools {
            resolve_cli_tools(CliTool::all())
        } else {
            resolve_cli_tools(&command.tools)
        },
        system_prompt: command.system_prompt,
        prompt: command.prompt,
        temperature: command.temperature,
        max_tokens: command.max_tokens,
        additional_params: parse_preset_additional_params(command.additional_params)?,
        enable_coco_shim: command.enable_coco_shim && !command.disable_coco_shim,
    })
}

fn resolve_cli_tools(tools: &[CliTool]) -> Vec<coco_mem::Tool> {
    tools
        .iter()
        .copied()
        .map(CliTool::as_str)
        .map(|name| {
            coco_llm::builtin_tool_definition(name)
                .expect("CliTool names should always map to built-in tool definitions")
        })
        .collect()
}

fn parse_preset_additional_params(additional_params: Option<String>) -> Result<Option<Value>> {
    let Some(additional_params) = additional_params else {
        return Ok(None);
    };

    let value: Value =
        serde_json::from_str(&additional_params).context(ParsePresetAdditionalParamsSnafu)?;
    ensure!(
        value.is_object(),
        crate::error::InvalidPresetAdditionalParamsTypeSnafu { value }
    );
    Ok(Some(value))
}

fn preset_summary_view(record: &PresetRecord) -> PresetSummaryView {
    PresetSummaryView {
        name: record.name.clone(),
        current_version: record.current_version,
        available_versions: record.versions.keys().copied().collect(),
        config: record
            .current_preset()
            .expect("preset record should always have a current version"),
    }
}

fn preset_version_view(version: &PresetVersion) -> PresetVersionView {
    PresetVersionView {
        version: version.version,
        created_at: version.created_at.to_string(),
        config: version.to_preset(),
    }
}

fn render_preset_list_text(presets: &[PresetSummaryView]) -> String {
    if presets.is_empty() {
        return "No presets found.".to_owned();
    }

    presets
        .iter()
        .map(|preset| {
            format!(
                "{} current={} available=[{}] role={} profile={} model={} shim={}",
                preset.name,
                preset.current_version,
                preset
                    .available_versions
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
                preset.config.role.as_str(),
                preset.config.provider_profile,
                preset.config.model,
                preset.config.enable_coco_shim
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_preset_summary_text(preset: &PresetSummaryView) -> String {
    format!(
        "name: {}\ncurrent_version: {}\navailable_versions: [{}]\n{}",
        preset.name,
        preset.current_version,
        preset
            .available_versions
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(","),
        render_preset_config_summary(&preset.config)
    )
}

fn render_preset_show_text(preset: &PresetShowView) -> String {
    let mut lines = vec![
        format!("name: {}", preset.name),
        format!("current_version: {}", preset.current_version),
        "versions:".to_owned(),
    ];

    for version in &preset.versions {
        lines.push(format!(
            "- version={} created_at={} {}",
            version.version,
            version.created_at,
            render_preset_config_summary(&version.config)
        ));
    }

    lines.join("\n")
}

fn render_preset_config_summary(config: &Preset) -> String {
    format!(
        "role={} profile={} model={} shim={}",
        config.role.as_str(),
        config.provider_profile,
        config.model,
        config.enable_coco_shim
    )
}

fn render_preset_delete_text(result: &PresetDeleteResult) -> String {
    format!("deleted preset: {}", result.name)
}

fn render_json<T>(value: T) -> String
where
    T: Serialize,
{
    serde_json::to_string_pretty(&value).expect("preset output should serialize")
}
