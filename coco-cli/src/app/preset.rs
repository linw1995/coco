use coco_mem::{BranchConfig, BranchConfigRecord, BranchConfigStore, BranchConfigVersion, FsStore};
use serde::Serialize;
use serde_json::Value;
use snafu::prelude::*;

use crate::{
    Result,
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
    config: BranchConfig,
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
    config: BranchConfig,
}

#[derive(Debug, Serialize, PartialEq)]
struct PresetDeleteResult {
    name: String,
}

pub(super) async fn run_preset_command(
    command: PresetCommand,
    store: &FsStore,
) -> Result<Option<String>> {
    match command.command {
        PresetSubcommand::Set(command) => Ok(Some(render_json(run_preset_set(command, store)?))),
        PresetSubcommand::List => Ok(Some(render_json(run_preset_list(store)?))),
        PresetSubcommand::Show(command) => Ok(Some(render_json(run_preset_show(command, store)?))),
        PresetSubcommand::Rollback(command) => {
            Ok(Some(render_json(run_preset_rollback(command, store)?)))
        }
        PresetSubcommand::Delete(command) => {
            Ok(Some(render_json(run_preset_delete(command, store)?)))
        }
    }
}

fn run_preset_set(command: PresetSetCommand, store: &FsStore) -> Result<PresetSummaryView> {
    let name = command.name.clone();
    let config = resolve_branch_config(command)?;
    let record = store.set_branch_config(&name, config).context(StoreSnafu)?;
    Ok(preset_summary_view(&record))
}

fn run_preset_list(store: &FsStore) -> Result<Vec<PresetSummaryView>> {
    let mut records = store
        .list_branch_config_records()
        .context(StoreSnafu)?
        .into_values()
        .collect::<Vec<_>>();
    records.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(records.iter().map(preset_summary_view).collect())
}

fn run_preset_show(command: PresetNameCommand, store: &FsStore) -> Result<PresetShowView> {
    let record = store
        .get_branch_config_record(&command.name)
        .context(StoreSnafu)?;
    Ok(PresetShowView {
        name: record.name,
        current_version: record.current_version,
        versions: record.versions.values().map(preset_version_view).collect(),
    })
}

fn run_preset_rollback(
    command: PresetRollbackCommand,
    store: &FsStore,
) -> Result<PresetSummaryView> {
    let record = store
        .rollback_branch_config(&command.name, command.to_version)
        .context(StoreSnafu)?;
    Ok(preset_summary_view(&record))
}

fn run_preset_delete(command: PresetNameCommand, store: &FsStore) -> Result<PresetDeleteResult> {
    store
        .delete_branch_config(&command.name)
        .context(StoreSnafu)?;
    Ok(PresetDeleteResult { name: command.name })
}

fn resolve_branch_config(command: PresetSetCommand) -> Result<BranchConfig> {
    Ok(BranchConfig {
        role: command.role.into(),
        provider: coco_llm::Provider::from(command.provider)
            .as_str()
            .to_owned(),
        model: command.model,
        tools: resolve_cli_tools(&command.tools),
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

fn preset_summary_view(record: &BranchConfigRecord) -> PresetSummaryView {
    PresetSummaryView {
        name: record.name.clone(),
        current_version: record.current_version,
        available_versions: record.versions.keys().copied().collect(),
        config: record
            .current_config()
            .expect("preset record should always have a current version"),
    }
}

fn preset_version_view(version: &BranchConfigVersion) -> PresetVersionView {
    PresetVersionView {
        version: version.version,
        created_at: version.created_at.to_string(),
        config: version.to_config(),
    }
}

fn render_json<T>(value: T) -> String
where
    T: Serialize,
{
    serde_json::to_string_pretty(&value).expect("preset output should serialize")
}
