use std::fs;

use coco_mem::{
    FsStore, SessionRole, SkillRecord, SkillStore, SkillUpdatePatch, SkillVersion, SkillVersionSpec,
};
use serde::Serialize;
use snafu::prelude::*;

use crate::{
    Result,
    cli::{
        SkillAddCommand, SkillCommand, SkillListCommand, SkillRollbackCommand, SkillShowCommand,
        SkillSubcommand, SkillUpdateCommand,
    },
    error::{ReadSkillFileSnafu, StoreSnafu},
};

#[derive(Debug, Serialize)]
struct SkillSummaryView {
    role: &'static str,
    name: String,
    current_version: u64,
    available_versions: Vec<u64>,
    description: String,
    enable_coco_shim: bool,
}

#[derive(Debug, Serialize)]
struct SkillShowView {
    role: &'static str,
    name: String,
    current_version: u64,
    versions: Vec<SkillVersionView>,
}

#[derive(Debug, Serialize)]
struct SkillVersionView {
    version: u64,
    created_at: String,
    description: String,
    enable_coco_shim: bool,
    body: String,
}

pub(super) async fn run_skill_command(
    command: SkillCommand,
    store: &FsStore,
) -> Result<Option<String>> {
    match command.command {
        SkillSubcommand::Add(command) => Ok(Some(render_json(run_skill_add(command, store)?))),
        SkillSubcommand::Update(command) => {
            Ok(Some(render_json(run_skill_update(command, store)?)))
        }
        SkillSubcommand::Rollback(command) => {
            Ok(Some(render_json(run_skill_rollback(command, store)?)))
        }
        SkillSubcommand::List(command) => Ok(Some(render_json(run_skill_list(command, store)?))),
        SkillSubcommand::Show(command) => Ok(Some(render_json(run_skill_show(command, store)?))),
    }
}

fn run_skill_add(command: SkillAddCommand, store: &FsStore) -> Result<SkillSummaryView> {
    let body = read_skill_body(&command.file)?;
    let record = store
        .add_skill(
            command.role.into(),
            &command.name,
            SkillVersionSpec {
                description: command.description,
                body,
                enable_coco_shim: command.enable_coco_shim,
            },
        )
        .context(StoreSnafu)?;
    Ok(skill_summary_view(command.role.into(), &record))
}

fn run_skill_update(command: SkillUpdateCommand, store: &FsStore) -> Result<SkillSummaryView> {
    let patch = SkillUpdatePatch {
        description: command.description,
        body: command
            .file
            .as_ref()
            .map(|path| read_skill_body(path.as_path()))
            .transpose()?,
        enable_coco_shim: if command.enable_coco_shim {
            Some(true)
        } else if command.disable_coco_shim {
            Some(false)
        } else {
            None
        },
    };
    let record = store
        .update_skill(command.role.into(), &command.name, &patch)
        .context(StoreSnafu)?;
    Ok(skill_summary_view(command.role.into(), &record))
}

fn run_skill_rollback(command: SkillRollbackCommand, store: &FsStore) -> Result<SkillSummaryView> {
    let record = store
        .rollback_skill(command.role.into(), &command.name, command.to_version)
        .context(StoreSnafu)?;
    Ok(skill_summary_view(command.role.into(), &record))
}

fn run_skill_list(command: SkillListCommand, store: &FsStore) -> Result<Vec<SkillSummaryView>> {
    let roles = command
        .role
        .map(Into::into)
        .map(|role| vec![role])
        .unwrap_or_else(|| vec![SessionRole::Orchestrator, SessionRole::Runner]);
    let mut skills = Vec::new();
    for role in roles {
        let mut records = store.list_skills(role).context(StoreSnafu)?;
        records.sort_by(|left, right| left.name.cmp(&right.name));
        skills.extend(
            records
                .iter()
                .map(|record| skill_summary_view(role, record)),
        );
    }
    Ok(skills)
}

fn run_skill_show(command: SkillShowCommand, store: &FsStore) -> Result<SkillShowView> {
    let role: SessionRole = command.role.into();
    let record = store.get_skill(role, &command.name).context(StoreSnafu)?;
    let versions = record
        .versions
        .values()
        .map(skill_version_view)
        .collect::<Vec<_>>();

    Ok(SkillShowView {
        role: role.as_str(),
        name: record.name,
        current_version: record.current_version,
        versions,
    })
}

fn read_skill_body(path: &std::path::Path) -> Result<String> {
    Ok(fs::read_to_string(path)
        .context(ReadSkillFileSnafu {
            path: path.to_path_buf(),
        })?
        .trim()
        .to_owned())
}

fn skill_summary_view(role: SessionRole, record: &SkillRecord) -> SkillSummaryView {
    let current = record
        .current()
        .expect("skill record should always have a current version");
    SkillSummaryView {
        role: role.as_str(),
        name: record.name.clone(),
        current_version: record.current_version,
        available_versions: record.versions.keys().copied().collect(),
        description: current.description.clone(),
        enable_coco_shim: current.enable_coco_shim,
    }
}

fn skill_version_view(version: &SkillVersion) -> SkillVersionView {
    SkillVersionView {
        version: version.version,
        created_at: version.created_at.to_string(),
        description: version.description.clone(),
        enable_coco_shim: version.enable_coco_shim,
        body: version.body.clone(),
    }
}

fn render_json<T>(value: T) -> String
where
    T: Serialize,
{
    serde_json::to_string_pretty(&value).expect("skill output should serialize")
}
