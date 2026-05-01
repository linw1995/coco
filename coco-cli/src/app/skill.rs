use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs;
use std::path::{Component, Path, PathBuf};

use coco_mem::{
    SessionRole, SkillRecord, SkillScript, SkillStore, SkillUpdatePatch, SkillVersion,
    SkillVersionSpec,
};
use serde::Serialize;
use snafu::prelude::*;

use crate::{
    Result,
    cli::{
        SkillAddCommand, SkillCommand, SkillListCommand, SkillRollbackCommand, SkillShowCommand,
        SkillSubcommand, SkillUpdateCommand,
    },
    error::{
        InvalidSkillScriptPathSnafu, ReadSkillFileSnafu, ReadSkillScriptDirectorySnafu, StoreSnafu,
    },
};

#[derive(Debug, Serialize)]
struct SkillSummaryView {
    role: &'static str,
    name: String,
    current_version: u64,
    available_versions: Vec<u64>,
    description: String,
    scripts: Vec<String>,
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
    scripts: Vec<SkillScript>,
    body: String,
}

pub(super) async fn run_skill_command(
    command: SkillCommand,
    store: &impl SkillStore,
) -> Result<Option<String>> {
    match command.command {
        SkillSubcommand::Add(command) => {
            let json = command.json;
            let skill = run_skill_add(command, store)?;
            Ok(Some(if json {
                render_json(skill)
            } else {
                render_skill_summary_text(&skill)
            }))
        }
        SkillSubcommand::Update(command) => {
            let json = command.json;
            let skill = run_skill_update(command, store)?;
            Ok(Some(if json {
                render_json(skill)
            } else {
                render_skill_summary_text(&skill)
            }))
        }
        SkillSubcommand::Rollback(command) => {
            let json = command.json;
            let skill = run_skill_rollback(command, store)?;
            Ok(Some(if json {
                render_json(skill)
            } else {
                render_skill_summary_text(&skill)
            }))
        }
        SkillSubcommand::List(command) => {
            let json = command.json;
            let skills = run_skill_list(command, store)?;
            Ok(Some(if json {
                render_json(skills)
            } else {
                render_skill_list_text(&skills)
            }))
        }
        SkillSubcommand::Show(command) => {
            let json = command.json;
            let skill = run_skill_show(command, store)?;
            Ok(Some(if json {
                render_json(skill)
            } else {
                render_skill_show_text(&skill)
            }))
        }
    }
}

fn run_skill_add(command: SkillAddCommand, store: &impl SkillStore) -> Result<SkillSummaryView> {
    let body = read_skill_body(&command.file)?;
    let scripts = read_skill_scripts(command.scripts, command.script_dir)?;
    let record = store
        .add_skill(
            command.role.into(),
            &command.name,
            SkillVersionSpec {
                description: command.description,
                body,
                scripts,
                enable_coco_shim: command.enable_coco_shim,
            },
        )
        .context(StoreSnafu)?;
    Ok(skill_summary_view(command.role.into(), &record))
}

fn run_skill_update(
    command: SkillUpdateCommand,
    store: &impl SkillStore,
) -> Result<SkillSummaryView> {
    let scripts = if command.clear_scripts {
        Some(Vec::new())
    } else if !command.scripts.is_empty() || command.script_dir.is_some() {
        Some(read_skill_scripts(command.scripts, command.script_dir)?)
    } else {
        None
    };
    let patch = SkillUpdatePatch {
        description: command.description,
        body: command
            .file
            .as_ref()
            .map(|path| read_skill_body(path.as_path()))
            .transpose()?,
        scripts,
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

fn run_skill_rollback(
    command: SkillRollbackCommand,
    store: &impl SkillStore,
) -> Result<SkillSummaryView> {
    let record = store
        .rollback_skill(command.role.into(), &command.name, command.to_version)
        .context(StoreSnafu)?;
    Ok(skill_summary_view(command.role.into(), &record))
}

fn run_skill_list(
    command: SkillListCommand,
    store: &impl SkillStore,
) -> Result<Vec<SkillSummaryView>> {
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

fn run_skill_show(command: SkillShowCommand, store: &impl SkillStore) -> Result<SkillShowView> {
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

fn read_skill_scripts(
    explicit_scripts: Vec<PathBuf>,
    script_dir: Option<PathBuf>,
) -> Result<Vec<SkillScript>> {
    let mut paths = explicit_scripts
        .into_iter()
        .map(|path| SkillScriptInput {
            source_path: path.clone(),
            stored_path: path,
        })
        .collect::<Vec<_>>();
    if let Some(script_dir) = script_dir {
        let entries = fs::read_dir(&script_dir).context(ReadSkillScriptDirectorySnafu {
            path: script_dir.clone(),
        })?;
        for entry in entries {
            let entry = entry.context(ReadSkillScriptDirectorySnafu {
                path: script_dir.clone(),
            })?;
            let file_type = entry.file_type().context(ReadSkillScriptDirectorySnafu {
                path: script_dir.clone(),
            })?;
            if file_type.is_file() && is_script_asset_name(&entry.file_name()) {
                paths.push(SkillScriptInput {
                    source_path: entry.path(),
                    stored_path: PathBuf::from("scripts").join(entry.file_name()),
                });
            }
        }
    }

    let mut scripts = paths
        .into_iter()
        .map(read_skill_script)
        .collect::<Result<Vec<_>>>()?;
    scripts.sort_by(|left, right| left.path.cmp(&right.path));

    let mut seen = HashSet::new();
    for script in &scripts {
        if !seen.insert(script.path.clone()) {
            return InvalidSkillScriptPathSnafu {
                path: PathBuf::from(&script.path),
                message: "duplicate script asset path".to_owned(),
            }
            .fail();
        }
    }

    Ok(scripts)
}

struct SkillScriptInput {
    source_path: PathBuf,
    stored_path: PathBuf,
}

fn read_skill_script(input: SkillScriptInput) -> Result<SkillScript> {
    let script_path = normalized_skill_script_path(&input.stored_path)?;
    let content = fs::read_to_string(&input.source_path).context(ReadSkillFileSnafu {
        path: input.source_path,
    })?;
    Ok(SkillScript {
        path: script_path,
        content,
    })
}

fn normalized_skill_script_path(path: &Path) -> Result<String> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return InvalidSkillScriptPathSnafu {
                    path: path.to_path_buf(),
                    message: "path must be relative and stay under scripts/".to_owned(),
                }
                .fail();
            }
        }
    }

    let starts_with_scripts = normalized.components().next().is_some_and(
        |component| matches!(component, Component::Normal(part) if part == OsStr::new("scripts")),
    );
    if !starts_with_scripts {
        return InvalidSkillScriptPathSnafu {
            path: path.to_path_buf(),
            message: "path must start with scripts/".to_owned(),
        }
        .fail();
    }

    let Some(script_path) = normalized.to_str() else {
        return InvalidSkillScriptPathSnafu {
            path: path.to_path_buf(),
            message: "path must be valid UTF-8".to_owned(),
        }
        .fail();
    };
    if !script_path.ends_with(".py") && !script_path.ends_with(".py.lock") {
        return InvalidSkillScriptPathSnafu {
            path: path.to_path_buf(),
            message: "path must end with .py or .py.lock".to_owned(),
        }
        .fail();
    }

    Ok(script_path.to_owned())
}

fn is_script_asset_name(name: &OsStr) -> bool {
    let name = name.to_string_lossy();
    name.ends_with(".py") || name.ends_with(".py.lock")
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
        scripts: script_paths(&current.scripts),
        enable_coco_shim: current.enable_coco_shim,
    }
}

fn skill_version_view(version: &SkillVersion) -> SkillVersionView {
    SkillVersionView {
        version: version.version,
        created_at: version.created_at.to_string(),
        description: version.description.clone(),
        enable_coco_shim: version.enable_coco_shim,
        scripts: version.scripts.clone(),
        body: version.body.clone(),
    }
}

fn script_paths(scripts: &[SkillScript]) -> Vec<String> {
    scripts.iter().map(|script| script.path.clone()).collect()
}

fn render_skill_list_text(skills: &[SkillSummaryView]) -> String {
    if skills.is_empty() {
        return "No skills found.".to_owned();
    }

    skills
        .iter()
        .map(|skill| {
            format!(
                "{} {} current={} available=[{}] scripts=[{}] shim={} - {}",
                skill.role,
                skill.name,
                skill.current_version,
                skill
                    .available_versions
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
                skill.scripts.join(","),
                skill.enable_coco_shim,
                skill.description
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_skill_summary_text(skill: &SkillSummaryView) -> String {
    format!(
        "role: {}\nname: {}\ncurrent_version: {}\navailable_versions: [{}]\nscripts: [{}]\nshim: {}\ndescription: {}",
        skill.role,
        skill.name,
        skill.current_version,
        skill
            .available_versions
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(","),
        skill.scripts.join(","),
        skill.enable_coco_shim,
        skill.description
    )
}

fn render_skill_show_text(skill: &SkillShowView) -> String {
    let mut lines = vec![
        format!("role: {}", skill.role),
        format!("name: {}", skill.name),
        format!("current_version: {}", skill.current_version),
        "versions:".to_owned(),
    ];

    for version in &skill.versions {
        lines.extend([
            format!(
                "- version={} created_at={} shim={} scripts=[{}] description={}",
                version.version,
                version.created_at,
                version.enable_coco_shim,
                script_paths(&version.scripts).join(","),
                version.description
            ),
            "  body:".to_owned(),
        ]);
        lines.extend(version.body.lines().map(|line| format!("  {line}")));
    }

    lines.join("\n")
}

fn render_json<T>(value: T) -> String
where
    T: Serialize,
{
    serde_json::to_string_pretty(&value).expect("skill output should serialize")
}
