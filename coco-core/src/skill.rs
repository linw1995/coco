use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Weak;

use async_trait::async_trait;
use coco_llm::coco_mem::{
    Anchor, BranchStore, Kind, NewNode, NodeStore, Role, RuntimeStore, SessionAnchor, SessionRole,
    SessionStore, SkillRecord, SkillRuntimeContext, SkillScript, SkillStore, ToolUse,
};
use coco_llm::{
    CompletionBackend, CompletionInput, CompletionOrigin, CompletionOverrides, CompletionRequest,
    Error as LlmError, ExecutorError, LlmService, SearchSkillToolRequest, SkillToolExecutionResult,
    SkillToolExecutor, SkillToolRequest, SkillToolRunResult, UseSkillToolRequest,
};
use indoc::formatdoc;
use serde::Serialize;
use snafu::prelude::*;

use crate::{ConversationEngine, EngineError};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SkillSearchResult {
    pub skills: Vec<SkillMatch>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SkillMatch {
    pub name: String,
    pub description: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkillEntry {
    name: String,
    description: String,
    path: PathBuf,
    body: String,
    scripts: Vec<SkillScript>,
    session_role: SessionRole,
    enable_coco_shim: bool,
    search_blob: String,
}

#[derive(Debug, Snafu)]
pub(crate) enum SkillError {
    #[snafu(display("failed to serialize skill tool output: {source}"))]
    SerializeOutput { source: serde_json::Error },

    #[snafu(display("failed to resolve configured skill root {path:?}: {source}"))]
    ResolveConfiguredRoot {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("configured skill root {path:?} must point to an existing directory"))]
    InvalidConfiguredRoot { path: PathBuf },

    #[snafu(display("failed to read skill file {path:?}: {source}"))]
    ReadSkillFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("failed to read skill directory {path:?}: {source}"))]
    ReadSkillDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("failed to inspect skill path {path:?}: {source}"))]
    InspectSkillPath {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("failed to create skill runtime directory {path:?}: {source}"))]
    CreateSkillRuntimeDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("failed to write skill runtime file {path:?}: {source}"))]
    WriteSkillRuntimeFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("invalid skill script path {path:?}: {message}"))]
    InvalidSkillScriptPath { path: String, message: String },

    #[snafu(display("invalid session_role {value:?} in skill file {path:?}"))]
    InvalidSkillSessionRole { path: PathBuf, value: String },

    #[snafu(display("invalid enable_coco_shim value {value:?} in skill file {path:?}"))]
    InvalidSkillCocoShim { path: PathBuf, value: String },

    #[snafu(display("failed to load skills from store: {source}"))]
    LoadSkills {
        source: coco_llm::coco_mem::StoreError,
    },

    #[snafu(display("no installed skill named {name:?}"))]
    SkillNotFound { name: String },

    #[snafu(display("multiple installed skills named {name:?}: {paths:?}"))]
    AmbiguousSkillName { name: String, paths: Vec<String> },
}

impl From<SkillError> for ExecutorError {
    fn from(error: SkillError) -> Self {
        Self::OperationFailed {
            message: error.to_string(),
            source: Some(Box::new(error)),
        }
    }
}

impl From<EngineError> for ExecutorError {
    fn from(error: EngineError) -> Self {
        Self::OperationFailed {
            message: error.to_string(),
            source: Some(Box::new(error)),
        }
    }
}

pub struct CoreSkillToolExecutor<B, S> {
    llm: Weak<LlmService<B, S>>,
}

impl<B, S> fmt::Debug for CoreSkillToolExecutor<B, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CoreSkillToolExecutor(..)")
    }
}

impl<B, S> CoreSkillToolExecutor<B, S> {
    pub fn new(llm: Weak<LlmService<B, S>>) -> Self {
        Self { llm }
    }
}

impl<B, S> CoreSkillToolExecutor<B, S>
where
    B: CompletionBackend + 'static,
    S: 'static,
{
    fn upgrade_engine(&self) -> std::result::Result<ConversationEngine<B, S>, ExecutorError> {
        let llm = self.llm.upgrade().ok_or(ExecutorError::Unavailable)?;
        Ok(ConversationEngine::new(llm))
    }
}

#[async_trait]
impl<B, S> SkillToolExecutor for CoreSkillToolExecutor<B, S>
where
    B: CompletionBackend + 'static,
    S: NodeStore
        + BranchStore
        + SessionStore
        + SkillStore
        + RuntimeStore
        + Clone
        + Send
        + Sync
        + 'static,
{
    async fn search_skill_tool(
        &self,
        request: SearchSkillToolRequest,
    ) -> std::result::Result<String, ExecutorError> {
        let result = self.upgrade_engine()?.search_skills(
            &request.workspace_root,
            request.session_role,
            &request.query,
            request.limit,
        )?;

        let output = serde_json::to_string_pretty(&result).context(SerializeOutputSnafu)?;
        Ok(output)
    }

    async fn execute_skill_tool(
        &self,
        request: UseSkillToolRequest,
    ) -> std::result::Result<SkillToolExecutionResult, ExecutorError> {
        let engine = self.upgrade_engine()?;
        let skill = resolve_skill(
            &request.workspace_root,
            engine.service().store(),
            request.session_role,
            &request.skill_name,
        )?;
        let skill_request = SkillToolRequest {
            base_branch: request.session_branch,
            parent_tool_use_id: request.parent_tool_use_id,
            skill_name: skill.name.clone(),
            skill_description: skill.description,
            skill_path: skill.path.display().to_string(),
            skill_body: skill.body,
            scripts: skill.scripts,
            session_role: skill.session_role,
            enable_coco_shim: skill.enable_coco_shim,
        };

        let runtime = materialize_skill_runtime(&skill_request)?;
        engine
            .execute_resolved_skill(skill_request, runtime)
            .await
            .map_err(executor_error_from_llm_error)
    }
}

fn executor_error_from_llm_error(error: LlmError) -> ExecutorError {
    let message = error.to_string();
    ExecutorError::OperationFailed {
        message,
        source: Some(Box::new(error)),
    }
}

#[derive(Debug)]
struct MaterializedSkillRuntime {
    context: Option<SkillRuntimeContext>,
    directory: Option<PathBuf>,
}

impl Drop for MaterializedSkillRuntime {
    fn drop(&mut self) {
        if let Some(directory) = &self.directory {
            let _ = fs::remove_dir_all(directory);
        }
    }
}

fn materialize_skill_runtime(
    request: &SkillToolRequest,
) -> std::result::Result<MaterializedSkillRuntime, SkillError> {
    if request.scripts.is_empty() {
        return Ok(MaterializedSkillRuntime {
            context: None,
            directory: None,
        });
    }

    let directory = std::env::temp_dir()
        .join("coco")
        .join("skill-sessions")
        .join(nanoid::nanoid!(10));
    fs::create_dir_all(&directory).context(CreateSkillRuntimeDirectorySnafu {
        path: directory.clone(),
    })?;

    let skill_file = directory.join("SKILL.md");
    fs::write(&skill_file, &request.skill_body)
        .context(WriteSkillRuntimeFileSnafu { path: skill_file })?;

    let mut script_paths = Vec::with_capacity(request.scripts.len());
    for script in &request.scripts {
        let relative_path = validate_runtime_script_path(&script.path)?;
        let target = directory.join(&relative_path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).context(CreateSkillRuntimeDirectorySnafu {
                path: parent.to_path_buf(),
            })?;
        }
        fs::write(&target, &script.content).context(WriteSkillRuntimeFileSnafu { path: target })?;
        script_paths.push(relative_path);
    }

    Ok(MaterializedSkillRuntime {
        context: Some(SkillRuntimeContext {
            name: request.skill_name.clone(),
            directory: directory.clone(),
            scripts: script_paths,
        }),
        directory: Some(directory),
    })
}

fn validate_runtime_script_path(path: &str) -> std::result::Result<String, SkillError> {
    let source_path = Path::new(path);
    let mut normalized = PathBuf::new();
    for component in source_path.components() {
        match component {
            Component::Normal(part) => normalized.push(part),
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return InvalidSkillScriptPathSnafu {
                    path: path.to_owned(),
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
            path: path.to_owned(),
            message: "path must start with scripts/".to_owned(),
        }
        .fail();
    }

    let Some(script_path) = normalized.to_str() else {
        return InvalidSkillScriptPathSnafu {
            path: path.to_owned(),
            message: "path must be valid UTF-8".to_owned(),
        }
        .fail();
    };
    if !script_path.ends_with(".py") && !script_path.ends_with(".py.lock") {
        return InvalidSkillScriptPathSnafu {
            path: path.to_owned(),
            message: "path must end with .py or .py.lock".to_owned(),
        }
        .fail();
    }

    Ok(script_path.to_owned())
}

impl<B, S> ConversationEngine<B, S>
where
    B: CompletionBackend + 'static,
    S: NodeStore
        + BranchStore
        + SessionStore
        + SkillStore
        + RuntimeStore
        + Clone
        + Send
        + Sync
        + 'static,
{
    pub fn search_skills(
        &self,
        workspace_root: &Path,
        session_role: SessionRole,
        query: &str,
        limit: usize,
    ) -> std::result::Result<SkillSearchResult, EngineError> {
        let result = search_skills(
            workspace_root,
            self.service().store(),
            session_role,
            query,
            limit,
        )?;
        Ok(result)
    }

    pub async fn execute_skill(
        &self,
        workspace_root: &Path,
        base_branch: &str,
        session_role: SessionRole,
        parent_tool_use_id: &str,
        skill_name: &str,
    ) -> std::result::Result<SkillToolExecutionResult, EngineError> {
        let skill = resolve_skill(
            workspace_root,
            self.service().store(),
            session_role,
            skill_name,
        )?;
        let request = SkillToolRequest {
            base_branch: base_branch.to_owned(),
            parent_tool_use_id: parent_tool_use_id.to_owned(),
            skill_name: skill.name.clone(),
            skill_description: skill.description.clone(),
            skill_path: skill.path.display().to_string(),
            skill_body: skill.body,
            scripts: skill.scripts,
            session_role: skill.session_role,
            enable_coco_shim: skill.enable_coco_shim,
        };
        let runtime = materialize_skill_runtime(&request)?;
        self.execute_resolved_skill(request, runtime)
            .await
            .map_err(EngineError::from)
    }

    async fn execute_resolved_skill(
        &self,
        request: SkillToolRequest,
        runtime: MaterializedSkillRuntime,
    ) -> std::result::Result<SkillToolExecutionResult, LlmError> {
        let service = self.service();
        let store = service.store();
        let child_branch = temporary_skill_branch_name(&request.base_branch, &request.skill_name);
        ensure_use_skill_node(store, &request.parent_tool_use_id)?;
        let child_session_anchor_id =
            append_skill_session_anchor(store, &request, runtime.context.clone())?;

        store
            .fork(&child_branch, &child_session_anchor_id)
            .map_err(|source| LlmError::Memory {
                source: Box::new(source),
            })?;
        let prompt_result = service
            .run(CompletionRequest {
                branch: child_branch.clone(),
                origin: CompletionOrigin::BranchHead,
                input: CompletionInput::Continue,
                overrides: CompletionOverrides::default(),
            })
            .await;
        let cleanup_result = service.delete_session_branch(&child_branch).await;

        match (prompt_result, cleanup_result) {
            (Ok(result), Ok(())) => Ok(SkillToolExecutionResult {
                result: SkillToolRunResult {
                    text: result.text.clone(),
                },
                response_node_id: result.response_node_id,
            }),
            (Err(workflow), Ok(())) => Err(workflow),
            (Ok(_), Err(cleanup)) => Err(LlmError::UseSkillCleanup {
                branch: child_branch,
                source: Box::new(cleanup),
            }),
            (Err(workflow), Err(cleanup)) => Err(LlmError::UseSkillWorkflowFailedCleanup {
                branch: child_branch,
                workflow: Box::new(workflow),
                cleanup: Box::new(cleanup),
            }),
        }
    }
}

fn ensure_use_skill_node<S>(store: &S, reference: &str) -> std::result::Result<(), LlmError>
where
    S: NodeStore,
{
    let node = store
        .get_node(reference)
        .map_err(|source| LlmError::Memory {
            source: Box::new(source),
        })?;

    if matches!(&node.kind, Kind::ToolUse(ToolUse { name, .. }) if name == "use_skill") {
        return Ok(());
    }

    Err(LlmError::InvalidAnchor {
        anchor_id: reference.to_owned(),
    })
}

fn append_skill_session_anchor<S>(
    store: &S,
    request: &SkillToolRequest,
    active_skill: Option<SkillRuntimeContext>,
) -> std::result::Result<String, LlmError>
where
    S: NodeStore,
{
    let inherited = resolve_parent_session_anchor(store, &request.parent_tool_use_id)?;
    store
        .append(NewNode {
            parent: request.parent_tool_use_id.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(
                vec![],
                SessionAnchor {
                    role: request.session_role,
                    provider_profile: inherited.provider_profile,
                    provider: inherited.provider,
                    model: inherited.model,
                    tools: inherited.tools,
                    system_prompt: inherited.system_prompt,
                    prompt: skill_execution_prompt(request),
                    temperature: inherited.temperature,
                    max_tokens: inherited.max_tokens,
                    additional_params: inherited.additional_params,
                    enable_coco_shim: request.enable_coco_shim,
                    active_skill,
                },
            )),
        })
        .map_err(|source| LlmError::Memory {
            source: Box::new(source),
        })
}

fn resolve_parent_session_anchor<S>(
    store: &S,
    start_id: &str,
) -> std::result::Result<SessionAnchor, LlmError>
where
    S: NodeStore,
{
    let mut node = store
        .get_node(start_id)
        .map_err(|source| LlmError::Memory {
            source: Box::new(source),
        })?;

    loop {
        if let Kind::Anchor(anchor) = &node.kind
            && let Some(session) = anchor.as_session()
        {
            return Ok(session.clone());
        }

        if node.is_root() {
            return Err(LlmError::InvalidAnchor {
                anchor_id: start_id.to_owned(),
            });
        }

        node = store
            .get_node(&node.parent)
            .map_err(|source| LlmError::Memory {
                source: Box::new(source),
            })?;
    }
}

fn temporary_skill_branch_name(base_branch: &str, skill_name: &str) -> String {
    let slug = skill_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_owned();
    let slug = if slug.is_empty() {
        "skill".to_owned()
    } else {
        slug
    };

    format!("{base_branch}/skill/{slug}-{}", nanoid::nanoid!(8))
}

fn skill_execution_prompt(request: &SkillToolRequest) -> String {
    let script_instructions = skill_script_instructions(request);
    formatdoc!(
        "
        You are executing the skill `{}` on an isolated child branch forked from `{}`.
        Follow the skill instructions below and do all exploration on this child branch only.
        When finished, return only the final result that should be sent back to the caller.

        Skill description:
        {}

        Skill source:
        {}

        Skill session role:
        {}

        {}

        Skill instructions:
        {}
        ",
        request.skill_name,
        request.base_branch,
        request.skill_description,
        request.skill_path,
        request.session_role.as_str(),
        script_instructions,
        request.skill_body,
    )
}

fn skill_script_instructions(request: &SkillToolRequest) -> String {
    let python_scripts = request
        .scripts
        .iter()
        .filter(|script| script.path.ends_with(".py"))
        .map(|script| {
            format!(
                "- {}\n  Run with: uv run --script \"$COCO_SKILL_DIR/{}\"",
                script.path, script.path
            )
        })
        .collect::<Vec<_>>();

    if python_scripts.is_empty() {
        return "Skill scripts:\nNo uv single-file scripts are attached.".to_owned();
    }

    format!(
        "Skill scripts:\n{}\nUse COCO_SKILL_DIR as the materialized skill directory. Do not edit the skill source in the store.",
        python_scripts.join("\n")
    )
}

fn configured_skill_roots(workspace_root: &Path) -> std::result::Result<Vec<PathBuf>, SkillError> {
    let Some(raw) = std::env::var_os("COCO_SKILLS_DIRS") else {
        return Ok(default_skill_roots(workspace_root));
    };

    let mut roots = Vec::new();
    for path in std::env::split_paths(&raw) {
        let resolved = path
            .canonicalize()
            .context(ResolveConfiguredRootSnafu { path: path.clone() })?;
        if !resolved.is_dir() {
            return InvalidConfiguredRootSnafu { path: resolved }.fail();
        }
        roots.push(resolved);
    }

    Ok(roots)
}

fn default_skill_roots(workspace_root: &Path) -> Vec<PathBuf> {
    let mut roots = vec![
        workspace_root.join(".codex/skills"),
        workspace_root.join(".agents/skills"),
    ];

    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        roots.push(home.join(".codex/skills"));
        roots.push(home.join(".agents/skills"));
    }

    roots
        .into_iter()
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>()
}

fn parse_frontmatter_value(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 {
        let first = value.as_bytes()[0];
        let last = value.as_bytes()[value.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return value[1..value.len() - 1].to_owned();
        }
    }
    value.to_owned()
}

fn parse_frontmatter_bool(value: &str) -> Option<bool> {
    match parse_frontmatter_value(value).to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn split_frontmatter(contents: &str) -> (Option<&str>, &str) {
    let mut lines = contents.lines();
    if lines.next() != Some("---") {
        return (None, contents);
    }

    let mut end_offset = "---\n".len();
    for line in lines {
        end_offset += line.len() + 1;
        if line == "---" {
            let frontmatter = &contents["---\n".len()..end_offset - line.len() - 1];
            let body = &contents[end_offset..];
            return (Some(frontmatter), body);
        }
    }

    (None, contents)
}

fn load_skill(path: &Path) -> std::result::Result<SkillEntry, SkillError> {
    let contents = fs::read_to_string(path).context(ReadSkillFileSnafu {
        path: path.to_path_buf(),
    })?;
    let (frontmatter, body) = split_frontmatter(&contents);

    let mut name = None;
    let mut description = None;
    let mut session_role = SessionRole::Runner;
    let mut enable_coco_shim = false;
    if let Some(frontmatter) = frontmatter {
        for line in frontmatter.lines() {
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            match key.trim() {
                "name" => name = Some(parse_frontmatter_value(value)),
                "description" => description = Some(parse_frontmatter_value(value)),
                "session_role" => {
                    let value = parse_frontmatter_value(value);
                    session_role =
                        SessionRole::parse(&value).context(InvalidSkillSessionRoleSnafu {
                            path: path.to_path_buf(),
                            value,
                        })?;
                }
                "enable_coco_shim" => {
                    let raw = parse_frontmatter_value(value);
                    enable_coco_shim =
                        parse_frontmatter_bool(value).context(InvalidSkillCocoShimSnafu {
                            path: path.to_path_buf(),
                            value: raw,
                        })?;
                }
                _ => {}
            }
        }
    }

    let name = name.unwrap_or_else(|| {
        path.parent()
            .and_then(Path::file_name)
            .unwrap_or_else(|| OsStr::new("unknown-skill"))
            .to_string_lossy()
            .into_owned()
    });
    let description = description.unwrap_or_default();
    let normalized_body = body.trim().to_owned();
    let search_blob = format!("{name}\n{description}\n{normalized_body}").to_ascii_lowercase();

    Ok(SkillEntry {
        name,
        description,
        path: path.to_path_buf(),
        body: normalized_body,
        scripts: Vec::new(),
        session_role,
        enable_coco_shim,
        search_blob,
    })
}

fn synthetic_skill_path(role: SessionRole, name: &str, version: u64) -> PathBuf {
    PathBuf::from(format!(
        "store://skills/{}/{}@{}",
        role.as_str(),
        name,
        version
    ))
}

fn collect_store_skills<S>(
    store: &S,
    session_role: SessionRole,
) -> std::result::Result<Vec<SkillEntry>, SkillError>
where
    S: SkillStore,
{
    let records = store.list_skills(session_role).context(LoadSkillsSnafu)?;
    Ok(skill_entries_from_store_records(session_role, &records))
}

fn skill_entries_from_store_records(role: SessionRole, records: &[SkillRecord]) -> Vec<SkillEntry> {
    records
        .iter()
        .filter_map(|record| {
            let current = record.current()?;
            let search_blob = format!("{}\n{}\n{}", record.name, current.description, current.body)
                .to_ascii_lowercase();
            Some(SkillEntry {
                name: record.name.clone(),
                description: current.description.clone(),
                path: synthetic_skill_path(role, &record.name, current.version),
                body: current.body.clone(),
                scripts: current.scripts.clone(),
                session_role: role,
                enable_coco_shim: current.enable_coco_shim,
                search_blob,
            })
        })
        .collect()
}

fn collect_skills_from_dir(
    root: &Path,
    skills: &mut Vec<SkillEntry>,
) -> std::result::Result<(), SkillError> {
    let entries = fs::read_dir(root).context(ReadSkillDirectorySnafu {
        path: root.to_path_buf(),
    })?;

    for entry in entries {
        let entry = entry.context(ReadSkillDirectorySnafu {
            path: root.to_path_buf(),
        })?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .context(InspectSkillPathSnafu { path: path.clone() })?;

        if file_type.is_dir() {
            collect_skills_from_dir(&path, skills)?;
            continue;
        }

        if file_type.is_file()
            && entry
                .file_name()
                .to_string_lossy()
                .eq_ignore_ascii_case("SKILL.md")
        {
            skills.push(load_skill(&path)?);
        }
    }

    Ok(())
}

fn collect_skills<S>(
    workspace_root: &Path,
    store: &S,
    session_role: SessionRole,
) -> std::result::Result<Vec<SkillEntry>, SkillError>
where
    S: SkillStore,
{
    let roots = configured_skill_roots(workspace_root)?;
    let mut skills = Vec::new();
    for root in roots {
        collect_skills_from_dir(&root, &mut skills)?;
    }
    skills.extend(collect_store_skills(store, session_role)?);
    skills.sort_by(|left, right| left.name.cmp(&right.name).then(left.path.cmp(&right.path)));
    Ok(skills)
}

fn score_skill(skill: &SkillEntry, query: &str) -> usize {
    if query.is_empty() {
        return 1;
    }

    let query = query.to_ascii_lowercase();
    let terms = query.split_whitespace().collect::<Vec<_>>();
    let name = skill.name.to_ascii_lowercase();
    let description = skill.description.to_ascii_lowercase();

    let mut score = 0;
    if name == query {
        score += 1000;
    } else if name.starts_with(&query) {
        score += 500;
    } else if name.contains(&query) {
        score += 250;
    }

    if description.contains(&query) {
        score += 150;
    }
    if skill.search_blob.contains(&query) {
        score += 75;
    }
    if !terms.is_empty() && terms.iter().all(|term| skill.search_blob.contains(term)) {
        score += 50;
    }

    score
}

fn search_skills(
    workspace_root: &Path,
    store: &impl SkillStore,
    session_role: SessionRole,
    query: &str,
    limit: usize,
) -> std::result::Result<SkillSearchResult, SkillError> {
    let mut matches = collect_skills(workspace_root, store, session_role)?
        .into_iter()
        .filter_map(|skill| {
            let score = score_skill(&skill, query);
            (score > 0).then_some((score, skill))
        })
        .collect::<Vec<_>>();

    matches.sort_by(|(left_score, left_skill), (right_score, right_skill)| {
        right_score
            .cmp(left_score)
            .then(left_skill.name.cmp(&right_skill.name))
            .then(left_skill.path.cmp(&right_skill.path))
    });

    Ok(SkillSearchResult {
        skills: matches
            .into_iter()
            .take(limit)
            .map(|(_, skill)| SkillMatch {
                name: skill.name,
                description: skill.description,
                path: skill.path.display().to_string(),
            })
            .collect(),
    })
}

fn resolve_skill(
    workspace_root: &Path,
    store: &impl SkillStore,
    session_role: SessionRole,
    name: &str,
) -> std::result::Result<SkillEntry, SkillError> {
    let matches = collect_skills(workspace_root, store, session_role)?
        .into_iter()
        .filter(|skill| skill.name == name)
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [] => SkillNotFoundSnafu {
            name: name.to_owned(),
        }
        .fail(),
        [skill] => Ok(skill.clone()),
        many => AmbiguousSkillNameSnafu {
            name: name.to_owned(),
            paths: many
                .iter()
                .map(|skill| skill.path.display().to_string())
                .collect::<Vec<_>>(),
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::future::Future;
    use std::sync::OnceLock;

    use tokio::sync::Mutex;

    use super::*;

    async fn with_env_async<T, F, Fut>(entries: &[(&str, Option<&OsStr>)], run: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
        let previous: Vec<_> = entries
            .iter()
            .map(|(name, _)| ((*name).to_owned(), std::env::var_os(name)))
            .collect();

        for (name, value) in entries {
            match value {
                Some(value) => unsafe { std::env::set_var(name, value) },
                None => unsafe { std::env::remove_var(name) },
            }
        }

        let output = run().await;

        for (name, value) in previous {
            match value {
                Some(value) => unsafe { std::env::set_var(name, value) },
                None => unsafe { std::env::remove_var(name) },
            }
        }

        output
    }

    fn write_skill(root: &Path, relative_dir: &str, body: &str) {
        let dir = root.join(relative_dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("SKILL.md"), body).unwrap();
    }

    #[test]
    fn skill_execution_prompt_includes_skill_context() {
        let prompt = skill_execution_prompt(&SkillToolRequest {
            base_branch: "main".to_owned(),
            parent_tool_use_id: "tool-use-node".to_owned(),
            skill_name: "find-skills".to_owned(),
            skill_description: "Find relevant skills.".to_owned(),
            skill_path: "/tmp/find-skills/SKILL.md".to_owned(),
            skill_body: "# Find Skills".to_owned(),
            scripts: vec![SkillScript {
                path: "scripts/inspect.py".to_owned(),
                content: "print('inspect')".to_owned(),
            }],
            session_role: SessionRole::Runner,
            enable_coco_shim: true,
        });

        assert!(prompt.contains("skill `find-skills`"));
        assert!(prompt.contains("forked from `main`"));
        assert!(prompt.contains("Skill description:\nFind relevant skills."));
        assert!(prompt.contains("Skill source:\n/tmp/find-skills/SKILL.md"));
        assert!(prompt.contains("Skill session role:\nrunner"));
        assert!(prompt.contains("uv run --script \"$COCO_SKILL_DIR/scripts/inspect.py\""));
        assert!(prompt.contains("Skill instructions:\n# Find Skills"));
        assert!(!prompt.contains("Additional task from caller:"));
    }

    #[tokio::test]
    async fn search_skills_finds_matches_by_name_and_description() {
        let root = tempfile::tempdir().unwrap();
        write_skill(
            root.path(),
            "openai-docs",
            r#"---
name: "openai-docs"
description: "Find OpenAI API documentation."
---

# OpenAI Docs
"#,
        );
        write_skill(
            root.path(),
            "rust-review",
            r#"---
name: "rust-review"
description: "Review Rust changes."
---

# Rust Review
"#,
        );
        let path_env = std::env::join_paths([root.path()]).unwrap();
        let store = coco_llm::coco_mem::MemoryStore::new();

        let result = with_env_async(
            &[("COCO_SKILLS_DIRS", Some(path_env.as_os_str()))],
            || async {
                search_skills(
                    root.path(),
                    &store,
                    SessionRole::Orchestrator,
                    "openai docs",
                    1,
                )
            },
        )
        .await
        .unwrap();

        assert_eq!(result.skills.len(), 1);
        assert_eq!(result.skills[0].name, "openai-docs");
    }

    #[tokio::test]
    async fn resolve_skill_rejects_ambiguous_names() {
        let first_root = tempfile::tempdir().unwrap();
        let second_root = tempfile::tempdir().unwrap();
        write_skill(
            first_root.path(),
            "shared-skill",
            r#"---
name: "shared-skill"
description: "First copy."
---

# Shared Skill
"#,
        );
        write_skill(
            second_root.path(),
            "shared-skill",
            r#"---
name: "shared-skill"
description: "Second copy."
---

# Shared Skill
"#,
        );
        let path_env = std::env::join_paths([first_root.path(), second_root.path()]).unwrap();
        let store = coco_llm::coco_mem::MemoryStore::new();

        let error = with_env_async(
            &[("COCO_SKILLS_DIRS", Some(path_env.as_os_str()))],
            || async {
                resolve_skill(
                    first_root.path(),
                    &store,
                    SessionRole::Orchestrator,
                    "shared-skill",
                )
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("multiple installed skills"));
    }

    #[tokio::test]
    async fn configured_skill_roots_validate_existing_directories() {
        let missing = OsString::from("/tmp/coco-core-skill-missing");

        let error = with_env_async(
            &[("COCO_SKILLS_DIRS", Some(missing.as_os_str()))],
            || async { configured_skill_roots(Path::new(".")) },
        )
        .await
        .unwrap_err();

        assert!(matches!(error, SkillError::ResolveConfiguredRoot { .. }));
    }

    #[test]
    fn collect_store_skills_only_returns_templates_for_current_session_role() {
        let store = coco_llm::coco_mem::MemoryStore::new();

        let runner = collect_store_skills(&store, SessionRole::Runner).unwrap();
        let orchestrator = collect_store_skills(&store, SessionRole::Orchestrator).unwrap();

        assert_eq!(
            runner
                .iter()
                .map(|skill| skill.name.as_str())
                .collect::<Vec<_>>(),
            vec!["coco-runner"]
        );
        assert_eq!(
            orchestrator
                .iter()
                .map(|skill| skill.name.as_str())
                .collect::<Vec<_>>(),
            vec!["coco-orchestrator", "new-skill"]
        );
    }
}
