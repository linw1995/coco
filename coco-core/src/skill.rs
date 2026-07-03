use std::collections::HashSet;
use std::ffi::OsStr;
use std::fmt::{self, Write as _};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Weak;

use async_trait::async_trait;
use coco_llm::coco_mem::{
    Anchor, BranchStore, Kind, NewNode, NodeStore, Role, SessionAnchor, SessionRole, SessionStore,
    SkillRecord, SkillRuntimeContext, SkillScript, SkillStore,
};
use coco_llm::{
    ActiveSkillRuntimeContext, COCO_SKILL_PERSIST_DIR_ENV, COCO_SKILL_PERSIST_ROOT_ENV,
    CompletionBackend, CompletionInput, CompletionOrigin, CompletionOverrides, CompletionRequest,
    Error as LlmError, ExecutorError, LlmService, SkillInvocationRequest, SkillInvocationResult,
    SkillSearchExecutor, SkillSearchRequest,
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
pub enum SkillError {
    #[snafu(display("failed to serialize skill tool output: {source}"))]
    SerializeOutput { source: serde_json::Error },

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

pub struct CoreSkillSearchExecutor<B, S> {
    llm: Weak<LlmService<B, S>>,
}

impl<B, S> fmt::Debug for CoreSkillSearchExecutor<B, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CoreSkillSearchExecutor(..)")
    }
}

impl<B, S> CoreSkillSearchExecutor<B, S> {
    pub fn new(llm: Weak<LlmService<B, S>>) -> Self {
        Self { llm }
    }
}

impl<B, S> CoreSkillSearchExecutor<B, S>
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
impl<B, S> SkillSearchExecutor for CoreSkillSearchExecutor<B, S>
where
    B: CompletionBackend + 'static,
    S: NodeStore + BranchStore + SessionStore + SkillStore + Clone + Send + Sync + 'static,
{
    async fn search_skill(
        &self,
        request: SkillSearchRequest,
    ) -> std::result::Result<String, ExecutorError> {
        let result = self
            .upgrade_engine()?
            .search_skills(
                &request.workspace_root,
                request.session_role,
                &request.query,
                request.limit,
            )
            .await?;

        let output = serde_json::to_string_pretty(&result).context(SerializeOutputSnafu)?;
        Ok(output)
    }
}

#[derive(Debug)]
struct MaterializedSkillRuntime {
    context: Option<ActiveSkillRuntimeContext>,
    directory: Option<PathBuf>,
}

#[derive(Debug)]
struct SkillRuntimeDirectoryGuard {
    directory: Option<PathBuf>,
}

impl Drop for MaterializedSkillRuntime {
    fn drop(&mut self) {
        if let Some(directory) = &self.directory {
            let _ = fs::remove_dir_all(directory);
        }
    }
}

impl SkillRuntimeDirectoryGuard {
    fn new(directory: PathBuf) -> Self {
        Self {
            directory: Some(directory),
        }
    }

    fn path(&self) -> &Path {
        self.directory
            .as_deref()
            .expect("skill runtime directory guard should own a directory")
    }

    fn into_runtime(mut self, context: ActiveSkillRuntimeContext) -> MaterializedSkillRuntime {
        MaterializedSkillRuntime {
            context: Some(context),
            directory: self.directory.take(),
        }
    }
}

impl Drop for SkillRuntimeDirectoryGuard {
    fn drop(&mut self) {
        if let Some(directory) = &self.directory {
            let _ = fs::remove_dir_all(directory);
        }
    }
}

fn materialize_skill_runtime(
    request: &SkillInvocationRequest,
) -> std::result::Result<MaterializedSkillRuntime, SkillError> {
    let persistent_directory = skill_persistent_directory(request);
    fs::create_dir_all(&persistent_directory).context(CreateSkillRuntimeDirectorySnafu {
        path: persistent_directory.clone(),
    })?;
    let directory = std::env::temp_dir()
        .join("coco")
        .join("skill-sessions")
        .join(nanoid::nanoid!(10));
    fs::create_dir_all(&directory).context(CreateSkillRuntimeDirectorySnafu {
        path: directory.clone(),
    })?;
    let runtime_dir = SkillRuntimeDirectoryGuard::new(directory);

    let skill_file = runtime_dir.path().join("SKILL.md");
    fs::write(&skill_file, &request.skill_body)
        .context(WriteSkillRuntimeFileSnafu { path: skill_file })?;

    for script in &request.scripts {
        let relative_path = validate_runtime_script_path(&script.path)?;
        let target = runtime_dir.path().join(&relative_path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).context(CreateSkillRuntimeDirectorySnafu {
                path: parent.to_path_buf(),
            })?;
        }
        fs::write(&target, &script.content).context(WriteSkillRuntimeFileSnafu { path: target })?;
    }

    let context = ActiveSkillRuntimeContext {
        name: request.skill_name.clone(),
        directory: runtime_dir.path().to_path_buf(),
        persistent_directory,
    };
    Ok(runtime_dir.into_runtime(context))
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
    S: NodeStore + BranchStore + SessionStore + SkillStore + Clone + Send + Sync + 'static,
{
    pub async fn search_skills(
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
        )
        .await?;
        Ok(result)
    }

    pub async fn execute_skill_invocation(
        &self,
        workspace_root: &Path,
        base_branch: &str,
        session_role: SessionRole,
        invocation_node_id: &str,
        skill_name: &str,
        handoff: Option<String>,
    ) -> std::result::Result<SkillInvocationResult, EngineError> {
        let skill = resolve_skill(
            workspace_root,
            self.service().store(),
            session_role,
            skill_name,
        )
        .await?;
        let request = SkillInvocationRequest {
            workspace_root: workspace_root.to_path_buf(),
            base_branch: base_branch.to_owned(),
            parent_tool_use_id: invocation_node_id.to_owned(),
            skill_name: skill.name.clone(),
            skill_description: skill.description.clone(),
            skill_path: skill.path.display().to_string(),
            skill_body: skill.body,
            scripts: skill.scripts,
            session_role: skill.session_role,
            enable_coco_shim: skill.enable_coco_shim,
            handoff,
        };
        let runtime = materialize_skill_runtime(&request)?;
        self.execute_resolved_skill(request, runtime)
            .await
            .map_err(EngineError::from)
    }

    async fn execute_resolved_skill(
        &self,
        request: SkillInvocationRequest,
        runtime: MaterializedSkillRuntime,
    ) -> std::result::Result<SkillInvocationResult, LlmError> {
        let service = self.service();
        let store = service.store();
        let child_branch = temporary_skill_branch_name(&request.base_branch, &request.skill_name);
        ensure_skill_invocation_node(store, &request.parent_tool_use_id, &request.skill_name)?;
        let child_session_anchor_id = append_skill_session_anchor(store, &request)?;

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
                active_skill_runtime: runtime.context.clone(),
            })
            .await;
        let cleanup_result = service.delete_session_branch(&child_branch).await;

        match (prompt_result, cleanup_result) {
            (Ok(result), Ok(())) => Ok(SkillInvocationResult {
                text: result.text.clone(),
                response_node_id: result.response_node_id,
            }),
            (Err(workflow), Ok(())) => Err(workflow),
            (Ok(_), Err(cleanup)) => Err(LlmError::SkillCleanup {
                branch: child_branch,
                source: Box::new(cleanup),
            }),
            (Err(workflow), Err(cleanup)) => Err(LlmError::SkillWorkflowFailedCleanup {
                branch: child_branch,
                workflow: Box::new(workflow),
                cleanup: Box::new(cleanup),
            }),
        }
    }
}

fn ensure_skill_invocation_node<S>(
    store: &S,
    reference: &str,
    skill_name: &str,
) -> std::result::Result<(), LlmError>
where
    S: NodeStore,
{
    let node = store
        .get_node(reference)
        .map_err(|source| LlmError::Memory {
            source: Box::new(source),
        })?;

    if let Kind::Anchor(anchor) = &node.kind
        && anchor
            .as_skill_invocation()
            .is_some_and(|invocation| invocation.skill_name == skill_name)
    {
        return Ok(());
    }

    Err(LlmError::InvalidAnchor {
        anchor_id: reference.to_owned(),
    })
}

fn append_skill_session_anchor<S>(
    store: &S,
    request: &SkillInvocationRequest,
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
                    active_skill: Some(SkillRuntimeContext {
                        name: request.skill_name.clone(),
                        handoff: request.handoff.clone(),
                    }),
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

fn skill_execution_prompt(request: &SkillInvocationRequest) -> String {
    let script_instructions = skill_script_instructions(request);
    let context_instruction = if let Some(handoff) = &request.handoff {
        format!(
            "The parent conversation history is intentionally hidden. Use this explicit handoff content as the bounded task:\n{handoff}"
        )
    } else {
        "The parent conversation history is available. Use it only when it is directly relevant to the skill task.".to_owned()
    };
    formatdoc!(
        "
        You are executing the skill `{}` on an isolated child branch forked from `{}`.
        Follow the skill instructions below and do all exploration on this child branch only.
        When finished, return final text that summarizes the process and outcome for the caller.
        Skills are usually single-task. If the caller's request required handling multiple distinct tasks, include a concise multi-task summary in the final text that lists each task, what was done, and its outcome.

        Skill description:
        {}

        Skill source:
        {}

        Skill session role:
        {}

        Context inheritance:
        {}

        Handoff behavior:
        {}

        Skill persistent paths:
        - Use ${} as the skill-specific persistent data directory. Store files here when they should survive future runs of this skill.
        - HOME is not the skill persistent directory. Do not rely on HOME for skill-specific persistent state.
        - COCO_SKILL_DIR is a temporary materialized skill source directory. Do not write persistent state there.

        {}

        Skill instructions:
        {}
        ",
        request.skill_name,
        request.base_branch,
        request.skill_description,
        request.skill_path,
        request.session_role.as_str(),
        context_instruction,
        skill_handoff_completion_instruction(request),
        COCO_SKILL_PERSIST_DIR_ENV,
        script_instructions,
        request.skill_body,
    )
}

fn skill_handoff_completion_instruction(request: &SkillInvocationRequest) -> &'static str {
    if request.handoff.is_some() {
        "Return the final result for the caller as a normal tool result. The parent model will inspect it before it continues."
    } else {
        "Return a normal tool result for the parent model to inspect before it continues."
    }
}

fn skill_persistent_directory(request: &SkillInvocationRequest) -> PathBuf {
    skill_persistence_root(&request.workspace_root)
        .join(request.session_role.as_str())
        .join(encode_path_segment(&request.skill_name))
        .join("data")
}

fn skill_persistence_root(workspace_root: &Path) -> PathBuf {
    if let Some(root) = absolute_env_path(COCO_SKILL_PERSIST_ROOT_ENV) {
        return root;
    }
    workspace_root.join(".coco-workspace").join("skills")
}

fn absolute_env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .filter(|path| path != Path::new("/"))
}

fn encode_path_segment(value: &str) -> String {
    let is_dot_only = value.bytes().all(|byte| byte == b'.');
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte == b'.' && is_dot_only {
            encoded.push_str("%2E");
        } else if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_') {
            encoded.push(char::from(byte));
        } else {
            write!(&mut encoded, "%{byte:02X}").expect("writing to String should not fail");
        }
    }

    if encoded.is_empty() {
        "skill".to_owned()
    } else {
        encoded
    }
}

fn skill_script_instructions(request: &SkillInvocationRequest) -> String {
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

fn synthetic_skill_path(role: SessionRole, name: &str, version: u64) -> PathBuf {
    PathBuf::from(format!(
        "store://skills/{}/{}@{}",
        role.as_str(),
        name,
        version
    ))
}

async fn collect_store_skills<S>(
    store: &S,
    session_role: SessionRole,
) -> std::result::Result<Vec<SkillEntry>, SkillError>
where
    S: SkillStore + Sync,
{
    let mut seen_names = HashSet::new();
    let mut skills = Vec::new();
    for role in accessible_skill_roles(session_role) {
        let records = store.list_skills(role).await.context(LoadSkillsSnafu)?;
        for skill in skill_entries_from_store_records(role, &records) {
            if seen_names.insert(skill.name.clone()) {
                skills.push(skill);
            }
        }
    }
    Ok(skills)
}

fn accessible_skill_roles(session_role: SessionRole) -> Vec<SessionRole> {
    match session_role {
        SessionRole::Orchestrator => vec![SessionRole::Orchestrator, SessionRole::Runner],
        SessionRole::Runner => vec![SessionRole::Runner],
    }
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

async fn collect_skills<S>(
    _workspace_root: &Path,
    store: &S,
    session_role: SessionRole,
) -> std::result::Result<Vec<SkillEntry>, SkillError>
where
    S: SkillStore + Sync,
{
    let mut skills = collect_store_skills(store, session_role).await?;
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

async fn search_skills(
    workspace_root: &Path,
    store: &(impl SkillStore + Sync),
    session_role: SessionRole,
    query: &str,
    limit: usize,
) -> std::result::Result<SkillSearchResult, SkillError> {
    let mut matches = collect_skills(workspace_root, store, session_role)
        .await?
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

async fn resolve_skill(
    workspace_root: &Path,
    store: &(impl SkillStore + Sync),
    session_role: SessionRole,
    name: &str,
) -> std::result::Result<SkillEntry, SkillError> {
    let matches = collect_skills(workspace_root, store, session_role)
        .await?
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
    use coco_llm::coco_mem::SkillVersionSpec;

    use super::*;

    fn test_store() -> coco_llm::coco_mem::SqliteStore {
        coco_llm::coco_mem::SqliteStore::open_temporary()
            .expect("temporary SQLite store should open")
    }

    #[test]
    fn skill_execution_prompt_includes_skill_context() {
        let prompt = skill_execution_prompt(&SkillInvocationRequest {
            workspace_root: PathBuf::from("/workspace"),
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
            handoff: Some("Find matching skills.".to_owned()),
        });

        assert!(prompt.contains("skill `find-skills`"));
        assert!(prompt.contains("forked from `main`"));
        assert!(prompt.contains("summarizes the process and outcome"));
        assert!(prompt.contains("Skills are usually single-task."));
        assert!(prompt.contains("include a concise multi-task summary"));
        assert!(prompt.contains("Skill description:\nFind relevant skills."));
        assert!(prompt.contains("Skill source:\n/tmp/find-skills/SKILL.md"));
        assert!(prompt.contains("Skill session role:\nrunner"));
        assert!(prompt.contains("Use $COCO_SKILL_PERSIST_DIR"));
        assert!(prompt.contains("HOME is not the skill persistent directory"));
        assert!(
            prompt.contains("COCO_SKILL_DIR is a temporary materialized skill source directory")
        );
        assert!(prompt.contains("uv run --script \"$COCO_SKILL_DIR/scripts/inspect.py\""));
        assert!(prompt.contains("Skill instructions:\n# Find Skills"));
        assert!(!prompt.contains("Additional task from caller:"));
    }

    #[test]
    fn encode_path_segment_escapes_dot_only_names() {
        assert_eq!(encode_path_segment("."), "%2E");
        assert_eq!(encode_path_segment(".."), "%2E%2E");
        assert_eq!(encode_path_segment("..."), "%2E%2E%2E");
        assert_eq!(encode_path_segment(".skill"), ".skill");
        assert_eq!(encode_path_segment("skill.name"), "skill.name");
    }

    #[test]
    fn skill_persistent_directory_keeps_dot_only_names_isolated() {
        let directory = skill_persistent_directory(&SkillInvocationRequest {
            workspace_root: PathBuf::from("/workspace"),
            base_branch: "main".to_owned(),
            parent_tool_use_id: "tool-use-node".to_owned(),
            skill_name: "..".to_owned(),
            skill_description: "Dot-only skill.".to_owned(),
            skill_path: "store://skills/runner/..@1".to_owned(),
            skill_body: "# Dot Skill".to_owned(),
            scripts: Vec::new(),
            session_role: SessionRole::Runner,
            enable_coco_shim: false,
            handoff: None,
        });

        assert!(directory.ends_with(Path::new("runner").join("%2E%2E").join("data")));
        assert!(
            !directory
                .components()
                .any(|component| matches!(component, Component::ParentDir))
        );
    }

    #[tokio::test]
    async fn search_skills_finds_store_matches_by_name_and_description() {
        let store = test_store();
        store
            .add_skill(
                SessionRole::Orchestrator,
                "openai-docs",
                SkillVersionSpec {
                    description: "Find OpenAI API documentation.".to_owned(),
                    body: "# OpenAI Docs".to_owned(),
                    scripts: Vec::new(),
                    enable_coco_shim: false,
                },
            )
            .await
            .unwrap();

        let result = search_skills(
            Path::new("."),
            &store,
            SessionRole::Orchestrator,
            "openai docs",
            1,
        )
        .await
        .unwrap();

        assert_eq!(result.skills.len(), 1);
        assert_eq!(result.skills[0].name, "openai-docs");
    }

    #[tokio::test]
    async fn search_skills_ignores_external_skill_roots() {
        let root = tempfile::tempdir().unwrap();
        let skill_dir = root.path().join("external-skill");
        fs::create_dir_all(&skill_dir).unwrap();
        fs::write(
            skill_dir.join("SKILL.md"),
            r#"---
name: "external-skill"
description: "External skill."
---

# External Skill
"#,
        )
        .unwrap();
        let store = test_store();

        let result = search_skills(
            root.path(),
            &store,
            SessionRole::Orchestrator,
            "external-skill",
            10,
        )
        .await
        .unwrap();

        assert!(result.skills.is_empty());
    }

    #[tokio::test]
    async fn collect_store_skills_respects_role_hierarchy() {
        let store = test_store();

        let runner = collect_store_skills(&store, SessionRole::Runner)
            .await
            .unwrap();
        let orchestrator = collect_store_skills(&store, SessionRole::Orchestrator)
            .await
            .unwrap();

        assert_eq!(
            runner
                .iter()
                .map(|skill| skill.name.as_str())
                .collect::<Vec<_>>(),
            vec!["coco-runner", "telegram"]
        );
        assert_eq!(
            orchestrator
                .iter()
                .map(|skill| skill.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "coco-orchestrator",
                "compact",
                "cronjob",
                "new-skill",
                "recovery",
                "coco-runner",
                "telegram"
            ]
        );
    }

    #[tokio::test]
    async fn collect_store_skills_prefers_current_role_on_name_conflict() {
        let store = test_store();
        for role in [SessionRole::Orchestrator, SessionRole::Runner] {
            store
                .add_skill(
                    role,
                    "shared-skill",
                    SkillVersionSpec {
                        description: format!("{} copy", role.as_str()),
                        body: "# Shared Skill".to_owned(),
                        scripts: Vec::new(),
                        enable_coco_shim: false,
                    },
                )
                .await
                .unwrap();
        }

        let skill = resolve_skill(
            Path::new("."),
            &store,
            SessionRole::Orchestrator,
            "shared-skill",
        )
        .await
        .unwrap();

        assert_eq!(skill.session_role, SessionRole::Orchestrator);
        assert_eq!(skill.description, "orchestrator copy");
    }
}
