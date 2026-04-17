use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Weak;

use async_trait::async_trait;
use coco_llm::coco_mem::{Anchor, Kind, NewNode, Role, SessionAnchor, SessionRole, Store, ToolUse};
use coco_llm::{
    CompletionBackend, CompletionInput, CompletionOrigin, CompletionOverrides, CompletionRequest,
    Error as LlmError, ExecutorError, LlmService, SearchSkillToolRequest, SkillToolExecutionResult,
    SkillToolExecutor, SkillToolHandoff, SkillToolRequest, SkillToolRunResult, UseSkillToolRequest,
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

    #[snafu(display("no installed skill named {name:?}"))]
    SkillNotFound { name: String },

    #[snafu(display("multiple installed skills named {name:?}: {paths:?}"))]
    AmbiguousSkillName { name: String, paths: Vec<String> },
}

impl From<SkillError> for ExecutorError {
    fn from(error: SkillError) -> Self {
        Self::OperationFailed {
            message: error.to_string(),
            handoff: None,
            source: Some(Box::new(error)),
        }
    }
}

impl From<EngineError> for ExecutorError {
    fn from(error: EngineError) -> Self {
        Self::OperationFailed {
            message: error.to_string(),
            handoff: None,
            source: Some(Box::new(error)),
        }
    }
}

pub struct CoreSkillToolExecutor<B, S>
where
    B: CompletionBackend,
    S: Store,
{
    llm: Weak<LlmService<B, S>>,
}

impl<B, S> fmt::Debug for CoreSkillToolExecutor<B, S>
where
    B: CompletionBackend,
    S: Store,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CoreSkillToolExecutor(..)")
    }
}

impl<B, S> CoreSkillToolExecutor<B, S>
where
    B: CompletionBackend + 'static,
    S: Store + 'static,
{
    pub fn new(llm: Weak<LlmService<B, S>>) -> Self {
        Self { llm }
    }

    fn upgrade_engine(&self) -> std::result::Result<ConversationEngine<B, S>, ExecutorError> {
        let llm = self.llm.upgrade().ok_or(ExecutorError::Unavailable)?;
        Ok(ConversationEngine::new(llm))
    }
}

#[async_trait]
impl<B, S> SkillToolExecutor for CoreSkillToolExecutor<B, S>
where
    B: CompletionBackend + 'static,
    S: Store + 'static,
{
    async fn search_skill_tool(
        &self,
        request: SearchSkillToolRequest,
    ) -> std::result::Result<String, ExecutorError> {
        let result = self.upgrade_engine()?.search_skills(
            &request.workspace_root,
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
        let skill = resolve_skill(&request.workspace_root, &request.skill_name)?;
        let skill_request = SkillToolRequest {
            base_branch: request.session_branch,
            parent_tool_use_id: request.parent_tool_use_id,
            skill_name: skill.name.clone(),
            skill_description: skill.description,
            skill_path: skill.path.display().to_string(),
            skill_body: skill.body,
            task: request.task,
        };

        engine
            .execute_resolved_skill(skill_request)
            .await
            .map_err(|error| executor_error_from_llm_error(&skill.name, error))
    }
}

fn handoff_from_llm_error(
    skill_name: &str,
    output: &str,
    error: &LlmError,
) -> Option<SkillToolHandoff> {
    match error {
        LlmError::Backend { context, .. } => Some(SkillToolHandoff {
            skill_name: skill_name.to_owned(),
            merge_parent: context.error_node_id.clone(),
            output: output.to_owned(),
        }),
        LlmError::UseSkillWorkflowFailedCleanup { workflow, .. } => {
            handoff_from_llm_error(skill_name, output, workflow)
        }
        _ => None,
    }
}

fn executor_error_from_llm_error(skill_name: &str, error: LlmError) -> ExecutorError {
    let message = error.to_string();
    ExecutorError::OperationFailed {
        handoff: handoff_from_llm_error(skill_name, &message, &error),
        message,
        source: Some(Box::new(error)),
    }
}

impl<B, S> ConversationEngine<B, S>
where
    B: CompletionBackend + 'static,
    S: Store,
{
    pub fn search_skills(
        &self,
        workspace_root: &Path,
        query: &str,
        limit: usize,
    ) -> std::result::Result<SkillSearchResult, EngineError> {
        let result = search_skills(workspace_root, query, limit)?;
        Ok(result)
    }

    pub async fn execute_skill(
        &self,
        workspace_root: &Path,
        base_branch: &str,
        parent_tool_use_id: &str,
        skill_name: &str,
        task: Option<&str>,
    ) -> std::result::Result<SkillToolExecutionResult, EngineError> {
        let skill = resolve_skill(workspace_root, skill_name)?;
        let request = SkillToolRequest {
            base_branch: base_branch.to_owned(),
            parent_tool_use_id: parent_tool_use_id.to_owned(),
            skill_name: skill.name.clone(),
            skill_description: skill.description.clone(),
            skill_path: skill.path.display().to_string(),
            skill_body: skill.body,
            task: task.map(str::to_owned),
        };
        self.execute_resolved_skill(request)
            .await
            .map_err(EngineError::from)
    }

    async fn execute_resolved_skill(
        &self,
        request: SkillToolRequest,
    ) -> std::result::Result<SkillToolExecutionResult, LlmError> {
        let service = self.service();
        let store = service.store();
        let child_branch = temporary_skill_branch_name(&request.base_branch, &request.skill_name);
        let child_session_anchor_id = store
            .append(NewNode {
                parent: request.parent_tool_use_id.clone(),
                role: Role::System,
                metadata: None,
                kind: Kind::Anchor(Anchor::session(
                    vec![],
                    child_session_anchor(
                        resolve_session_anchor(store, &request.parent_tool_use_id)?,
                        skill_execution_prompt(&request),
                    ),
                )),
            })
            .map_err(|source| LlmError::Memory { source })?;

        store
            .fork(&child_branch, &child_session_anchor_id)
            .map_err(|source| LlmError::Memory { source })?;
        let prompt_result = service
            .run(CompletionRequest {
                branch: child_branch.clone(),
                origin: CompletionOrigin::BranchHead,
                input: CompletionInput::Continue,
                overrides: CompletionOverrides::default(),
            })
            .await;
        let cleanup_result = store.delete_branch(&child_branch);

        match (prompt_result, cleanup_result) {
            (Ok(result), Ok(())) => Ok(SkillToolExecutionResult {
                result: SkillToolRunResult {
                    text: result.text.clone(),
                },
                handoff: SkillToolHandoff {
                    skill_name: request.skill_name,
                    merge_parent: result.branch_head,
                    output: result.text,
                },
            }),
            (Err(workflow), Ok(())) => Err(workflow),
            (Ok(_), Err(cleanup)) => Err(LlmError::UseSkillCleanup {
                branch: child_branch,
                source: cleanup,
            }),
            (Err(workflow), Err(cleanup)) => Err(LlmError::UseSkillWorkflowFailedCleanup {
                branch: child_branch,
                workflow: Box::new(workflow),
                cleanup,
            }),
        }
    }
}

fn resolve_session_anchor<S: Store>(
    store: &S,
    reference: &str,
) -> std::result::Result<SessionAnchor, LlmError> {
    let ancestry = store
        .ancestry(reference)
        .map_err(|source| LlmError::Memory { source })?;
    let head = ancestry.first().ok_or_else(|| LlmError::InvalidAnchor {
        anchor_id: reference.to_owned(),
    })?;

    match &head.kind {
        Kind::ToolUse(ToolUse { name, .. }) if name == "use_skill" => {}
        _ => {
            return Err(LlmError::InvalidAnchor {
                anchor_id: reference.to_owned(),
            });
        }
    }

    ancestry
        .into_iter()
        .find_map(|node| match &node.kind {
            Kind::Anchor(anchor) => anchor.as_session().cloned(),
            _ => None,
        })
        .ok_or_else(|| LlmError::InvalidAnchor {
            anchor_id: reference.to_owned(),
        })
}

fn child_session_anchor(parent: SessionAnchor, prompt: String) -> SessionAnchor {
    SessionAnchor {
        role: SessionRole::Runner,
        provider: parent.provider,
        model: parent.model,
        tools: parent.tools,
        system_prompt: parent.system_prompt,
        prompt,
        temperature: parent.temperature,
        max_tokens: parent.max_tokens,
        additional_params: parent.additional_params,
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
    let mut prompt = formatdoc!(
        "
        You are executing the skill `{}` on an isolated child branch forked from `{}`.
        Follow the skill instructions below and do all exploration on this child branch only.
        When finished, return only the final handoff content that should be carried back to the base branch.

        Skill description:
        {}

        Skill source:
        {}

        Skill instructions:
        {}
        ",
        request.skill_name,
        request.base_branch,
        request.skill_description,
        request.skill_path,
        request.skill_body,
    );
    if let Some(task) = &request.task
        && !task.trim().is_empty()
    {
        prompt.push_str(&formatdoc!(
            "

            Additional task from caller:
            {}
            ",
            task.trim(),
        ));
    }
    prompt
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
    if let Some(frontmatter) = frontmatter {
        for line in frontmatter.lines() {
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            match key.trim() {
                "name" => name = Some(parse_frontmatter_value(value)),
                "description" => description = Some(parse_frontmatter_value(value)),
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
        search_blob,
    })
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

fn collect_skills(workspace_root: &Path) -> std::result::Result<Vec<SkillEntry>, SkillError> {
    let roots = configured_skill_roots(workspace_root)?;
    let mut skills = Vec::new();
    for root in roots {
        collect_skills_from_dir(&root, &mut skills)?;
    }
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
    query: &str,
    limit: usize,
) -> std::result::Result<SkillSearchResult, SkillError> {
    let mut matches = collect_skills(workspace_root)?
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

fn resolve_skill(workspace_root: &Path, name: &str) -> std::result::Result<SkillEntry, SkillError> {
    let matches = collect_skills(workspace_root)?
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
    fn skill_execution_prompt_includes_skill_context_and_optional_task() {
        let prompt = skill_execution_prompt(&SkillToolRequest {
            base_branch: "main".to_owned(),
            parent_tool_use_id: "tool-use-node".to_owned(),
            skill_name: "find-skills".to_owned(),
            skill_description: "Find relevant skills.".to_owned(),
            skill_path: "/tmp/find-skills/SKILL.md".to_owned(),
            skill_body: "# Find Skills".to_owned(),
            task: Some("Search the ecosystem".to_owned()),
        });

        assert!(prompt.contains("skill `find-skills`"));
        assert!(prompt.contains("forked from `main`"));
        assert!(prompt.contains("Skill description:\nFind relevant skills."));
        assert!(prompt.contains("Skill source:\n/tmp/find-skills/SKILL.md"));
        assert!(prompt.contains("Skill instructions:\n# Find Skills"));
        assert!(prompt.contains("Additional task from caller:\nSearch the ecosystem"));
    }

    #[test]
    fn skill_execution_prompt_skips_blank_optional_task() {
        let prompt = skill_execution_prompt(&SkillToolRequest {
            base_branch: "main".to_owned(),
            parent_tool_use_id: "tool-use-node".to_owned(),
            skill_name: "find-skills".to_owned(),
            skill_description: "Find relevant skills.".to_owned(),
            skill_path: "/tmp/find-skills/SKILL.md".to_owned(),
            skill_body: "# Find Skills".to_owned(),
            task: Some("   ".to_owned()),
        });

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

        let result = with_env_async(
            &[("COCO_SKILLS_DIRS", Some(path_env.as_os_str()))],
            || async { search_skills(root.path(), "openai docs", 1) },
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

        let error = with_env_async(
            &[("COCO_SKILLS_DIRS", Some(path_env.as_os_str()))],
            || async { resolve_skill(first_root.path(), "shared-skill") },
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
}
