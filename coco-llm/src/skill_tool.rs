use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use coco_mem::Tool;
use serde::Serialize;
use serde_json::Value;
use snafu::prelude::*;

use crate::{BashToolContext, SkillToolRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SkillToolKind {
    Search,
    Use,
}

#[derive(Debug, Clone)]
pub struct SkillToolRuntime {
    definition: Tool,
    workspace_root: PathBuf,
    context: BashToolContext,
    kind: SkillToolKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkillEntry {
    name: String,
    description: String,
    path: PathBuf,
    body: String,
    search_blob: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct SearchSkillResult {
    skills: Vec<SkillMatch>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct SkillMatch {
    name: String,
    description: String,
    path: String,
}

#[derive(Debug, Snafu)]
pub enum SkillToolError {
    #[snafu(display("skill tool expects a JSON object input"))]
    InvalidInputType,

    #[snafu(display("skill tool arguments must be valid JSON: {source}"))]
    ParseArgs { source: serde_json::Error },

    #[snafu(display("skill tool `limit` must be a positive integer"))]
    InvalidLimit,

    #[snafu(display("use_skill requires a string field `name`"))]
    MissingSkillName,

    #[snafu(display("skill tool executor is unavailable"))]
    ExecutorUnavailable,

    #[snafu(display("skill execution failed: {message}"))]
    ExecuteSkill { message: String },

    #[snafu(display("failed to resolve configured skill root {path:?}: {source}"))]
    ResolveConfiguredRoot {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("configured skill root {path:?} must point to an existing directory"))]
    InvalidConfiguredRoot { path: PathBuf },

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

    #[snafu(display("failed to read skill file {path:?}: {source}"))]
    ReadSkillFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("failed to serialize skill tool output: {source}"))]
    SerializeOutput { source: serde_json::Error },

    #[snafu(display("no installed skill named {name:?}"))]
    SkillNotFound { name: String },

    #[snafu(display("multiple installed skills named {name:?}: {paths:?}"))]
    AmbiguousSkillName { name: String, paths: Vec<String> },
}

pub fn search_runtime_tool(
    definition: Tool,
    workspace_root: PathBuf,
) -> Box<dyn rig::tool::ToolDyn> {
    Box::new(SkillToolRuntime {
        definition,
        workspace_root,
        context: BashToolContext {
            session_branch: String::new(),
            store_path: None,
            cli_bridge: None,
            skill_executor: None,
            skill_handoff_recorder: crate::SkillToolHandoffRecorder::default(),
        },
        kind: SkillToolKind::Search,
    })
}

pub fn run_runtime_tool(
    definition: Tool,
    workspace_root: PathBuf,
    context: BashToolContext,
) -> Box<dyn rig::tool::ToolDyn> {
    Box::new(SkillToolRuntime {
        definition,
        workspace_root,
        context,
        kind: SkillToolKind::Use,
    })
}

fn configured_skill_roots() -> std::result::Result<Option<Vec<PathBuf>>, SkillToolError> {
    let Some(raw) = std::env::var_os("COCO_SKILLS_DIRS") else {
        return Ok(None);
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

    Ok(Some(roots))
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

fn resolve_skill_roots(workspace_root: &Path) -> std::result::Result<Vec<PathBuf>, SkillToolError> {
    Ok(match configured_skill_roots()? {
        Some(roots) => roots,
        None => default_skill_roots(workspace_root),
    })
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

fn load_skill(path: &Path) -> std::result::Result<SkillEntry, SkillToolError> {
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
) -> std::result::Result<(), SkillToolError> {
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

fn collect_skills(workspace_root: &Path) -> std::result::Result<Vec<SkillEntry>, SkillToolError> {
    let roots = resolve_skill_roots(workspace_root)?;
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
    args: &Value,
    workspace_root: &Path,
) -> std::result::Result<String, SkillToolError> {
    let object = args.as_object().context(InvalidInputTypeSnafu)?;
    let query = object
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_owned();
    let limit = match object.get("limit").and_then(Value::as_u64) {
        Some(0) => return InvalidLimitSnafu.fail(),
        Some(limit) => usize::try_from(limit).map_err(|_| InvalidLimitSnafu.build())?,
        None => 10,
    };

    let mut matches = collect_skills(workspace_root)?
        .into_iter()
        .filter_map(|skill| {
            let score = score_skill(&skill, &query);
            (score > 0).then_some((score, skill))
        })
        .collect::<Vec<_>>();

    matches.sort_by(|(left_score, left_skill), (right_score, right_skill)| {
        right_score
            .cmp(left_score)
            .then(left_skill.name.cmp(&right_skill.name))
            .then(left_skill.path.cmp(&right_skill.path))
    });

    let result = SearchSkillResult {
        skills: matches
            .into_iter()
            .take(limit)
            .map(|(_, skill)| SkillMatch {
                name: skill.name,
                description: skill.description,
                path: skill.path.display().to_string(),
            })
            .collect(),
    };

    serde_json::to_string_pretty(&result).context(SerializeOutputSnafu)
}

fn resolve_use_skill_request(
    args: &Value,
    workspace_root: &Path,
    context: &BashToolContext,
) -> std::result::Result<SkillToolRequest, SkillToolError> {
    let object = args.as_object().context(InvalidInputTypeSnafu)?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .context(MissingSkillNameSnafu)?;
    let task = object
        .get("task")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|task| !task.is_empty())
        .map(str::to_owned);

    let matches = collect_skills(workspace_root)?
        .into_iter()
        .filter(|skill| skill.name == name)
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [] => SkillNotFoundSnafu {
            name: name.to_owned(),
        }
        .fail(),
        [skill] => Ok(SkillToolRequest {
            base_branch: context.session_branch.clone(),
            skill_name: skill.name.clone(),
            skill_description: skill.description.clone(),
            skill_path: skill.path.display().to_string(),
            skill_body: skill.body.clone(),
            task,
        }),
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

impl rig::tool::ToolDyn for SkillToolRuntime {
    fn name(&self) -> String {
        self.definition.name.clone()
    }

    fn definition(
        &self,
        _prompt: String,
    ) -> rig::wasm_compat::WasmBoxedFuture<'_, rig::completion::ToolDefinition> {
        let definition = rig::completion::ToolDefinition {
            name: self.definition.name.clone(),
            description: self.definition.description.clone(),
            parameters: self.definition.input_schema.clone(),
        };
        Box::pin(async move { definition })
    }

    fn call(
        &self,
        args: String,
    ) -> rig::wasm_compat::WasmBoxedFuture<'_, std::result::Result<String, rig::tool::ToolError>>
    {
        use rig::tool::ToolError;

        let workspace_root = self.workspace_root.clone();
        let context = self.context.clone();
        let kind = self.kind;

        Box::pin(async move {
            let result = async {
                let args: Value = serde_json::from_str(&args).context(ParseArgsSnafu)?;
                match kind {
                    SkillToolKind::Search => search_skills(&args, &workspace_root),
                    SkillToolKind::Use => {
                        let request = resolve_use_skill_request(&args, &workspace_root, &context)?;
                        let executor = context
                            .skill_executor
                            .clone()
                            .context(ExecutorUnavailableSnafu)?;
                        let result =
                            executor
                                .execute_skill_tool(request)
                                .await
                                .map_err(|source| SkillToolError::ExecuteSkill {
                                    message: source.to_string(),
                                })?;
                        context.skill_handoff_recorder.record(result.handoff);
                        serde_json::to_string_pretty(&result.result).context(SerializeOutputSnafu)
                    }
                }
            }
            .await;

            match result {
                Ok(output) => Ok(output),
                Err(source) => Err(ToolError::ToolCallError(Box::new(source))),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::future::Future;
    use std::sync::{Arc, OnceLock};

    use async_trait::async_trait;
    use tokio::sync::Mutex;

    use super::*;
    use crate::SkillToolRunResult;

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

    fn search_tool_definition() -> Tool {
        crate::builtin_tool_definition("search_skill").expect("builtin tool should exist")
    }

    fn run_tool_definition() -> Tool {
        crate::builtin_tool_definition("use_skill").expect("builtin tool should exist")
    }

    fn run_context(executor: Arc<dyn crate::SkillToolExecutor>) -> BashToolContext {
        BashToolContext {
            session_branch: "main".to_owned(),
            store_path: None,
            cli_bridge: None,
            skill_executor: Some(executor),
            skill_handoff_recorder: crate::SkillToolHandoffRecorder::default(),
        }
    }

    #[derive(Debug)]
    struct FakeExecutor {
        requests: Arc<Mutex<Vec<SkillToolRequest>>>,
    }

    #[async_trait]
    impl crate::SkillToolExecutor for FakeExecutor {
        async fn execute_skill_tool(
            &self,
            request: SkillToolRequest,
        ) -> std::result::Result<crate::SkillToolExecutionResult, crate::SkillToolExecutorError>
        {
            self.requests.lock().await.push(request);
            Ok(crate::SkillToolExecutionResult {
                result: SkillToolRunResult {
                    text: "Executed skill result".to_owned(),
                },
                handoff: crate::SkillToolHandoff {
                    skill_name: "find-skills".to_owned(),
                    merge_parent: "node-1".to_owned(),
                    output: "Executed skill result".to_owned(),
                },
            })
        }
    }

    fn write_skill(root: &Path, relative_dir: &str, body: &str) {
        let dir = root.join(relative_dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("SKILL.md"), body).unwrap();
    }

    #[tokio::test]
    async fn search_skill_runtime_finds_matches_by_name_and_description() {
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
        let runtime = search_runtime_tool(search_tool_definition(), root.path().to_path_buf());
        let path_env = std::env::join_paths([root.path()]).unwrap();

        let output = with_env_async(
            &[("COCO_SKILLS_DIRS", Some(path_env.as_os_str()))],
            || async {
                runtime
                    .call(r#"{"query":"openai docs","limit":1}"#.to_owned())
                    .await
            },
        )
        .await
        .unwrap();
        let value: Value = serde_json::from_str(&output).unwrap();

        assert_eq!(value["skills"].as_array().unwrap().len(), 1);
        assert_eq!(value["skills"][0]["name"], "openai-docs");
    }

    #[tokio::test]
    async fn use_skill_runtime_executes_on_child_branch_via_executor() {
        let root = tempfile::tempdir().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(FakeExecutor {
            requests: requests.clone(),
        });
        write_skill(
            root.path(),
            "find-skills",
            r#"---
name: "find-skills"
description: "Find skills."
---

# Find Skills

Use this skill to discover other skills.
"#,
        );
        let context = run_context(executor);
        let handoff_recorder = context.skill_handoff_recorder.clone();
        let runtime = run_runtime_tool(run_tool_definition(), root.path().to_path_buf(), context);
        let path_env = std::env::join_paths([root.path()]).unwrap();

        let output = with_env_async(
            &[("COCO_SKILLS_DIRS", Some(path_env.as_os_str()))],
            || async {
                runtime
                    .call(r#"{"name":"find-skills","task":"Search the ecosystem"}"#.to_owned())
                    .await
            },
        )
        .await
        .unwrap();
        let value: Value = serde_json::from_str(&output).unwrap();
        let handoff = handoff_recorder
            .take_next()
            .expect("expected skill handoff");
        let requests = requests.lock().await;

        assert_eq!(value.as_object().expect("expected JSON object").len(), 1);
        assert_eq!(value["text"], "Executed skill result");
        assert_eq!(handoff.skill_name, "find-skills");
        assert_eq!(handoff.merge_parent, "node-1");
        assert_eq!(handoff.output, "Executed skill result");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].base_branch, "main");
        assert_eq!(requests[0].skill_name, "find-skills");
        assert_eq!(requests[0].task.as_deref(), Some("Search the ecosystem"));
        assert!(requests[0].skill_body.contains("Use this skill"));
    }

    #[tokio::test]
    async fn use_skill_runtime_rejects_ambiguous_names() {
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
        let runtime = run_runtime_tool(
            run_tool_definition(),
            first_root.path().to_path_buf(),
            run_context(Arc::new(FakeExecutor {
                requests: Arc::new(Mutex::new(Vec::new())),
            })),
        );
        let path_env = std::env::join_paths([first_root.path(), second_root.path()]).unwrap();

        let error = with_env_async(
            &[("COCO_SKILLS_DIRS", Some(path_env.as_os_str()))],
            || async { runtime.call(r#"{"name":"shared-skill"}"#.to_owned()).await },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("multiple installed skills"));
    }

    #[tokio::test]
    async fn configured_skill_roots_validate_existing_directories() {
        let missing = OsString::from("/tmp/coco-skill-tool-missing");

        let error = with_env_async(
            &[("COCO_SKILLS_DIRS", Some(missing.as_os_str()))],
            || async { configured_skill_roots() },
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            SkillToolError::ResolveConfiguredRoot { .. }
                | SkillToolError::InvalidConfiguredRoot { .. }
        ));
    }
}
