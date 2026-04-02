use std::collections::VecDeque;
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use indoc::formatdoc;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::{
    CompletionBackend, Error, LlmService, MemorySnafu, PromptRequest, Result, SessionState, Store,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillToolRequest {
    pub base_branch: String,
    pub skill_name: String,
    pub skill_description: String,
    pub skill_path: String,
    pub skill_body: String,
    pub task: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillToolRunResult {
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillToolHandoff {
    pub skill_name: String,
    pub merge_parent: String,
    pub output: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillToolExecutionResult {
    pub result: SkillToolRunResult,
    pub handoff: SkillToolHandoff,
}

#[derive(Clone, Default)]
pub struct SkillToolHandoffRecorder {
    inner: Arc<StdMutex<VecDeque<SkillToolHandoff>>>,
}

impl std::fmt::Debug for SkillToolHandoffRecorder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SkillToolHandoffRecorder(..)")
    }
}

impl SkillToolHandoffRecorder {
    pub fn record(&self, handoff: SkillToolHandoff) {
        self.inner
            .lock()
            .expect("skill handoff recorder should not be poisoned")
            .push_back(handoff);
    }

    pub fn take_next(&self) -> Option<SkillToolHandoff> {
        self.inner
            .lock()
            .expect("skill handoff recorder should not be poisoned")
            .pop_front()
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum SkillToolExecutorError {
    #[snafu(display("skill tool executor is unavailable"))]
    ExecutorUnavailable,

    #[snafu(display("{source}"))]
    WorkflowFailed { source: Error },
}

#[async_trait]
pub trait SkillToolExecutor: Send + Sync {
    async fn execute_skill_tool(
        &self,
        request: SkillToolRequest,
    ) -> std::result::Result<SkillToolExecutionResult, SkillToolExecutorError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchHandoff {
    pub tool_id: String,
    pub skill_name: String,
    pub merge_parent: String,
    pub output: String,
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

impl<B, S> LlmService<B, S>
where
    B: CompletionBackend,
    S: Store,
{
    pub async fn use_skill_workflow(
        &self,
        request: SkillToolRequest,
    ) -> Result<SkillToolExecutionResult> {
        let base_head_id = self
            .store
            .get_branch_head(&request.base_branch)
            .context(MemorySnafu)?;
        let child_branch = temporary_skill_branch_name(&request.base_branch, &request.skill_name);

        self.store
            .fork(&child_branch, &base_head_id)
            .context(MemorySnafu)?;
        self.store
            .set_session_state(
                &child_branch,
                None,
                SessionState::Attached {
                    target_branch: request.base_branch.clone(),
                    base_head_id,
                },
            )
            .context(MemorySnafu)?;

        let prompt_result = self
            .prompt(PromptRequest {
                branch: child_branch.clone(),
                prompt: skill_execution_prompt(&request),
                merge_parents: vec![],
            })
            .await;

        match prompt_result {
            Ok(result) => {
                let run_result = SkillToolExecutionResult {
                    result: SkillToolRunResult {
                        text: result.text.clone(),
                    },
                    handoff: SkillToolHandoff {
                        skill_name: request.skill_name,
                        merge_parent: result.branch_head,
                        output: result.text,
                    },
                };
                self.store
                    .delete_branch(&child_branch)
                    .context(crate::UseSkillCleanupSnafu {
                        branch: child_branch,
                    })?;
                Ok(run_result)
            }
            Err(workflow) => match self.store.delete_branch(&child_branch) {
                Ok(()) => Err(workflow),
                Err(cleanup) => Err(crate::Error::UseSkillWorkflowFailedCleanup {
                    branch: child_branch,
                    workflow: Box::new(workflow),
                    cleanup,
                }),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SkillToolRequest, skill_execution_prompt};

    #[test]
    fn skill_execution_prompt_includes_skill_context_and_optional_task() {
        let prompt = skill_execution_prompt(&SkillToolRequest {
            base_branch: "main".to_owned(),
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
            skill_name: "find-skills".to_owned(),
            skill_description: "Find relevant skills.".to_owned(),
            skill_path: "/tmp/find-skills/SKILL.md".to_owned(),
            skill_body: "# Find Skills".to_owned(),
            task: Some("   ".to_owned()),
        });

        assert!(!prompt.contains("Additional task from caller:"));
    }
}
