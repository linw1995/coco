use std::collections::{HashMap, HashSet};

use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::StoreResult as Result;
use crate::error::{
    AmbiguousNodePrefixSnafu, BranchConfigNotFoundSnafu, BranchConfigVersionNotFoundSnafu,
    BranchExistsSnafu, BranchHeadMovedSnafu, BranchNotFoundSnafu, DuplicateMergeParentSnafu,
    InvalidAnchorSnafu, InvalidSchedulerTaskSnafu, InvalidSkillNameSnafu,
    MergeParentMatchesParentSnafu, MissingSessionAnchorSnafu, MultipleShadowParentsSnafu,
    NotFoundSnafu, ParentNotFoundSnafu, PromptJobActiveOnBranchSnafu, PromptJobMovedSnafu,
    PromptJobNotFoundSnafu, ProviderProfileNotFoundSnafu, RefsNotConnectedSnafu,
    SchedulerTaskAlreadyExistsSnafu, SchedulerTaskNotFoundSnafu, SchedulerTaskUpdateEmptySnafu,
    SessionStateMovedSnafu, SkillAlreadyExistsSnafu, SkillNotFoundSnafu, SkillUpdateEmptySnafu,
    SkillVersionNotFoundSnafu,
};
use crate::{
    Anchor, AnchorPayload, BranchConfig, BranchConfigRecord, Job, JobStatus, Kind, NewNode,
    NewSchedulerTask, Node, PauseReason, ProviderProfile, Role, SchedulerTask, SchedulerTaskPatch,
    SessionAnchor, SessionAnchorPatch, SessionRole, SessionState, SkillGroups, SkillRecord,
    SkillUpdatePatch, SkillVersionSpec, default_skill_groups,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreState {
    pub nodes: HashMap<String, Node>,
    pub children: HashMap<String, HashSet<String>>,
    pub root: String,
    pub branches: HashMap<String, String>,
    pub sessions: HashMap<String, SessionState>,
    pub branch_configs: HashMap<String, BranchConfigRecord>,
    pub provider_profiles: HashMap<String, ProviderProfile>,
    pub jobs: HashMap<String, Job>,
    pub scheduler_tasks: HashMap<String, SchedulerTask>,
    pub skill_groups: SkillGroups,
}

#[derive(Debug, Clone)]
pub struct RebasePlan {
    pub branch: String,
    pub expected_old_head: String,
    pub new_head: String,
    pub nodes: Vec<Node>,
}

#[derive(Debug, Clone)]
pub struct ForkPlan {
    pub head_id: String,
}

impl Default for StoreState {
    fn default() -> Self {
        Self::new()
    }
}

impl StoreState {
    pub fn new() -> Self {
        let root = Node::new(
            String::new(),
            Role::System,
            None,
            Kind::Text("The Big Bang".to_owned()),
            "1970-01-01T00:00:00Z"
                .parse()
                .expect("root timestamp should parse"),
        );
        let root_id = root.id.clone();

        let mut nodes = HashMap::new();
        nodes.insert(root_id.clone(), root);

        Self {
            nodes,
            children: HashMap::new(),
            root: root_id,
            branches: HashMap::new(),
            sessions: HashMap::new(),
            branch_configs: HashMap::new(),
            provider_profiles: HashMap::new(),
            jobs: HashMap::new(),
            scheduler_tasks: HashMap::new(),
            skill_groups: default_skill_groups(),
        }
    }

    pub fn from_root(root: Node) -> Self {
        let root_id = root.id.clone();
        let mut nodes = HashMap::new();
        nodes.insert(root_id.clone(), root);

        Self {
            nodes,
            children: HashMap::new(),
            root: root_id,
            branches: HashMap::new(),
            sessions: HashMap::new(),
            branch_configs: HashMap::new(),
            provider_profiles: HashMap::new(),
            jobs: HashMap::new(),
            scheduler_tasks: HashMap::new(),
            skill_groups: default_skill_groups(),
        }
    }

    pub fn root_id(&self) -> &str {
        &self.root
    }

    pub fn root_node(&self) -> &Node {
        self.nodes
            .get(&self.root)
            .expect("root node should always exist")
    }

    pub fn plan_append_node(&self, node: NewNode) -> Result<Node> {
        let node = Node::new(
            node.parent,
            node.role,
            node.metadata,
            node.kind,
            Timestamp::now(),
        );
        self.validate_new_node(&node)?;
        Ok(node)
    }

    pub fn insert_existing_node(&mut self, node: Node) -> Result<String> {
        self.validate_new_node(&node)?;
        self.insert_existing_node_unchecked(node)
    }

    pub fn plan_fork(&self, name: &str, from_ref: &str) -> Result<ForkPlan> {
        ensure!(
            !self.branches.contains_key(name),
            BranchExistsSnafu {
                name: name.to_owned(),
            }
        );
        Ok(ForkPlan {
            head_id: self.resolve_ref_id(from_ref)?.to_owned(),
        })
    }

    pub fn apply_fork(&mut self, name: String, head_id: String) -> Result<()> {
        ensure!(
            !self.branches.contains_key(&name),
            BranchExistsSnafu { name: name.clone() }
        );
        ensure!(
            self.nodes.contains_key(&head_id),
            NotFoundSnafu {
                id: head_id.clone(),
            }
        );
        self.branches.insert(name.clone(), head_id);
        self.sessions.insert(name, SessionState::Active);
        Ok(())
    }

    pub fn apply_set_branch_head(
        &mut self,
        name: String,
        expected_old_head: &str,
        new_head: String,
    ) -> Result<()> {
        let actual = self.get_branch_head(&name)?.to_owned();
        ensure!(
            actual == expected_old_head,
            BranchHeadMovedSnafu {
                name: name.clone(),
                expected: expected_old_head.to_owned(),
                actual,
            }
        );
        ensure!(
            self.nodes.contains_key(&new_head),
            NotFoundSnafu {
                id: new_head.clone(),
            }
        );
        self.branches.insert(name, new_head);
        Ok(())
    }

    pub fn get_branch_head(&self, name: &str) -> Result<&str> {
        self.branches
            .get(name)
            .map(String::as_str)
            .context(BranchNotFoundSnafu {
                name: name.to_owned(),
            })
    }

    pub fn delete_branch(&mut self, name: &str) -> Result<()> {
        self.branches.remove(name).context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })?;
        self.sessions.remove(name).context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })?;
        Ok(())
    }

    pub fn ancestry(&self, head_ref: &str) -> Result<Vec<&Node>> {
        let mut node = self.resolve_ref(head_ref)?;
        let mut ancestry = vec![];

        loop {
            ancestry.push(node);
            if node.is_root() {
                break;
            }

            node = self.nodes.get(&node.parent).context(ParentNotFoundSnafu {
                id: node.parent.clone(),
            })?;
        }

        Ok(ancestry)
    }

    pub fn log(&self, base_ref: &str, head_ref: &str) -> Result<Vec<&Node>> {
        let base_id = self.resolve_ref_id(base_ref)?;
        let mut node = self.resolve_ref(head_ref)?;

        let mut ans = vec![];
        loop {
            ans.push(node);
            if node.id == base_id {
                break;
            }

            let parent = &node.parent;
            ensure!(
                !parent.is_empty(),
                RefsNotConnectedSnafu {
                    base_ref: base_ref.to_owned(),
                    head_ref: head_ref.to_owned(),
                }
            );

            node = self.nodes.get(parent).context(ParentNotFoundSnafu {
                id: parent.to_owned(),
            })?;
        }

        Ok(ans)
    }

    pub fn get_node(&self, id: &str) -> Result<Node> {
        self.resolve_node_ref(id).cloned()
    }

    pub fn list_children(&self, node_id: &str) -> Result<Vec<Node>> {
        self.nodes.get(node_id).context(NotFoundSnafu {
            id: node_id.to_owned(),
        })?;

        let mut nodes = self
            .children
            .get(node_id)
            .into_iter()
            .flat_map(|children| children.iter())
            .map(|child_id| {
                self.nodes.get(child_id).cloned().context(NotFoundSnafu {
                    id: child_id.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        nodes.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(nodes)
    }

    pub fn list_session_states(&self) -> HashMap<String, SessionState> {
        self.sessions.clone()
    }

    pub fn get_session_state(&self, name: &str) -> Result<SessionState> {
        self.sessions
            .get(name)
            .cloned()
            .context(BranchNotFoundSnafu {
                name: name.to_owned(),
            })
    }

    pub fn set_session_state(
        &mut self,
        name: &str,
        expected: Option<&SessionState>,
        next: SessionState,
    ) -> Result<SessionState> {
        let state = self.sessions.get(name).context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })?;
        if let Some(expected) = expected {
            ensure!(
                state == expected,
                SessionStateMovedSnafu {
                    name: name.to_owned(),
                    expected: format!("{expected:?}"),
                    actual: format!("{state:?}"),
                }
            );
        }
        self.validate_session_state(&next)?;
        let state = self.sessions.get_mut(name).context(BranchNotFoundSnafu {
            name: name.to_owned(),
        })?;
        *state = next;
        Ok(state.clone())
    }

    pub fn list_branch_configs(&self) -> Result<HashMap<String, BranchConfig>> {
        self.branch_configs
            .iter()
            .map(|(name, record)| {
                let config = record
                    .current_config()
                    .context(BranchConfigVersionNotFoundSnafu {
                        name: name.clone(),
                        version: record.current_version,
                    })?;
                Ok((name.clone(), config))
            })
            .collect()
    }

    pub fn list_branch_config_records(&self) -> HashMap<String, BranchConfigRecord> {
        self.branch_configs.clone()
    }

    pub fn get_branch_config(&self, name: &str) -> Result<BranchConfig> {
        let record = self.get_branch_config_record(name)?;
        record
            .current_config()
            .context(BranchConfigVersionNotFoundSnafu {
                name: name.to_owned(),
                version: record.current_version,
            })
    }

    pub fn get_branch_config_record(&self, name: &str) -> Result<BranchConfigRecord> {
        self.branch_configs
            .get(name)
            .cloned()
            .context(BranchConfigNotFoundSnafu {
                name: name.to_owned(),
            })
    }

    pub fn set_branch_config(
        &mut self,
        name: &str,
        config: BranchConfig,
    ) -> Result<BranchConfigRecord> {
        let record = if let Some(record) = self.branch_configs.get_mut(name) {
            let current_version = record.current_version;
            record
                .update(config)
                .context(BranchConfigVersionNotFoundSnafu {
                    name: name.to_owned(),
                    version: current_version,
                })?;
            record.clone()
        } else {
            let record = BranchConfigRecord::new(name.to_owned(), config);
            self.branch_configs.insert(name.to_owned(), record.clone());
            record
        };
        Ok(record)
    }

    pub fn rollback_branch_config(
        &mut self,
        name: &str,
        target_version: u64,
    ) -> Result<BranchConfigRecord> {
        let record = self
            .branch_configs
            .get_mut(name)
            .context(BranchConfigNotFoundSnafu {
                name: name.to_owned(),
            })?;
        record
            .rollback(target_version)
            .context(BranchConfigVersionNotFoundSnafu {
                name: name.to_owned(),
                version: target_version,
            })?;
        Ok(record.clone())
    }

    pub fn delete_branch_config(&mut self, name: &str) -> Result<()> {
        self.branch_configs
            .remove(name)
            .map(|_| ())
            .context(BranchConfigNotFoundSnafu {
                name: name.to_owned(),
            })
    }

    pub fn list_provider_profiles(&self) -> HashMap<String, ProviderProfile> {
        self.provider_profiles.clone()
    }

    pub fn get_provider_profile(&self, name: &str) -> Result<ProviderProfile> {
        self.provider_profiles
            .get(name)
            .cloned()
            .context(ProviderProfileNotFoundSnafu {
                name: name.to_owned(),
            })
    }

    pub fn submit_job(&mut self, branch: &str, base: &str) -> Result<Job> {
        self.get_branch_head(branch)?;
        self.get_node(base)?;
        if let Some(active_job) = self
            .jobs
            .values()
            .find(|job| job.branch == branch && !matches!(job.status, JobStatus::Finished))
        {
            return PromptJobActiveOnBranchSnafu {
                branch: branch.to_owned(),
                job_id: active_job.job_id.clone(),
            }
            .fail();
        }
        let job = Job::new(self.next_job_id(), branch, base);
        self.jobs.insert(job.job_id.clone(), job.clone());
        Ok(job)
    }

    pub fn skill_groups(&self) -> SkillGroups {
        self.skill_groups.clone()
    }

    pub fn list_skills(&self, role: SessionRole) -> Vec<SkillRecord> {
        self.skill_groups.for_role(role).values().cloned().collect()
    }

    pub fn get_skill(&self, role: SessionRole, name: &str) -> Result<SkillRecord> {
        self.skill_groups
            .for_role(role)
            .get(name)
            .cloned()
            .context(SkillNotFoundSnafu {
                role: role.as_str().to_owned(),
                name: name.to_owned(),
            })
    }

    pub fn add_skill(
        &mut self,
        role: SessionRole,
        name: &str,
        spec: SkillVersionSpec,
    ) -> Result<SkillRecord> {
        validate_skill_name(name)?;
        let skills = self.skill_groups.for_role_mut(role);
        ensure!(
            !skills.contains_key(name),
            SkillAlreadyExistsSnafu {
                role: role.as_str().to_owned(),
                name: name.to_owned(),
            }
        );

        let record = SkillRecord::new(name.to_owned(), spec);
        skills.insert(name.to_owned(), record.clone());
        Ok(record)
    }

    pub fn update_skill(
        &mut self,
        role: SessionRole,
        name: &str,
        patch: &SkillUpdatePatch,
    ) -> Result<SkillRecord> {
        ensure!(
            !patch.is_empty(),
            SkillUpdateEmptySnafu {
                role: role.as_str().to_owned(),
                name: name.to_owned(),
            }
        );

        let record =
            self.skill_groups
                .for_role_mut(role)
                .get_mut(name)
                .context(SkillNotFoundSnafu {
                    role: role.as_str().to_owned(),
                    name: name.to_owned(),
                })?;
        let current_version = record.current_version;
        record.update(patch).context(SkillVersionNotFoundSnafu {
            role: role.as_str().to_owned(),
            name: name.to_owned(),
            version: current_version,
        })?;
        Ok(record.clone())
    }

    pub fn rollback_skill(
        &mut self,
        role: SessionRole,
        name: &str,
        target_version: u64,
    ) -> Result<SkillRecord> {
        let record =
            self.skill_groups
                .for_role_mut(role)
                .get_mut(name)
                .context(SkillNotFoundSnafu {
                    role: role.as_str().to_owned(),
                    name: name.to_owned(),
                })?;
        record
            .rollback(target_version)
            .context(SkillVersionNotFoundSnafu {
                role: role.as_str().to_owned(),
                name: name.to_owned(),
                version: target_version,
            })?;
        Ok(record.clone())
    }

    pub fn get_job(&self, job_id: &str) -> Result<Job> {
        self.jobs
            .get(job_id)
            .cloned()
            .context(PromptJobNotFoundSnafu {
                job_id: job_id.to_owned(),
            })
    }

    pub fn list_jobs(&self) -> HashMap<String, Job> {
        self.jobs.clone()
    }

    pub fn add_scheduler_task(&mut self, task: NewSchedulerTask) -> Result<SchedulerTask> {
        validate_scheduler_task_input(&task.id, &task.branch, &task.prompt, task.interval_secs)?;
        self.get_branch_head(&task.branch)?;
        ensure!(
            !self.scheduler_tasks.contains_key(&task.id),
            SchedulerTaskAlreadyExistsSnafu {
                id: task.id.clone(),
            }
        );

        let task = SchedulerTask::new(task);
        self.scheduler_tasks.insert(task.id.clone(), task.clone());
        Ok(task)
    }

    pub fn get_scheduler_task(&self, id: &str) -> Result<SchedulerTask> {
        self.scheduler_tasks
            .get(id)
            .cloned()
            .context(SchedulerTaskNotFoundSnafu { id: id.to_owned() })
    }

    pub fn list_scheduler_tasks(&self) -> HashMap<String, SchedulerTask> {
        self.scheduler_tasks.clone()
    }

    pub fn update_scheduler_task(
        &mut self,
        id: &str,
        patch: &SchedulerTaskPatch,
    ) -> Result<SchedulerTask> {
        ensure!(
            !patch.is_empty(),
            SchedulerTaskUpdateEmptySnafu { id: id.to_owned() }
        );

        let current = self.get_scheduler_task(id)?;
        let branch = patch.branch.as_deref().unwrap_or(&current.branch);
        let prompt = patch.prompt.as_deref().unwrap_or(&current.prompt);
        let interval_secs = patch.interval_secs.unwrap_or(current.interval_secs);
        validate_scheduler_task_input(id, branch, prompt, interval_secs)?;
        self.get_branch_head(branch)?;

        let task = self
            .scheduler_tasks
            .get_mut(id)
            .context(SchedulerTaskNotFoundSnafu { id: id.to_owned() })?;
        task.apply_patch(patch);
        Ok(task.clone())
    }

    pub fn delete_scheduler_task(&mut self, id: &str) -> Result<()> {
        self.scheduler_tasks
            .remove(id)
            .map(|_| ())
            .context(SchedulerTaskNotFoundSnafu { id: id.to_owned() })
    }

    pub fn claim_due_scheduler_tasks(
        &mut self,
        now: Timestamp,
        limit: usize,
    ) -> Vec<SchedulerTask> {
        if limit == 0 {
            return Vec::new();
        }

        let mut due_ids = self
            .scheduler_tasks
            .values()
            .filter(|task| task.enabled && task.next_run_at <= now)
            .map(|task| (task.next_run_at, task.id.clone()))
            .collect::<Vec<_>>();
        due_ids.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

        let mut claimed = Vec::with_capacity(limit.min(due_ids.len()));
        for (_, id) in due_ids.into_iter().take(limit) {
            let task = self
                .scheduler_tasks
                .get_mut(&id)
                .expect("due scheduler task should still exist");
            task.mark_claimed(now);
            claimed.push(task.clone());
        }
        claimed
    }

    pub fn set_job_status(
        &mut self,
        job_id: &str,
        expected: JobStatus,
        next: JobStatus,
    ) -> Result<Job> {
        let job = self.jobs.get_mut(job_id).context(PromptJobNotFoundSnafu {
            job_id: job_id.to_owned(),
        })?;
        ensure!(
            job.status == expected,
            PromptJobMovedSnafu {
                job_id: job_id.to_owned(),
                expected: format!("{expected:?}"),
                actual: format!("{:?}", job.status),
            }
        );
        job.status = next;
        job.finished_at = match next {
            JobStatus::Finished => Some(Timestamp::now()),
            _ => None,
        };
        Ok(job.clone())
    }

    fn next_job_id(&self) -> String {
        loop {
            let candidate = format!("job-{}", nanoid::nanoid!());
            if !self.jobs.contains_key(&candidate) {
                return candidate;
            }
        }
    }

    pub fn plan_rebase_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
    ) -> Result<RebasePlan> {
        self.plan_rebase_session_with(name, |session_anchor| session_anchor.apply_patch(patch))
    }

    pub fn plan_rebase_session_system_prompt(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        system_prompt: &str,
    ) -> Result<RebasePlan> {
        self.plan_rebase_session_with(name, |session_anchor| {
            let mut rebased = session_anchor.apply_patch(patch);
            rebased.system_prompt = system_prompt.to_owned();
            rebased
        })
    }

    fn plan_rebase_session_with(
        &self,
        name: &str,
        rebase_session_anchor: impl FnOnce(&SessionAnchor) -> SessionAnchor,
    ) -> Result<RebasePlan> {
        let branch = name.to_owned();
        let expected_old_head = self.get_branch_head(name)?.to_owned();
        let chain_ids = self
            .session_chain_ids(name)?
            .into_iter()
            .rev()
            .collect::<Vec<_>>();
        let session_node = self
            .nodes
            .get(
                chain_ids
                    .first()
                    .expect("session chain should not be empty"),
            )
            .expect("session chain node should exist");
        let session_anchor = match &session_node.kind {
            Kind::Anchor(anchor) => anchor
                .as_session()
                .expect("session chain should start with session anchor"),
            _ => unreachable!("session chain should start with anchor"),
        }
        .clone();
        let rebased_session_anchor = rebase_session_anchor(&session_anchor);

        let mut temp = self.clone();
        let mut previous_new_id = None;
        let mut new_head = String::new();
        let mut nodes = Vec::with_capacity(chain_ids.len());

        for (index, node_id) in chain_ids.into_iter().enumerate() {
            let node = self
                .nodes
                .get(&node_id)
                .cloned()
                .context(NotFoundSnafu { id: node_id })?;
            let parent = previous_new_id
                .clone()
                .unwrap_or_else(|| node.parent.clone());
            let kind = if index == 0 {
                let Kind::Anchor(anchor) = &node.kind else {
                    unreachable!("session chain should start with anchor");
                };
                Kind::Anchor(Anchor::session(
                    anchor.merge_parents().to_vec(),
                    rebased_session_anchor.clone(),
                ))
            } else {
                node.kind.clone()
            };
            let new_node = Node::new(parent, node.role, node.metadata, kind, node.created_at);
            temp.insert_existing_node(new_node.clone())?;
            previous_new_id = Some(new_node.id.clone());
            new_head = new_node.id.clone();
            nodes.push(new_node);
        }

        temp.apply_set_branch_head(branch.clone(), &expected_old_head, new_head.clone())?;

        Ok(RebasePlan {
            branch,
            expected_old_head,
            new_head,
            nodes,
        })
    }

    fn session_chain_ids(&self, reference: &str) -> Result<Vec<String>> {
        let branch = reference.to_owned();
        let mut node = self.resolve_ref(reference)?;
        let mut chain_ids = vec![];

        loop {
            chain_ids.push(node.id.clone());
            if matches!(
                node.kind,
                Kind::Anchor(Anchor {
                    payload: AnchorPayload::Session(_),
                    ..
                })
            ) {
                break;
            }

            ensure!(
                !node.is_root(),
                MissingSessionAnchorSnafu {
                    branch: branch.clone(),
                }
            );

            node = self.nodes.get(&node.parent).context(ParentNotFoundSnafu {
                id: node.parent.clone(),
            })?;
        }

        Ok(chain_ids)
    }

    fn validate_new_node(&self, node: &Node) -> Result<()> {
        ensure!(
            self.nodes.contains_key(&node.parent),
            ParentNotFoundSnafu {
                id: node.parent.clone()
            }
        );
        self.validate_anchor_merge_parents(&node.parent, &node.kind)?;
        Ok(())
    }

    pub fn validate_session_records(&self) -> Result<()> {
        for (branch, state) in &self.sessions {
            self.get_branch_head(branch)?;
            self.validate_session_state(state)?;
        }

        for branch in self.branches.keys() {
            ensure!(
                self.sessions.contains_key(branch),
                BranchNotFoundSnafu {
                    name: branch.clone(),
                }
            );
        }

        Ok(())
    }

    fn insert_existing_node_unchecked(&mut self, node: Node) -> Result<String> {
        let id = node.id.clone();

        for parent in parent_ids(&node) {
            self.children
                .entry(parent.to_owned())
                .or_default()
                .insert(id.clone());
        }
        self.nodes.insert(id.clone(), node);

        Ok(id)
    }

    fn resolve_ref_id<'a>(&'a self, reference: &str) -> Result<&'a str> {
        Ok(&self.resolve_ref(reference)?.id)
    }

    fn resolve_ref<'a>(&'a self, reference: &str) -> Result<&'a Node> {
        if let Some(node) = self.nodes.get(reference) {
            return Ok(node);
        }

        if let Some(head_id) = self.branches.get(reference) {
            return self.nodes.get(head_id).context(NotFoundSnafu {
                id: head_id.clone(),
            });
        }

        ensure!(
            is_node_id(reference),
            BranchNotFoundSnafu {
                name: reference.to_owned(),
            }
        );

        NotFoundSnafu {
            id: reference.to_owned(),
        }
        .fail()
    }

    fn resolve_node_ref<'a>(&'a self, reference: &str) -> Result<&'a Node> {
        if let Some(head_id) = self.branches.get(reference) {
            return self.nodes.get(head_id).context(NotFoundSnafu {
                id: head_id.clone(),
            });
        }

        if let Some(node) = self.nodes.get(reference) {
            return Ok(node);
        }

        let matches = self
            .nodes
            .keys()
            .filter(|node_id| node_id.starts_with(reference))
            .cloned()
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [matched] => self.nodes.get(matched).context(NotFoundSnafu {
                id: matched.clone(),
            }),
            [] => NotFoundSnafu {
                id: reference.to_owned(),
            }
            .fail(),
            _ => AmbiguousNodePrefixSnafu {
                prefix: reference.to_owned(),
                matches,
            }
            .fail(),
        }
    }

    fn validate_anchor_merge_parents(&self, parent: &str, kind: &Kind) -> Result<()> {
        let Kind::Anchor(anchor) = kind else {
            return Ok(());
        };

        let mut seen = HashSet::new();
        let mut shadow_parents = Vec::new();
        for merge_parent in anchor.merge_parents() {
            let node_id = merge_parent.node_id();
            ensure!(
                node_id != parent,
                MergeParentMatchesParentSnafu {
                    id: node_id.to_owned(),
                }
            );
            ensure!(
                seen.insert(node_id),
                DuplicateMergeParentSnafu {
                    id: node_id.to_owned(),
                }
            );
            ensure!(
                self.nodes.contains_key(node_id),
                ParentNotFoundSnafu {
                    id: node_id.to_owned(),
                }
            );
            if merge_parent.is_shadow() {
                shadow_parents.push(node_id.to_owned());
            }
        }
        ensure!(
            shadow_parents.len() <= 1,
            MultipleShadowParentsSnafu {
                ids: shadow_parents,
            }
        );

        Ok(())
    }

    fn validate_session_state(&self, state: &SessionState) -> Result<()> {
        match state {
            SessionState::Active => Ok(()),
            SessionState::Attached {
                target_branch,
                base_head_id,
            } => self.validate_ref_on_branch(target_branch, base_head_id),
            SessionState::Paused {
                target_branch,
                reason,
            } => match reason {
                PauseReason::Merged { merged_anchor_id } => {
                    self.validate_anchor_on_branch(target_branch, merged_anchor_id)
                }
                PauseReason::Closed => {
                    if target_branch.is_empty() {
                        return Ok(());
                    }
                    self.get_branch_head(target_branch).map(|_| ())
                }
            },
        }
    }

    fn validate_ref_on_branch(&self, branch: &str, node_id: &str) -> Result<()> {
        self.get_branch_head(branch)?;
        self.nodes.get(node_id).context(NotFoundSnafu {
            id: node_id.to_owned(),
        })?;
        let visible = self
            .ancestry(branch)?
            .into_iter()
            .any(|node| node.id == node_id);
        ensure!(
            visible,
            RefsNotConnectedSnafu {
                base_ref: node_id.to_owned(),
                head_ref: branch.to_owned(),
            }
        );
        Ok(())
    }

    fn validate_anchor_on_branch(&self, branch: &str, node_id: &str) -> Result<()> {
        let node = self.nodes.get(node_id).context(NotFoundSnafu {
            id: node_id.to_owned(),
        })?;
        ensure!(
            matches!(node.kind, Kind::Anchor(_)),
            InvalidAnchorSnafu {
                id: node_id.to_owned()
            }
        );
        self.validate_ref_on_branch(branch, node_id)
    }
}

fn validate_scheduler_task_input(
    id: &str,
    branch: &str,
    prompt: &str,
    interval_secs: u64,
) -> Result<()> {
    ensure!(
        !id.trim().is_empty(),
        InvalidSchedulerTaskSnafu {
            id: id.to_owned(),
            message: "id is empty".to_owned(),
        }
    );
    ensure!(
        !branch.trim().is_empty(),
        InvalidSchedulerTaskSnafu {
            id: id.to_owned(),
            message: "branch is empty".to_owned(),
        }
    );
    ensure!(
        !prompt.trim().is_empty(),
        InvalidSchedulerTaskSnafu {
            id: id.to_owned(),
            message: "prompt is empty".to_owned(),
        }
    );
    ensure!(
        interval_secs > 0,
        InvalidSchedulerTaskSnafu {
            id: id.to_owned(),
            message: "interval_secs must be greater than zero".to_owned(),
        }
    );
    Ok(())
}

fn is_node_id(reference: &str) -> bool {
    reference.len() == 64 && reference.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_skill_name(name: &str) -> Result<()> {
    let trimmed = name.trim();
    ensure!(
        !trimmed.is_empty(),
        InvalidSkillNameSnafu {
            name: name.to_owned(),
            message: "name must not be empty".to_owned(),
        }
    );
    ensure!(
        trimmed == name,
        InvalidSkillNameSnafu {
            name: name.to_owned(),
            message: "name must not have leading or trailing whitespace".to_owned(),
        }
    );
    Ok(())
}

fn parent_ids(node: &Node) -> Vec<&str> {
    let mut parents = vec![node.parent.as_str()];
    if let Kind::Anchor(anchor) = &node.kind {
        parents.extend(anchor.merge_parents().iter().map(|parent| parent.node_id()));
    }
    parents
}
