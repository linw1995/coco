use std::collections::{HashMap, HashSet};

use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;

use crate::StoreResult as Result;
use crate::error::{
    AmbiguousNodePrefixSnafu, BranchExistsSnafu, BranchHeadMovedSnafu, BranchNotFoundSnafu,
    DuplicateMergeParentSnafu, InvalidAnchorSnafu, InvalidSessionHandoffPromptSnafu,
    InvalidSkillNameSnafu, MergeParentMatchesParentSnafu, MissingSessionAnchorSnafu,
    MultipleShadowParentsSnafu, NotFoundSnafu, ParentNotFoundSnafu, PresetNotFoundSnafu,
    PresetVersionNotFoundSnafu, PromptJobActiveOnBranchSnafu, PromptJobAlreadyExistsSnafu,
    PromptJobInvalidStatusTransitionSnafu, PromptJobMovedSnafu, PromptJobNotFoundSnafu,
    RefsNotConnectedSnafu, SessionStateMovedSnafu, SkillAlreadyExistsSnafu, SkillNotFoundSnafu,
    SkillUpdateEmptySnafu, SkillVersionNotFoundSnafu,
};
use crate::{
    Anchor, AnchorPayload, Job, JobStatus, Kind, MessageQueueItem, NewNode, Node, PauseReason,
    Preset, PresetRecord, Role, SessionAnchor, SessionAnchorPatch, SessionRole, SessionState,
    SkillGroups, SkillRecord, SkillUpdatePatch, SkillVersionSpec, default_skill_groups,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreState {
    pub nodes: HashMap<String, Node>,
    pub children: HashMap<String, HashSet<String>>,
    pub root: String,
    pub branches: HashMap<String, String>,
    pub sessions: HashMap<String, SessionState>,
    pub presets: HashMap<String, PresetRecord>,
    pub jobs: HashMap<String, Job>,
    pub message_queues: HashMap<String, Vec<MessageQueueItem>>,
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
pub struct HandoffPlan {
    pub branch: String,
    pub expected_old_head: String,
    pub new_head: String,
    pub node: Node,
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
            presets: HashMap::new(),
            jobs: HashMap::new(),
            message_queues: HashMap::new(),
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
            presets: HashMap::new(),
            jobs: HashMap::new(),
            message_queues: HashMap::new(),
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

    pub fn list_preset_records(&self) -> HashMap<String, PresetRecord> {
        self.presets.clone()
    }

    pub fn get_preset_record(&self, name: &str) -> Result<PresetRecord> {
        self.presets
            .get(name)
            .cloned()
            .context(PresetNotFoundSnafu {
                name: name.to_owned(),
            })
    }

    pub fn set_preset(&mut self, name: &str, config: Preset) -> Result<PresetRecord> {
        let record = if let Some(record) = self.presets.get_mut(name) {
            let current_version = record.current_version;
            record.update(config).context(PresetVersionNotFoundSnafu {
                name: name.to_owned(),
                version: current_version,
            })?;
            record.clone()
        } else {
            let record = PresetRecord::new(name.to_owned(), config);
            self.presets.insert(name.to_owned(), record.clone());
            record
        };
        Ok(record)
    }

    pub fn rollback_preset(&mut self, name: &str, target_version: u64) -> Result<PresetRecord> {
        let record = self.presets.get_mut(name).context(PresetNotFoundSnafu {
            name: name.to_owned(),
        })?;
        record
            .rollback(target_version)
            .context(PresetVersionNotFoundSnafu {
                name: name.to_owned(),
                version: target_version,
            })?;
        Ok(record.clone())
    }

    pub fn delete_preset(&mut self, name: &str) -> Result<()> {
        self.presets
            .remove(name)
            .map(|_| ())
            .context(PresetNotFoundSnafu {
                name: name.to_owned(),
            })
    }

    pub fn submit_job(&mut self, branch: &str, base: &str) -> Result<Job> {
        let job_id = self.next_job_id();
        self.submit_job_with_id(&job_id, branch, base)
    }

    pub fn submit_job_with_id(&mut self, job_id: &str, branch: &str, base: &str) -> Result<Job> {
        self.get_branch_head(branch)?;
        self.get_node(base)?;
        ensure!(
            !self.jobs.contains_key(job_id),
            PromptJobAlreadyExistsSnafu {
                job_id: job_id.to_owned(),
            }
        );
        if let Some(active_job) = self
            .jobs
            .values()
            .find(|job| job_uses_active_branch(job, branch))
        {
            return PromptJobActiveOnBranchSnafu {
                branch: branch.to_owned(),
                job_id: active_job.job_id.clone(),
            }
            .fail();
        }
        let job = Job::new(job_id, branch, base);
        self.jobs.insert(job.job_id.clone(), job.clone());
        Ok(job)
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
        let mut job = self
            .jobs
            .get(job_id)
            .cloned()
            .context(PromptJobNotFoundSnafu {
                job_id: job_id.to_owned(),
            })?;
        job.normalize_work_branch();
        Ok(job)
    }

    pub fn list_jobs(&self) -> HashMap<String, Job> {
        self.jobs
            .iter()
            .map(|(job_id, job)| {
                let mut job = job.clone();
                job.normalize_work_branch();
                (job_id.clone(), job)
            })
            .collect()
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
        ensure!(
            job.status.can_transition_to(next),
            PromptJobInvalidStatusTransitionSnafu {
                job_id: job_id.to_owned(),
                current: format!("{:?}", job.status),
                next: format!("{next:?}"),
            }
        );
        job.status = next;
        job.finished_at = match next {
            JobStatus::Finished => Some(Timestamp::now()),
            _ => None,
        };
        Ok(job.clone())
    }

    pub fn set_job_work_branch(
        &mut self,
        job_id: &str,
        expected_work_branch: &str,
        next_work_branch: &str,
    ) -> Result<Job> {
        self.get_branch_head(next_work_branch)?;
        let existing_active_job = self
            .jobs
            .values()
            .find(|job| job.job_id != job_id && job_uses_active_branch(job, next_work_branch));
        if let Some(active_job) = existing_active_job {
            return PromptJobActiveOnBranchSnafu {
                branch: next_work_branch.to_owned(),
                job_id: active_job.job_id.clone(),
            }
            .fail();
        }

        let job = self.jobs.get_mut(job_id).context(PromptJobNotFoundSnafu {
            job_id: job_id.to_owned(),
        })?;
        job.normalize_work_branch();
        ensure!(
            !matches!(job.status, JobStatus::Finished),
            PromptJobInvalidStatusTransitionSnafu {
                job_id: job_id.to_owned(),
                current: format!("{:?}", job.status),
                next: "work_branch_changed".to_owned(),
            }
        );
        ensure!(
            job.work_branch == expected_work_branch,
            PromptJobMovedSnafu {
                job_id: job_id.to_owned(),
                expected: expected_work_branch.to_owned(),
                actual: job.work_branch.clone(),
            }
        );
        job.work_branch = next_work_branch.to_owned();
        Ok(job.clone())
    }

    pub fn enqueue_message(&mut self, queue: &str, payload: serde_json::Value) -> MessageQueueItem {
        let item = MessageQueueItem::new(queue, payload, Timestamp::now());
        self.message_queues
            .entry(queue.to_owned())
            .or_default()
            .push(item.clone());
        item
    }

    pub fn dequeue_message(&mut self, queue: &str) -> Option<MessageQueueItem> {
        let messages = self.message_queues.get_mut(queue)?;
        if messages.is_empty() {
            return None;
        }

        let item = messages.remove(0);
        if messages.is_empty() {
            self.message_queues.remove(queue);
        }
        Some(item)
    }

    pub fn peek_message(&self, queue: &str) -> Option<MessageQueueItem> {
        self.message_queues
            .get(queue)
            .and_then(|messages| messages.first())
            .cloned()
    }

    pub fn list_queue_messages(&self, queue: &str) -> Vec<MessageQueueItem> {
        self.message_queues.get(queue).cloned().unwrap_or_default()
    }

    pub fn list_message_queues(&self) -> Vec<String> {
        let mut queues = self.message_queues.keys().cloned().collect::<Vec<_>>();
        queues.sort();
        queues
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

    pub fn plan_handoff_session(
        &self,
        name: &str,
        patch: &SessionAnchorPatch,
        prompt: &str,
    ) -> Result<HandoffPlan> {
        let prompt = prompt.trim();
        ensure!(!prompt.is_empty(), InvalidSessionHandoffPromptSnafu);

        let branch = name.to_owned();
        let expected_old_head = self.get_branch_head(name)?.to_owned();
        let session_node_id = self
            .session_chain_ids(name)?
            .into_iter()
            .last()
            .expect("session chain should not be empty");
        let session_node = self.nodes.get(&session_node_id).context(NotFoundSnafu {
            id: session_node_id,
        })?;
        let session_anchor = match &session_node.kind {
            Kind::Anchor(anchor) => anchor
                .as_session()
                .expect("session chain should end with session anchor"),
            _ => unreachable!("session chain should end with anchor"),
        }
        .clone();
        let mut handoff_session_anchor = session_anchor.apply_patch(patch);
        handoff_session_anchor.prompt = prompt.to_owned();
        let node = self.plan_append_node(NewNode {
            parent: expected_old_head.clone(),
            role: Role::System,
            metadata: None,
            kind: Kind::Anchor(Anchor::session(vec![], handoff_session_anchor)),
        })?;
        let new_head = node.id.clone();

        let mut temp = self.clone();
        temp.insert_existing_node(node.clone())?;
        temp.apply_set_branch_head(branch.clone(), &expected_old_head, new_head.clone())?;

        Ok(HandoffPlan {
            branch,
            expected_old_head,
            new_head,
            node,
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

fn job_uses_active_branch(job: &Job, branch: &str) -> bool {
    if matches!(job.status, JobStatus::Finished) {
        return false;
    }
    let work_branch = if job.work_branch.is_empty() {
        job.branch.as_str()
    } else {
        job.work_branch.as_str()
    };
    job.branch == branch || work_branch == branch
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
