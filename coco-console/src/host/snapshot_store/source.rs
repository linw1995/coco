use super::*;

pub struct MaterializationSourceSnapshot {
    root_id: String,
    nodes: BTreeMap<String, Node>,
    children: BTreeMap<String, Vec<Node>>,
    branches: BTreeMap<String, String>,
    sessions: HashMap<String, SessionState>,
}

impl MaterializationSourceSnapshot {
    pub async fn from_store(
        store: &(impl BranchStore + NodeStore + SessionStore),
        session_states: &[(String, SessionState)],
    ) -> crate::Result<Self> {
        let mut branches = BTreeMap::new();
        for (branch, _) in session_states {
            branches.insert(
                branch.clone(),
                store
                    .get_branch_head(branch)
                    .await
                    .context(crate::error::StoreSnafu)?,
            );
        }

        let root_id = store.root_id();
        let mut nodes = BTreeMap::new();
        let mut children = BTreeMap::new();
        let mut pending = vec![root_id.clone()];
        while let Some(node_id) = pending.pop() {
            if nodes.contains_key(&node_id) {
                continue;
            }
            let node = store
                .get_node(&node_id)
                .await
                .context(crate::error::StoreSnafu)?;
            let node_children = store
                .list_children(&node_id)
                .await
                .context(crate::error::StoreSnafu)?;
            pending.extend(node_children.iter().map(|child| child.id.clone()));
            children.insert(node_id, node_children);
            nodes.insert(node.id.clone(), node);
        }

        Ok(Self {
            root_id,
            nodes,
            children,
            branches,
            sessions: session_states.iter().cloned().collect(),
        })
    }

    pub fn branch_head(&self, name: &str) -> coco_mem::StoreResult<String> {
        self.branches
            .get(name)
            .cloned()
            .ok_or_else(|| coco_mem::StoreError::BranchNotFound {
                name: name.to_owned(),
            })
    }

    pub fn ancestry_nodes(&self, head_ref: &str) -> coco_mem::StoreResult<Vec<Node>> {
        let mut node_id = self.resolve_ref_id(head_ref)?;
        let mut nodes = Vec::new();
        loop {
            let node = self.nodes.get(&node_id).cloned().ok_or_else(|| {
                coco_mem::StoreError::NotFound {
                    id: node_id.clone(),
                }
            })?;
            let parent = node.parent.clone();
            nodes.push(node);
            if parent.is_empty() {
                return Ok(nodes);
            }
            if !self.nodes.contains_key(&parent) {
                return Err(coco_mem::StoreError::ParentNotFound { id: parent });
            }
            node_id = parent;
        }
    }

    pub fn log_nodes(&self, base_ref: &str, head_ref: &str) -> coco_mem::StoreResult<Vec<Node>> {
        let base_id = self.resolve_ref_id(base_ref)?;
        let mut nodes = self.ancestry_nodes(head_ref)?;
        let Some(index) = nodes.iter().position(|node| node.id == base_id) else {
            return Err(coco_mem::StoreError::RefsNotConnected {
                base_ref: base_ref.to_owned(),
                head_ref: head_ref.to_owned(),
            });
        };
        nodes.truncate(index + 1);
        Ok(nodes)
    }

    pub fn node(&self, id: &str) -> coco_mem::StoreResult<Node> {
        let id = self.resolve_ref_id(id)?;
        self.nodes
            .get(&id)
            .cloned()
            .ok_or(coco_mem::StoreError::NotFound { id })
    }

    pub fn children(&self, node_id: &str) -> coco_mem::StoreResult<Vec<Node>> {
        self.nodes
            .get(node_id)
            .ok_or_else(|| coco_mem::StoreError::NotFound {
                id: node_id.to_owned(),
            })?;
        Ok(self.children.get(node_id).cloned().unwrap_or_default())
    }

    pub fn resolve_ref_id(&self, reference: &str) -> coco_mem::StoreResult<String> {
        if self.nodes.contains_key(reference) {
            return Ok(reference.to_owned());
        }
        if let Some(head_id) = self.branches.get(reference) {
            return Ok(head_id.clone());
        }
        let matches = self
            .nodes
            .keys()
            .filter(|node_id| node_id.starts_with(reference))
            .cloned()
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [matched] => Ok(matched.clone()),
            [] => Err(coco_mem::StoreError::NotFound {
                id: reference.to_owned(),
            }),
            matches => Err(coco_mem::StoreError::AmbiguousNodePrefix {
                prefix: reference.to_owned(),
                matches: matches.to_vec(),
            }),
        }
    }

    pub fn read_only_error<T>() -> coco_mem::StoreResult<T> {
        Err(coco_mem::StoreError::StoreReadOnly {
            path: PathBuf::from("console graph materialization source snapshot"),
        })
    }
}

#[async_trait]
impl NodeStore for MaterializationSourceSnapshot {
    fn root_id(&self) -> String {
        self.root_id.clone()
    }

    async fn append(&self, _node: NewNode) -> coco_mem::StoreResult<String> {
        Self::read_only_error()
    }

    async fn ancestry(&self, head_ref: &str) -> coco_mem::StoreResult<Vec<Node>> {
        self.ancestry_nodes(head_ref)
    }

    async fn log(&self, base_ref: &str, head_ref: &str) -> coco_mem::StoreResult<Vec<Node>> {
        self.log_nodes(base_ref, head_ref)
    }

    async fn get_node(&self, id: &str) -> coco_mem::StoreResult<Node> {
        self.node(id)
    }

    async fn list_children(&self, node_id: &str) -> coco_mem::StoreResult<Vec<Node>> {
        self.children(node_id)
    }
}

#[async_trait]
impl BranchStore for MaterializationSourceSnapshot {
    async fn fork(&self, _name: &str, _from_ref: &str) -> coco_mem::StoreResult<String> {
        Self::read_only_error()
    }

    async fn get_branch_head(&self, name: &str) -> coco_mem::StoreResult<String> {
        self.branch_head(name)
    }

    async fn delete_branch(&self, _name: &str) -> coco_mem::StoreResult<()> {
        Self::read_only_error()
    }

    async fn set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _new_head: &str,
    ) -> coco_mem::StoreResult<()> {
        Self::read_only_error()
    }

    async fn append_nodes_and_set_branch_head(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _nodes: Vec<coco_mem::NewNodeContent>,
    ) -> coco_mem::StoreResult<String> {
        Self::read_only_error()
    }

    async fn append_nodes_and_set_branch_head_to(
        &self,
        _name: &str,
        _expected_old_head: &str,
        _parent: &str,
        _new_head: &str,
        _nodes: Vec<coco_mem::NewNodeContent>,
    ) -> coco_mem::StoreResult<String> {
        Self::read_only_error()
    }

    async fn append_nodes_and_set_branch_head_with_session_state(
        &self,
        _update: coco_mem::BranchAppendSessionState,
    ) -> coco_mem::StoreResult<String> {
        Self::read_only_error()
    }
}

#[async_trait]
impl SessionStore for MaterializationSourceSnapshot {
    async fn list_session_states(&self) -> coco_mem::StoreResult<HashMap<String, SessionState>> {
        Ok(self.sessions.clone())
    }

    async fn get_session_state(&self, name: &str) -> coco_mem::StoreResult<SessionState> {
        self.sessions
            .get(name)
            .cloned()
            .ok_or_else(|| coco_mem::StoreError::BranchNotFound {
                name: name.to_owned(),
            })
    }

    async fn set_session_state(
        &self,
        _name: &str,
        _expected: Option<&SessionState>,
        _next: SessionState,
    ) -> coco_mem::StoreResult<SessionState> {
        Self::read_only_error()
    }

    async fn rebase_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
    ) -> coco_mem::StoreResult<String> {
        Self::read_only_error()
    }

    async fn handoff_session(
        &self,
        _name: &str,
        _patch: &SessionAnchorPatch,
        _prompt: &str,
    ) -> coco_mem::StoreResult<String> {
        Self::read_only_error()
    }
}
