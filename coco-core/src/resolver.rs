use crate::{BranchResolveError, InboundMessage};

pub trait BranchResolver: Send + Sync {
    fn resolve_branch(
        &self,
        message: &InboundMessage,
    ) -> std::result::Result<String, BranchResolveError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedBranchResolver {
    branch: String,
}

impl FixedBranchResolver {
    pub fn new(branch: impl Into<String>) -> Self {
        Self {
            branch: branch.into(),
        }
    }
}

impl BranchResolver for FixedBranchResolver {
    fn resolve_branch(
        &self,
        _message: &InboundMessage,
    ) -> std::result::Result<String, BranchResolveError> {
        Ok(self.branch.clone())
    }
}
