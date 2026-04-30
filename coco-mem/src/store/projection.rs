use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct ProjectionContext {
    entity: String,
    history_path: PathBuf,
}

impl ProjectionContext {
    pub fn new(entity: impl Into<String>, history_path: impl Into<PathBuf>) -> Self {
        Self {
            entity: entity.into(),
            history_path: history_path.into(),
        }
    }

    pub fn entity(&self) -> &str {
        &self.entity
    }

    pub fn history_path(&self) -> &Path {
        &self.history_path
    }
}
