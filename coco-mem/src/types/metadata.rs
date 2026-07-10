use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionMetadata {
    pub execution_id: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderMetadata {
    pub call_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct BackendMetadata {
    pub execution_id: Option<String>,
    // Provider-specific metadata such as rig's optional call_id should stay at
    // the metadata boundary instead of leaking into domain payload types.
    pub call_id: Option<String>,
}

pub type NodeMetadata = Vec<BackendMetadata>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackendMetadataBuilder {
    execution_id: Option<String>,
    call_id: Option<String>,
}

impl ExecutionMetadata {
    pub fn new(execution_id: String) -> Self {
        Self { execution_id }
    }
}

impl ProviderMetadata {
    pub fn new(call_id: Option<String>) -> Self {
        Self { call_id }
    }
}

impl BackendMetadata {
    pub fn builder() -> BackendMetadataBuilder {
        BackendMetadataBuilder::default()
    }

    pub fn from_parts(
        execution: Option<&ExecutionMetadata>,
        provider: Option<&ProviderMetadata>,
    ) -> Option<NodeMetadata> {
        Self::builder()
            .maybe_execution(execution)
            .maybe_provider(provider)
            .build()
    }
}

impl BackendMetadataBuilder {
    pub fn execution(mut self, metadata: &ExecutionMetadata) -> Self {
        self.execution_id = Some(metadata.execution_id.clone());
        self
    }

    pub fn maybe_execution(self, metadata: Option<&ExecutionMetadata>) -> Self {
        match metadata {
            Some(metadata) => self.execution(metadata),
            None => self,
        }
    }

    pub fn provider(mut self, metadata: &ProviderMetadata) -> Self {
        self.call_id = metadata.call_id.clone();
        self
    }

    pub fn maybe_provider(self, metadata: Option<&ProviderMetadata>) -> Self {
        match metadata {
            Some(metadata) => self.provider(metadata),
            None => self,
        }
    }

    pub fn build(self) -> Option<NodeMetadata> {
        if self.execution_id.is_none() && self.call_id.is_none() {
            return None;
        }

        Some(vec![BackendMetadata {
            execution_id: self.execution_id,
            call_id: self.call_id,
        }])
    }

    pub fn build_item(self) -> Option<BackendMetadata> {
        if self.execution_id.is_none() && self.call_id.is_none() {
            return None;
        }

        Some(BackendMetadata {
            execution_id: self.execution_id,
            call_id: self.call_id,
        })
    }
}
