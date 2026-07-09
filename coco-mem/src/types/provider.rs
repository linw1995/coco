use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderProfile {
    pub provider: String,
    #[serde(default)]
    pub secrets: BTreeMap<String, String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub default_model: Option<String>,
    #[serde(default)]
    #[serde(flatten)]
    pub spec: ProviderSpec,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderSpec {
    #[serde(flatten)]
    pub gpt: GptProviderSpec,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GptProviderSpec {
    #[serde(default)]
    pub reasoning_level: Option<String>,
    #[serde(default)]
    pub service_tier: Option<String>,
}
