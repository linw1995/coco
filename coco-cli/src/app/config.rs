#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use coco_mem::{ProviderProfile, ProviderProfileStore, StoreError, StoreResult};
use serde::Deserialize;
use snafu::prelude::*;

use crate::{
    Result,
    error::{ParseConfigFileSnafu, ReadConfigFileSnafu, ResolveCurrentDirSnafu},
};

const CONFIG_FILE_NAME: &str = "config.toml";

#[derive(Clone, Debug, Default)]
pub(crate) struct ProviderProfiles {
    profiles: HashMap<String, ProviderProfile>,
}

#[derive(Debug, Default, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    providers: HashMap<String, ProviderProfile>,
}

impl ProviderProfiles {
    fn new(profiles: HashMap<String, ProviderProfile>) -> Self {
        Self { profiles }
    }

    #[cfg(test)]
    pub(crate) fn from_profiles(profiles: HashMap<String, ProviderProfile>) -> Self {
        Self::new(profiles)
    }
}

impl ProviderProfileStore for ProviderProfiles {
    fn list_provider_profiles(&self) -> StoreResult<HashMap<String, ProviderProfile>> {
        Ok(self.profiles.clone())
    }

    fn get_provider_profile(&self, name: &str) -> StoreResult<ProviderProfile> {
        self.profiles
            .get(name)
            .cloned()
            .ok_or_else(|| StoreError::ProviderProfileNotFound {
                name: name.to_owned(),
            })
    }
}

pub(crate) fn load_cwd_provider_profiles() -> Result<ProviderProfiles> {
    let current_dir = std::env::current_dir().context(ResolveCurrentDirSnafu)?;
    load_provider_profiles_from(current_dir.join(CONFIG_FILE_NAME))
}

pub(crate) fn load_provider_profiles_from(path: impl AsRef<Path>) -> Result<ProviderProfiles> {
    let path = path.as_ref();
    let data = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(source) if source.kind() == ErrorKind::NotFound => return Ok(missing_config_profiles()),
        Err(source) => {
            return Err(source).context(ReadConfigFileSnafu {
                path: path.to_path_buf(),
            });
        }
    };
    let config: ConfigFile = toml::from_str(&data).context(ParseConfigFileSnafu {
        path: PathBuf::from(path),
    })?;
    Ok(ProviderProfiles::new(config.providers))
}

#[cfg(not(test))]
fn missing_config_profiles() -> ProviderProfiles {
    ProviderProfiles::default()
}

#[cfg(test)]
fn missing_config_profiles() -> ProviderProfiles {
    test_provider_profiles()
}

#[cfg(test)]
fn test_provider_profiles() -> ProviderProfiles {
    ProviderProfiles::from_profiles(HashMap::from([(
        "openai-codex".to_owned(),
        ProviderProfile {
            provider: "chatgpt".to_owned(),
            secrets: BTreeMap::new(),
            base_url: None,
            default_model: Some("gpt-5.4".to_owned()),
        },
    )]))
}
