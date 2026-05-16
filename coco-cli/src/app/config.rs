#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use coco_mem::{ProviderProfile, ProviderProfileStore, StoreError, StoreResult};
use serde::Deserialize;
use snafu::prelude::*;

use crate::{
    Result,
    error::{
        InvalidChannelSecretReferenceSnafu, ParseConfigFileSnafu, ReadChannelSecretEnvSnafu,
        ReadConfigFileSnafu, ResolveCurrentDirSnafu,
    },
};

const CONFIG_FILE_NAME: &str = "config.toml";

#[derive(Clone, Debug, Default)]
pub(crate) struct Config {
    pub provider_profiles: ProviderProfiles,
    pub channels: ChannelConfigs,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ProviderProfiles {
    profiles: HashMap<String, ProviderProfile>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub(crate) struct ChannelConfigs {
    #[serde(default)]
    pub telegram: Option<TelegramChannelConfig>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct TelegramChannelConfig {
    #[serde(default)]
    pub enabled: bool,
    pub token: String,
    #[serde(default = "default_telegram_branch")]
    pub branch: String,
    #[serde(default = "default_telegram_poll_timeout_secs")]
    pub poll_timeout_secs: u64,
    #[serde(default)]
    pub allowed_chat_ids: BTreeSet<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    providers: HashMap<String, ProviderProfile>,
    #[serde(default)]
    channels: ChannelConfigs,
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
    Ok(load_cwd_config()?.provider_profiles)
}

pub(crate) fn load_cwd_config() -> Result<Config> {
    let current_dir = std::env::current_dir().context(ResolveCurrentDirSnafu)?;
    load_config_from(current_dir.join(CONFIG_FILE_NAME))
}

pub(crate) fn load_config_from(path: impl AsRef<Path>) -> Result<Config> {
    let path = path.as_ref();
    let data = match fs::read_to_string(path) {
        Ok(data) => Some(data),
        Err(source) if source.kind() == ErrorKind::NotFound => None,
        Err(source) => {
            return Err(source).context(ReadConfigFileSnafu {
                path: path.to_path_buf(),
            });
        }
    };
    let config = match data {
        Some(data) => toml::from_str::<ConfigFile>(&data).context(ParseConfigFileSnafu {
            path: PathBuf::from(path),
        })?,
        None => missing_config_file(),
    };
    Ok(Config {
        provider_profiles: ProviderProfiles::new(config.providers),
        channels: config.channels,
    })
}

pub(crate) fn resolve_channel_secret(channel: &str, value: &str) -> Result<String> {
    let name = parse_env_placeholder(value).ok_or_else(|| {
        InvalidChannelSecretReferenceSnafu {
            channel: channel.to_owned(),
            value: value.to_owned(),
        }
        .build()
    })?;

    std::env::var(name).context(ReadChannelSecretEnvSnafu {
        name: name.to_owned(),
    })
}

#[cfg(not(test))]
fn missing_config_file() -> ConfigFile {
    ConfigFile::default()
}

#[cfg(test)]
fn missing_config_file() -> ConfigFile {
    ConfigFile {
        providers: test_provider_profiles().profiles,
        channels: ChannelConfigs::default(),
    }
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
            spec: Default::default(),
        },
    )]))
}

fn default_telegram_branch() -> String {
    "main".to_owned()
}

fn default_telegram_poll_timeout_secs() -> u64 {
    30
}

fn parse_env_placeholder(value: &str) -> Option<&str> {
    value
        .strip_prefix("${")
        .and_then(|value| value.strip_suffix('}'))
        .filter(|value| !value.is_empty())
}
