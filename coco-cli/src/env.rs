use snafu::prelude::*;

use crate::{
    Result,
    cli::{CliProvider, CliTool},
    error::{
        InvalidProviderConfigurationSnafu, InvalidToolConfigurationSnafu, MissingConfigurationSnafu,
    },
};

pub fn resolve_env_provider() -> Result<CliProvider> {
    let value = read_env("COCO_PROVIDER").context(MissingConfigurationSnafu {
        name: "COCO_PROVIDER",
    })?;
    CliProvider::parse(&value).context(InvalidProviderConfigurationSnafu {
        source_name: "COCO_PROVIDER",
        value,
    })
}

pub fn read_env(name: &'static str) -> Option<String> {
    std::env::var(name).ok()
}

pub fn resolve_env_tools() -> Result<Vec<CliTool>> {
    let Some(value) = read_env("COCO_TOOLS") else {
        return Ok(vec![]);
    };

    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            CliTool::parse(value).context(InvalidToolConfigurationSnafu {
                source_name: "COCO_TOOLS",
                value: value.to_owned(),
            })
        })
        .collect()
}
