use snafu::prelude::*;

use crate::{
    Result,
    cli::CliProvider,
    error::{InvalidProviderConfigurationSnafu, MissingConfigurationSnafu},
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
