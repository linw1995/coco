use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::sync::Arc;

use coco_console::{ConsolePublisher, ConsoleStore};
use coco_core::ConversationEngine;
use coco_core::CoreSkillToolExecutor;
use coco_llm::{
    BashToolCliBridgeHandle, CocoCliRuntimeResponse, CompletionBackend, LlmRuntimeBridge,
    LlmService, ProviderRuntimeConfig, RigBackend,
};
use coco_mem::{ProviderProfileStore, Store};
use snafu::prelude::*;

#[cfg(test)]
use crate::cli::SessionCreateCommand;
use crate::{
    Cli, Result,
    cli::Command,
    error::{LlmSnafu, StoreSnafu},
    store::open_store_for_command,
};
#[cfg(test)]
use coco_llm::SessionConfig;

pub(crate) mod config;
pub(crate) mod daemon;
mod preset;
mod prompt;
pub(crate) mod runtime;
mod session;
mod skill;

pub async fn run<R>(cli: Cli, reader: &mut R) -> Result<Option<String>>
where
    R: Read,
{
    run_with_backend(cli, reader, RigBackend).await
}

pub async fn run_with_backend<B, R>(cli: Cli, reader: &mut R, backend: B) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: Read,
{
    let Cli {
        daemon_socket,
        store_path,
        command,
    } = cli;
    let shared_store = open_store_for_command(&store_path, &command)?;
    let provider_profiles = config::load_cwd_provider_profiles()?;
    let provider_configs = resolve_provider_runtime_configs(&provider_profiles)?;
    match command {
        Command::Daemon(command) => {
            let console_publisher = ConsolePublisher::new();
            let shared_store = ConsoleStore::new(shared_store, console_publisher.clone());
            let llm = build_llm_service(
                shared_store.clone(),
                backend,
                provider_profiles.clone(),
                provider_configs,
            );
            daemon::run_daemon_command(
                command,
                &shared_store,
                &llm,
                &provider_profiles,
                Some(console_publisher),
            )
            .await
        }
        command => {
            let cli = Cli {
                daemon_socket,
                store_path,
                command,
            };
            let llm = build_llm_service(
                shared_store.clone(),
                backend,
                provider_profiles.clone(),
                provider_configs,
            );

            run_with_services_with_provider_profiles(
                cli,
                reader,
                &shared_store,
                &llm,
                &provider_profiles,
            )
            .await
        }
    }
}

#[allow(dead_code)]
pub async fn run_with_services<B, R, S>(
    cli: Cli,
    reader: &mut R,
    shared_store: &S,
    llm: &Arc<LlmService<B, S>>,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: Read,
    S: Store + Clone + Send + Sync + 'static,
{
    let provider_profiles = config::load_cwd_provider_profiles()?;
    run_with_services_with_provider_profiles(cli, reader, shared_store, llm, &provider_profiles)
        .await
}

async fn run_with_services_with_provider_profiles<B, R, S>(
    cli: Cli,
    reader: &mut R,
    shared_store: &S,
    llm: &Arc<LlmService<B, S>>,
    provider_profiles: &config::ProviderProfiles,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: Read,
    S: Store + Clone + Send + Sync + 'static,
{
    runtime::run_with_services(
        cli,
        reader,
        runtime::RuntimeServices {
            shared_store,
            llm,
            provider_profiles,
            shared_engine: None,
        },
        false,
    )
    .await
}

fn build_llm_service<B, S>(
    shared_store: S,
    backend: B,
    provider_profiles: config::ProviderProfiles,
    provider_configs: HashMap<String, ProviderRuntimeConfig>,
) -> Arc<LlmService<B, S>>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    Arc::new_cyclic(|weak_llm| {
        let provider_profiles = provider_profiles.clone();
        let bash_bridge_impl = Arc::new(LlmRuntimeBridge::new(weak_llm.clone(), {
            let shared_store = shared_store.clone();
            move |request, llm| {
                let shared_store = shared_store.clone();
                let provider_profiles = provider_profiles.clone();
                async move {
                    run_forwarded_with_services(
                        runtime::ForwardedRuntimeInputs {
                            args: &request.args,
                            stdin: &request.stdin,
                            branch_env: request.branch_env.as_deref(),
                            session_role: request.session_role,
                            store_path_env: request.store_path_env.as_deref(),
                            parent_tool_use_id_env: request.parent_tool_use_id_env.as_deref(),
                        },
                        runtime::RuntimeServices {
                            shared_store: &shared_store,
                            llm: &llm,
                            provider_profiles: &provider_profiles,
                            shared_engine: None::<&Arc<ConversationEngine<B, S>>>,
                        },
                    )
                    .await
                }
            }
        }));
        let bash_bridge = BashToolCliBridgeHandle::new(bash_bridge_impl);
        let skill_bridge = Arc::new(CoreSkillToolExecutor::new(weak_llm.clone()));
        LlmService::builder(shared_store.clone(), backend)
            .with_provider_configs(provider_configs)
            .with_bash_tool_cli_bridge(bash_bridge)
            .with_skill_tool_executor(skill_bridge)
            .build()
    })
}

fn resolve_provider_runtime_configs(
    store: &impl ProviderProfileStore,
) -> Result<HashMap<String, ProviderRuntimeConfig>> {
    store
        .list_provider_profiles()
        .context(StoreSnafu)?
        .into_iter()
        .map(|(name, profile)| {
            let provider = coco_llm::Provider::parse(&profile.provider).context(LlmSnafu)?;
            let secrets = resolve_provider_secrets(&name, profile.secrets)?;
            Ok((
                name,
                ProviderRuntimeConfig {
                    provider,
                    secrets,
                    base_url: profile.base_url,
                    default_model: profile.default_model,
                },
            ))
        })
        .collect()
}

fn resolve_provider_secrets(
    profile: &str,
    secret_refs: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    secret_refs
        .into_iter()
        .map(|(key, value)| {
            let env = parse_env_placeholder(&value).ok_or_else(|| {
                crate::Error::InvalidProviderSecretReference {
                    profile: profile.to_owned(),
                    key: key.clone(),
                    value: value.clone(),
                }
            })?;
            Ok((key, env.to_owned()))
        })
        .collect()
}

fn parse_env_placeholder(value: &str) -> Option<&str> {
    value
        .strip_prefix("${")
        .and_then(|value| value.strip_suffix('}'))
        .filter(|value| !value.is_empty())
}

pub async fn run_forwarded_with_services<B, S>(
    inputs: runtime::ForwardedRuntimeInputs<'_>,
    services: runtime::RuntimeServices<'_, B, S>,
) -> CocoCliRuntimeResponse
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    runtime::run_forwarded_with_services(inputs, services).await
}

#[cfg(test)]
pub fn resolve_session_config(command: SessionCreateCommand) -> Result<SessionConfig> {
    let mut profiles = HashMap::new();
    profiles.insert(
        "test-default".to_owned(),
        coco_mem::ProviderProfile {
            provider: "chatgpt".to_owned(),
            secrets: BTreeMap::new(),
            base_url: None,
            default_model: Some("gpt-5.4".to_owned()),
        },
    );
    let provider_profiles = config::ProviderProfiles::from_profiles(profiles);
    session::resolve_session_config(command, &provider_profiles)
}
