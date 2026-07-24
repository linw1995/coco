use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use coco_console::{ConsolePublisher, ConsoleStore};
use coco_core::ConversationEngine;
use coco_core::CoreSkillSearchExecutor;
use coco_llm::{
    CocoCliRuntimeResponse, CompletionBackend, LlmRuntimeBridge, LlmService, ProviderRuntimeConfig,
    RigBackend, UnifiedExecCliBridgeHandle,
};
use coco_mem::{ProcessShareableStore, Store};
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

pub mod config;
pub mod daemon;
mod preset;
mod prompt;
pub mod runtime;
mod session;
mod skill;

#[cfg(test)]
pub use session::resolve_session_config as resolve_session_config_with_store;

pub async fn run<R>(cli: Cli, reader: &mut R) -> Result<Option<String>>
where
    R: Read,
{
    run_with_backend(cli, reader, RigBackend::default()).await
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
    let shared_store = open_store_for_command(&store_path, &command).await?;
    let config = config::load_cwd_config()?;
    let provider_profiles = config.provider_profiles;
    let channel_configs = config.channels;
    let credential_routes = config.credential_routes;
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
                Some(shared_store.store_path().to_path_buf()),
                credential_routes,
            );
            daemon::run_daemon_command(
                command,
                &shared_store,
                &llm,
                &provider_profiles,
                &channel_configs,
                Some(console_publisher),
                shared_store.store_path().to_path_buf(),
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
                Some(shared_store.store_path().to_path_buf()),
                credential_routes,
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
    store_path: Option<PathBuf>,
    credential_routes: Vec<coco_llm::NonoCredentialRoute>,
) -> Arc<LlmService<B, S>>
where
    B: CompletionBackend + 'static,
    S: Store + Clone + Send + Sync + 'static,
{
    Arc::new_cyclic(|weak_llm| {
        let provider_profiles = provider_profiles.clone();
        let unified_exec_bridge_impl = Arc::new(LlmRuntimeBridge::new(weak_llm.clone(), {
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
        let unified_exec_bridge = UnifiedExecCliBridgeHandle::new(unified_exec_bridge_impl);
        let skill_bridge = Arc::new(CoreSkillSearchExecutor::new(weak_llm.clone()));
        LlmService::builder(shared_store.clone(), backend)
            .with_provider_configs(provider_configs)
            .with_unified_exec_cli_bridge(unified_exec_bridge)
            .with_skill_search_executor(skill_bridge)
            .with_optional_store_path(store_path)
            .with_credential_routes(credential_routes)
            .build()
    })
}

fn resolve_provider_runtime_configs(
    store: &impl config::ProviderProfileLookup,
) -> Result<HashMap<String, ProviderRuntimeConfig>> {
    store
        .list_provider_profiles()
        .context(StoreSnafu)?
        .into_iter()
        .map(|(name, profile)| {
            let provider = coco_llm::Provider::parse(&profile.provider).context(LlmSnafu)?;
            let additional_params = coco_llm::provider_profile_additional_params(&profile);
            let secrets = resolve_provider_secrets(&name, profile.secrets)?;
            Ok((
                name,
                ProviderRuntimeConfig {
                    provider,
                    secrets,
                    base_url: profile.base_url,
                    default_model: profile.default_model,
                    additional_params,
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
            spec: Default::default(),
        },
    );
    let provider_profiles = config::ProviderProfiles::from_profiles(profiles);
    session::resolve_session_config(command, &provider_profiles)
}
