use std::io::Read;
use std::sync::Arc;

use coco_core::ConversationEngine;
use coco_core::CoreSkillToolExecutor;
use coco_llm::{
    BashToolCliBridgeHandle, CocoCliRuntimeResponse, CompletionBackend, LlmRuntimeBridge,
    LlmService, RigBackend, SessionConfig,
};
use coco_mem::FsStore;

use crate::{Cli, Result, cli::SessionCreateCommand, store::open_store};

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
    let shared_store = open_store(&cli.store_path)?;
    let llm = build_llm_service(shared_store.clone(), backend);

    run_with_services(cli, reader, &shared_store, &llm).await
}

pub async fn run_with_services<B, R>(
    cli: Cli,
    reader: &mut R,
    shared_store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
) -> Result<Option<String>>
where
    B: CompletionBackend + 'static,
    R: Read,
{
    runtime::run_with_services(
        cli,
        reader,
        runtime::RuntimeServices {
            shared_store,
            llm,
            shared_engine: None,
        },
        false,
    )
    .await
}

fn build_llm_service<B>(shared_store: FsStore, backend: B) -> Arc<LlmService<B, FsStore>>
where
    B: CompletionBackend + 'static,
{
    Arc::new_cyclic(|weak_llm| {
        let bash_bridge_impl = Arc::new(LlmRuntimeBridge::new(weak_llm.clone(), {
            let shared_store = shared_store.clone();
            move |request, llm| {
                let shared_store = shared_store.clone();
                async move {
                    run_forwarded_with_services(
                        runtime::ForwardedRuntimeInputs {
                            args: &request.args,
                            stdin: &request.stdin,
                            branch_env: request.branch_env.as_deref(),
                            session_role: request.session_role,
                            store_path_env: request.store_path_env.as_deref(),
                        },
                        runtime::RuntimeServices {
                            shared_store: &shared_store,
                            llm: &llm,
                            shared_engine: None::<&Arc<ConversationEngine<B, FsStore>>>,
                        },
                    )
                    .await
                }
            }
        }));
        let bash_bridge = BashToolCliBridgeHandle::new(bash_bridge_impl);
        let skill_bridge = Arc::new(CoreSkillToolExecutor::new(weak_llm.clone()));
        LlmService::builder(shared_store.clone(), backend)
            .with_bash_tool_cli_bridge(bash_bridge)
            .with_skill_tool_executor(skill_bridge)
            .build()
    })
}

pub async fn run_forwarded_with_services<B>(
    inputs: runtime::ForwardedRuntimeInputs<'_>,
    services: runtime::RuntimeServices<'_, B>,
) -> CocoCliRuntimeResponse
where
    B: CompletionBackend + 'static,
{
    runtime::run_forwarded_with_services(inputs, services).await
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn resolve_session_config(command: SessionCreateCommand) -> Result<SessionConfig> {
    session::resolve_session_config(command)
}
