use std::io::Read;
use std::sync::Arc;

use coco_core::CoreSkillToolExecutor;
use coco_llm::{
    BashToolCliBridgeHandle, CocoCliRuntimeResponse, CompletionBackend, LlmRuntimeBridge,
    LlmService, RigBackend, SessionConfig,
};
use coco_mem::FsStore;

use crate::{Cli, Result, cli::SessionCreateCommand, store::open_store};

mod prompt;
mod runtime;
mod session;

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
    let llm = Arc::new_cyclic(|weak_llm| {
        let bash_bridge_impl = Arc::new(LlmRuntimeBridge::new(weak_llm.clone(), {
            let shared_store = shared_store.clone();
            move |request, llm| {
                let shared_store = shared_store.clone();
                async move {
                    run_forwarded_with_services(
                        &request.args,
                        &request.stdin,
                        request.branch_env.as_deref(),
                        request.store_path_env.as_deref(),
                        &shared_store,
                        &llm,
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
    });

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
    runtime::run_with_services(cli, reader, shared_store, llm, false).await
}

pub async fn run_forwarded_with_services<B>(
    args: &[String],
    stdin: &[u8],
    branch_env: Option<&str>,
    store_path_env: Option<&str>,
    shared_store: &FsStore,
    llm: &Arc<LlmService<B, FsStore>>,
) -> CocoCliRuntimeResponse
where
    B: CompletionBackend + 'static,
{
    runtime::run_forwarded_with_services(args, stdin, branch_env, store_path_env, shared_store, llm)
        .await
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn resolve_session_config(command: SessionCreateCommand) -> Result<SessionConfig> {
    session::resolve_session_config(command)
}
