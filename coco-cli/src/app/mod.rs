use std::io::Read;
use std::sync::{Arc, Weak};

use coco_llm::{
    BashToolCliBridge, BashToolCliBridgeError, BashToolCliBridgeHandle, CocoCliRuntimeRequest,
    CocoCliRuntimeResponse, CompletionBackend, LlmService, RigBackend, SessionConfig,
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
        let bridge = BashToolCliBridgeHandle::new(Arc::new(CocoCliRuntimeBridge {
            store: shared_store.clone(),
            llm: weak_llm.clone(),
        }));
        LlmService::new(shared_store.clone(), backend).with_bash_tool_cli_bridge(bridge)
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
    B: CompletionBackend,
    R: Read,
{
    runtime::run_with_services(cli, reader, shared_store, llm).await
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
    B: CompletionBackend,
{
    runtime::run_forwarded_with_services(args, stdin, branch_env, store_path_env, shared_store, llm)
        .await
}

#[derive(Debug)]
struct CocoCliRuntimeBridge<B>
where
    B: CompletionBackend,
{
    store: FsStore,
    llm: Weak<LlmService<B, FsStore>>,
}

#[async_trait::async_trait]
impl<B> BashToolCliBridge for CocoCliRuntimeBridge<B>
where
    B: CompletionBackend,
{
    async fn execute_coco_cli(
        &self,
        request: CocoCliRuntimeRequest,
    ) -> std::result::Result<CocoCliRuntimeResponse, BashToolCliBridgeError> {
        let llm = self
            .llm
            .upgrade()
            .ok_or(BashToolCliBridgeError::Unavailable)?;
        Ok(run_forwarded_with_services(
            &request.args,
            &request.stdin,
            request.branch_env.as_deref(),
            request.store_path_env.as_deref(),
            &self.store,
            &llm,
        )
        .await)
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn resolve_session_config(command: SessionCreateCommand) -> Result<SessionConfig> {
    session::resolve_session_config(command)
}
