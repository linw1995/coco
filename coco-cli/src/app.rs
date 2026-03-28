use std::io::Read;
use std::sync::Arc;

use coco_core::{ConversationEngine, CoreService, FixedBranchResolver, InboundMessage};
use coco_llm::{CompletionBackend, LlmService, RigBackend, SessionConfig};
use snafu::prelude::*;

use crate::{
    Result,
    cli::{Cli, Command, PromptCommand, SessionCommand, SessionCreateCommand, SessionSubcommand},
    env::{read_env, resolve_env_provider},
    error::{CoreSnafu, EmptyPromptSnafu, LlmSnafu, MissingConfigurationSnafu, ReadStdinSnafu},
    store::open_store,
};

pub async fn run<R>(cli: Cli, reader: &mut R) -> Result<Option<String>>
where
    R: Read,
{
    run_with_backend(cli, reader, RigBackend).await
}

pub async fn run_with_backend<B, R>(cli: Cli, reader: &mut R, backend: B) -> Result<Option<String>>
where
    B: CompletionBackend,
    R: Read,
{
    let shared_store = open_store(&cli.store_path)?;
    let llm = Arc::new(LlmService::new(shared_store.clone(), backend));

    match cli.command {
        Command::Prompt(command) => {
            let input = resolve_prompt_input(&command, reader)?;
            let service = CoreService::new(
                FixedBranchResolver::new(command.branch),
                ConversationEngine::new(llm),
            );
            let response = service
                .handle_message(InboundMessage::cli("cli", "cli", input))
                .await
                .context(CoreSnafu)?;
            Ok(Some(response.text))
        }
        Command::Session(SessionCommand {
            command: SessionSubcommand::Create(command),
        }) => {
            let config = resolve_session_config(command)?;
            llm.create_session(config).await.context(LlmSnafu)?;
            Ok(None)
        }
    }
}

pub fn resolve_session_config(command: SessionCreateCommand) -> Result<SessionConfig> {
    let provider = resolve_env_provider()?;
    let model = read_env("COCO_MODEL").context(MissingConfigurationSnafu { name: "COCO_MODEL" })?;

    Ok(SessionConfig {
        branch: command.branch,
        merge_parents: vec![],
        provider: provider.into(),
        model,
        system_prompt: command.system_prompt,
        prompt: command.prompt,
        tools: vec![],
        temperature: command.temperature,
        max_tokens: command.max_tokens,
        additional_params: None,
    })
}

pub fn resolve_prompt_input<R>(command: &PromptCommand, reader: &mut R) -> Result<String>
where
    R: Read,
{
    let text = if command.text.is_empty() {
        let mut buffer = String::new();
        reader.read_to_string(&mut buffer).context(ReadStdinSnafu)?;
        buffer.trim_end_matches(['\r', '\n']).to_owned()
    } else {
        command.text.join(" ")
    };

    ensure!(!text.trim().is_empty(), EmptyPromptSnafu);
    Ok(text)
}
