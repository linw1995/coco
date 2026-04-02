use std::io::Read;

use snafu::prelude::*;

use crate::{
    Result,
    cli::PromptCommand,
    error::{EmptyPromptSnafu, ReadStdinSnafu},
};

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
