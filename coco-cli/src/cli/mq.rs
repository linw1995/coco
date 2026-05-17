use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub struct MqCommand {
    #[command(subcommand)]
    pub command: MqSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum MqSubcommand {
    Enqueue(MqEnqueueCommand),
}

#[derive(Debug, Args)]
pub struct MqEnqueueCommand {
    #[arg(long)]
    pub queue: String,

    #[arg(long = "payload-json")]
    pub payload_json: String,

    #[arg(long)]
    pub json: bool,
}
