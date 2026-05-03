use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub struct SchedulerCommand {
    #[command(subcommand)]
    pub command: SchedulerSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum SchedulerSubcommand {
    Add(SchedulerAddCommand),
    Update(SchedulerUpdateCommand),
    Delete(SchedulerDeleteCommand),
    List(SchedulerListCommand),
    Show(SchedulerShowCommand),
}

#[derive(Debug, Args)]
pub struct SchedulerAddCommand {
    #[arg(long)]
    pub id: String,

    #[arg(long, default_value = "main")]
    pub branch: String,

    #[arg(long)]
    pub interval_secs: u64,

    #[arg(long, conflicts_with = "initial_delay_secs")]
    pub next_run_at: Option<String>,

    #[arg(long, conflicts_with = "next_run_at")]
    pub initial_delay_secs: Option<u64>,

    #[arg(long)]
    pub disabled: bool,

    #[arg(long)]
    pub json: bool,

    #[arg(required = true)]
    pub prompt: Vec<String>,
}

#[derive(Debug, Args)]
pub struct SchedulerUpdateCommand {
    pub id: String,

    #[arg(long)]
    pub branch: Option<String>,

    #[arg(long)]
    pub prompt: Option<String>,

    #[arg(long)]
    pub interval_secs: Option<u64>,

    #[arg(long, conflicts_with = "initial_delay_secs")]
    pub next_run_at: Option<String>,

    #[arg(long, conflicts_with = "next_run_at")]
    pub initial_delay_secs: Option<u64>,

    #[arg(long, conflicts_with = "disable")]
    pub enable: bool,

    #[arg(long, conflicts_with = "enable")]
    pub disable: bool,

    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct SchedulerDeleteCommand {
    pub id: String,
}

#[derive(Debug, Args)]
pub struct SchedulerListCommand {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct SchedulerShowCommand {
    pub id: String,

    #[arg(long)]
    pub json: bool,
}
