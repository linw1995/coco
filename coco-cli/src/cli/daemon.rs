use std::path::PathBuf;

use clap::{Args, Subcommand};

use crate::COCO_DAEMON_SOCKET_ENV;

#[derive(Debug, Args)]
pub struct DaemonCommand {
    #[command(subcommand)]
    pub command: DaemonSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum DaemonSubcommand {
    Serve(DaemonServeCommand),
}

#[derive(Debug, Args)]
pub struct DaemonServeCommand {
    #[arg(long, env = COCO_DAEMON_SOCKET_ENV)]
    pub socket: Option<PathBuf>,
}
