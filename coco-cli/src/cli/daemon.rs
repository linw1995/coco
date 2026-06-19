use std::net::SocketAddr;
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
    Profile(DaemonProfileCommand),
}

#[derive(Debug, Args)]
pub struct DaemonServeCommand {
    #[arg(long, env = COCO_DAEMON_SOCKET_ENV)]
    pub socket: Option<PathBuf>,

    #[arg(long, default_value = "127.0.0.1:17667")]
    pub console_addr: SocketAddr,

    #[arg(long)]
    pub no_console: bool,
}

#[derive(Debug, Args)]
pub struct DaemonProfileCommand {
    #[command(subcommand)]
    pub command: DaemonProfileSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum DaemonProfileSubcommand {
    Graph(DaemonProfileGraphCommand),
}

#[derive(Debug, Args)]
pub struct DaemonProfileGraphCommand {
    #[arg(long)]
    pub all: bool,

    #[arg(long)]
    pub json: bool,
}
