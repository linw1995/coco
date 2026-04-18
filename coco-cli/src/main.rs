use std::io::Read;

use clap::Parser;
use coco_llm::{
    COCO_CLI_RUNTIME_SOCKET_ENV, COCO_COMMAND_SHIM_MODE_ENV, COCO_SESSION_BRANCH_ENV,
    COCO_SESSION_ROLE_ENV, COCO_STORE_PATH_ENV, CocoCliRuntimeRequest, CocoCliRuntimeResponse,
};
use coco_mem::SessionRole;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use coco_cli::{Cli, run};

#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if shim_mode_is_disabled() {
        eprintln!(
            "coco command is not enabled for this bash session; enable the coco shim to use CoCo CLI commands."
        );
        std::process::exit(1);
    }
    if let Some(socket_path) = resolve_runtime_socket(&args[1..]) {
        forward_to_runtime(&socket_path, &args[1..]).await;
    }

    let cli = Cli::parse();

    match run(cli, &mut std::io::stdin()).await {
        Ok(Some(output)) => println!("{output}"),
        Ok(None) => {}
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn shim_mode_is_disabled() -> bool {
    matches!(
        std::env::var(COCO_COMMAND_SHIM_MODE_ENV).ok().as_deref(),
        Some("disabled")
    )
}

fn resolve_runtime_socket(args: &[String]) -> Option<String> {
    let _ = args;
    std::env::var(COCO_CLI_RUNTIME_SOCKET_ENV).ok()
}

async fn forward_to_runtime(socket_path: &str, args: &[String]) -> ! {
    let mut stdin = Vec::new();
    std::io::stdin()
        .read_to_end(&mut stdin)
        .expect("failed to read stdin for coco-cli runtime forwarding");

    let request = CocoCliRuntimeRequest {
        args: args.to_vec(),
        stdin,
        branch_env: std::env::var(COCO_SESSION_BRANCH_ENV).ok(),
        session_role: std::env::var(COCO_SESSION_ROLE_ENV)
            .ok()
            .and_then(|value| SessionRole::parse(&value)),
        store_path_env: std::env::var(COCO_STORE_PATH_ENV).ok(),
    };
    let payload =
        serde_json::to_vec(&request).expect("failed to serialize coco-cli runtime request");

    let mut stream = UnixStream::connect(socket_path)
        .await
        .expect("failed to connect to coco-cli runtime socket");
    stream
        .write_all(&payload)
        .await
        .expect("failed to send coco-cli runtime request");
    stream
        .shutdown()
        .await
        .expect("failed to close coco-cli runtime request stream");

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("failed to read coco-cli runtime response");
    let response: CocoCliRuntimeResponse =
        serde_json::from_slice(&response).expect("failed to parse coco-cli runtime response");
    print!("{}", response.stdout);
    eprint!("{}", response.stderr);
    std::process::exit(response.exit_code);
}
