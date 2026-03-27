use clap::Parser;

use coco_cli::{Cli, run};

#[tokio::main]
async fn main() {
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
