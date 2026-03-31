use clap::Args;

#[derive(Debug, Args)]
pub struct PromptCommand {
    #[arg(long, env = "COCO_BRANCH", default_value = "main")]
    pub branch: String,

    #[arg(value_name = "TEXT")]
    pub text: Vec<String>,
}
