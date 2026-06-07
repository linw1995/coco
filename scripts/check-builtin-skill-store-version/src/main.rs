use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

const DEFAULT_SKILLS_PREFIX: &str = "coco-mem/src/default_skills/";
const STORE_FILE: &str = "coco-mem/src/store/fs.rs";
const STORE_VERSION_MARKER: &str = "const STORE_FORMAT_VERSION:";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let root = repository_root()?;
    let base = comparison_base(&root)?;
    let files = changed_files(&root, &base)?;
    let builtin_skill_changes = files
        .iter()
        .filter(|path| path.starts_with(DEFAULT_SKILLS_PREFIX))
        .collect::<Vec<_>>();

    if builtin_skill_changes.is_empty() || store_format_version_changed(&root, &base)? {
        return Ok(());
    }

    let mut message = String::from(
        "Builtin skill defaults changed without updating STORE_FORMAT_VERSION.\n\
         Update coco-mem/src/store/fs.rs with a new STORE_FORMAT_VERSION and store migration \
         so existing stores can receive the builtin skill migration.\n\
         Changed builtin skill files:",
    );
    for path in builtin_skill_changes {
        message.push_str("\n  ");
        message.push_str(path);
    }
    Err(message)
}

fn repository_root() -> Result<PathBuf, String> {
    let root = git_output(Path::new("."), &["rev-parse", "--show-toplevel"])?;
    Ok(PathBuf::from(root))
}

fn comparison_base(root: &Path) -> Result<String, String> {
    if ref_exists(root, "origin/main") {
        let merge_base = git_output(root, &["merge-base", "origin/main", "HEAD"])?;
        if !merge_base.is_empty() {
            return Ok(merge_base);
        }
    }

    if ref_exists(root, "HEAD^1") {
        return Ok("HEAD^1".to_owned());
    }

    Err("unable to determine a git base for builtin skill store version check".to_owned())
}

fn ref_exists(root: &Path, reference: &str) -> bool {
    git_status(root, &["rev-parse", "--verify", "--quiet", reference]).is_ok()
}

fn changed_files(root: &Path, base: &str) -> Result<Vec<String>, String> {
    let mut files = BTreeSet::new();
    for args in [
        vec!["diff", "--name-only", &format!("{base}..HEAD")],
        vec!["diff", "--name-only", "--cached"],
        vec!["diff", "--name-only"],
    ] {
        for path in git_output(root, &args)?.lines() {
            files.insert(path.to_owned());
        }
    }
    Ok(files.into_iter().collect())
}

fn store_format_version_changed(root: &Path, base: &str) -> Result<bool, String> {
    for args in [
        vec!["diff", "-U0", &format!("{base}..HEAD"), "--", STORE_FILE],
        vec!["diff", "-U0", "--cached", "--", STORE_FILE],
        vec!["diff", "-U0", "--", STORE_FILE],
    ] {
        let diff = git_output(root, &args)?;
        if diff.lines().any(store_format_version_diff_line) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn store_format_version_diff_line(line: &str) -> bool {
    (line.starts_with('+') || line.starts_with('-'))
        && !line.starts_with("+++")
        && !line.starts_with("---")
        && line.contains(STORE_VERSION_MARKER)
}

fn git_output(root: &Path, args: &[&str]) -> Result<String, String> {
    let output = git(root, args)?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn git_status(root: &Path, args: &[&str]) -> Result<(), String> {
    let output = git(root, args)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
    }
}

fn git(root: &Path, args: &[&str]) -> Result<std::process::Output, String> {
    Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .map_err(|source| format!("failed to run git {}: {source}", args.join(" ")))
}
