use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

use snafu::prelude::*;

type BuildResult<T> = Result<T, BuildError>;

const WASM_TARGET: &str = "wasm32-unknown-unknown";
const SKIP_ENV: &str = "COCO_CONSOLE_SKIP_WASM_BUILD";
const BUILDING_ENV: &str = "COCO_CONSOLE_BUILDING_WASM";
const JS_ASSET: &str = "coco_console.js";
const WASM_ASSET: &str = "coco_console_bg.wasm";
const COVERAGE_ENV_VARS: &[&str] = &["RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "LLVM_PROFILE_FILE"];

#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("Failed to read environment variable {name}: {source}"))]
    ReadEnv {
        name: &'static str,
        source: env::VarError,
    },

    #[snafu(display("Failed to create wasm package directory {}: {source}", path.display()))]
    CreatePackageDirectory { path: PathBuf, source: io::Error },

    #[snafu(display("Failed to run {program}: {source}"))]
    RunCommand { program: String, source: io::Error },

    #[snafu(display("{program} exited with {status}"))]
    CommandFailed { program: String, status: ExitStatus },

    #[snafu(display("Failed to read generated loader {}: {source}", path.display()))]
    ReadGeneratedLoader { path: PathBuf, source: io::Error },

    #[snafu(display("Failed to write generated loader {}: {source}", path.display()))]
    WriteGeneratedLoader { path: PathBuf, source: io::Error },
}

fn main() {
    if let Err(error) = run() {
        panic!("failed to build coco-console wasm client: {error}");
    }
}

fn run() -> BuildResult<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/client.rs");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-env-changed={SKIP_ENV}");

    if env::var("TARGET").is_ok_and(|target| target == WASM_TARGET)
        || env::var_os(BUILDING_ENV).is_some()
    {
        return Ok(());
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").context(ReadEnvSnafu {
        name: "CARGO_MANIFEST_DIR",
    })?);
    let out_dir = PathBuf::from(env::var("OUT_DIR").context(ReadEnvSnafu { name: "OUT_DIR" })?);
    let wasm_target_dir = out_dir.join("wasm-target");
    let wasm_file = wasm_target_dir
        .join(WASM_TARGET)
        .join("debug")
        .join("coco_console.wasm");
    let pkg_dir = out_dir.join("pkg");

    if env::var_os(SKIP_ENV).is_some() {
        prepare_skipped_package(&pkg_dir)?;
        return Ok(());
    }

    let mut wasm_build = Command::new("cargo");
    wasm_build
        .arg("build")
        .arg("--manifest-path")
        .arg(manifest_dir.join("Cargo.toml"))
        .arg("--target")
        .arg(WASM_TARGET)
        .env(BUILDING_ENV, "1")
        .env("CARGO_TARGET_DIR", &wasm_target_dir);
    // Host coverage flags require a profiler runtime that wasm32-unknown-unknown does not provide.
    // Keep coverage enabled for host tests, but build the generated wasm client without those flags.
    remove_host_coverage_env(&mut wasm_build);
    run_command(&mut wasm_build)?;

    fs::create_dir_all(&pkg_dir).context(CreatePackageDirectorySnafu {
        path: pkg_dir.clone(),
    })?;
    run_command(
        Command::new("wasm-bindgen")
            .arg("--target")
            .arg("web")
            .arg("--out-dir")
            .arg(&pkg_dir)
            .arg(&wasm_file),
    )?;
    append_auto_start(&pkg_dir.join(JS_ASSET))?;

    Ok(())
}

fn prepare_skipped_package(pkg_dir: &Path) -> BuildResult<()> {
    fs::create_dir_all(pkg_dir).context(CreatePackageDirectorySnafu {
        path: pkg_dir.to_path_buf(),
    })?;
    write_skipped_package_stubs(pkg_dir)?;
    Ok(())
}

fn write_skipped_package_stubs(pkg_dir: &Path) -> BuildResult<()> {
    let js_path = pkg_dir.join(JS_ASSET);
    fs::write(
        &js_path,
        "throw new Error('coco-console wasm client was not built');\n",
    )
    .context(WriteGeneratedLoaderSnafu { path: js_path })?;
    let wasm_path = pkg_dir.join(WASM_ASSET);
    fs::write(&wasm_path, []).context(WriteGeneratedLoaderSnafu { path: wasm_path })?;
    Ok(())
}

fn run_command(command: &mut Command) -> BuildResult<()> {
    let program = command.get_program().to_string_lossy().into_owned();
    let status = command.status().context(RunCommandSnafu {
        program: program.clone(),
    })?;
    ensure!(status.success(), CommandFailedSnafu { program, status });
    Ok(())
}

fn remove_host_coverage_env(command: &mut Command) {
    if !host_coverage_is_enabled() {
        return;
    }

    for name in COVERAGE_ENV_VARS {
        command.env_remove(name);
    }
}

fn host_coverage_is_enabled() -> bool {
    env::var_os("LLVM_PROFILE_FILE").is_some()
        || env::var("RUSTFLAGS").is_ok_and(|value| value.contains("instrument-coverage"))
        || env::var("CARGO_ENCODED_RUSTFLAGS")
            .is_ok_and(|value| value.contains("instrument-coverage"))
}

fn append_auto_start(path: &Path) -> BuildResult<()> {
    let mut js = fs::read_to_string(path).context(ReadGeneratedLoaderSnafu {
        path: path.to_path_buf(),
    })?;
    if !js.contains("__wbg_init();") {
        js.push_str("\n__wbg_init();\n");
        fs::write(path, js).context(WriteGeneratedLoaderSnafu {
            path: path.to_path_buf(),
        })?;
    }
    Ok(())
}
