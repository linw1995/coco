// grcov: ignore-start
use std::env;
use std::fs;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::process::{Command, ExitStatus};

use flate2::Compression;
use flate2::write::GzEncoder;
use snafu::prelude::*;

type BuildResult<T> = Result<T, BuildError>;

const WASM_TARGET: &str = "wasm32-unknown-unknown";
const WASM_PROFILE: &str = "wasm-release";
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

    #[snafu(display("Failed to read wasm asset {}: {source}", path.display()))]
    ReadAsset { path: PathBuf, source: io::Error },

    #[snafu(display("Failed to write wasm asset {}: {source}", path.display()))]
    WriteAsset { path: PathBuf, source: io::Error },

    #[snafu(display("Failed to replace {} with {}: {source}", target.display(), source_path.display()))]
    ReplaceAsset {
        source_path: PathBuf,
        target: PathBuf,
        source: io::Error,
    },

    #[snafu(display("Failed to compress wasm asset with {encoding}: {source}"))]
    CompressAsset {
        encoding: &'static str,
        source: io::Error,
    },

    #[snafu(display("Failed to run {program}: {source}"))]
    RunCommand { program: String, source: io::Error },

    #[snafu(display("{program} exited with {status}"))]
    CommandFailed { program: String, status: ExitStatus },
}

fn main() {
    if let Err(error) = run() {
        panic!("failed to build coco-console wasm client: {error}");
    }
}

fn run() -> BuildResult<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/panels.rs");
    println!("cargo:rerun-if-changed=src/api.rs");
    println!("cargo:rerun-if-changed=src/wasm/anchor_range.rs");
    println!("cargo:rerun-if-changed=src/wasm/client.rs");
    println!("cargo:rerun-if-changed=src/wasm/viewport.rs");
    println!("cargo:rerun-if-changed=web-graph-migrations");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=../coco-types/Cargo.toml");
    println!("cargo:rerun-if-changed=../coco-types/src");

    if env::var("TARGET").is_ok_and(|target| target == WASM_TARGET) {
        return Ok(());
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").context(ReadEnvSnafu {
        name: "CARGO_MANIFEST_DIR",
    })?);
    let out_dir = PathBuf::from(env::var("OUT_DIR").context(ReadEnvSnafu { name: "OUT_DIR" })?);
    let wasm_target_dir = out_dir.join("wasm-target");
    let wasm_file = wasm_target_dir
        .join(WASM_TARGET)
        .join(WASM_PROFILE)
        .join("coco_console.wasm");
    let pkg_dir = out_dir.join("pkg");

    let mut wasm_build = Command::new("cargo");
    wasm_build
        .arg("rustc")
        .arg("--manifest-path")
        .arg(manifest_dir.join("Cargo.toml"))
        .arg("--target")
        .arg(WASM_TARGET)
        .arg("--profile")
        .arg(WASM_PROFILE)
        .arg("--lib")
        .arg("--crate-type")
        .arg("cdylib")
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
    optimize_wasm(&pkg_dir)?;
    precompress_wasm(&pkg_dir)?;
    let asset_version = asset_version(&pkg_dir)?;
    println!("cargo:rustc-env=COCO_CONSOLE_ASSET_VERSION={asset_version}");
    Ok(())
}

fn optimize_wasm(pkg_dir: &std::path::Path) -> BuildResult<()> {
    let wasm_path = pkg_dir.join("coco_console_bg.wasm");
    let optimized_path = pkg_dir.join("coco_console_bg.opt.wasm");
    run_command(
        Command::new("wasm-opt")
            .arg("-Oz")
            .arg(&wasm_path)
            .arg("-o")
            .arg(&optimized_path),
    )?;
    fs::rename(&optimized_path, &wasm_path).context(ReplaceAssetSnafu {
        source_path: optimized_path,
        target: wasm_path,
    })
}

fn precompress_wasm(pkg_dir: &std::path::Path) -> BuildResult<()> {
    let wasm_path = pkg_dir.join("coco_console_bg.wasm");
    let bytes = fs::read(&wasm_path).context(ReadAssetSnafu { path: wasm_path })?;

    let mut brotli_bytes = Vec::new();
    brotli::CompressorReader::new(bytes.as_slice(), 4096, 11, 22)
        .read_to_end(&mut brotli_bytes)
        .context(CompressAssetSnafu { encoding: "br" })?;
    let brotli_path = pkg_dir.join("coco_console_bg.wasm.br");
    fs::write(&brotli_path, brotli_bytes).context(WriteAssetSnafu { path: brotli_path })?;

    let mut gzip = GzEncoder::new(Vec::new(), Compression::best());
    gzip.write_all(&bytes)
        .context(CompressAssetSnafu { encoding: "gzip" })?;
    let gzip_bytes = gzip
        .finish()
        .context(CompressAssetSnafu { encoding: "gzip" })?;
    let gzip_path = pkg_dir.join("coco_console_bg.wasm.gz");
    fs::write(&gzip_path, gzip_bytes).context(WriteAssetSnafu { path: gzip_path })?;
    Ok(())
}

fn asset_version(pkg_dir: &std::path::Path) -> BuildResult<String> {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for name in ["coco_console.js", "coco_console_bg.wasm"] {
        let path = pkg_dir.join(name);
        let bytes = fs::read(&path).context(ReadAssetSnafu { path })?;
        for byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    Ok(format!("{hash:016x}"))
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

// grcov: ignore-end
