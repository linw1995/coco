// grcov: ignore-start
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

use flate2::Compression;
use flate2::write::GzEncoder;
use sha2::{Digest, Sha256};
use snafu::prelude::*;

type BuildResult<T> = Result<T, BuildError>;

const WASM_TARGET: &str = "wasm32-unknown-unknown";
const WASM_PROFILE: &str = "wasm-release";
const SPLIT_LINK_PLACEHOLDER: &str = "./__wasm_split.______________________.js";
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

    #[snafu(display("Failed to remove wasm package directory {}: {source}", path.display()))]
    RemovePackageDirectory { path: PathBuf, source: io::Error },

    #[snafu(display("Failed to read wasm package directory {}: {source}", path.display()))]
    ReadPackageDirectory { path: PathBuf, source: io::Error },

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

    #[snafu(display("Failed to split wasm client: {source}"))]
    SplitWasm {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to serialize wasm split manifest: {source}"))]
    SerializeSplitManifest { source: serde_json::Error },

    #[snafu(display("WASM split link placeholder was not found in {}", path.display()))]
    MissingSplitLinkPlaceholder { path: PathBuf },
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
    println!("cargo:rerun-if-changed=src/wasm");
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
    let split_wasm_file = wasm_file.with_file_name("coco_console_split.wasm");
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
    append_wasm_split_rustflag(&mut wasm_build);
    run_command(&mut wasm_build)?;

    if pkg_dir.exists() {
        fs::remove_dir_all(&pkg_dir).context(RemovePackageDirectorySnafu {
            path: pkg_dir.clone(),
        })?;
    }
    fs::create_dir_all(&pkg_dir).context(CreatePackageDirectorySnafu {
        path: pkg_dir.clone(),
    })?;
    let split_modules = split_wasm(&wasm_file, &split_wasm_file, &pkg_dir)?;
    run_command(
        Command::new("wasm-bindgen")
            .arg("--target")
            .arg("web")
            .arg("--keep-lld-exports")
            .arg("--no-demangle")
            .arg("--out-name")
            .arg("coco_console")
            .arg("--out-dir")
            .arg(&pkg_dir)
            .arg(&split_wasm_file),
    )?;
    let final_base_wasm = pkg_dir.join("coco_console_bg.wasm");
    let mut wasm_assets = split_modules;
    wasm_assets.push(final_base_wasm);
    optimize_wasm_assets(&wasm_assets)?;
    precompress_wasm_assets(&wasm_assets)?;
    let assets = runtime_assets(&pkg_dir)?;
    let asset_version = asset_version(&assets)?;
    generate_asset_table(&out_dir, &assets)?;
    println!("cargo:rustc-env=COCO_CONSOLE_ASSET_VERSION={asset_version}");
    Ok(())
}

fn split_wasm(
    wasm_file: &Path,
    split_wasm_file: &Path,
    pkg_dir: &Path,
) -> BuildResult<Vec<PathBuf>> {
    let input_wasm = fs::read(wasm_file).context(ReadAssetSnafu {
        path: wasm_file.to_path_buf(),
    })?;
    let split = wasm_split_cli_support::transform({
        let mut options = wasm_split_cli_support::Options::new(&input_wasm);
        options.output_dir = pkg_dir;
        options.main_out_path = split_wasm_file;
        options.main_module = "./coco_console.js";
        options.link_name = SPLIT_LINK_PLACEHOLDER;
        options
    })
    .map_err(Box::<dyn std::error::Error + Send + Sync>::from)
    .context(SplitWasmSnafu)?;
    finalize_split_link(pkg_dir, split_wasm_file)?;
    let prefetch_map = split.prefetch_map.into_iter().collect::<BTreeMap<_, _>>();
    let manifest = serde_json::to_vec_pretty(&prefetch_map).context(SerializeSplitManifestSnafu)?;
    let manifest_path = pkg_dir.join("__wasm_split_manifest.json");
    fs::write(&manifest_path, manifest).context(WriteAssetSnafu {
        path: manifest_path,
    })?;
    Ok(split.split_modules)
}

fn finalize_split_link(pkg_dir: &Path, split_wasm_file: &Path) -> BuildResult<()> {
    let placeholder_name = SPLIT_LINK_PLACEHOLDER.trim_start_matches("./");
    let placeholder_path = pkg_dir.join(placeholder_name);
    let link_bytes = fs::read(&placeholder_path).context(ReadAssetSnafu {
        path: placeholder_path.clone(),
    })?;
    let hash = truncated_sha256(&link_bytes);
    let final_name = format!("__wasm_split.{hash}.js");
    let final_link = format!("./{final_name}");
    debug_assert_eq!(SPLIT_LINK_PLACEHOLDER.len(), final_link.len());

    let mut wasm = fs::read(split_wasm_file).context(ReadAssetSnafu {
        path: split_wasm_file.to_path_buf(),
    })?;
    let positions = wasm
        .windows(SPLIT_LINK_PLACEHOLDER.len())
        .enumerate()
        .filter_map(|(position, bytes)| {
            (bytes == SPLIT_LINK_PLACEHOLDER.as_bytes()).then_some(position)
        })
        .collect::<Vec<_>>();
    ensure!(
        !positions.is_empty(),
        MissingSplitLinkPlaceholderSnafu {
            path: split_wasm_file.to_path_buf(),
        }
    );
    for position in positions {
        wasm[position..position + final_link.len()].copy_from_slice(final_link.as_bytes());
    }
    fs::write(split_wasm_file, wasm).context(WriteAssetSnafu {
        path: split_wasm_file.to_path_buf(),
    })?;

    let final_path = pkg_dir.join(final_name);
    fs::rename(&placeholder_path, &final_path).context(ReplaceAssetSnafu {
        source_path: placeholder_path,
        target: final_path,
    })?;
    Ok(())
}

fn truncated_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)[..11]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn optimize_wasm_assets(wasm_assets: &[PathBuf]) -> BuildResult<()> {
    for wasm_path in wasm_assets {
        let optimized_path = wasm_path.with_extension("opt.wasm");
        run_command(
            Command::new("wasm-opt")
                .arg("-Oz")
                .arg("--enable-bulk-memory")
                .arg("--enable-nontrapping-float-to-int")
                .arg(wasm_path)
                .arg("-o")
                .arg(&optimized_path),
        )?;
        fs::rename(&optimized_path, wasm_path).context(ReplaceAssetSnafu {
            source_path: optimized_path,
            target: wasm_path.clone(),
        })?;
    }
    Ok(())
}

fn precompress_wasm_assets(wasm_assets: &[PathBuf]) -> BuildResult<()> {
    for wasm_path in wasm_assets {
        precompress_wasm(wasm_path)?;
    }
    Ok(())
}

fn precompress_wasm(wasm_path: &Path) -> BuildResult<()> {
    let bytes = fs::read(wasm_path).context(ReadAssetSnafu {
        path: wasm_path.to_path_buf(),
    })?;

    let mut brotli_bytes = Vec::new();
    brotli::CompressorReader::new(bytes.as_slice(), 4096, 11, 22)
        .read_to_end(&mut brotli_bytes)
        .context(CompressAssetSnafu { encoding: "br" })?;
    let brotli_path = wasm_path.with_extension("wasm.br");
    fs::write(&brotli_path, brotli_bytes).context(WriteAssetSnafu { path: brotli_path })?;

    let mut gzip = GzEncoder::new(Vec::new(), Compression::best());
    gzip.write_all(&bytes)
        .context(CompressAssetSnafu { encoding: "gzip" })?;
    let gzip_bytes = gzip
        .finish()
        .context(CompressAssetSnafu { encoding: "gzip" })?;
    let gzip_path = wasm_path.with_extension("wasm.gz");
    fs::write(&gzip_path, gzip_bytes).context(WriteAssetSnafu { path: gzip_path })?;
    Ok(())
}

fn runtime_assets(pkg_dir: &Path) -> BuildResult<Vec<PathBuf>> {
    let entries = fs::read_dir(pkg_dir).context(ReadPackageDirectorySnafu {
        path: pkg_dir.to_path_buf(),
    })?;
    let mut assets = Vec::new();
    for entry in entries {
        let entry = entry.context(ReadPackageDirectorySnafu {
            path: pkg_dir.to_path_buf(),
        })?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.ends_with(".d.ts") || name.ends_with(".br") || name.ends_with(".gz") {
            continue;
        }
        assets.push(path);
    }
    assets.sort();
    Ok(assets)
}

fn asset_version(assets: &[PathBuf]) -> BuildResult<String> {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for path in assets {
        if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
            for byte in name.bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(FNV_PRIME);
            }
        }
        let bytes = fs::read(path).context(ReadAssetSnafu { path })?;
        for byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    Ok(format!("{hash:016x}"))
}

fn generate_asset_table(out_dir: &Path, assets: &[PathBuf]) -> BuildResult<()> {
    let mut source = String::from("static CLIENT_ASSETS: &[ClientAsset] = &[\n");
    for path in assets {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        let path_literal = format!("{:?}", path.to_string_lossy());
        let content_type = match path.extension().and_then(|extension| extension.to_str()) {
            Some("js") => "text/javascript; charset=utf-8",
            Some("wasm") => "application/wasm",
            Some("json") => "application/json; charset=utf-8",
            _ => "application/octet-stream",
        };
        let (brotli, gzip) = if path
            .extension()
            .is_some_and(|extension| extension == "wasm")
        {
            let brotli = format!("{:?}", path.with_extension("wasm.br").to_string_lossy());
            let gzip = format!("{:?}", path.with_extension("wasm.gz").to_string_lossy());
            (
                format!("Some(include_bytes!({brotli}))"),
                format!("Some(include_bytes!({gzip}))"),
            )
        } else {
            ("None".to_owned(), "None".to_owned())
        };
        source.push_str(&format!(
            "    ClientAsset {{ path: {name:?}, content_type: {content_type:?}, identity: include_bytes!({path_literal}), brotli: {brotli}, gzip: {gzip} }},\n"
        ));
    }
    source.push_str("];\n");
    let generated_path = out_dir.join("client_assets.rs");
    fs::write(&generated_path, source).context(WriteAssetSnafu {
        path: generated_path,
    })
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

fn append_wasm_split_rustflag(command: &mut Command) {
    const FLAG: &str = "-Clink-args=--emit-relocs";
    if host_coverage_is_enabled() {
        command.env("CARGO_ENCODED_RUSTFLAGS", FLAG);
        return;
    }
    if let Ok(flags) = env::var("CARGO_ENCODED_RUSTFLAGS") {
        let flags = if flags.is_empty() {
            FLAG.to_owned()
        } else {
            format!("{flags}\u{1f}{FLAG}")
        };
        command.env("CARGO_ENCODED_RUSTFLAGS", flags);
    } else {
        let flags = env::var("RUSTFLAGS").unwrap_or_default();
        let flags = format!("{flags} {FLAG}").trim().to_owned();
        command.env("RUSTFLAGS", flags);
    }
}

fn host_coverage_is_enabled() -> bool {
    env::var_os("LLVM_PROFILE_FILE").is_some()
        || env::var("RUSTFLAGS").is_ok_and(|value| value.contains("instrument-coverage"))
        || env::var("CARGO_ENCODED_RUSTFLAGS")
            .is_ok_and(|value| value.contains("instrument-coverage"))
}

// grcov: ignore-end
