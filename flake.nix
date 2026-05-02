rec {
  description = "CoCo - An AI Copilot";

  inputs = {
    utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix.url = "github:nix-community/fenix";
    fenix.inputs.nixpkgs.follows = "nixpkgs";
    crane.url = "github:ipetkov/crane";
  };

  outputs = {
    self,
    nixpkgs,
    utils,
    crane,
    ...
  } @ inputs:
    utils.lib.eachDefaultSystem
    (
      system: let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [
            inputs.fenix.overlays.default
          ];
        };
        lib = pkgs.lib;
        rustToolchainFor = p:
          with p.fenix;
            combine [
              stable.cargo
              stable.rustc
              targets.wasm32-unknown-unknown.stable.rust-std
            ];
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchainFor;
        src = lib.fileset.toSource {
          root = ./.;
          fileset = lib.fileset.unions [
            (craneLib.fileset.commonCargoSources ./.)
            (lib.fileset.maybeMissing ./coco-console/src/native/style.css)
            (lib.fileset.maybeMissing ./coco-mem/src/default_skills)
          ];
        };
        cargoArgs = {
          pname = "coco-cli";
          inherit version src;
          strictDeps = true;

          cargoExtraArgs = "--package coco-cli";

          outputHashes = {
            "git+https://github.com/0xPlaygrounds/rig?branch=main#6dc36d803adaf4f89de774577b9c4f7ac9057644" = "sha256-tnSi5EOq9BCEXlJxj2bFzlynG33qB+UuPDufW92kAj8=";
          };

          nativeBuildInputs = [
            pkgs.wasm-bindgen-cli
          ];
        };
        cargoArtifacts = craneLib.buildDepsOnly cargoArgs;
        version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
      in {
        packages = rec {
          default = coco-cli;

          coco-cli = craneLib.buildPackage (cargoArgs
            // {
              inherit cargoArtifacts;

              meta = {
                inherit description;
                mainProgram = "coco-cli";
              };
            });

          coco-image = pkgs.dockerTools.buildLayeredImage {
            name = "coco";
            tag = "latest";
            contents = [
              coco-cli
              pkgs.bash
              pkgs.coreutils
              pkgs.nono
              pkgs.uv
              pkgs.cacert
              pkgs.tzdata
            ];
            config = {
              Env = [
                "PATH=${lib.makeBinPath [coco-cli pkgs.bash pkgs.coreutils pkgs.nono pkgs.uv]}"
                "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
                "TZDIR=${pkgs.tzdata}/share/zoneinfo"
                "XDG_CONFIG_HOME=/data/.config"
                "COCO_STORE_PATH=/data/.coco-store"
                "COCO_LOG_DIR=/data/logs"
                "COCO_LOG=info"
                "COCO_EXEC_SANDBOX=nono"
                "COCO_EXEC_WORKSPACE=/workspace"
              ];
              WorkingDir = "/data";
              Volumes = {
                "/data" = {};
                "/workspace" = {};
              };
              ExposedPorts = {
                "17667/tcp" = {};
              };
              Cmd = [
                "${coco-cli}/bin/coco"
                "daemon"
                "serve"
                "--console-addr"
                "0.0.0.0:17667"
              ];
            };
          };
        };

        devShells = {
          default = pkgs.mkShell {
            nativeBuildInputs = with pkgs.fenix; [
              (combine [
                stable.cargo
                stable.clippy
                stable.rust-src
                stable.rustc
                stable.rustfmt
                stable.rust-analyzer
                stable.llvm-tools
                targets.wasm32-unknown-unknown.stable.rust-std
              ])
            ];
            packages = with pkgs; [
              prek
              grcov

              cargo-nextest
              wasm-bindgen-cli

              nono
              uv
            ];
          };
        };
      }
    );
}
