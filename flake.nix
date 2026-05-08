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
        hostLib = pkgs.lib;
        hostSystem = system;
        rustToolchainFor = p:
          with p.fenix;
            combine [
              stable.cargo
              stable.rustc
              targets.wasm32-unknown-unknown.stable.rust-std
            ];
        dockerImageSystem = hostLib.attrByPath [hostSystem] hostSystem {
          aarch64-darwin = "aarch64-linux";
          x86_64-darwin = "x86_64-linux";
        };
        mkPackageSet = targetSystem: let
          packagePkgs = import nixpkgs {
            system = targetSystem;
            overlays = [
              inputs.fenix.overlays.default
            ];
          };
          lib = packagePkgs.lib;
          craneLib = (crane.mkLib packagePkgs).overrideToolchain rustToolchainFor;
          fhsDynamicLinker = lib.attrByPath [targetSystem] null {
            x86_64-linux = "/lib64/ld-linux-x86-64.so.2";
            aarch64-linux = "/lib/ld-linux-aarch64.so.1";
            i686-linux = "/lib/ld-linux.so.2";
          };
          fhsDynamicLinkerSymlink = lib.attrByPath [targetSystem] null {
            x86_64-linux = {
              link = "/lib64/ld-linux-x86-64.so.2";
              target = "/lib/ld-linux-x86-64.so.2";
            };
          };
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
              packagePkgs.wasm-bindgen-cli
            ];
          };
          cargoArtifacts = craneLib.buildDepsOnly cargoArgs;
          cocoDockerEntrypoint = packagePkgs.writeShellApplication {
            name = "coco-docker-entrypoint";
            runtimeInputs = [
              packagePkgs.coreutils
              packagePkgs.gnugrep
              packagePkgs.supercronic
              packagePkgs.util-linux
            ];
            text = builtins.readFile ./docker/coco-docker-entrypoint.sh;
          };
          coco-cli = craneLib.buildPackage (cargoArgs
            // {
              inherit cargoArtifacts;

              meta = {
                inherit description;
                mainProgram = "coco-cli";
              };
            });
          coco-image = packagePkgs.dockerTools.buildLayeredImage {
            name = "coco";
            tag = "latest";
            contents =
              [
                coco-cli
                cocoDockerEntrypoint
                packagePkgs.bash
                packagePkgs.coreutils
                packagePkgs.nono
                packagePkgs.supercronic
                packagePkgs.uv
              ]
              ++ [
                packagePkgs.diffutils
                packagePkgs.findutils
                packagePkgs.gawk
                packagePkgs.gnugrep
                packagePkgs.gnused
                packagePkgs.jq
                packagePkgs.less
                packagePkgs.procps
                packagePkgs.ripgrep
                packagePkgs.which
              ]
              ++ [
                packagePkgs.cacert
                packagePkgs.tzdata
              ]
              ++ lib.optionals (fhsDynamicLinker != null) [
                packagePkgs.glibc
              ];
            extraCommands =
              ''
                mkdir -p tmp
                chmod 1777 tmp
                mkdir -p etc var/run
                printf '%s\n' 'root:x:0:0:root:/data:/bin/bash' > etc/passwd
                printf '%s\n' 'root:x:0:' > etc/group
              ''
              + lib.optionalString (fhsDynamicLinkerSymlink != null) ''
                mkdir -p .${builtins.dirOf fhsDynamicLinkerSymlink.link}
                if [ ! -e .${fhsDynamicLinkerSymlink.link} ]; then
                  ln -s ${fhsDynamicLinkerSymlink.target} .${fhsDynamicLinkerSymlink.link}
                fi
              '';
            config = {
              Env = [
                "PATH=/bin"
                "SSL_CERT_FILE=${packagePkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
                "TZDIR=${packagePkgs.tzdata}/share/zoneinfo"
                "TZ=UTC"
                "HOME=/data"
                "XDG_CACHE_HOME=/data/.cache"
                "XDG_CONFIG_HOME=/data/.config"
                "XDG_DATA_HOME=/data/.local/share"
                "XDG_STATE_HOME=/data/.local/state"
                "COCO_STORE_PATH=/data/.coco-store"
                "COCO_SKILL_PERSIST_ROOT=/data/skills"
                "COCO_LOG_DIR=/data/logs"
                "COCO_CRONTAB_FILE=/data/skills/orchestrator/cronjob/data/install/crontab"
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
              Entrypoint = [
                "${cocoDockerEntrypoint}/bin/coco-docker-entrypoint"
              ];
              Cmd = [
                "${coco-cli}/bin/coco"
                "daemon"
                "serve"
                "--console-addr"
                "0.0.0.0:17667"
              ];
            };
          };
        in {
          inherit coco-cli coco-image;
        };
        hostPackages = mkPackageSet hostSystem;
        defaultDockerImagePackages = mkPackageSet dockerImageSystem;
        amd64LinuxPackages = mkPackageSet "x86_64-linux";
        arm64LinuxPackages = mkPackageSet "aarch64-linux";
        version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
      in {
        packages = rec {
          default = coco-cli;
          coco-cli = hostPackages.coco-cli;
          coco-image = defaultDockerImagePackages.coco-image;
          coco-image-linux-amd64 = amd64LinuxPackages.coco-image;
          coco-image-linux-arm64 = arm64LinuxPackages.coco-image;
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
