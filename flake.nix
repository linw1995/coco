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
        fhsDynamicLinker = lib.attrByPath [system] null {
          x86_64-linux = "/lib64/ld-linux-x86-64.so.2";
          aarch64-linux = "/lib/ld-linux-aarch64.so.1";
          i686-linux = "/lib/ld-linux.so.2";
        };
        fhsDynamicLinkerSymlink = lib.attrByPath [system] null {
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
            pkgs.wasm-bindgen-cli
          ];
        };
        cargoArtifacts = craneLib.buildDepsOnly cargoArgs;
        version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
        cocoDockerEntrypoint = pkgs.writeShellApplication {
          name = "coco-docker-entrypoint";
          runtimeInputs = [
            pkgs.coreutils
            pkgs.supercronic
          ];
          text = ''
            if [ -n "''${TZ:-}" ] && [ -n "''${TZDIR:-}" ] && [ -f "''${TZDIR}/''${TZ}" ]; then
              ln -snf "''${TZDIR}/''${TZ}" /etc/localtime 2>/dev/null || true
              printf '%s\n' "''${TZ}" >/etc/timezone 2>/dev/null || true
            fi

            if [ "''${COCO_START_CRON:-1}" = "1" ]; then
              cronjob_install_dir="''${COCO_SKILL_PERSIST_ROOT:-/data/skills}/orchestrator/cronjob/data/install"
              cronjob_crontab_file="''${COCO_CRONTAB_FILE:-''${cronjob_install_dir}/crontab}"
              mkdir -p "$(dirname "''${cronjob_crontab_file}")"
              if [ ! -f "''${cronjob_crontab_file}" ]; then
                printf '# CoCo cronjobs\n' >"''${cronjob_crontab_file}"
              fi
              export COCO_CRONTAB_FILE="''${cronjob_crontab_file}"
              if [ -f "''${cronjob_install_dir}/cronjob_restore.py" ] && [ -f "''${cronjob_install_dir}/managed-crontab" ]; then
                uv run --script "''${cronjob_install_dir}/cronjob_restore.py" \
                  --snapshot "''${cronjob_install_dir}/managed-crontab" \
                  --crontab-file "''${cronjob_crontab_file}" \
                  || printf 'warning: failed to restore managed CoCo cronjobs\n' >&2
              fi
              supercronic -inotify "''${cronjob_crontab_file}" &
            fi

            exec "$@"
          '';
        };
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
            contents =
              [
                coco-cli
                cocoDockerEntrypoint
                pkgs.bash
                pkgs.coreutils
                pkgs.nono
                pkgs.supercronic
                pkgs.uv
              ]
              ++ [
                pkgs.diffutils
                pkgs.findutils
                pkgs.gawk
                pkgs.gnugrep
                pkgs.gnused
                pkgs.jq
                pkgs.less
                pkgs.procps
                pkgs.ripgrep
                pkgs.which
              ]
              ++ [
                pkgs.cacert
                pkgs.tzdata
              ]
              ++ lib.optionals (fhsDynamicLinker != null) [
                pkgs.glibc
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
                "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
                "TZDIR=${pkgs.tzdata}/share/zoneinfo"
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
