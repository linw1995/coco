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
        rustToolchain = with pkgs.fenix;
          combine [
            stable.cargo
            stable.rustc
            targets.wasm32-unknown-unknown.stable.rust-std
          ];
        rustPlatform = pkgs.makeRustPlatform {
          cargo = rustToolchain;
          rustc = rustToolchain;
        };
        version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
      in {
        packages = rec {
          default = coco-cli;

          coco-cli = rustPlatform.buildRustPackage {
            pname = "coco-cli";
            inherit version;

            src = lib.cleanSource ./.;
            strictDeps = true;

            cargoLock = {
              lockFile = ./Cargo.lock;
              outputHashes = {
                "rig-core-0.35.0" = "sha256-tnSi5EOq9BCEXlJxj2bFzlynG33qB+UuPDufW92kAj8=";
                "rig-derive-0.1.12" = "sha256-tnSi5EOq9BCEXlJxj2bFzlynG33qB+UuPDufW92kAj8=";
              };
            };
            cargoBuildFlags = ["--package" "coco-cli"];
            cargoTestFlags = ["--package" "coco-cli"];

            nativeBuildInputs = [
              pkgs.wasm-bindgen-cli
            ];

            meta = {
              inherit description;
              mainProgram = "coco-cli";
            };
          };

          coco-image = pkgs.dockerTools.buildLayeredImage {
            name = "coco";
            tag = "latest";
            contents = [
              coco-cli
              pkgs.bash
              pkgs.coreutils
              pkgs.cacert
              pkgs.tzdata
            ];
            config = {
              Env = [
                "PATH=${lib.makeBinPath [coco-cli pkgs.bash pkgs.coreutils]}"
                "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
                "TZDIR=${pkgs.tzdata}/share/zoneinfo"
                "COCO_STORE_PATH=/data/.coco-store"
                "COCO_LOG_DIR=/data/logs"
                "COCO_LOG=info"
              ];
              WorkingDir = "/data";
              Volumes = {
                "/data" = {};
              };
              ExposedPorts = {
                "17667/tcp" = {};
              };
              Cmd = [
                "${coco-cli}/bin/coco-cli"
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
            ];
          };
        };
      }
    );
}
