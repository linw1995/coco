{
  inputs,
  root,
  description,
}: let
  rust = import ./rust.nix;
  mkPackageSet = import ./packages.nix {
    inherit inputs root description;
    version = (builtins.fromTOML (builtins.readFile (root + /Cargo.toml))).workspace.package.version;
    rustToolchainFor = rust.toolchainFor;
  };
in
  inputs.utils.lib.eachDefaultSystem (
    system: let
      pkgs = import inputs.nixpkgs {
        inherit system;
        overlays = [
          inputs.fenix.overlays.default
        ];
      };
      hostLib = pkgs.lib;
      dockerImageSystem = hostLib.attrByPath [system] system {
        aarch64-darwin = "aarch64-linux";
        x86_64-darwin = "x86_64-linux";
      };
      hostPackages = mkPackageSet system;
      defaultDockerImagePackages = mkPackageSet dockerImageSystem;
      amd64LinuxPackages = mkPackageSet "x86_64-linux";
      arm64LinuxPackages = mkPackageSet "aarch64-linux";
      coverageScript = pkgs.writeShellApplication {
        name = "coco-coverage";
        runtimeInputs = with pkgs; [
          bash
          nix
        ];
        text = ''
          workspace_root="''${COCO_WORKSPACE_ROOT:-$(pwd -P)}"
          flake_ref="''${COCO_COVERAGE_FLAKE:-''${workspace_root}}"

          cd "''${workspace_root}"

          if [[ ! -f scripts/run-cov.sh || ! -f scripts/run-wasm-cov.sh || ! -f scripts/merge-cov.sh ]]; then
            echo "Coverage scripts were not found. Run this command from the repository root or set COCO_WORKSPACE_ROOT." >&2
            exit 1
          fi

          nix develop "''${flake_ref}#ci-coverage" --command bash scripts/run-cov.sh "$@"
          nix develop "''${flake_ref}#ci-wasm-coverage" --command bash scripts/run-wasm-cov.sh
          bash scripts/merge-cov.sh

          echo "Merged coverage report: ''${workspace_root}/target/coverage/result/lcov.info"
        '';
      };
    in {
      packages = rec {
        default = coco-cli;
        coco-cli = hostPackages.coco-cli;
        coco-debug-cli = hostPackages.coco-debug-cli;
        coverage = coverageScript;
        coco-image = defaultDockerImagePackages.coco-image;
        coco-image-linux-amd64 = amd64LinuxPackages.coco-image;
        coco-image-linux-arm64 = arm64LinuxPackages.coco-image;
        coco-debug-image = defaultDockerImagePackages.coco-debug-image;
        coco-debug-image-linux-amd64 = amd64LinuxPackages.coco-debug-image;
        coco-debug-image-linux-arm64 = arm64LinuxPackages.coco-debug-image;
      };

      apps = rec {
        coverage = {
          type = "app";
          program = "${coverageScript}/bin/coco-coverage";
        };
        full-coverage = coverage;
      };

      devShells = import ./dev-shells.nix {
        inherit pkgs;
        rustCrapToolchainFor = rust.crapToolchainFor;
        rustDevToolchainFor = rust.devToolchainFor;
        rustWasmCoverageToolchainFor = rust.wasmCoverageToolchainFor;
      };
    }
  )
