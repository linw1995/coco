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
        version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
      in {
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
