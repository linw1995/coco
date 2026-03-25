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
              (combine (with stable;[
                cargo
                clippy
                rust-src
                rustc
                rustfmt
                rust-analyzer
                llvm-tools
              ]))
            ];
            packages = with pkgs; [
              prek
              grcov

              cargo-nextest
            ];
          };
        };
      }
    );
}
