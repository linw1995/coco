rec {
  description = "CoCo - An AI Copilot";
  inputs = {
    utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix.url = "github:nix-community/fenix";
    fenix.inputs.nixpkgs.follows = "nixpkgs";
    crane.url = "github:ipetkov/crane";
  };

  outputs = inputs:
    import ./nix/outputs.nix {
      inherit inputs description;
      root = ./.;
    };
}
