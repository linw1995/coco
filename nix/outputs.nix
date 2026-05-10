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
    in {
      packages = rec {
        default = coco-cli;
        coco-cli = hostPackages.coco-cli;
        coco-image = defaultDockerImagePackages.coco-image;
        coco-image-linux-amd64 = amd64LinuxPackages.coco-image;
        coco-image-linux-arm64 = arm64LinuxPackages.coco-image;
      };

      devShells = import ./dev-shells.nix {
        inherit pkgs;
        rustDevToolchainFor = rust.devToolchainFor;
      };
    }
  )
