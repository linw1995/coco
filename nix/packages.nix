{
  inputs,
  root,
  version,
  description,
  rustToolchainFor,
}: targetSystem: let
  packagePkgs = import inputs.nixpkgs {
    system = targetSystem;
    overlays = [
      inputs.fenix.overlays.default
    ];
  };
  lib = packagePkgs.lib;
  craneLib = (inputs.crane.mkLib packagePkgs).overrideToolchain rustToolchainFor;
  src = lib.fileset.toSource {
    inherit root;
    fileset = lib.fileset.unions [
      (craneLib.fileset.commonCargoSources root)
      (lib.fileset.maybeMissing (root + /coco-console/src/native/style.css))
      (lib.fileset.maybeMissing (root + /coco-mem/src/default_skills))
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

    nativeBuildInputs = with packagePkgs; [
      wasm-bindgen-cli
    ];
  };
  cargoArtifacts = craneLib.buildDepsOnly cargoArgs;
  cocoDockerEntrypoint = packagePkgs.writeShellApplication {
    name = "coco-docker-entrypoint";
    runtimeInputs = with packagePkgs; [
      coreutils
      gnugrep
      supercronic
      util-linux
    ];
    text = builtins.readFile (root + /docker/coco-docker-entrypoint.sh);
  };
  coco-cli = craneLib.buildPackage (cargoArgs
    // {
      inherit cargoArtifacts;

      meta = {
        inherit description;
        mainProgram = "coco-cli";
      };
    });
  coco-image = import ./docker-image.nix {
    inherit packagePkgs lib targetSystem coco-cli cocoDockerEntrypoint;
  };
in {
  inherit coco-cli coco-image;
}
