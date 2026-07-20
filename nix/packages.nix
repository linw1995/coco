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
  spdxLicenseListVersion = "v3.28.0";
  fetchSpdxLicense = {
    name,
    hash,
  }:
    packagePkgs.fetchurl {
      inherit name hash;
      url = "https://raw.githubusercontent.com/spdx/license-list-data/${spdxLicenseListVersion}/text/${name}";
    };
  containerLicenseTexts = {
    gpl20 = fetchSpdxLicense {
      name = "GPL-2.0-only.txt";
      hash = "sha256-qvE1Ry+BxbSg3Kk2flu16XUAMrW+vlRCs25MCkdDDfM=";
    };
    gpl30 = fetchSpdxLicense {
      name = "GPL-3.0-only.txt";
      hash = "sha256-+5gWaMGKJ54oX8TYP7oeg2zITdTapzyWl9PP0tispuA=";
    };
    lgpl21 = fetchSpdxLicense {
      name = "LGPL-2.1-only.txt";
      hash = "sha256-V0l4XIve+vy115gnDtCpZwNv4spj3O2t4WJ1Zd/vgdI=";
    };
    lgpl30 = fetchSpdxLicense {
      name = "LGPL-3.0-only.txt";
      hash = "sha256-mWrwUT3yH3SWKIlRxBQooDwXTp5KnWNmXFfWcPhFzLE=";
    };
    mpl20 = fetchSpdxLicense {
      name = "MPL-2.0.txt";
      hash = "sha256-ZqMQfVrWoFiqt1PqrCBHzLLtDjlGXdD+WETaPjANUXI=";
    };
  };
  src = lib.fileset.toSource {
    inherit root;
    fileset = lib.fileset.unions [
      (craneLib.fileset.commonCargoSources root)
      (lib.fileset.maybeMissing (root + /coco-console/src/host/style.css))
      (lib.fileset.maybeMissing (root + /coco-console/src/host/templates))
      (lib.fileset.maybeMissing (root + /coco-console/templates))
      (lib.fileset.maybeMissing (root + /coco-console/web-graph-migrations))
      (lib.fileset.maybeMissing (root + /coco-mem/migrations))
      (lib.fileset.maybeMissing (root + /coco-mem/src/default_skills))
      (lib.fileset.maybeMissing (root + /LICENSE))
      (lib.fileset.maybeMissing (root + /NOTICE))
      (lib.fileset.maybeMissing (root + /THIRD_PARTY_NOTICES.html))
    ];
  };
  cargoArgs = {
    pname = "coco-cli";
    inherit version src;
    strictDeps = true;

    cargoExtraArgs = "--package coco-cli";

    outputHashes = builtins.fromJSON (builtins.readFile (root + /nix/cargo-git-output-hashes.json));

    nativeBuildInputs = with packagePkgs; [
      wasm-bindgen-cli
    ];
  };
  installLicenses = ''
    install -Dm644 LICENSE "$out/share/licenses/coco/LICENSE"
    install -Dm644 NOTICE "$out/share/licenses/coco/NOTICE"
    install -Dm644 THIRD_PARTY_NOTICES.html "$out/share/licenses/coco/THIRD_PARTY_NOTICES.html"
  '';
  cargoArtifacts = craneLib.buildDepsOnly cargoArgs;
  debugCargoArgs =
    cargoArgs
    // {
      CARGO_PROFILE_RELEASE_DEBUG = "1";
      CARGO_PROFILE_RELEASE_STRIP = "none";
      RUSTFLAGS = "-C force-frame-pointers=yes";
      dontStrip = true;
    };
  debugCargoArtifacts = craneLib.buildDepsOnly debugCargoArgs;
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
      postInstall = installLicenses;

      meta = {
        inherit description;
        license = lib.licenses.asl20;
        mainProgram = "coco-cli";
      };
    });
  coco-debug-cli = craneLib.buildPackage (debugCargoArgs
    // {
      cargoArtifacts = debugCargoArtifacts;
      postInstall = installLicenses;

      meta = {
        inherit description;
        license = lib.licenses.asl20;
        mainProgram = "coco-cli";
      };
    });
  containerLegal = packagePkgs.runCommand "coco-container-legal-${version}" {} ''
    legal_dir="$out/share/licenses/coco-container"

    install -Dm644 ${containerLicenseTexts.gpl20} "$legal_dir/GPL-2.0-only.txt"
    install -Dm644 ${containerLicenseTexts.gpl30} "$legal_dir/GPL-3.0-only.txt"
    install -Dm644 ${containerLicenseTexts.lgpl21} "$legal_dir/LGPL-2.1-only.txt"
    install -Dm644 ${containerLicenseTexts.lgpl30} "$legal_dir/LGPL-3.0-only.txt"
    install -Dm644 ${containerLicenseTexts.mpl20} "$legal_dir/MPL-2.0.txt"
    install -Dm644 ${root + /docker/CONTAINER_SOURCE.md} "$legal_dir/CONTAINER_SOURCE.md"
  '';
  coco-image = import ./docker-image.nix {
    inherit packagePkgs lib targetSystem coco-cli cocoDockerEntrypoint containerLegal;
  };
  coco-debug-image = import ./docker-image.nix {
    inherit packagePkgs lib targetSystem cocoDockerEntrypoint containerLegal;
    coco-cli = coco-debug-cli;
    debugTools = true;
  };
in {
  inherit coco-cli coco-debug-cli coco-image coco-debug-image;
}
