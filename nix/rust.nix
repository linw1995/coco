{
  crapToolchainFor = p:
    with p.fenix;
      combine [
        stable.cargo
      ];

  toolchainFor = p:
    with p.fenix;
      combine [
        stable.cargo
        stable.rustc
        targets.wasm32-unknown-unknown.stable.rust-std
      ];

  devToolchainFor = p:
    with p.fenix;
      combine [
        stable.cargo
        stable.clippy
        stable.rust-src
        stable.rustc
        stable.rustfmt
        stable.rust-analyzer
        stable.llvm-tools
        targets.wasm32-unknown-unknown.stable.rust-std
      ];

  wasmCoverageToolchainFor = p: let
    nightly = p.fenix.fromToolchainName {
      name = "nightly-2026-01-01";
      sha256 = "sha256-KTCPimYDgP3en6gZzClSIezJ75wuFRnhhja93KsVxA0=";
    };
    wasmTarget = p.fenix.targets.wasm32-unknown-unknown.fromToolchainName {
      name = "nightly-2026-01-01";
      sha256 = "sha256-KTCPimYDgP3en6gZzClSIezJ75wuFRnhhja93KsVxA0=";
    };
  in
    p.fenix.combine [
      nightly.cargo
      nightly.rustc
      nightly.llvm-tools
      wasmTarget.rust-std
    ];
}
