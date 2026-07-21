{
  pkgs,
  rustCrapToolchainFor,
  rustDevToolchainFor,
  rustWasmCoverageToolchainFor,
}: let
  cargo-crap = pkgs.rustPlatform.buildRustPackage rec {
    pname = "cargo-crap";
    version = "0.2.2";

    src = pkgs.fetchurl {
      url = "https://static.crates.io/crates/${pname}/${pname}-${version}.crate";
      name = "${pname}-${version}.tar.gz";
      hash = "sha256-Ej+k1P4Am2AXD8PVhrcrCdisA/0AAOI/8j2x0ULuOmY=";
    };

    cargoHash = "sha256-vzkGNzQrVOtfpGLniGTdPRQfwA9jn5elXhudrFC7w9g=";
    doCheck = false;
  };
in {
  builtin-skill-scripts = pkgs.mkShell {
    packages = with pkgs; [
      python3
    ];
  };

  crap = pkgs.mkShell {
    nativeBuildInputs = [
      (rustCrapToolchainFor pkgs)
    ];
    packages = [
      cargo-crap
    ];
  };

  default = pkgs.mkShell {
    nativeBuildInputs = [
      (rustDevToolchainFor pkgs)
    ];
    packages = with pkgs; [
      prek
      diesel-cli
      grcov

      cargo-about
      cargo-deny
      cargo-nextest
      wasm-bindgen-cli
      chromedriver

      nono
      ruff
      taplo
      uv
    ];
  };

  ci-lint = pkgs.mkShell {
    nativeBuildInputs = [
      (rustDevToolchainFor pkgs)
    ];
    packages = with pkgs; [
      prek
      diesel-cli
      wasm-bindgen-cli
      cargo-about
      cargo-deny
      ruff
      taplo
      uv
    ];
  };

  ci-coverage = pkgs.mkShell {
    nativeBuildInputs = [
      (rustDevToolchainFor pkgs)
    ];
    packages = with pkgs; [
      cargo-nextest
      grcov
      wasm-bindgen-cli
    ];
  };

  ci-wasm-coverage = pkgs.mkShell {
    nativeBuildInputs = [
      (rustWasmCoverageToolchainFor pkgs)
    ];
    packages = with pkgs;
      [
        llvmPackages.clang-unwrapped
        chromedriver
        wasm-bindgen-cli
      ]
      ++ lib.optionals stdenv.isLinux [
        firefox
        geckodriver
      ];
    shellHook = ''
      export CC_wasm32_unknown_unknown="${pkgs.llvmPackages.clang-unwrapped}/bin/clang"
      export WASM_COVERAGE_CLANG="${pkgs.llvmPackages.clang-unwrapped}/bin/clang"
    '';
  };
}
