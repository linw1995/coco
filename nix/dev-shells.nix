{
  pkgs,
  rustDevToolchainFor,
}: let
  cargo-crap = pkgs.rustPlatform.buildRustPackage rec {
    pname = "cargo-crap";
    version = "0.2.1";

    src = pkgs.fetchCrate {
      inherit pname version;
      hash = "sha256-hjLl+FOTHMive61zGdhmAVGxDiApVxSBz1Nn5nKJTT8=";
    };

    cargoHash = "sha256-+C6MBqV1RJqZapMYhMVYyczLDPGrqSwVX0tKs2fJ4n0=";
    doCheck = false;
  };

  rustNativeBuildInputs = [
    (rustDevToolchainFor pkgs)
  ];

  lintPackages = with pkgs; [
    prek
    ruff
    taplo
    uv
  ];
in {
  default = pkgs.mkShell {
    nativeBuildInputs = rustNativeBuildInputs;
    packages = with pkgs; [
      grcov

      cargo-crap
      cargo-nextest
      wasm-bindgen-cli

      nono
    ] ++ lintPackages;
  };

  lint = pkgs.mkShell {
    nativeBuildInputs = rustNativeBuildInputs;
    packages = lintPackages;
  };
}
