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
in {
  default = pkgs.mkShell {
    nativeBuildInputs = [
      (rustDevToolchainFor pkgs)
    ];
    packages = with pkgs; [
      prek
      grcov

      cargo-crap
      cargo-nextest
      wasm-bindgen-cli

      nono
      ruff
      taplo
      uv
    ];
  };
}
