{
  pkgs,
  rustDevToolchainFor,
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
