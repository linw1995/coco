{
  pkgs,
  rustDevToolchainFor,
}: {
  default = pkgs.mkShell {
    nativeBuildInputs = [
      (rustDevToolchainFor pkgs)
    ];
    packages = with pkgs; [
      prek
      grcov

      cargo-nextest
      wasm-bindgen-cli

      nono
      uv
    ];
  };
}
