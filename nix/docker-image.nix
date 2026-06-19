{
  packagePkgs,
  lib,
  targetSystem,
  coco-cli,
  cocoDockerEntrypoint,
  debugTools ? false,
}: let
  fhsDynamicLinker = lib.attrByPath [targetSystem] null {
    x86_64-linux = "/lib64/ld-linux-x86-64.so.2";
    aarch64-linux = "/lib/ld-linux-aarch64.so.1";
    i686-linux = "/lib/ld-linux.so.2";
  };
  fhsDynamicLinkerSymlink = lib.attrByPath [targetSystem] null {
    x86_64-linux = {
      link = "/lib64/ld-linux-x86-64.so.2";
      target = "/lib/ld-linux-x86-64.so.2";
    };
  };
in
  with packagePkgs;
    dockerTools.buildLayeredImage {
      name = "coco";
      tag = "latest";
      contents =
        [
          coco-cli
          cocoDockerEntrypoint
          bash
          coreutils
          curl
          nono
          supercronic
          uv
        ]
        ++ [
          diffutils
          findutils
          gawk
          gnugrep
          gnused
          jq
          less
          procps
          ripgrep
          which
        ]
        ++ [
          cacert
          tzdata
        ]
        ++ lib.optionals debugTools [
          perf
        ]
        ++ lib.optionals (fhsDynamicLinker != null) [
          glibc
        ];
      extraCommands =
        ''
          mkdir -p tmp
          chmod 1777 tmp
          mkdir -p etc var/run
          printf '%s\n' 'root:x:0:0:root:/data:/bin/bash' > etc/passwd
          printf '%s\n' 'root:x:0:' > etc/group
        ''
        + lib.optionalString (fhsDynamicLinkerSymlink != null) ''
          mkdir -p .${builtins.dirOf fhsDynamicLinkerSymlink.link}
          if [ ! -e .${fhsDynamicLinkerSymlink.link} ]; then
            ln -s ${fhsDynamicLinkerSymlink.target} .${fhsDynamicLinkerSymlink.link}
          fi
        '';
      config = {
        Env = [
          "PATH=/bin"
          "SSL_CERT_FILE=${cacert}/etc/ssl/certs/ca-bundle.crt"
          "TZDIR=${tzdata}/share/zoneinfo"
          "TZ=UTC"
          "HOME=/data"
          "XDG_CACHE_HOME=/data/.cache"
          "XDG_CONFIG_HOME=/data/.config"
          "XDG_DATA_HOME=/data/.local/share"
          "XDG_STATE_HOME=/data/.local/state"
          "COCO_STORE_PATH=/data/store"
          "COCO_SKILL_PERSIST_ROOT=/data/skills"
          "COCO_LOG_DIR=/data/logs"
          "COCO_LOG=info"
          "COCO_EXEC_SANDBOX=nono"
          "COCO_EXEC_WORKSPACE=/workspace"
        ];
        WorkingDir = "/data";
        Volumes = {
          "/data" = {};
          "/workspace" = {};
        };
        ExposedPorts = {
          "17667/tcp" = {};
        };
        Entrypoint = [
          "${cocoDockerEntrypoint}/bin/coco-docker-entrypoint"
        ];
        Cmd = [
          "${coco-cli}/bin/coco"
          "daemon"
          "serve"
          "--console-addr"
          "0.0.0.0:17667"
        ];
      };
    }
