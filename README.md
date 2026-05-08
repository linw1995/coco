# CoCo

[![CI](https://img.shields.io/github/actions/workflow/status/linw1995/coco/CI.yaml?branch=main&label=CI)](https://github.com/linw1995/coco/actions/workflows/CI.yaml)
[![CD](https://img.shields.io/github/actions/workflow/status/linw1995/coco/CD.yaml?branch=main&label=CD)](https://github.com/linw1995/coco/actions/workflows/CD.yaml)
[![codecov](https://codecov.io/gh/linw1995/coco/graph/badge.svg?token=A99GUI386I)](https://codecov.io/gh/linw1995/coco)
[![ghcr](https://img.shields.io/badge/ghcr.io-linw1995%2Fcoco-blue?logo=github)](https://github.com/linw1995/coco/pkgs/container/coco)

An AI Copilot.

## Quick Start

Docker Compose is the recommended deployment path:

```bash
cp .env.example .env
${EDITOR:-vi} .env
mkdir -p .coco-data .coco-workspace
docker compose up -d
```

Set `COCO_UID` to `id -u` and `COCO_GID` to `id -g` before starting Compose.
See [Docker Compose Deployment](docs/docker-compose.md) for `.env`,
ChatGPT OAuth, Telegram channel, cronjob skill, and runtime UID/GID details.

## Local Image Build

```bash
nix build .#coco-image
docker load < result
```

Use `.#coco-image-linux-amd64` or `.#coco-image-linux-arm64` when you need a
specific Linux image architecture. See
[Docker Compose Deployment](docs/docker-compose.md#local-image-builds) for
published image and local build notes.

After loading the local image, set `COCO_IMAGE=coco:latest` in `.env`.

## Documentation

- [CoCo Docs](docs/README.md)
- [Docker Compose Deployment](docs/docker-compose.md)

## Acknowledgements

Thanks to `codex` and `bub` for the inspiration and implementation references.
