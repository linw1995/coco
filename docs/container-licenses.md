# Container Licenses and Corresponding Source

The published container image is an aggregate of CoCo and separately licensed
runtime tools and libraries. CoCo is Apache-2.0 licensed. Runtime components
retain their own licenses, including GPL, LGPL, and MPL components supplied by
the pinned Nixpkgs revision.

## License Files in the Image

The image contains the following directories:

- `/share/licenses/coco`: CoCo's `LICENSE`, `NOTICE`, and generated Rust
  dependency notices.
- `/share/licenses/coco-container`: standard GPL-2.0, GPL-3.0, LGPL-2.1,
  LGPL-3.0, and MPL-2.0 license texts, plus source retrieval instructions.

The image also carries OCI labels for the project license, source repository,
documentation, and the corresponding-source tag convention.

## Matching Source Image

For every image tag `TAG` published at `ghcr.io/linw1995/coco:TAG`, the CD
workflow publishes a matching source image at
`ghcr.io/linw1995/coco:TAG-sources`.

The source image is generated from the actual built image, rather than from a
manually maintained package list. For every Nix store path present in the
runtime image, the exporter records its derivation and collects the derivation's
source, patch, Cargo vendor, and Go module inputs. It also includes the exact
CoCo flake source and the locked Nixpkgs source used to build the image.

The exporter fingerprints the dependency source closure discovered from the
actual runtime image together with the export format. CI uses an Actions cache
for that dependency layer. CD instead stores the layer in GHCR under a stable
platform-and-fingerprint tag. When the tag already exists, the architecture
job reuses its manifest by digest without generating, restoring, or passing the
large layer through workflow artifacts. A missing tag is regenerated before
publication. Release-specific source and metadata layers are always generated.

The matching `-sources` tag is a multi-platform OCI index. Every successfully
built runtime platform has a corresponding source manifest containing its
dependency and release layers. Each platform's `/sources` directory contains:

- `flake/coco`: the exact CoCo source archive.
- `flake/nixpkgs`: the exact pinned Nixpkgs source archive and packaging rules.
- `nix-store`: source and patch inputs under their original Nix store names.
- `derivations`: JSON descriptions of runtime derivations.
- `RUNTIME_STORE_PATHS-<platform>.txt`: runtime paths found in each platform's
  image layers.
- `RUNTIME_DERIVATIONS-<platform>.tsv`: runtime path-to-derivation mappings.
- `SOURCE_STORE_PATHS-<platform>.txt`: the collected source input paths.
- `SOURCE_DERIVATIONS-<platform>.tsv`: source path-to-parent-derivation
  mappings.
- `DEPENDENCY_SOURCE_KEY-<platform>.txt`: the dependency source closure
  fingerprint used to identify reusable source layers.

## Extract Sources

Prefer an immutable `sha-*` tag when matching sources for an audit. Select the
same platform as the runtime image being audited:

```bash
image_ref="ghcr.io/linw1995/coco:sha-0123456789ab"
source_ref="${image_ref}-sources"
platform="linux/amd64"
source_container="$(docker create --platform "${platform}" "${source_ref}" /bin/true)"
docker cp "${source_container}:/sources" ./coco-container-sources-amd64
docker rm "${source_container}"
```

The source image does not need to run. The explicit `/bin/true` command only
allows Docker to create a stopped container for `docker cp`. Repeat with
`linux/arm64` when auditing the ARM64 runtime manifest.

If `crane` is available, extract the source filesystem directly:

```bash
crane export --platform "${platform}" "${source_ref}" - | tar -xf -
```

Mutable image aliases, including `latest`, receive a matching `-sources` alias
in the same workflow run. The immutable SHA pair remains the stable record when
an alias later moves.
