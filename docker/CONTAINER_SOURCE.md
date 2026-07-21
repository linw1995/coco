# Container Licenses and Corresponding Source

This container image is an aggregate of CoCo and separately licensed runtime
tools and libraries. CoCo remains licensed under Apache-2.0; inclusion in the
same image does not change the licenses of the other components.

License material is available inside the image at:

- `/share/licenses/coco` for CoCo and its Rust dependencies.
- `/share/licenses/coco-container` for the GPL, LGPL, and MPL license texts
  used by container runtime components.

For every published image tag `TAG` at `ghcr.io/linw1995/coco:TAG`, the CD
workflow publishes the corresponding source image at
`ghcr.io/linw1995/coco:TAG-sources`. The source image contains the source
inputs for every architecture present in the runtime image and includes:

- The exact CoCo flake source and locked Nixpkgs source.
- Runtime Nix derivations and their exact source, patch, Cargo vendor, and Go
  module inputs.
- Per-platform manifests mapping runtime store paths to derivations and source
  store paths.

Extract the source bundle with Docker without running the source image:

```bash
image_ref="ghcr.io/linw1995/coco:sha-0123456789ab"
source_ref="${image_ref}-sources"
source_container="$(docker create "${source_ref}" /bin/true)"
docker cp "${source_container}:/sources" ./coco-container-sources
docker rm "${source_container}"
```

Alternatively, use `crane export`:

```bash
crane export "${source_ref}" - | tar -xf -
```

The immutable `sha-*` tag pair is the preferred reference for audits and
long-term source matching. Mutable aliases such as `latest` also receive a
matching `-sources` alias whenever they are published.
