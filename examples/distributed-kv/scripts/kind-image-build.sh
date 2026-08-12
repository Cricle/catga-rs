#!/usr/bin/env bash
# Builds the distributed-kv image and imports it into the kind cluster.
#
#   examples/distributed-kv/scripts/kind-image-build.sh
#
# Env knobs:
#   IMAGE                  image ref (default localhost/catga-distributed-kv:local)
#   KIND_CLUSTER           kind cluster name (default catga-kv)
#   CARGO_MIRROR           sparse registry mirror build-arg (default rsproxy.cn;
#                          set empty to build against crates.io directly)
#   PODMAN_BUILD_DNS       extra --dns for RUN steps (needed when the podman
#                          machine's gateway resolver is broken, e.g. 223.5.5.5)
#   IGNOREFILE             build-context ignorefile (default the example's own
#                          .dockerignore, which also skips .qoder worktrees)
#
# All podman pulls run under `timeout` with retries; if a base image is
# already present locally the build proceeds from the cache (--pull=false).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${repo_root}"

IMAGE="${IMAGE:-localhost/catga-distributed-kv:local}"
KIND_CLUSTER="${KIND_CLUSTER:-catga-kv}"
CARGO_MIRROR="${CARGO_MIRROR-sparse+https://rsproxy.cn/index/}"
IGNOREFILE="${IGNOREFILE:-examples/distributed-kv/.dockerignore}"
LOG_DIR=target/kind-battery
mkdir -p "${LOG_DIR}"

pull_with_retries() { # <image> — tolerate failure when the image is already local
  local img="$1" attempt
  for attempt in 1 2 3; do
    if timeout 300 podman pull "$img"; then
      return 0
    fi
    echo "pull retry ${attempt} for ${img}" >&2
  done
  if podman image exists "$img"; then
    echo "pull failed but ${img} is cached locally; using cache" >&2
    return 0
  fi
  echo "FATAL: cannot pull ${img} and no local copy exists" >&2
  return 1
}

pull_with_retries docker.io/library/rust:1.96-bookworm
pull_with_retries docker.io/library/debian:bookworm-slim

build_args=(--pull=false --ignorefile "${IGNOREFILE}" -f examples/distributed-kv/Dockerfile -t "${IMAGE}")
if [ -n "${CARGO_MIRROR}" ]; then
  build_args+=(--build-arg "CARGO_REGISTRY_SPARSE_MIRROR=${CARGO_MIRROR}")
fi
if [ -n "${PODMAN_BUILD_DNS:-}" ]; then
  build_args+=(--dns "${PODMAN_BUILD_DNS}")
fi

timeout 1700 podman build "${build_args[@]}" .

TAR="${LOG_DIR}/distributed-kv-image.tar"
timeout 300 podman save -o "${TAR}" "${IMAGE}"
KIND_EXPERIMENTAL_PROVIDER=podman timeout 600 kind load image-archive "${TAR}" --name "${KIND_CLUSTER}"
echo "image ${IMAGE} loaded into kind cluster ${KIND_CLUSTER}"
