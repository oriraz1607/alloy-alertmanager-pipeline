#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

engine="${CONTAINER_ENGINE:-}"
if [[ -z "$engine" ]]; then
  if command -v docker >/dev/null 2>&1; then
    engine="docker"
  elif command -v podman >/dev/null 2>&1; then
    engine="podman"
  else
    echo "error: install Docker or Podman, or set CONTAINER_ENGINE" >&2
    exit 1
  fi
fi

if ! command -v "$engine" >/dev/null 2>&1; then
  echo "error: container engine '$engine' was not found" >&2
  exit 1
fi

version="${VERSION:-$(./scripts/image-tag)}"
if [[ ! "$version" =~ ^[A-Za-z0-9][A-Za-z0-9._+-]*$ ]]; then
  echo "error: VERSION may contain only letters, numbers, dot, underscore, plus, and hyphen" >&2
  exit 1
fi
safe_version="${version#v}"
safe_version="${safe_version//+/-}"

output_dir="$repo_root/dist-custom"
stage_root="$(mktemp -d)"
container_id=""
registry_conf=""

cleanup() {
  if [[ -n "$container_id" ]]; then
    "$engine" rm -f "$container_id" >/dev/null 2>&1 || true
  fi
  rm -rf "$stage_root"
}
trap cleanup EXIT

mkdir -p "$output_dir"
find "$output_dir" -mindepth 1 -maxdepth 1 -type f -delete

image="localhost/alloy-alertmanager-pipeline:${safe_version}"
build_args=(
  build
  --platform linux/amd64
  --build-arg "RELEASE_BUILD=1"
  --build-arg "VERSION=$version"
  --tag "$image"
  --file Dockerfile
  .
)

if [[ "$engine" == "podman" ]]; then
  registry_conf="$stage_root/registries.conf"
  printf '%s\n' \
    'unqualified-search-registries = ["docker.io"]' \
    'short-name-mode = "permissive"' >"$registry_conf"
  CONTAINERS_REGISTRIES_CONF="$registry_conf" podman "${build_args[@]}"
else
  DOCKER_BUILDKIT=1 "$engine" "${build_args[@]}"
fi

container_id="$($engine create "$image")"
bundle_dir="$stage_root/alloy-${safe_version}-rhel-amd64"
mkdir -p "$bundle_dir"
"$engine" cp "$container_id:/bin/alloy" "$bundle_dir/alloy"
cp LICENSE "$bundle_dir/LICENSE"
cp packaging/custom/README.md "$bundle_dir/README.md"
cp packaging/custom/config.alloy "$bundle_dir/config.alloy"
cp packaging/custom/alloy.service "$bundle_dir/alloy.service"
cp packaging/custom/kubernetes-example.yaml "$bundle_dir/kubernetes-example.yaml"
cp packaging/custom/alertmanager-source-example.yml "$bundle_dir/alertmanager-source-example.yml"

upstream_commit="$(git rev-parse HEAD)"
build_time="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
printf 'Version: %s\nGit commit: %s\nBuilt at: %s\nPlatform: linux/amd64\n' \
  "$version" "$upstream_commit" "$build_time" >"$bundle_dir/BUILD-INFO.txt"

"$engine" run --rm --platform linux/amd64 \
  --entrypoint /bin/alloy "$image" --version

tar -C "$stage_root" -czf \
  "$output_dir/alloy-${safe_version}-rhel-amd64.tar.gz" \
  "$(basename "$bundle_dir")"

image_archive="$output_dir/alloy-alertmanager-pipeline-${safe_version}-linux-amd64.docker-image.tar"
"$engine" save --output "$image_archive" "$image"

(
  cd "$output_dir"
  sha256sum \
    "alloy-${safe_version}-rhel-amd64.tar.gz" \
    "$(basename "$image_archive")" >SHA256SUMS
  sha256sum --check SHA256SUMS
)

echo "Release files created in $output_dir"
