#!/bin/bash -e

# SPDX-FileCopyrightText: 2024 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -eo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../utils/log_utils.sh"

usage() {
  echo
  echo "Usage: $0 [options] -t|--tag <image_tag> -s|--rpm-src <rpm source>"
  echo
  echo "Builds an image based on the CTA rpms"
  echo "  -t, --tag <image_tag>:          Docker image tag. For example \"-t dev\""
  echo "  -s, --rpm-src <rpm source>:     Path to the RPMs to be installed. Can be absolute or relative to where the script is executed from. For example \"-s build_rpm/RPM/RPMS/x86_64\""
  echo
  echo "options:"
  echo "  -h, --help:                         Shows help output."
  echo "  -l, --load-into-k8s:                Load images from the selected container runtime into the detected local Kubernetes setup."
  echo "  -c, --container-runtime <runtime>:  Container runtime to use. Docker requires the Buildx plugin. Defaults to podman."
  echo "      --dockerfile <path>:            Path to the Dockerfile (default: 'ci/docker/cta/{defaultplatform}/prod.Dockerfile')."
  echo "      --enable-internal-repos:        Use the internal yum repos instead of the public repos."
  echo "      --enable-oracle-support:        Build the images for use with the Oracle catalogue."
  echo
  echo "When fzf is available in an interactive terminal, image status and selectable"
  echo "live logs are shown in an fzf dashboard. Otherwise logs are streamed with"
  echo "a different color for each image."
  echo
  exit 1
}

project_root=$(git rev-parse --show-toplevel)

# Default values
rpm_src=""
image_tag=""
container_runtime="podman"
default_platform=$(jq -r .dev.defaultPlatform "${project_root}/project.json")
dockerfile_path="ci/docker/cta/${default_platform}/prod.Dockerfile"
load_into_k8s=false
enable_debug_image=false
enable_internal_repos="0"
enable_oracle_support="0"

while [[ "$#" -gt 0 ]]; do
  case "$1" in
  -h | --help) usage ;;
  -c | --container-runtime)
    if [[ $# -gt 1 ]]; then
      if [[ "$2" != "docker" ]] && [[ "$2" != "podman" ]]; then
        error_usage "-c | --container-runtime is \"$2\" but must be one of [docker, podman]."
      fi
      container_runtime="$2"
      shift
    else
      error_usage "-c | --container-runtime requires an argument"
    fi
    ;;
  -s | --rpm-src)
    if [[ $# -gt 1 ]]; then
      rpm_src=$(realpath "$2")
      shift
    else
      error_usage "-s|--rpm-src requires an argument"
    fi
    ;;
  -t | --tag)
    if [[ $# -gt 1 ]]; then
      image_tag="$2"
      shift
    else
      error_usage "-t|--tag requires an argument"
    fi
    ;;
  -l | --load-into-k8s) load_into_k8s=true ;;
  --enable-debug-image) enable_debug_image=true ;;
  --enable-internal-repos) enable_internal_repos="1" ;;
  --enable-oracle-support) enable_oracle_support="1" ;;
  --dockerfile)
    if [[ $# -gt 1 ]]; then
      dockerfile_path="$2"
      shift
    else
      error_usage "--dockerfile requires an argument"
    fi
    ;;
  *)
    die_usage "Unsupported argument: $1"
    ;;
  esac
  shift
done

if [[ -z "${image_tag}" ]]; then
  die_usage "Missing mandatory argument -t | --tag"
fi

if [[ -z "${rpm_src}" ]]; then
  die_usage "Missing mandatory argument -s | --rpm-src"
fi

build_command=("$container_runtime" build)
if [[ $container_runtime == docker ]]; then
  if ! docker buildx version >/dev/null 2>&1; then
    die "Docker Buildx is required to build CTA images. Install the Docker Buildx plugin or use Podman."
  fi
  build_command=(docker buildx build --load)
fi

cd "$(dirname ${dockerfile_path})"
dockerfile="$(basename ${dockerfile_path})"

colors=(
  $_log_blue
  $_log_yellow
  $_log_magenta
  $_log_cyan
  $_log_orange
  $_log_purple
  $_log_teal
  $_log_sky
)
targets=(
  "cta-taped"
  "cta-maintd"
  "cta-rmcd"
  "cta-frontend"
  "cta-tools"
)
if [[ "$enable_debug_image" == "true" ]]; then
  targets+=( "cta-debug" )
fi

BUILD_ID=$(date +%Y%m%d-%H%M%S)
SECONDS=0

pids=()

declare -A pid_by_target log_by_target exit_by_target end_by_target start_by_target color_by_target

log_dir="${project_root}/build_rpm/logs/image-build"
status_file="${log_dir}/status.tsv"
mkdir -p "$log_dir"

# The directory always contains only the most recent image build.
previous_files=("${log_dir}"/*)
for previous_file in "${previous_files[@]}"; do
  [[ -e "$previous_file" ]] && rm -f -- "$previous_file"
done

build_target() {
  local target="$1"
  local image_ref="$2"

  "${build_command[@]}" . -f "${dockerfile}" \
    -t "${image_ref}" \
    --build-context rpm_context="${rpm_src}" \
    --build-arg ENABLE_INTERNAL_REPOS=${enable_internal_repos} \
    --build-arg ENABLE_ORACLE_SUPPORT=${enable_oracle_support} \
    --network host \
    --label build.id="$BUILD_ID" \
    --target "$target" || return $?

  # Note that the below checks are rather crude (for speed)
  if [[ "$load_into_k8s" == "true" ]]; then
    # Load into minikube (use stdin to avoid a temp file)
    if command -v minikube >/dev/null 2>&1; then
      log_task "Loading ${image_ref} into minikube..."
      ${container_runtime} save "${image_ref}" | minikube image load --overwrite - || return $?
    fi

    # Load into k3s (stream into containerd)
    if command -v k3s >/dev/null 2>&1; then
      log_task "Loading ${image_ref} into k3s/containerd..."
      ${container_runtime} save "${image_ref}" | sudo /usr/local/bin/k3s ctr images import - || return $?
    fi
  fi
}

color_for_status() {
  case "$1" in
    RUNNING) printf '\033[1;33m🏃 %s\033[0m' "$1" ;;
    DONE) printf '\033[1;32m✅ %s\033[0m' "$1" ;;
    FAILED*) printf '\033[1;31m❌ %s\033[0m' "$1" ;;
    *) printf '\033[1;90m❓ %s\033[0m' "$1" ;;
  esac
}

write_status() {
  local now target exit_code elapsed build_status target_display status_display
  now=$(date +%s)
  {
    for target in "${targets[@]}"; do
      if [[ -f ${exit_by_target[$target]} ]]; then
        elapsed=$(( $(<"${end_by_target[$target]}") - start_by_target[$target] ))
        exit_code=$(<"${exit_by_target[$target]}")
        if [[ $exit_code == 0 ]]; then
          build_status="DONE"
        else
          build_status="FAILED(${exit_code})"
        fi
      elif kill -0 "${pid_by_target[$target]}" 2>/dev/null; then
        elapsed=$((now - start_by_target[$target]))
        build_status="RUNNING"
      else
        elapsed=$((now - start_by_target[$target]))
        build_status="UNKNOWN"
      fi
      target_display=$(printf '%s%s\033[0m' "${color_by_target[$target]}" "$target")
      status_display=$(color_for_status "$build_status")
      printf '%s\t%s\t%ss\t%s\n' \
        "$target_display" "$status_display" "$elapsed" "${log_by_target[$target]}"
    done
  } > "${status_file}.tmp"
  mv -f "${status_file}.tmp" "$status_file"
}

use_fzf=false
if command -v fzf >/dev/null 2>&1 && [[ -t 0 && -t 1 && "${TERM:-dumb}" != dumb ]]; then
  use_fzf=true
fi

# Build and load all targets
i=0
for target in "${targets[@]}"; do
  color="${colors[$((i % ${#colors[@]}))]}"
  (( ++i ))
  image_ref="cta/ctageneric/${target}:${image_tag}"
  log_by_target[$target]="${log_dir}/${target}.log"
  exit_by_target[$target]="${log_dir}/${target}.exit"
  end_by_target[$target]="${log_dir}/${target}.end"
  start_by_target[$target]=$(date +%s)
  color_by_target[$target]="$color"

  if [[ $use_fzf == true ]]; then
    (
      set +e
      build_target "$target" "$image_ref" >"${log_by_target[$target]}" 2>&1
      build_status=$?
      date +%s > "${end_by_target[$target]}"
      printf '%s\n' "$build_status" > "${exit_by_target[$target]}"
      exit "$build_status"
    ) &
  else
    (
      set +e
      set -o pipefail
      build_target "$target" "$image_ref" 2>&1 |
        tee "${log_by_target[$target]}" |
        awk -v prefix="[$target]:" -v color="$color" '
          {
            printf "%s%s\033[0m %s\n", color, prefix, $0
            fflush()
          }
        '
      build_status=$?
      date +%s > "${end_by_target[$target]}"
      printf '%s\n' "$build_status" > "${exit_by_target[$target]}"
      exit "$build_status"
    ) &
  fi
  pid_by_target[$target]=$!
  pids+=($!)
done

monitor_pid=""
push_pid=""
cleanup_status_jobs() {
  [[ -n $monitor_pid ]] && kill "$monitor_pid" 2>/dev/null || true
  [[ -n $push_pid ]] && kill "$push_pid" 2>/dev/null || true
}
trap cleanup_status_jobs EXIT

if [[ $use_fzf == true ]]; then
  write_status
  (
    while true; do
      write_status
      sleep 1
    done
  ) &
  monitor_pid=$!

  listen_args=()
  if command -v curl >/dev/null 2>&1 && fzf --help 2>&1 | grep -q -- '--listen'; then
    fzf_port=$((10000 + RANDOM % 20000))
    listen_args=(--listen "$fzf_port")
    (
      while kill -0 "$monitor_pid" 2>/dev/null; do
        curl --noproxy '*' --silent --request POST "localhost:${fzf_port}" \
          --data "reload(cat ${status_file})" >/dev/null 2>&1 || true
        sleep 1
      done
    ) &
    push_pid=$!
  fi

  FZF_DEFAULT_OPTS= fzf \
    --ansi \
    --delimiter=$'\t' \
    --with-nth=1,2,3 \
    --no-sort \
    --header='ctrl-r: refresh   ctrl-o: open full log   enter/esc: close viewer' \
    --preview='tail -n 300 -f {4}' \
    --preview-window='right,70%,follow' \
    --bind="ctrl-r:reload(cat ${status_file})" \
    --bind="ctrl-o:execute(${PAGER:-less} -R {4})" \
    "${listen_args[@]}" \
    < "$status_file" >/dev/null || true

  cleanup_status_jobs
  echo
  log_task "Waiting for any image builds that are still running..."
fi

status=0
for target in "${targets[@]}"; do
  wait "${pid_by_target[$target]}" || status=1
done

echo
printf '%-20s %-12s %s\n' "IMAGE" "STATUS" "LOG"
for target in "${targets[@]}"; do
  exit_code=$(<"${exit_by_target[$target]}")
  if [[ $exit_code == 0 ]]; then
    build_status="DONE"
  else
    build_status="FAILED(${exit_code})"
  fi
  printf '%s%-20s\033[0m %-12s %s\n' \
    "${color_by_target[$target]}" "$target" "$build_status" "${log_by_target[$target]}"
done
echo
echo "Image build logs: ${log_dir}"

if [[ $status == 1 ]]; then
  log_error "Failed to build or load one or more container images."
  exit "$status"
fi

echo
echo "Built images:"
${container_runtime} images --filter "label=build.id=$BUILD_ID"
echo
log_success "Built and loaded container images in ${SECONDS} seconds."
