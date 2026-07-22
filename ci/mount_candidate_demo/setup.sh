#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

setup_usage() {
  cat <<EOF
Usage:
  source ci/mount_candidate_demo/setup.sh [options]

Options:
      --namespace <namespace>       Kubernetes namespace. Defaults to dev.
      --skip-system-test-setup      Only expose wrappers and verify the instance.
  -h, --help                        Show this help.
EOF
}

setup_script_path="$0"
setup_sourced=false
if [[ -n "${BASH_VERSION:-}" ]]; then
  setup_script_path="${BASH_SOURCE[0]}"
fi
if [[ -n "${ZSH_VERSION:-}" ]]; then
  setup_script_path="${(%):-%x}"
fi
[[ "$setup_script_path" != "$0" ]] && setup_sourced=true

setup_main() {
  local namespace="${CTA_DEMO_NAMESPACE:-dev}"
  local run_system_test_setup=true

  while [[ $# -gt 0 ]]; do
    case "$1" in
      -h|--help)
        setup_usage
        return 0
        ;;
      --namespace)
        [[ -n "${2:-}" && "$2" != -* ]] || { echo "--namespace requires an argument" >&2; return 1; }
        namespace="$2"
        shift
        ;;
      --skip-system-test-setup)
        run_system_test_setup=false
        ;;
      *)
        echo "Unsupported argument: $1" >&2
        setup_usage >&2
        return 1
        ;;
    esac
    shift
  done

  command -v git >/dev/null 2>&1 || { echo "git is required" >&2; return 1; }
  command -v jq >/dev/null 2>&1 || { echo "jq is required" >&2; return 1; }
  command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required" >&2; return 1; }

  local script_dir project_root wrappers_dir
  script_dir="$(cd "$(dirname "$setup_script_path")" && pwd)"
  project_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
  wrappers_dir="${script_dir}/wrappers"

  export CTA_DEMO_NAMESPACE="$namespace"
  export PATH="${wrappers_dir}:$PATH"
  chmod +x "${wrappers_dir}/cta-admin" "${wrappers_dir}/eos" "${wrappers_dir}/xrdcp" "${wrappers_dir}/xrdfs" "${wrappers_dir}/xrdfs-retrieve"

  kubectl -n "$namespace" get pod cta-cli-0 >/dev/null
  kubectl -n "$namespace" get pod cta-client-0 >/dev/null

  if [[ "$run_system_test_setup" == true ]]; then
    local python_bin="python3"
    [[ -x "${project_root}/ci/system_tests/.venv/bin/python" ]] && python_bin="${project_root}/ci/system_tests/.venv/bin/python"

    (
      cd "${project_root}/ci/system_tests" || exit 1
      "$python_bin" -m pytest \
        tests/setup/setup_cta_test.py \
        tests/setup/setup_eos_test.py \
        tests/setup/setup_dcache_test.py \
        --namespace "$namespace" \
        --cleanup-first
    ) || return 1
  fi

  local scheduler_backend selected_library
  scheduler_backend=$(cta-admin --json version | jq -r '.[0].schedulerBackendName // ""')
  if [[ "$scheduler_backend" != "postgres" ]]; then
    echo "This demo requires the postgres scheduler backend, got: ${scheduler_backend:-unknown}" >&2
    echo 'Redeploy with: ./ci/cta-dev.sh up --scheduler-type pgsched --spawn-options "--one-logical-library --max-drives 2"' >&2
    return 1
  fi

  selected_library=$(
    jq -n \
      --argjson drives "$(cta-admin --json dr ls)" \
      --argjson tapes "$(cta-admin --json ta ls --all)" \
      -r '
        [$drives[].logicalLibrary] | unique[] as $ll
        | select(([$drives[] | select(.logicalLibrary == $ll)] | length) >= 2)
        | select(([$tapes[] | select(.logicalLibrary == $ll)] | length) >= 3)
        | $ll
      ' | head -n 1
  )

  if [[ -z "$selected_library" ]]; then
    echo "No logical library with at least 2 drives and 3 tapes was found in namespace $namespace" >&2
    echo 'Redeploy with: ./ci/cta-dev.sh up --scheduler-type pgsched --spawn-options "--one-logical-library --max-drives 2"' >&2
    return 1
  fi

  echo "CTA_DEMO_NAMESPACE=$CTA_DEMO_NAMESPACE"
  echo "Wrapper directory added to PATH: $wrappers_dir"
  echo "Demo logical library candidate: $selected_library"

  if [[ "$setup_sourced" != true ]]; then
    echo "To update your current shell, run:"
    echo "  export CTA_DEMO_NAMESPACE=$namespace"
    echo "  export PATH=$wrappers_dir:\$PATH"
  fi
}

if ! setup_main "$@"; then
  [[ "$setup_sourced" == true ]] && return 1
  exit 1
fi
