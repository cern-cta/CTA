#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

usage() {
  echo "Usage: $0 <pod> [-n namespace] [--image debug-image]"
}

namespace=dev
pod=""
image=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--namespace|--image)
      if [[ $# -lt 2 || -z "$2" || "$2" == -* ]]; then
        usage >&2
        exit 1
      fi
      if [[ "$1" == --image ]]; then image="$2"; else namespace="$2"; fi
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    -*) usage >&2; exit 1 ;;
    *)
      if [[ -n "$pod" ]]; then usage >&2; exit 1; fi
      pod="$1"
      shift
      ;;
  esac
done
if [[ -z "$pod" ]]; then usage >&2; exit 1; fi

if [[ -z "$image" ]]; then
  # Use the first application container's image exactly as specified in the pod.
  image=$(kubectl -n "$namespace" get pod "$pod" -o jsonpath='{.spec.containers[0].image}')
  if [[ -z "$image" ]]; then
    echo "Pod '$pod' has no container image; specify --image." >&2
    exit 1
  fi
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

# Mount the existing pod's files without depending on its crashing process.
exec kubectl -n "$namespace" debug "pod/$pod" -it \
  --image="$image" \
  --profile=general \
  --custom="${script_dir}/orchestration/manifests/cta-debug-profile.yaml" \
  -- bash
