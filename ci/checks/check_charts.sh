#!/bin/sh

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Lint and render the CI Helm charts locally or in CI, without a Kubernetes cluster.

set -eu

case "${1:-}" in
  --help)
    echo "Usage: $0 [output-directory]"
    echo "Requires Helm 3 and access to the chart repositories."
    exit 0 ;;
  -*) echo "Unsupported option: $1" >&2; exit 2 ;;
esac
[ "$#" -le 1 ] || { echo "Usage: $0 [output-directory]" >&2; exit 2; }

# Resolve output paths before switching to the temporary chart copy.
source_dir=$(CDPATH='' cd -- "$(dirname -- "$0")/../orchestration" && pwd)
output=${1:-"$source_dir/../../build/helm-checks"}
mkdir -p "$output"
output=$(CDPATH='' cd -- "$output" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
trap 'exit 1' HUP INT TERM

# Prepare clean dependencies without changing the developer's chart archives or locks.
tar -C "$source_dir" --exclude=charts --exclude=Chart.lock -cf "$work/source.tar" helm presets
tar -C "$work" -xf "$work/source.tar"
cd "$work"
# Package local dependencies before their consumers; no repository access is needed.
. "$source_dir/../utils/helm_utils.sh"
for chart in $cta_local_charts; do
  helm dependency update "helm/$chart" --skip-refresh --repository-config /dev/null
done
# Infrastructure also needs the pinned upstream telemetry charts.
helm dependency update helm/ci-infra

# dCache consumes the infrastructure database Secret through upstream template helpers.
helm dependency update helm/dcache

# Lint and render identical values, keeping manifests as CI artifacts.
render() {
  chart=$1
  scenario=$2
  shift 2
  helm lint --strict "helm/$chart" "$@"
  helm template check "helm/$chart" --namespace chart-check "$@" > "$output/$scenario.yaml"
}

# Render the actual umbrella with deployment presets and a representative tape drive.
for backend in vfs postgres ceph; do
  render cta "cta-$backend" \
    -f presets/cta/base.yaml \
    -f presets/cta/auth/frontend-admin-jwt.yaml \
    -f presets/cta/auth/frontend-wfe-jwt.yaml \
    -f "presets/cta/scheduler/$backend.yaml" \
    --set global.image.tag=chart-check \
    --set 'taped.drives[0].name=test-drive,taped.drives[0].device=/dev/nst0' \
    --set 'taped.drives[0].controlPath=/dev/sg0,taped.drives[0].logicalLibraryName=test-library'
  render cta-tools "tools-$backend" \
    -f presets/cta/base.yaml \
    -f presets/cta/auth/frontend-admin-jwt.yaml \
    -f "presets/cta/scheduler/$backend.yaml" \
    --set backend.enabled=true,cli.enabled=true,reset.catalogue=true,reset.scheduler=true \
    --set backend.resources.requests.memory=128Mi,cli.resources.requests.memory=256Mi,reset.resources.requests.memory=512Mi
done

# Cover administration modes independently of the scheduler backend matrix.
render cta-tools tools-default
render cta-tools tools-external \
  --set global.catalogue.existingSecret=external-catalogue,global.scheduler.existingSecret=external-scheduler \
  --set global.scheduler.backend=postgres,reset.catalogue=true,reset.scheduler=true
render cta-tools tools-liquibase \
  --set liquibase.enabled=true,liquibase.catalogueSourceVersion=15.0,liquibase.catalogueDestinationVersion=16.0 \
  --set liquibase.resources.requests.memory=1Gi

# Exercise optional infrastructure provisioning and telemetry.
render ci-infra infra-default --set kerberos.resources.requests.memory=128Mi
render ci-infra infra-postgres \
  -f presets/infra/catalogue-postgres.yaml -f presets/infra/scheduler-postgres.yaml -f presets/infra/dcache.yaml
render ci-infra infra-vfs -f presets/infra/scheduler-vfs.yaml
render ci-infra infra-telemetry -f presets/infra/telemetry.yaml
render dcache dcache
