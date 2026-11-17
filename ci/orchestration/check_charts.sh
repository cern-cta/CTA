#!/bin/sh

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Lint/render or unit-test the CI Helm charts locally or in CI, without a Kubernetes cluster.

set -eu

# Select the CI job's check and an optional artifact directory.
case "${1:-}" in
  lint|test) mode=$1 ;;
  --help)
    echo "Usage: $0 lint|test [output-directory]"
    echo "Requires Helm 3; test also requires helm-unittest. Dependencies are downloaded explicitly."
    exit 0 ;;
  *) echo "Usage: $0 lint|test [output-directory]" >&2; exit 2 ;;
esac
[ "$#" -le 2 ] || { echo "Too many arguments" >&2; exit 2; }

# Resolve output paths before switching to the temporary chart copy.
source_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
output=${2:-"$source_dir/../../build/helm-checks/$mode"}
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
for chart in common frontend-admin frontend-wfe taped rmcd maintd cta-admin-tools cta; do
  helm dependency update "helm/$chart" --skip-refresh --repository-config /dev/null
done
# Infrastructure also needs the pinned upstream telemetry charts.
helm dependency update helm/ci-infra

if [ "$mode" = test ]; then
  # Only our suites run: dependency chart tests belong to their upstream projects.
  helm unittest --strict --with-subchart=false --output-type JUnit \
    --output-file "$output/helm.xml" helm/cta helm/cta-admin-tools helm/ci-infra
  exit
fi

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
  render cta-admin-tools "tools-$backend" \
    -f "presets/cta/scheduler/$backend.yaml" \
    --set admin.enabled=true,client.enabled=true,reset.allUnprotected=true
done

# Cover administration modes independently of the scheduler backend matrix.
render cta-admin-tools tools-default
render cta-admin-tools tools-protected \
  --set global.catalogue.protected=true,global.scheduler.protected=true,reset.allUnprotected=true
render cta-admin-tools tools-liquibase \
  --set liquibase.enabled=true,liquibase.catalogueSourceVersion=15.0,liquibase.catalogueDestinationVersion=16.0

# Exercise optional infrastructure provisioning and telemetry.
render ci-infra infra-default
render ci-infra infra-postgres \
  -f presets/infra/catalogue-postgres.yaml -f presets/infra/scheduler-postgres.yaml -f presets/infra/dcache.yaml
render ci-infra infra-vfs -f presets/infra/scheduler-vfs.yaml
render ci-infra infra-telemetry -f presets/infra/telemetry.yaml

# Schema failures must be caught before templates reach Kubernetes.
reject_values() {
  chart=$1
  scenario=$2
  shift 2
  if helm template check "helm/$chart" "$@" > "$output/$scenario.log" 2>&1; then
    echo "Expected values schema rejection: $scenario" >&2
    exit 1
  fi
  if ! grep -q 'values don.t meet the specifications of the schema' "$output/$scenario.log"; then
    cat "$output/$scenario.log" >&2
    exit 1
  fi
}

reject_values ci-infra invalid-postgres-password --set postgres.instances.example.password=123
reject_values ci-infra invalid-postgres-option --set postgres.instances.example.user=cta
reject_values ci-infra missing-pvc-size --set persistentVolumeClaims.example.storageClass=local-path
reject_values ci-infra invalid-pvc-access-mode --set 'persistentVolumeClaims.example.size=1Gi,persistentVolumeClaims.example.accessModes[0]=ReadWrite'
reject_values ci-infra invalid-secret-file --set secretFiles.example.connection=123
reject_values ci-infra invalid-telemetry-toggle --set-string prometheus.enabled=false
reject_values cta-admin-tools invalid-protection-toggle --set-string global.catalogue.protected=false
reject_values cta-admin-tools invalid-reset-toggle --set-string reset.catalogue=false
reject_values cta-admin-tools invalid-scheduler-backend --set global.scheduler.backend=unknown
reject_values cta-admin-tools missing-vfs-claim --set-string global.scheduler.existingClaim=
reject_values cta-admin-tools invalid-client-replicas --set client.replicas=-1
reject_values cta-admin-tools invalid-pull-policy --set image.pullPolicy=Sometimes
reject_values cta-admin-tools obsolete-database-toggle --set database.enabled=true
reject_values cta-admin-tools obsolete-reset-toggle --set reset.unprotected=true
reject_values cta-admin-tools obsolete-shared-resources --set resources.requests.memory=128Mi
reject_values ci-infra obsolete-credential-image --set secrets.image=example:latest
