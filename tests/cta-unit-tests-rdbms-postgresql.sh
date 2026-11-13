#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set -euo pipefail

usage() {
  echo "Usage: $0 [--analysis <mode>] [--output-dir <directory>]"
  echo "Run the installed CTA PostgreSQL integration tests as root with a temporary database."
  echo "Requires PostgreSQL server/client tools, runuser and CTA catalogue/test commands on PATH."
  echo "Analysis: none (default), valgrind, address, undefined, address,undefined or thread."
  echo "Sanitizer modes require instrumented packages; valgrind also requires Valgrind on PATH."
  echo "JUnit and logs go under test-results/ and test-logs/ in the output directory (default: current directory)."
}

analysis=none
output_dir="$PWD"
while (( $# )); do
  case "$1" in
    --analysis|--output-dir)
      if [[ $# -lt 2 || -z "$2" ]]; then
        echo "Missing value for $1" >&2
        exit 2
      fi
      if [[ "$1" == --analysis ]]; then analysis="$2"; else output_dir="$2"; fi
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

case "$analysis" in
  none|valgrind|address|undefined|address,undefined|thread) ;;
  *) echo "Unsupported analysis mode: $analysis" >&2; exit 2 ;;
esac
if (( EUID != 0 )); then
  echo "Run as root; PostgreSQL will run as the postgres user." >&2
  exit 1
fi

test_command=(cta-unit-tests-rdbms)
[[ "$analysis" != valgrind ]] || test_command=(valgrind "${test_command[@]}")
for command in initdb pg_ctl createuser createdb runuser cta-catalogue-schema-create "${test_command[@]}"; do
  command -v "$command" >/dev/null || { echo "Required command not found: $command" >&2; exit 1; }
done

mkdir -p "$output_dir/test-results" "$output_dir/test-logs"
output_dir=$(realpath "$output_dir")
work_dir=$(mktemp -d /tmp/cta-postgresql-tests.XXXXXX)
data_dir="$work_dir/data"
postgres_log="$work_dir/postgres.log"
catalogue_config="$work_dir/catalogue.conf"

cleanup() {
  local status=$? cleanup_status=0 stopped=true
  trap - EXIT
  if runuser -u postgres -- pg_ctl status -D "$data_dir" >/dev/null 2>&1; then
    runuser -u postgres -- pg_ctl stop -w -m fast -D "$data_dir" || { cleanup_status=1; stopped=false; }
  fi
  if [[ -f "$postgres_log" ]]; then
    cp "$postgres_log" "$output_dir/test-logs/cta_test_postgres.log" || cleanup_status=1
  fi
  # Keep database files if shutdown fails; never remove a running server's data directory.
  if [[ "$stopped" == true ]]; then rm -rf -- "$work_dir" || cleanup_status=1; fi
  if (( status == 0 )); then status=$cleanup_status; fi
  exit "$status"
}
trap cleanup EXIT

chown postgres:postgres "$work_dir"
runuser -u postgres -- initdb --auth=trust -D "$data_dir"
# Keep the Unix socket inside the temporary directory instead of relying on distribution defaults.
runuser -u postgres -- pg_ctl start -w -t 10 -D "$data_dir" -l "$postgres_log" -o "-k $work_dir"
runuser -u postgres -- createuser --host "$work_dir" cta
# Database ownership permits schema creation with PostgreSQL 15+ defaults.
runuser -u postgres -- createdb --host "$work_dir" --owner=cta cta
printf 'postgresql:postgresql://cta@localhost/cta\n' > "$catalogue_config"
cta-catalogue-schema-create "$catalogue_config"

# pipefail preserves test failures while tee retains output for the CI artifact.
GTEST_OUTPUT="xml:$output_dir/test-results/cta-unit-tests-rdbms.xml" \
  "${test_command[@]}" "$catalogue_config" 2>&1 | tee "$output_dir/test-logs/cta-unit-tests-rdbms.log"
