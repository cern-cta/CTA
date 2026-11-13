#!/bin/bash

# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Shared by the CI jobs and tests/cta-unit-tests-rdbms-postgresql.sh. Callers must enable pipefail and
# initialise test_status=0. JUnit XML and logs go under test-results/ and test-logs/ in CTA_TEST_OUTPUT_DIR
# (default: current directory), which must already contain both directories.

# Run a test command, recording failures in test_status without skipping later tests or database cleanup.
run_cpp_test() {
  local name="$1" output_dir="${CTA_TEST_OUTPUT_DIR:-$PWD}"
  shift
  GTEST_OUTPUT="xml:${output_dir}/test-results/${name}.xml" \
    "$@" 2>&1 | tee "${output_dir}/test-logs/${name}.log" || test_status=1
}
