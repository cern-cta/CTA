# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json

from system_tests.helpers.error_expectations import ErrorExpectations
from system_tests.helpers.restart_expectations import RestartExpectations
from system_tests.helpers.test_env import TestEnv


def test_no_coredumps(env: TestEnv) -> None:
    hosts = [*env.cta_cli, *env.cta_admin_api, *env.cta_workflow_api, *env.cta_taped, *env.cta_rmcd, *env.cta_maintd]
    total_core_dumps_found = 0
    for host in hosts:
        core_dump_files = host.exec_with_output(
            "sudo find /var/log/tmp/ -type f -name '*.core' 2>/dev/null"
        ).splitlines()
        num_core_dumps = len(core_dump_files)
        if num_core_dumps > 0:
            total_core_dumps_found += num_core_dumps
            print(f"Found {num_core_dumps} core dumps in {host.description}.")
    assert total_core_dumps_found == 0, "core dumps were found"


def test_no_uncaught_exceptions_and_errors(env: TestEnv, error_expectations: ErrorExpectations) -> None:
    hosts = [*env.cta_admin_api, *env.cta_workflow_api, *env.cta_taped, *env.cta_rmcd, *env.cta_maintd]
    entries = []
    for host in hosts:
        # collect logs
        cmd = f"""grep -aE '\\"log_level\\": *\\"(ERROR|CRITICAL)\\"' {host.log_file_path} || true"""
        output = host.exec_with_output(cmd).splitlines()
        if not output:
            # Nothing found, happy :D
            continue
        for line in output:
            try:
                entry = json.loads(line)
            except Exception as error:
                raise RuntimeError(f"Found non-json log line in {host.name}") from error

            msg = entry.get("message", "")
            if not msg:
                raise RuntimeError(f'Found error message with missing "message" field in {host.name}')

            entries.append(entry)

    error_expectations.verify(entries)


def test_no_unexpected_restarts(env: TestEnv, restart_expectations: RestartExpectations) -> None:
    restart_expectations.verify()
