# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import re
from collections import Counter
from pathlib import Path

from system_tests.helpers.test_env import TestEnv


def test_no_coredumps(env: TestEnv) -> None:
    hosts = [*env.cta_cli, *env.cta_admin_api, *env.cta_workflow_api, *env.cta_taped, *env.cta_rmcd]
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


def test_no_uncaught_exceptions_and_errors(env: TestEnv, error_whitelist: set[str]) -> None:
    hosts = [*env.cta_admin_api, *env.cta_workflow_api, *env.cta_taped, *env.cta_rmcd]
    error_messages = []  # for summaries
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

            error_messages.append(msg)

    # No errors found :D
    if not error_messages:
        return

    # For each message, how often did it occur
    error_counts: dict[str, int] = Counter(error_messages)

    # Evaluate against whitelist and collect violations
    total_non_whitelisted_errors = 0
    print("\nSummary of logged error messages:")
    for msg, count in sorted(error_counts.items(), key=lambda x: -x[1]):
        whitelist_prefix = "(whitelisted)"
        prefix = " " * len(whitelist_prefix)
        # Check if we can pattern match the message based on a pattern in the whitelist
        if any(pattern in msg for pattern in error_whitelist):
            prefix = whitelist_prefix
        else:
            total_non_whitelisted_errors += count
        print(f'{prefix} Count: {count}, Message: "{msg}"')
    assert total_non_whitelisted_errors == 0, f"Found {total_non_whitelisted_errors} non-whitelisted logged errors"


def test_no_sanitizer_errors(env: TestEnv, tmp_path: Path) -> None:
    hosts = [*env.cta_admin_api, *env.cta_workflow_api, *env.cta_taped, *env.cta_rmcd, *env.cta_maintd]
    # Sanitizers write plain text to stderr, outside the JSON application logs.
    diagnostic = re.compile(
        r"(?:ERROR|WARNING|SUMMARY): (?:Address|Leak|Thread|Memory|UndefinedBehavior)Sanitizer"
        r"|AddressSanitizer:DEADLYSIGNAL|runtime error:"
        r"|LeakSanitizer has encountered a fatal error|ASan runtime does not come first"
    )
    failures = []
    for host in hosts:
        logs = host.conn.get_service_logs()
        if diagnostic.search(logs):
            log_path = tmp_path / f"{host.name}.log"
            log_path.write_text(logs)
            matches = "\n".join(line for line in logs.splitlines() if diagnostic.search(line))
            print(f"Sanitizer diagnostics from {host.description}:\n{matches}\nFull log: {log_path}")
            failures.append(host.name)
    assert not failures, f"Sanitizer diagnostics found on: {', '.join(failures)}"
