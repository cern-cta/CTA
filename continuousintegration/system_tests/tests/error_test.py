import json
import re
from collections import Counter


def test_no_coredumps(env):
    hosts = env.disk_client + env.cta_cli + env.cta_frontend + env.cta_taped + env.cta_rmcd
    total_core_dumps_found = 0
    for host in hosts:
        core_dump_files = host.execWithOutput("find /var/log/tmp/ -type f -name '*.core' 2>/dev/null").splitlines()
        num_core_dumps = len(core_dump_files)
        if num_core_dumps > 0:
            total_core_dumps_found += num_core_dumps
            print(f"Found {num_core_dumps} core dumps in {host.description}.")
    assert total_core_dumps_found == 0, "core dumps were found"


# Perhaps this should be done after each test
# That gives us more insight into where exactly the error messages were produced
# Only if this can be executed fast enough...
# Something to test at some point
def test_no_uncaught_exceptions(env, error_whitelist):
    hosts = env.cta_frontend + env.cta_taped + env.cta_rmcd
    error_messages = []  # for summaries
    for host in hosts:
        # collect logs
        cmd = f"cat {host.log_file_location} | grep -a -E '(ERROR|CRITICAL)' || true"
        output = host.execWithOutput(cmd).splitlines()
        if not output:
            # Nothing found, happy :D
            continue
        for line in output:
            try:
                entry = json.loads(line)
            except Exception:
                raise RuntimeError(f"Found non-json log line in {host.name}")

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
        if any(re.search(pattern, msg) for pattern in error_whitelist):
            total_non_whitelisted_errors += count
            prefix = whitelist_prefix
        print(f'{prefix} Count: {count}, Message: "{msg}"')

    assert total_non_whitelisted_errors == 0, f"Found {total_non_whitelisted_errors} non-whitelisted logged errors"
