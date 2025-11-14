import json
import re


def test_no_coredumps(env):
    hosts = env.client + env.ctacli + env.ctafrontend + env.ctataped + env.ctarmcd
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
    hosts = env.ctafrontend + env.ctataped + env.ctarmcd
    all_errors = []  # for summaries
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
                raise RuntimeError(f'Found error message with "message" field in {host.name}')

            all_errors.append(msg)

    # Count occurrences
    error_counts: dict[str, int] = {}
    for msg in all_errors:
        error_counts[msg] = error_counts.get(msg, 0) + 1

    # Evaluate against whitelist and collect violations
    total_non_whitelisted_errors = 0
    for msg, count in error_counts.items():
        matched = any(re.search(pattern, msg) for pattern in error_whitelist)
        if not matched:
            total_non_whitelisted_errors += count

    if error_counts:
        print("\nSummary of logged error messages:")
        for msg, count in sorted(error_counts.items(), key=lambda x: -x[1]):
            tag = "(whitelisted)" if msg in error_whitelist else "             "
            print(f'{tag} Count: {count}, Message: "{msg}"')

    assert total_non_whitelisted_errors == 0, f"Found {total_non_whitelisted_errors} non-whitelisted logged errors"
