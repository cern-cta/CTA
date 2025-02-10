def test_no_coredumps(env):
    hosts = env.client + env.ctacli + env.ctafrontend + env.ctataped
    total_core_dumps_found=0
    for host in hosts:
        core_dump_files=host.execWithOutput("find /var/log/tmp/ -type f -name '*.core' 2>/dev/null").splitlines()
        num_core_dumps=len(core_dump_files)
        if (num_core_dumps > 0):
            total_core_dumps_found+=num_core_dumps
            print(f"Found {num_core_dumps} core dumps in {host.get_short_description()}.")
    assert total_core_dumps_found == 0, "core dumps were found"


def test_no_uncaught_exceptions(env):
    ...
