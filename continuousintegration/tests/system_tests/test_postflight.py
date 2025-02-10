from continuousintegration.tests.helpers.hosts.remote_host import RemoteHost


def test_no_coredumps(env):
    hosts: list[RemoteHost] = env.client + env.ctacli + env.ctafrontend + env.taped
    total_core_dumps_found=0
    for host in hosts:
        core_dump_files=host.execWithOutput("find /var/log/tmp/ -type f -name '*.core' 2>/dev/null")
        num_core_dumps=len(core_dump_files)
        if (num_core_dumps > 0):
            total_core_dumps_found+=num_core_dumps
            print(f"Found core {num_core_dumps} in {host.getName()}.")
    assert total_core_dumps_found == 0


def test_no_uncaught_exceptions(env):
    ...
