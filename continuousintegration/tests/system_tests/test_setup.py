from ..helpers.tape import list_all_tapes_in_libraries
import pytest


def test_check_catalogue(env):
    env.ctafrontend[0].exec("cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf")


def test_add_admins(env):
    env.ctafrontend[0].exec("cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf --username ctaadmin1 --comment ctaadmin1")

    print("Adding super client capabilities")
    env.ctacli[0].exec("cta-admin admin add --username ctaadmin2 --comment ctaadmin2")
    # TODO: this should go together with the kinit stuff, then this file can be renamed to test_setup_catalogue
    env.client[0].exec("mkdir -p /tmp/ctaadmin2")
    env.client[0].exec("mkdir -p /tmp/poweruser1")
    env.client[0].exec("mkdir -p /tmp/eosadmin1")

def test_version_info(env):
    print("Versions:")
    env.ctacli[0].exec("cta-admin version")
    env.eosmgm[0].exec("eos version")

def test_populate_catalogue(env):
    print("Populating catalogue")
    # TODO: figure out a nice way to do these paths
    env.ctacli[0].copyTo("system_tests/scripts/populate_catalogue.sh", "/root/populate_catalogue.sh", permissions="+x")
    env.ctacli[0].exec(f"./root/populate_catalogue.sh {env.disk_instance_name}")

def test_populate_catalogue_tapes(env):
    tape_drives_in_use: list[str] = [taped.drive_name() for taped in env.ctataped]
    print("Using drives:")
    for drive in tape_drives_in_use:
        print(f"  - {drive}")

    libraries_in_use: list[str] = [taped.library_device() for taped in env.ctataped]
    print("Using libraries:")
    for lib in libraries_in_use:
        print(f"  - {lib}")

    for drive in tape_drives_in_use:
        # Each drive will have its own logical library. This has to do with how the scheduler works
        add_ll_cmd: str = f"cta-admin logicallibrary add \
                                --name {drive} \
                                --comment \"ctasystest library mapped to drive {drive}\""
        env.ctacli[0].exec(add_ll_cmd)

    tapes: list[str] = list_all_tapes_in_libraries(libraries_in_use)
    print("Using tapes:")
    for tape in tapes:
        print(f"  - {tape}")

    # TODO: collect and execute at once?

    for idx, tape in enumerate(tapes):
        # The logical libraries correspond to the tape drive names by design.
        # For reference, look at how the drives are registered in the catalogue
        logical_library: str = tape_drives_in_use[idx % len(tape_drives_in_use)]
        add_tape_cmd: str = f"cta-admin tape add \
                                --mediatype LTO8 \
                                --purchaseorder order \
                                --vendor vendor \
                                --logicallibrary {logical_library} \
                                --tapepool ctasystest \
                                --comment ctasystest \
                                --vid {tape} \
                                --full false \
                                --comment ctasystest"
        env.ctacli[0].exec(add_tape_cmd)

def test_label_tapes(env):
    libraries_in_use: list[str] = [taped.library_device() for taped in env.ctataped]
    tapes: list[str] = list_all_tapes_in_libraries(libraries_in_use)
    for tape in tapes:
        env.ctataped[0].exec(f"cta-tape-label --vid {tape} --force")

def test_set_all_drives_up(env):
    env.ctacli[0].set_all_drives_up()

def test_cleanup_eos(env):
    env.eosmgm[0].exec("eos rm -rF --no-confirmation /eos/ctaeos/cta/* 2>/dev/null")
    env.eosmgm[0].exec("eos rm -rF --no-confirmation /eos/ctaeos/preprod/* 2>/dev/null")

# For now this is here so that we can easily update new scripts without running the setup again
# This should at some point just be part of the setup
def test_setup_client_scripts_and_certs(env):
    env.client[0].copyTo("system_tests/scripts/client/", "/root/", permissions="+x")
    env.client[0].exec("mv /root/client/* /root && rm -d /root/client")
    env.eosmgm[0].copyFrom("etc/grid-security/certificates/", "/tmp/certificates/")
    env.client[0].copyTo("/tmp/certificates/", "/etc/grid-security/")
    env.execLocal("rm -rf /tmp/certificates")

# At some point the cleanup should be in a separate suite. This allows for skipping of the cleanup
# As this can potentially take some extra time and is completely unnecessary with a fresh instance
# We also still need to add the cleanup of the catalogue here
