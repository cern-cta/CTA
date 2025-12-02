from ...helpers.hosts.cta.cta_rmcd_host import CtaRmcdHost
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle


#####################################################################################################################
# Prerequisites
#####################################################################################################################


def test_hosts_present_cta_setup(env):
    assert len(env.disk_instance) > 0
    assert len(env.cta_frontend) > 0
    assert len(env.cta_taped) > 0
    assert len(env.cta_cli) > 0


#####################################################################################################################
# Script copying
#####################################################################################################################


def test_copy_scripts_to_ctacli(env):
    for cta_cli in env.cta_cli:
        cta_cli.copyTo("tests/remote_scripts/cta_cli/", "/test/", permissions="+x")


#####################################################################################################################
# Kerberos
#####################################################################################################################


def test_kinit_clients(env, krb5_realm):
    for cta_cli in env.cta_cli:
        cta_cli.exec(f"kinit -kt /root/ctaadmin1.keytab ctaadmin1@{krb5_realm}")


#####################################################################################################################
# Catalogue initialization
#####################################################################################################################


def test_verify_catalogue(env):
    env.cta_frontend[0].exec("cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf")


def test_add_admins(env):
    env.cta_frontend[0].exec(
        "cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf --username ctaadmin1 --comment ctaadmin1"
    )
    print("Adding user ctaadmin2 as CTA admin")
    # TODO: we should explicitly specify the user we are executing admin commands (should we?)
    env.cta_cli[0].exec("cta-admin admin add --username ctaadmin2 --comment ctaadmin2")


def test_version_info(env):
    print("Versions:")
    env.cta_cli[0].exec("cta-admin --json version | jq")


def test_populate_catalogue(env):
    print("Populating catalogue")
    env.cta_cli[0].exec(f"./test/populate_catalogue.sh {env.disk_instance[0].instance_name}")


def test_populate_catalogue_tapes(env):
    tape_drives_in_use: list[str] = [taped.drive_name for taped in env.cta_taped]
    print("Using drives:")
    for drive in tape_drives_in_use:
        print(f"  - {drive}")

    libraries_in_use: list[str] = [rmcd.library_device for rmcd in env.cta_rmcd]
    print("Using libraries:")
    for lib in libraries_in_use:
        print(f"  - {lib}")

    for drive in tape_drives_in_use:
        # Each drive will have its own logical library. This has to do with how the scheduler works
        add_ll_cmd: str = (
            f'cta-admin logicallibrary add \
                                --name {drive} \
                                --comment "ctasystest library mapped to drive {drive}"'
        )
        env.cta_cli[0].exec(add_ll_cmd)

    tapes: list[str] = CtaRmcdHost.list_all_tapes_in_libraries(env.cta_rmcd)
    print("Using tapes:")
    for tape in tapes:
        print(f"  - {tape}")

    for idx, tape in enumerate(tapes):
        # The logical libraries correspond to the tape drive names by design.
        # For reference, look at how the drives are registered in the catalogue
        logical_library: str = tape_drives_in_use[idx % len(tape_drives_in_use)]
        add_tape_cmd: str = (
            f"cta-admin tape add \
                                --mediatype LTO8 \
                                --purchaseorder order \
                                --vendor vendor \
                                --logicallibrary {logical_library} \
                                --tapepool ctasystest \
                                --vid {tape} \
                                --full false \
                                --comment ctasystest"
        )
        env.cta_cli[0].exec(add_tape_cmd)


#####################################################################################################################
# Tape infrastructure initialisation
#####################################################################################################################


def test_reset_tapes(env):
    for ctarmcd in env.cta_rmcd:
        ctarmcd.unload_tapes()


def test_reset_drive_devices(env):
    for ctataped in env.cta_taped:
        ctataped.exec(f"sg_turs {ctataped.drive_device} 2>&1 > /dev/null || true")


def test_label_tapes(env):
    tapes: list[str] = CtaRmcdHost.list_all_tapes_in_libraries(env.cta_rmcd)
    max_workers = len(env.cta_taped)

    # Run labeling on every drive
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = []
        for tape, taped in zip(tapes, cycle(env.cta_taped)):
            futures.append(pool.submit(taped.label_tape, tape))

        # force errors to surface
        for f in futures:
            f.result()


def test_set_all_drives_up(env):
    env.cta_cli[0].set_all_drives_up()
