from ..helpers.hosts.cta_rmcd_host import CtaRmcdHost
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle

# Script copying

def test_copy_scripts_to_client(env):
    for client in env.client:
        client.copyTo("tests/remote_scripts/client/", "/test/", permissions="+x")

def test_copy_scripts_to_ctacli(env):
    for ctacli in env.ctacli:
        ctacli.copyTo("tests/remote_scripts/ctacli/", "/test/", permissions="+x")

# Kerberos

def test_kinit_clients(env):
    # TODO: this together with other options should go into a config file somewhere
    krb5_realm="TEST.CTA"
    env.ctacli[0].exec(f"kinit -kt /root/ctaadmin1.keytab ctaadmin1@{krb5_realm}")
    env.client[0].exec(f"kinit -kt /root/user1.keytab user1@{krb5_realm}")


# Catalogue initialization

def test_check_catalogue(env):
    env.ctafrontend[0].exec("cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf")

def test_add_admins(env):
    env.ctafrontend[0].exec("cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf --username ctaadmin1 --comment ctaadmin1")

    print("Adding user ctaadmin2 as CTA admin")
    env.ctacli[0].exec("cta-admin admin add --username ctaadmin2 --comment ctaadmin2")

def test_version_info(env):
    print("Versions:")
    env.ctacli[0].exec("cta-admin version")
    env.eosmgm[0].exec("eos version")

def test_populate_catalogue(env):
    print("Populating catalogue")
    env.ctacli[0].exec(f"./test/populate_catalogue.sh ctaeos")

def test_populate_catalogue_tapes(env):
    tape_drives_in_use: list[str] = [taped.drive_name for taped in env.ctataped]
    print("Using drives:")
    for drive in tape_drives_in_use:
        print(f"  - {drive}")

    libraries_in_use: list[str] = [rmcd.library_device for rmcd in env.ctarmcd]
    print("Using libraries:")
    for lib in libraries_in_use:
        print(f"  - {lib}")

    for drive in tape_drives_in_use:
        # Each drive will have its own logical library. This has to do with how the scheduler works
        add_ll_cmd: str = f"cta-admin logicallibrary add \
                                --name {drive} \
                                --comment \"ctasystest library mapped to drive {drive}\""
        env.ctacli[0].exec(add_ll_cmd)

    tapes: list[str] = CtaRmcdHost.list_all_tapes_in_libraries(env.ctarmcd)
    print("Using tapes:")
    for tape in tapes:
        print(f"  - {tape}")

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

# Tape infrastructure initialisation

def test_reset_tapes(env):
    for ctarmcd in env.ctarmcd:
        ctarmcd.unload_tapes()

def test_reset_drive_devices(env):
    for ctataped in env.ctataped:
        ctataped.exec(f"sg_turs {ctataped.drive_device} 2>&1 > /dev/null || true")

def test_label_tapes(env):
    tapes: list[str] = CtaRmcdHost.list_all_tapes_in_libraries(env.ctarmcd)
    max_workers = len(env.ctataped)

    # Run labeling on every drive
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = []
        for tape, taped in zip(tapes, cycle(env.ctataped)):
            futures.append(pool.submit(taped.label_tape, tape))

        # force errors to surface
        for f in futures:
            f.result()

def test_set_all_drives_up(env):
    env.ctacli[0].set_all_drives_up()
