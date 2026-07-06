# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


from concurrent.futures import ThreadPoolExecutor

from ...helpers.hosts.cta_rmcd_host import CtaRmcdHost

#####################################################################################################################
# Authentication
#####################################################################################################################


def test_kinit_clients(env, krb5_realm):
    # This whole kerberos thing needs to be revised in the future
    # We are relying too much on the default principal in many cases
    # We also need a clean way to manage the different users in a more flexible way instead of hardcoding this everywhere
    # Finally, various tests assume that they have a kerberos ticket for the relevant principal. This may not be the case
    # Instead of doing this before all tests, the relevant tests should kinit (or rather, a fixture should)
    for cta_cli in env.cta_cli:
        cta_cli.exec(f"kinit -kt /root/ctaadmin1.keytab ctaadmin1@{krb5_realm}")

    for eos_client in env.eos_client:
        eos_client.exec("mkdir -p /tmp/eosadmin1")
        eos_client.exec("mkdir -p /tmp/ctaadmin2")
        eos_client.exec("mkdir -p /tmp/poweruser1")
        eos_client.exec(f"kinit -kt /root/eosadmin1.keytab eosadmin1@{krb5_realm}")
        eos_client.exec(f"kinit -kt /root/ctaadmin2.keytab ctaadmin2@{krb5_realm}")
        eos_client.exec(f"kinit -kt /root/user1.keytab user1@{krb5_realm}")


#####################################################################################################################
# Catalogue initialization
#####################################################################################################################


def test_verify_catalogue(cta_frontend):
    cta_frontend.exec("cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf")


def test_add_admins(cta_frontend, cta_cli):
    cta_frontend.exec(
        "cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf --username ctaadmin1 --comment ctaadmin1"
    )
    print("Adding user ctaadmin2 as CTA admin")
    # TODO: we should explicitly specify the user we are executing admin commands (should we?)
    cta_cli.exec("cta-admin admin add --username ctaadmin2 --comment ctaadmin2")


def test_version_info(cta_cli):
    print("Versions:")
    cta_cli.exec("cta-admin --json version | jq")


def test_populate_catalogue(cta_cli, disk_instance_name, cta_storage_class):
    cta_cli.copy_to("tests/remote_scripts/cta_cli/populate_catalogue.sh", "/root/", permissions="+x")
    print("Populating catalogue")
    cta_cli.exec(f"./root/populate_catalogue.sh {disk_instance_name} {cta_storage_class}")


def test_register_logical_libraries_in_catalogue(env, cta_cli):
    logical_library_names_in_use = {taped.logical_library_name for taped in env.cta_taped}
    print("Using logical libraries:")
    for logical_library_name in logical_library_names_in_use:
        print(f"  - {logical_library_name}")

    library_devices_in_use: list[str] = [rmcd.library_device for rmcd in env.cta_rmcd]
    print("Using library devices:")
    for lib in library_devices_in_use:
        print(f"  - {lib}")
    for logical_library_name in logical_library_names_in_use:
        add_ll_cmd: str = (
            f'cta-admin logicallibrary add \
                                --name {logical_library_name} \
                                --comment "ctasystest logical library {logical_library_name} was registered in the catalogue"'
        )
        cta_cli.exec(add_ll_cmd)


def test_register_tapes_per_logical_library_in_catalogue(env, cta_cli):
    logical_library_names_in_use: list[str] = [taped.logical_library_name for taped in env.cta_taped]
    print("Using logical libraries:")
    for logical_library_name in logical_library_names_in_use:
        print(f"  - {logical_library_name}")

    tapes: list[str] = CtaRmcdHost.list_all_tapes_in_libraries(env.cta_rmcd)
    print("Using tapes:")
    for tape in tapes:
        print(f"  - {tape}")

    for idx, tape in enumerate(tapes):
        logical_library: str = logical_library_names_in_use[idx % len(logical_library_names_in_use)]
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
        cta_cli.exec(add_tape_cmd)


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

    # Run labeling on every drive in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = []
        num_drives = len(env.cta_taped)
        for idx, taped in enumerate(env.cta_taped):
            # Give each drive a set of tapes to label
            tapes_to_label = tapes[idx::num_drives]
            futures.append(pool.submit(taped.label_tapes, tapes_to_label))

        # force errors to surface
        for f in futures:
            f.result()


def test_set_all_drives_up(cta_cli):
    cta_cli.set_all_drives_up()
