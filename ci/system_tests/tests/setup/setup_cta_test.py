# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


from concurrent.futures import ThreadPoolExecutor

from system_tests.helpers.hosts import CtaAdminApiHost, CtaCliHost, CtaRmcdHost, DiskInstanceHost
from system_tests.helpers.test_env import TestEnv

# =========================================================================
#  Authentication
# =========================================================================


# Should not be necessary once we rely only on JWT
def test_kinit_cta_admin(env: TestEnv, krb5_realm: str) -> None:
    for cta_cli in env.cta_cli:
        cta_cli.exec(f"kinit -kt /root/ctaadmin1.keytab ctaadmin1@{krb5_realm}")


# =========================================================================
#  Catalogue initialization
# =========================================================================


def test_verify_catalogue(cta_admin_api: CtaAdminApiHost) -> None:
    cta_admin_api.exec("cta-catalogue-schema-verify /etc/cta/cta-catalogue.conf")


def test_add_admins(cta_admin_api: CtaAdminApiHost, cta_cli: CtaCliHost) -> None:
    cta_admin_api.exec(
        "cta-catalogue-admin-user-create /etc/cta/cta-catalogue.conf --username ctaadmin1 --comment ctaadmin1"
    )
    print("Adding user ctaadmin2 as CTA admin")
    # TODO: we should explicitly specify the user we are executing admin commands (should we?)
    cta_cli.exec("cta-admin admin add --username ctaadmin2 --comment ctaadmin2")


def test_version_info(cta_cli: CtaCliHost) -> None:
    print("Versions:")
    cta_cli.exec("cta-admin --json version | jq")


def test_add_media_types(cta_cli: CtaCliHost) -> None:
    media_types = [
        ("3592JC7T", 7_000_000_000_000, 84, "3592JC", "IBM 3592JC cartridge formatted at 7 TB"),
        ("3592JD15T", 15_000_000_000_000, 85, "3592JD", "IBM 3592JD cartridge formatted at 15 TB"),
        ("3592JE20T", 20_000_000_000_000, 87, "3592JE", "IBM 3592JE cartridge formatted at 20 TB"),
        ("3592JF50T", 50_000_000_000_000, 89, "3592JF", "IBM 3592JF cartridge formatted at 50 TB"),
        ("LTO7M", 9_000_000_000_000, 93, "LTO-7", "LTO-7 M8 cartridge formatted at 9 TB"),
        ("LTO8", 12_000_000_000_000, 94, "LTO-8", "LTO-8 cartridge formatted at 12 TB"),
        ("LTO9", 18_000_000_000_000, 96, "LTO-9", "LTO-9 cartridge formatted at 18 TB"),
        ("LTO10S", 30_000_000_000_000, 98, "LTO-10", "LTO-10 standard cartridge formatted at 30 TB"),
    ]
    for name, capacity, density_code, cartridge, comment in media_types:
        cta_cli.exec(
            f"cta-admin mediatype add --name {name} --capacity {capacity} "
            f"--primarydensitycode {density_code} --cartridge {cartridge} --comment '{comment}'"
        )


def test_register_logical_libraries_in_catalogue(env: TestEnv, cta_cli: CtaCliHost) -> None:
    logical_library_names_in_use = {taped.logical_library_name for taped in env.cta_taped}
    print("Using logical libraries:")
    for logical_library_name in logical_library_names_in_use:
        print(f"  - {logical_library_name}")

    library_devices_in_use: list[str] = [rmcd.library_device for rmcd in env.cta_rmcd]
    print("Using library devices:")
    for lib in library_devices_in_use:
        print(f"  - {lib}")
    for logical_library_name in logical_library_names_in_use:
        comment = f"ctasystest logical library {logical_library_name} was registered in the catalogue"
        add_ll_cmd = f"cta-admin logicallibrary add   --name {logical_library_name}   --comment '{comment}'"
        cta_cli.exec(add_ll_cmd)


def test_configure_disk_instance_in_catalogue(
    disk_instance: DiskInstanceHost,
    cta_cli: CtaCliHost,
    cta_storage_class: str,
    cta_default_tape_pool: str,
) -> None:
    disk_instance_name = disk_instance.instance_name
    commands = [
        f"cta-admin diskinstance add --name {disk_instance_name} --comment 'Disk instance'",
        (
            "cta-admin virtualorganization add --vo vo --readmaxdrives 1 --writemaxdrives 1 "
            f"--diskinstance {disk_instance_name} --comment 'VO for system tests'"
        ),
        (f"cta-admin storageclass add --name {cta_storage_class} --numberofcopies 1 --vo vo --comment ctasystest"),
        (f"cta-admin tapepool add --name {cta_default_tape_pool} --vo vo --partialtapesnumber 5 --comment ctasystest"),
        (
            f"cta-admin archiveroute add --storageclass {cta_storage_class} --copynb 1 "
            f"--tapepool {cta_default_tape_pool} --comment ctasystest"
        ),
        (
            "cta-admin mountpolicy add --name ctasystest --archivepriority 1 --minarchiverequestage 1 "
            "--retrievepriority 1 --minretrieverequestage 1 --comment ctasystest"
        ),
        (
            f"cta-admin requestermountrule add --instance {disk_instance_name} --name adm "
            "--mountpolicy ctasystest --comment ctasystest"
        ),
        (
            f"cta-admin groupmountrule add --instance {disk_instance_name} --name {disk_instance.archive_group} "
            "--mountpolicy ctasystest --comment ctasystest"
        ),
        (
            f"cta-admin groupmountrule add --instance {disk_instance_name} --name powerusers "
            "--mountpolicy ctasystest --comment ctasystest"
        ),
        (
            f"cta-admin activitymountrule add --instance {disk_instance_name} --name powerusers "
            "--activityregex '^T0Reprocess$' --mountpolicy ctasystest --comment ctasystest"
        ),
    ]
    for command in commands:
        cta_cli.exec(command)


def test_register_tapes_per_logical_library_in_catalogue(
    env: TestEnv,
    cta_cli: CtaCliHost,
    cta_default_tape_pool: str,
) -> None:
    logical_library_names = [taped.logical_library_name for taped in env.cta_taped]
    tapes = CtaRmcdHost.list_all_tapes_in_libraries(env.cta_rmcd)

    print("Using tapes:")
    for tape in tapes:
        print(f"  - {tape}")

    for index, tape in enumerate(tapes):
        logical_library = logical_library_names[index % len(logical_library_names)]
        cta_cli.exec(
            "cta-admin tape add "
            "--mediatype LTO8 "
            "--purchaseorder order "
            "--vendor vendor "
            f"--logicallibrary {logical_library} "
            f"--tapepool {cta_default_tape_pool} "
            f"--vid {tape} "
            "--full false "
            "--comment ctasystest"
        )


# =========================================================================
#  Tape infrastructure initialisation
# =========================================================================


def test_reset_tapes(env: TestEnv) -> None:
    for rmcd in env.cta_rmcd:
        rmcd.unload_tapes()


def test_reset_drive_devices(env: TestEnv) -> None:
    for taped in env.cta_taped:
        taped.exec(f"sudo sg_turs {taped.drive_device} 2>&1 > /dev/null || true")


def test_label_tapes(env: TestEnv) -> None:
    tapes = CtaRmcdHost.list_all_tapes_in_libraries(env.cta_rmcd)
    with ThreadPoolExecutor(max_workers=len(env.cta_taped)) as pool:
        futures = []
        num_drives = len(env.cta_taped)
        for index, taped in enumerate(env.cta_taped):
            futures.append(pool.submit(taped.label_tapes, tapes[index::num_drives]))
        for future in futures:
            future.result()


def test_set_all_drives_up(cta_cli: CtaCliHost) -> None:
    assert cta_cli.exec_with_output("cta-admin --json drive ls") != "[]", "No drives found in CTA"
    cta_cli.set_all_drives_up()
