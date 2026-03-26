# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import json
import uuid

import pytest

from ..helpers.hosts import CtaCliHost, CtaTapedHost
from ..helpers.utils import (
    TempDiskInstanceSpace,
    TempLogicalLibrary,
    TempMountPolicy,
    TempPhysicalLibrary,
    TempStorageClass,
    TempTape,
    TempTapePool,
    TempVirtualOrganization,
    assert_dict_equals,
    wait_for_condition,
    canonicalize,
)

# NOTE: these tests are only meant for cta-admin tests. Other tools should get their own test suite
#
# The tests below are relative simple as they all (mostly) follow the same pattern:
#
# 1. Create the resource
# 2. Execute an `ls` command and assert the created resource has the expected attributes
# 3. Update the resource
# 4. Execute another `ls` command and assert that the result is the same after the create, except for the update keys
# 5. Assert that the updated attributes of the update resource are as expected
# 6. Delete the resource
#
# There are two important things to consider when writing these system tests:
#
# - A passing test case MUST leave the system in the same state as it was. I.e. tests MUST be idempotent.
# - If a test builds on certain assumptions, then the test MUST assert these assumptions beforehand.
# - Most CRUD tests use a try: finally: construct to clean up the resource even if the read/update fails.
#   Note that this is not 100% foolproof, but it is the simplest solution giving us improved idempotency.
#


#####################################################################################################################
# Helpers
#####################################################################################################################


def is_in_repacking_state(cta_cli, vid_to_check):
    ta_ls_out = cta_cli.execWithOutput(f"cta-admin --json ta ls -v {vid_to_check}")
    ta_ls_json = json.loads(ta_ls_out)
    if len(ta_ls_json) != 1:
        return False
    return ta_ls_json[0]["state"] == "REPACKING"


#####################################################################################################################
# Tests
#####################################################################################################################


def test_hosts_present_tools(env):
    assert len(env.cta_taped) > 0
    assert len(env.cta_frontend) > 0
    assert len(env.cta_cli) > 0
    assert len(env.disk_client) > 0


# -------------------------------------------------------------------------------------------------
# Misc - v
# -------------------------------------------------------------------------------------------------


def test_cta_admin_version(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    out = cta_cli.execWithOutput("cta-admin --json v")
    assert out.strip() != ""


# -------------------------------------------------------------------------------------------------
# Users - ad, vo
# -------------------------------------------------------------------------------------------------


def test_cta_admin_admin(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    ad_name = "test_cta_admin_admin"

    ls_before = cta_cli.execWithOutput("cta-admin --json ad ls")

    # Create
    cta_cli.exec(f"cta-admin ad add -u {ad_name} -m 'Create {ad_name}'")
    try:
        # Read
        ad_created = cta_cli.get_single_ls_item("ad ls", lambda x: x["user"] == ad_name)
        assert ad_created["comment"] == f"Create {ad_name}"

        # Update
        cta_cli.exec(f"cta-admin ad ch -u {ad_name} -m 'Update {ad_name}'")
        ad_updated = cta_cli.get_single_ls_item("ad ls", lambda x: x["user"] == ad_name)
        assert_dict_equals(ad_updated, ad_created, ["lastModificationLog", "comment"])
        assert ad_updated["comment"] == f"Update {ad_name}"

    finally:
        # Delete
        cta_cli.exec(f"cta-admin ad rm -u {ad_name}")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json ad ls")


def test_cta_admin_virtual_organization(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    vo_name = "test_cta_admin_virtual_organization_vo"

    ls_before = cta_cli.execWithOutput("cta-admin --json vo ls")

    # Create
    cta_cli.exec(f"cta-admin vo add --vo '{vo_name}' --rmd 1 --wmd 1 --di '{disk_instance_name}' -m 'Create {vo_name}'")
    try:
        # Read
        vo_created = cta_cli.get_single_ls_item("vo ls", lambda x: x["name"] == vo_name)
        assert vo_created["comment"] == f"Create {vo_name}"
        assert int(vo_created["readMaxDrives"]) == 1
        assert int(vo_created["writeMaxDrives"]) == 1
        assert vo_created["diskinstance"] == disk_instance_name
        assert vo_created["isRepackVo"] is False

        # Update
        cta_cli.exec(f"cta-admin vo ch --vo '{vo_name}' --wmd 2 --mfs 100 --isrepackvo false -m 'Update {vo_name}'")
        vo_updated = cta_cli.get_single_ls_item("vo ls", lambda x: x["name"] == vo_name)
        assert_dict_equals(
            vo_updated, vo_created, ["lastModificationLog", "writeMaxDrives", "maxFileSize", "isRepackVo", "comment"]
        )
        assert vo_updated["comment"] == f"Update {vo_name}"
        assert int(vo_updated["writeMaxDrives"]) == 2
        assert vo_updated["isRepackVo"] is False
        assert int(vo_updated["maxFileSize"]) == 100
    finally:
        # Delete
        cta_cli.exec(f"cta-admin vo rm --vo '{vo_name}'")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json vo ls")


# -------------------------------------------------------------------------------------------------
# Disk - di, dis, ds
# -------------------------------------------------------------------------------------------------


def test_cta_admin_disk_instance(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    di_name = "test_cta_admin_disk_instance_di"

    ls_before = cta_cli.execWithOutput("cta-admin --json di ls")

    # Create
    cta_cli.exec(f"cta-admin di add -n {di_name} -m 'Create {di_name}'")
    try:
        # Read
        di_created = cta_cli.get_single_ls_item("di ls", lambda x: x["name"] == di_name)
        assert di_created["comment"] == f"Create {di_name}"

        # Update
        cta_cli.exec(f"cta-admin di ch -n {di_name} -m 'Update {di_name}'")
        di_updated = cta_cli.get_single_ls_item("di ls", lambda x: x["name"] == di_name)
        assert_dict_equals(di_updated, di_created, ["lastModificationLog", "comment"])
        assert di_updated["comment"] == f"Update {di_name}"
    finally:
        # Delete
        cta_cli.exec(f"cta-admin di rm -n {di_name}")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json di ls")


def test_cta_admin_disk_instance_space(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    dis_name = "test_cta_admin_disk_instance_space_dis"

    ls_before = cta_cli.execWithOutput("cta-admin --json dis ls")

    # Create
    cta_cli.exec(
        f"cta-admin dis add -n {dis_name} --di {disk_instance_name} -i 10 -u eosSpace:default -m 'Create {dis_name}'"
    )
    try:
        # Read
        dis_created = cta_cli.get_single_ls_item("dis ls", lambda x: x["name"] == dis_name)
        assert dis_created["comment"] == f"Create {dis_name}"
        assert dis_created["diskInstance"] == disk_instance_name
        assert dis_created["freeSpaceQueryUrl"] == "eosSpace:default"
        assert int(dis_created["refreshInterval"]) == 10

        # Update
        cta_cli.exec(f"cta-admin dis ch -n {dis_name} --di {disk_instance_name} -i 100 -m 'Update {dis_name}'")
        dis_updated = cta_cli.get_single_ls_item("dis ls", lambda x: x["name"] == dis_name)
        assert_dict_equals(dis_updated, dis_created, ["lastModificationLog", "refreshInterval", "comment"])
        assert dis_updated["comment"] == f"Update {dis_name}"
        assert int(dis_updated["refreshInterval"]) == 100
    finally:
        # Delete
        cta_cli.exec(f"cta-admin dis rm -n {dis_name} --di {disk_instance_name}")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json dis ls")


def test_cta_admin_disk_system(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    ds_name = "test_cta_admin_disk_system_ds"
    dis_name = "test_cta_admin_disk_system_dis"

    ls_before = cta_cli.execWithOutput("cta-admin --json ds ls")

    with TempDiskInstanceSpace(cta_cli, dis_name, disk_instance_name):
        # Create
        cta_cli.exec(
            f"cta-admin ds add -n {ds_name} --di {disk_instance_name} --dis {dis_name} "
            f"-r root://{disk_instance_name}//eos/ctaeos/cta/ -f 10000 -s 10 -m 'Create {ds_name}'"
        )
        try:
            # Read
            ds_created = cta_cli.get_single_ls_item("ds ls", lambda x: x["name"] == ds_name)
            assert ds_created["comment"] == f"Create {ds_name}"
            assert ds_created["diskInstance"] == disk_instance_name
            assert ds_created["diskInstanceSpace"] == dis_name
            assert ds_created["fileRegexp"] == f"root://{disk_instance_name}//eos/ctaeos/cta/"
            assert int(ds_created["targetedFreeSpace"]) == 10000
            assert int(ds_created["sleepTime"]) == 10

            # Update
            cta_cli.exec(f"cta-admin ds ch -n {ds_name} -s 100 -m 'Update {ds_name}'")
            ds_updated = cta_cli.get_single_ls_item("ds ls", lambda x: x["name"] == ds_name)
            assert_dict_equals(ds_updated, ds_created, ["lastModificationLog", "sleepTime", "comment"])
            assert ds_updated["comment"] == f"Update {ds_name}"
            assert int(ds_updated["sleepTime"]) == 100
        finally:
            # Delete
            cta_cli.exec(f"cta-admin ds rm -n {ds_name}")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json ds ls")


# -------------------------------------------------------------------------------------------------
# Tape - ta, tf, tp, dr, pl, ll, mt, rtf
# -------------------------------------------------------------------------------------------------


def test_cta_admin_tape(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    pl_name = "test_cta_admin_tape_pl1"
    ll_name = "test_cta_admin_tape_ll1"
    ta_vid = "ULT9999"
    tp_name = "ctasystest"

    ls_before = cta_cli.execWithOutput("cta-admin --json ta ls --all")

    with TempPhysicalLibrary(cta_cli, pl_name), TempLogicalLibrary(cta_cli, ll_name, pl_name):
        # Create
        cta_cli.exec(
            f"cta-admin ta add -v {ta_vid} --mt LTO8 --ve testvendor -l {ll_name} -t {tp_name} -f true --purchaseorder order1 -m 'Create {ta_vid}'"
        )
        try:
            # Read
            ta_created = cta_cli.get_single_ls_item("ta ls --all", lambda x: x["vid"] == ta_vid)
            assert ta_created["mediaType"] == "LTO8"
            assert ta_created["vendor"] == "testvendor"
            assert ta_created["logicalLibrary"] == ll_name
            assert ta_created["tapepool"] == tp_name
            assert ta_created["purchaseOrder"] == "order1"
            assert ta_created["full"]
            assert ta_created["dirty"]
            assert ta_created["comment"] == f"Create {ta_vid}"
            assert ta_created["state"] == "ACTIVE"
            assert ta_created["labelFormat"] == "CTA"
            assert int(ta_created["occupancy"]) == 0
            assert int(ta_created["lastFseq"]) == 0
            assert int(ta_created["readMountCount"]) == 0
            assert int(ta_created["writeMountCount"]) == 0
            assert int(ta_created["nbMasterFiles"]) == 0
            assert int(ta_created["masterDataInBytes"]) == 0

            # Update Reclaim
            cta_cli.exec(f"cta-admin ta reclaim -v {ta_vid}")
            ta_updated1 = cta_cli.get_single_ls_item("ta ls --all", lambda x: x["vid"] == ta_vid)
            assert_dict_equals(ta_updated1, ta_created, ["dirty", "full", "lastModificationLog", "stateUpdateTime"])
            assert not ta_updated1["full"]
            assert not ta_updated1["dirty"]

            # Update State
            cta_cli.exec(f"cta-admin ta ch -v {ta_vid} -s 'REPACKING' -r 'Update state change'")
            wait_for_condition(lambda: is_in_repacking_state(cta_cli, ta_vid))
            ta_updated2 = cta_cli.get_single_ls_item("ta ls --all", lambda x: x["vid"] == ta_vid)
            assert_dict_equals(
                ta_updated2,
                ta_updated1,
                ["state", "stateReason", "lastModificationLog", "stateUpdateTime", "stateModifiedBy"],
            )
            assert ta_updated2["state"] == "REPACKING"
            assert ta_updated2["stateReason"] == "Update state change"

            cta_cli.exec(f"cta-admin ta ch -v {ta_vid} --purchaseorder 'order2'")
            ta_updated3 = cta_cli.get_single_ls_item("ta ls --all", lambda x: x["vid"] == ta_vid)
            assert_dict_equals(ta_updated3, ta_updated2, ["purchaseOrder", "lastModificationLog"])
            assert ta_updated3["purchaseOrder"] == "order2"
        finally:
            # Delete
            cta_cli.exec(f"cta-admin ta rm -v {ta_vid}")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json ta ls --all")


@pytest.mark.eos
def test_cta_admin_tape_file(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    vids: list[str] = cta_cli.list_all_tape_vids()
    assert vids, "No tape VIDs available to test tape file commands."

    ls_before = [cta_cli.execWithOutput(f"cta-admin --json tf ls -v {vid}") for vid in vids]

    # Archive one file
    test_file_path = "/eos/ctaeos/cta/cta_admin_tf_testfile_" + str(uuid.uuid4())[:8]
    env.disk_client[0].generate_and_archive_file(disk_instance_name, destination_path=test_file_path, wait=True)

    # Figure out the fxid and VID
    file_info_out = env.disk_instance[0].execWithOutput(f"eos -j file info {test_file_path}")
    fxid = json.loads(file_info_out)["fxid"]

    tf_ls_out = cta_cli.execWithOutput(f"cta-admin --json tf ls --fxid {fxid} -i ctaeos")
    tf_ls_json = json.loads(tf_ls_out)[0]
    vid = tf_ls_json["tf"]["vid"]
    archive_id = tf_ls_json["af"]["archiveId"]

    # Removing should fail (single copy)
    with pytest.raises(RuntimeError):
        print("Expected failure after attempt to remove single copy:")
        cta_cli.exec(f"cta-admin tf rm -v {vid} -i ctaeos -I {archive_id} -r Test")

    env.disk_client[0].delete_file(disk_instance_name, path=test_file_path)

    wait_for_condition(lambda: not cta_cli.file_exists_in_cta(vid, archive_id))

    ls_after = [cta_cli.execWithOutput(f"cta-admin --json tf ls -v {vid}") for vid in vids]
    assert ls_before == ls_after


def test_cta_admin_tape_pool(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    tp_name = "test_cta_admin_tape_pool_tp"
    vo_name = "test_cta_admin_tape_pool_vo"

    ls_before = cta_cli.execWithOutput("cta-admin --json tp ls")

    with TempVirtualOrganization(cta_cli, vo_name, disk_instance_name):
        # Create
        cta_cli.exec(f"cta-admin tp add -n '{tp_name}' --vo {vo_name} -p 0 -m 'Create {tp_name}'")
        try:
            # Read
            tp_created = cta_cli.get_single_ls_item("tp ls", lambda x: x["name"] == tp_name)
            assert tp_created["vo"] == vo_name
            assert int(tp_created["numTapes"]) == 0
            assert int(tp_created["numPartialTapes"]) == 0
            assert int(tp_created["numPhysicalFiles"]) == 0
            assert int(tp_created["dataBytes"]) == 0
            assert int(tp_created["encrypt"]) == 0
            assert tp_created["encryptionKeyName"] == ""
            assert tp_created["comment"] == f"Create {tp_name}"

            # Update
            cta_cli.exec(f"cta-admin tp ch -n '{tp_name}' -k encrypt_key_name")
            tp_updated = cta_cli.get_single_ls_item("tp ls", lambda x: x["name"] == tp_name)
            assert_dict_equals(tp_updated, tp_created, ["modified", "encrypt", "encryptionKeyName"])
            assert tp_updated["encrypt"]
            assert tp_updated["encryptionKeyName"] == "encrypt_key_name"
        finally:
            # Delete
            cta_cli.exec(f"cta-admin tp rm -n '{tp_name}'")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json tp ls")


def test_cta_admin_drive(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    cta_taped: CtaTapedHost = env.cta_taped[0]
    pl_name = "test_cta_admin_tape_pl1"
    ll_name = "test_cta_admin_tape_ll1"
    dr_name = cta_taped.drive_name

    ls_before = cta_cli.execWithOutput("cta-admin --json dr ls")

    cta_cli.set_all_drives_down(wait=True)

    with TempPhysicalLibrary(cta_cli, pl_name), TempLogicalLibrary(cta_cli, ll_name, pl_name):
        # No create, because drives auto-create on start

        # Update
        cta_cli.exec(f"cta-admin dr up {dr_name} -r 'cta-admin systest up'")
        dr_updated1 = cta_cli.get_single_ls_item("dr ls", lambda x: x["driveName"] == dr_name)
        assert dr_updated1["reason"] == "cta-admin systest up"
        assert dr_updated1["desiredDriveState"] == "UP"

        cta_cli.exec(f"cta-admin dr down {dr_name} -r 'cta-admin systest down'")
        dr_updated2 = cta_cli.get_single_ls_item("dr ls", lambda x: x["driveName"] == dr_name)
        assert_dict_equals(
            dr_updated2, dr_updated1, ["driveStatusSince", "timeSinceLastUpdate", "reason", "desiredDriveState"]
        )
        assert dr_updated2["reason"] == "cta-admin systest down"
        assert dr_updated2["desiredDriveState"] == "DOWN"

        cta_cli.exec(f"cta-admin dr ch {dr_name} -m 'Updated {dr_name}'")
        dr_updated3 = cta_cli.get_single_ls_item("dr ls", lambda x: x["driveName"] == dr_name)
        assert_dict_equals(dr_updated3, dr_updated2, ["driveStatusSince", "timeSinceLastUpdate", "comment"])
        assert dr_updated3["comment"] == f"Updated {dr_name}"

        # Delete
        cta_cli.exec(f"cta-admin dr rm {dr_name}")

    # Restart taped for the drive in question to ensure it ends up back in the catalogue again
    cta_taped.restart(wait_for_restart=True)
    # Until taped gets a correct readiness probe, we need this to ensure the drive registers itself in the catalogue. Ideally this is more deterministic...
    wait_for_condition(
        lambda: cta_cli.execWithOutput(f"cta-admin --json dr ls {dr_name} | jq -r '.[].reason'", throw_on_failure=False)
        == "[cta-taped] INFO Startup"
    )
    env.cta_cli[0].set_all_drives_up(wait=True)
    # Since dr ls has things like "time since" in its output, we need to filter certain keys
    ls_after = cta_cli.execWithOutput("cta-admin --json dr ls")
    ignore_keys = ["driveStatusSince", "timeSinceLastUpdate"]
    ls_before_filtered = [
        {k: v for k, v in dr_dict.items() if k not in ignore_keys} for dr_dict in json.loads(ls_before)
    ]
    ls_after_filtered = [{k: v for k, v in dr_dict.items() if k not in ignore_keys} for dr_dict in json.loads(ls_after)]
    # dr ls returns some nested lists where the order of items may change
    # Logically they are sets, but stored as lists, so we need to ensure order is ignored when testing for equality
    assert canonicalize(ls_before_filtered) == canonicalize(ls_after_filtered)


def test_cta_admin_physical_library(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    pl_name = "test_cta_admin_physical_library_pl1"

    ls_before = cta_cli.execWithOutput("cta-admin --json pl ls")

    # Create
    cta_cli.exec(
        f"cta-admin pl add --name '{pl_name}' --manufacturer 'manA' --model 'modA' --location 'locA' "
        "--type 'typeA' --guiurl 'guicamUrlA' --webcamurl 'webcamUrlA' "
        f"--nbphysicalcartridgeslots 4 --nbavailablecartridgeslots 3 --nbphysicaldriveslots 2 --comment 'Create {pl_name}'"
    )
    try:
        # Read
        pl_created = cta_cli.get_single_ls_item("pl ls", lambda x: x["name"] == pl_name)
        assert pl_created["manufacturer"] == "manA"
        assert pl_created["model"] == "modA"
        assert pl_created["location"] == "locA"
        assert pl_created["type"] == "typeA"
        assert pl_created["guiUrl"] == "guicamUrlA"
        assert pl_created["webcamUrl"] == "webcamUrlA"
        assert int(pl_created["nbPhysicalCartridgeSlots"]) == 4
        assert int(pl_created["nbAvailableCartridgeSlots"]) == 3
        assert int(pl_created["nbPhysicalDriveSlots"]) == 2
        assert pl_created["comment"] == f"Create {pl_name}"
        assert not pl_created["isDisabled"]
        assert pl_created["disabledReason"] == ""

        # Update
        cta_cli.exec(
            f"cta-admin pl ch --name '{pl_name}' --location 'locB' --guiurl 'guicamUrlB' --webcamurl 'webcamUrlB' "
            f"--nbphysicalcartridgeslots 8 --nbavailablecartridgeslots 6 --nbphysicaldriveslots 4 --comment 'Update {pl_name}'"
        )
        pl_updated1 = cta_cli.get_single_ls_item("pl ls", lambda x: x["name"] == pl_name)
        assert_dict_equals(
            pl_updated1,
            pl_created,
            [
                "location",
                "guiUrl",
                "webcamUrl",
                "nbPhysicalCartridgeSlots",
                "nbAvailableCartridgeSlots",
                "nbPhysicalDriveSlots",
                "comment",
                "lastModificationLog",
            ],
        )
        assert pl_updated1["location"] == "locB"
        assert pl_updated1["guiUrl"] == "guicamUrlB"
        assert pl_updated1["webcamUrl"] == "webcamUrlB"
        assert int(pl_updated1["nbPhysicalCartridgeSlots"]) == 8
        assert int(pl_updated1["nbAvailableCartridgeSlots"]) == 6
        assert int(pl_updated1["nbPhysicalDriveSlots"]) == 4
        assert pl_updated1["comment"] == f"Update {pl_name}"

        # Duplicate add should fail
        with pytest.raises(RuntimeError):
            print("Expected failure after adding a duplicate physical library:")
            cta_cli.exec(
                f"cta-admin pl add --name '{pl_name}' --manufacturer 'manA' --model 'modA' --location 'locA' "
                "--type 'typeA' --guiurl 'urlA' --webcamurl 'urlA' "
                "--nbphysicalcartridgeslots 4 --nbavailablecartridgeslots 3 --nbphysicaldriveslots 2 --comment 'commentA'"
            )

        # Update
        cta_cli.exec(f"cta-admin pl ch --name '{pl_name}' --disabled true --dr 'disabled_reason_provided'")
        pl_updated2 = cta_cli.get_single_ls_item("pl ls", lambda x: x["name"] == pl_name)
        assert_dict_equals(pl_updated2, pl_updated1, ["isDisabled", "disabledReason", "lastModificationLog"])
        assert pl_updated2["isDisabled"]
        assert pl_updated2["disabledReason"] == "disabled_reason_provided"
    finally:
        # Delete
        cta_cli.exec(f"cta-admin pl rm --name '{pl_name}'")

    ls_after = cta_cli.execWithOutput("cta-admin --json pl ls")
    assert ls_before == ls_after


def test_cta_admin_logical_library(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    pl1_name = "test_cta_admin_logical_library_pl1"
    pl2_name = "test_cta_admin_logical_library_pl2"
    ll_name = "test_cta_admin_logical_library_ll1"

    ls_before = cta_cli.execWithOutput("cta-admin --json ll ls")

    with TempPhysicalLibrary(cta_cli, pl1_name), TempPhysicalLibrary(cta_cli, pl2_name):
        # Create
        cta_cli.exec(f"cta-admin ll add -n '{ll_name}' -d false --pl {pl1_name} -m 'Create {ll_name}'")
        try:
            # Read
            ll_created = cta_cli.get_single_ls_item("ll ls", lambda x: x["name"] == ll_name)
            assert ll_created["physicalLibrary"] == pl1_name
            assert not ll_created["isDisabled"]

            # Update
            cta_cli.exec(f"cta-admin ll ch -n '{ll_name}' -d true --pl {pl2_name} --dr 'disabled_reason_provided'")
            ll_updated = cta_cli.get_single_ls_item("ll ls", lambda x: x["name"] == ll_name)
            assert_dict_equals(
                ll_updated, ll_created, ["physicalLibrary", "isDisabled", "disabledReason", "lastModificationLog"]
            )
            assert ll_updated["physicalLibrary"] == pl2_name
            assert ll_updated["isDisabled"]
            assert ll_updated["disabledReason"] == "disabled_reason_provided"
        finally:
            # Delete
            cta_cli.exec(f"cta-admin ll rm -n {ll_name}")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json ll ls")


def test_cta_admin_media_type(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    mt_name = "test_cta_admin_media_type_mt"

    ls_before = cta_cli.execWithOutput("cta-admin --json mt ls")

    # Create
    cta_cli.exec(f"cta-admin mt add -n '{mt_name}' -t 12345C -c 5000000000000 -p 50 -m 'Create {mt_name}'")
    try:
        # Read
        mt_created = cta_cli.get_single_ls_item("mt ls", lambda x: x["name"] == mt_name)
        assert mt_created["cartridge"] == "12345C"
        assert int(mt_created["capacity"]) == 5000000000000
        assert int(mt_created["primaryDensityCode"]) == 50
        assert int(mt_created["secondaryDensityCode"]) == 0
        assert int(mt_created["numberOfWraps"]) == 0
        assert int(mt_created["minLpos"]) == 0
        assert int(mt_created["maxLpos"]) == 0
        assert mt_created["comment"] == f"Create {mt_name}"

        # Update
        cta_cli.exec(f"cta-admin mt ch -n '{mt_name}' -w 10 -m 'Update {mt_name}'")
        mt_updated = cta_cli.get_single_ls_item("mt ls", lambda x: x["name"] == mt_name)
        assert_dict_equals(mt_updated, mt_created, ["numberOfWraps", "comment", "lastModificationLog"])
        assert mt_updated["comment"] == f"Update {mt_name}"
        assert int(mt_updated["numberOfWraps"]) == 10
    finally:
        # Delete
        cta_cli.exec(f"cta-admin mt rm -n '{mt_name}'")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json mt ls")


@pytest.mark.eos
def test_cta_admin_recycle_tape_file_ls(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    vids: list[str] = cta_cli.list_all_tape_vids()
    assert vids, "Need at least one VID for rtf ls test."

    # Archive one file
    test_file_path = "/eos/ctaeos/cta/cta_admin_rtf_testfile_" + str(uuid.uuid4())[:8]
    env.disk_client[0].generate_and_archive_file(disk_instance_name, destination_path=test_file_path, wait=True)

    # Figure out the fxid and VID
    file_info_out = env.disk_instance[0].execWithOutput(f"eos -j file info {test_file_path}")
    file_info_json = json.loads(file_info_out)
    fxid = file_info_json["fxid"]

    tf_created = cta_cli.get_single_ls_item(f"tf ls --fxid {fxid} -i {disk_instance_name}", lambda x: True)
    vid = tf_created["tf"]["vid"]
    archive_id = tf_created["af"]["archiveId"]

    # Delete the file
    env.disk_client[0].delete_file(disk_instance_name, path=test_file_path)

    rtf_created = cta_cli.get_single_ls_item(f"rtf ls -v {vid}", lambda x: x["archiveFileId"] == archive_id)
    assert rtf_created["vid"] == vid
    assert rtf_created["diskFilePath"] == test_file_path
    assert rtf_created["diskInstance"] == disk_instance_name
    assert int(rtf_created["diskFileUid"]) == int(file_info_json["uid"])
    assert int(rtf_created["diskFileGid"]) == int(file_info_json["gid"])
    assert rtf_created["storageClass"] == file_info_json["xattr"]["sys.archive.storage_class"]


# -------------------------------------------------------------------------------------------------
# Workflow - amr, gmr, rmr, ar, mp, sc
# -------------------------------------------------------------------------------------------------


def test_cta_admin_activity_mount_rule(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    requester_name = "test_cta_admin_activity_mount_rule_user"
    mp_name = "test_cta_admin_activity_mount_rule_mp"

    ls_before = cta_cli.execWithOutput("cta-admin --json amr ls")

    with TempMountPolicy(cta_cli, mp_name):
        # Create
        cta_cli.exec(
            f"cta-admin amr add -i {disk_instance_name} -n {requester_name} --ar ^T1Reprocess -u {mp_name} -m 'Create {requester_name}'"
        )
        try:
            # Read
            amr_created = cta_cli.get_single_ls_item("amr ls", lambda x: x["activityMountRule"] == requester_name)
            assert amr_created["diskInstance"] == disk_instance_name
            assert amr_created["activityRegex"] == "^T1Reprocess"
            assert amr_created["mountPolicy"] == mp_name
            assert amr_created["comment"] == f"Create {requester_name}"

            # Update
            cta_cli.exec(
                f"cta-admin amr ch -i {disk_instance_name} -n {requester_name} --ar ^T1Reprocess -m 'Update {requester_name}'"
            )
            amr_updated = cta_cli.get_single_ls_item("amr ls", lambda x: x["activityMountRule"] == requester_name)
            assert_dict_equals(amr_updated, amr_created, ["comment", "lastModificationLog"])
            assert amr_updated["comment"] == f"Update {requester_name}"
        finally:
            # Delete
            cta_cli.exec(f"cta-admin amr rm -i {disk_instance_name} -n {requester_name} --ar ^T1Reprocess")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json amr ls")


def test_cta_admin_group_mount_rule(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    requester_name = "test_cta_admin_group_mount_rule_user"
    mp1_name = "test_cta_admin_group_mount_rule_mp1"
    mp2_name = "test_cta_admin_group_mount_rule_mp2"

    ls_before = cta_cli.execWithOutput("cta-admin --json gmr ls")

    with TempMountPolicy(cta_cli, mp1_name), TempMountPolicy(cta_cli, mp2_name):
        # Create
        cta_cli.exec(
            f"cta-admin gmr add -i {disk_instance_name} -n {requester_name} -u {mp1_name} -m 'Create {requester_name}'"
        )
        try:
            # Read
            gmr_created = cta_cli.get_single_ls_item("gmr ls", lambda x: x["groupMountRule"] == requester_name)
            assert gmr_created["diskInstance"] == disk_instance_name
            assert gmr_created["mountPolicy"] == mp1_name
            assert gmr_created["comment"] == f"Create {requester_name}"

            # Update
            cta_cli.exec(
                f"cta-admin gmr ch -i {disk_instance_name} -n {requester_name} -u {mp2_name} -m 'Update {requester_name}'"
            )
            gmr_updated = cta_cli.get_single_ls_item("gmr ls", lambda x: x["groupMountRule"] == requester_name)
            assert_dict_equals(gmr_updated, gmr_created, ["mountPolicy", "comment", "lastModificationLog"])
            assert gmr_updated["mountPolicy"] == mp2_name
            assert gmr_updated["comment"] == f"Update {requester_name}"
        finally:
            # Delete
            cta_cli.exec(f"cta-admin gmr rm -i {disk_instance_name} -n {requester_name}")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json gmr ls")


def test_cta_admin_requester_mount_rule(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    requester_name = "test_cta_admin_requester_mount_rule_user"
    mp1_name = "test_cta_admin_requester_mount_rule_mp1"
    mp2_name = "test_cta_admin_requester_mount_rule_mp2"

    ls_before = cta_cli.execWithOutput("cta-admin --json rmr ls")

    with TempMountPolicy(cta_cli, mp1_name), TempMountPolicy(cta_cli, mp2_name):
        # Create
        cta_cli.exec(
            f"cta-admin rmr add -i {disk_instance_name} -n {requester_name} -u {mp1_name} -m 'Create {requester_name}'"
        )
        try:
            # Read
            rmr_created = cta_cli.get_single_ls_item("rmr ls", lambda x: x["requesterMountRule"] == requester_name)
            assert rmr_created["diskInstance"] == disk_instance_name
            assert rmr_created["mountPolicy"] == mp1_name
            assert rmr_created["comment"] == f"Create {requester_name}"

            # Update
            cta_cli.exec(
                f"cta-admin rmr ch -i {disk_instance_name} -n {requester_name} -u {mp2_name} -m 'Update {requester_name}'"
            )
            rmr_updated = cta_cli.get_single_ls_item("rmr ls", lambda x: x["requesterMountRule"] == requester_name)
            assert_dict_equals(rmr_updated, rmr_created, ["mountPolicy", "comment", "lastModificationLog"])
            assert rmr_updated["mountPolicy"] == mp2_name
            assert rmr_updated["comment"] == f"Update {requester_name}"
        finally:
            # Delete
            cta_cli.exec(f"cta-admin rmr rm -i {disk_instance_name} -n {requester_name}")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json rmr ls")


def test_cta_admin_archive_route(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    vo_name = "test_cta_admin_archive_route_vo"
    sc_name = "test_cta_admin_archive_route_sc"
    tp1_name = "test_cta_admin_archive_route_tp1"
    tp2_name = "test_cta_admin_archive_route_tp2"

    ls_before = cta_cli.execWithOutput("cta-admin --json ar ls")

    with TempVirtualOrganization(cta_cli, vo_name, disk_instance_name), TempStorageClass(
        cta_cli, sc_name, vo_name
    ), TempTapePool(cta_cli, tp1_name, vo_name), TempTapePool(cta_cli, tp2_name, vo_name):
        # Create
        cta_cli.exec(f"cta-admin ar add -s {sc_name} -c 2 --art DEFAULT -t {tp1_name} -m 'Create {sc_name}'")
        try:
            # Read
            ar_created = cta_cli.get_single_ls_item("ar ls", lambda x: x["storageClass"] == sc_name)
            assert ar_created["tapepool"] == tp1_name
            assert int(ar_created["copyNumber"]) == 2
            assert ar_created["archiveRouteType"] == "DEFAULT"
            assert ar_created["comment"] == f"Create {sc_name}"

            # Update
            cta_cli.exec(f"cta-admin ar ch -s {sc_name} -c 2 --art DEFAULT -t {tp2_name} -m 'Update {sc_name}'")
            ar_updated = cta_cli.get_single_ls_item("ar ls", lambda x: x["storageClass"] == sc_name)
            assert_dict_equals(ar_updated, ar_created, ["tapepool", "comment", "lastModificationLog"])
            assert ar_updated["tapepool"] == tp2_name
            assert ar_updated["comment"] == f"Update {sc_name}"
        finally:
            # Delete
            cta_cli.exec(f"cta-admin ar rm -s {sc_name} -c 2 --art DEFAULT")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json ar ls")


def test_cta_admin_mount_policy(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    mp_name = "test_cta_admin_mount_policy"

    ls_before = cta_cli.execWithOutput("cta-admin --json mp ls")

    # Create
    cta_cli.exec(f"cta-admin mp add -n {mp_name} --ap 4 --aa 3 --rp 2 --ra 1 -m 'Create {mp_name}'")
    try:
        # Read
        mp_created = cta_cli.get_single_ls_item("mp ls", lambda x: x["name"] == mp_name)
        assert int(mp_created["archivePriority"]) == 4
        assert int(mp_created["archiveMinRequestAge"]) == 3
        assert int(mp_created["retrievePriority"]) == 2
        assert int(mp_created["retrieveMinRequestAge"]) == 1
        assert mp_created["comment"] == f"Create {mp_name}"

        # Update
        cta_cli.exec(f"cta-admin mp ch -n {mp_name} --ap 42 -m 'Update {mp_name}'")
        mp_updated = cta_cli.get_single_ls_item("mp ls", lambda x: x["name"] == mp_name)
        assert_dict_equals(mp_updated, mp_created, ["archivePriority", "comment", "lastModificationLog"])
        assert int(mp_updated["archivePriority"]) == 42
        assert mp_updated["comment"] == f"Update {mp_name}"
    finally:
        # Delete
        cta_cli.exec(f"cta-admin mp rm -n {mp_name}")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json mp ls")


def test_cta_admin_storage_class(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    sc_name = "test_cta_admin_storage_class"
    vo_name = "test_cta_admin_storage_class_vo"

    ls_before = cta_cli.execWithOutput("cta-admin --json sc ls")

    with TempVirtualOrganization(cta_cli, vo_name, disk_instance_name):
        # Create
        cta_cli.exec(f"cta-admin sc add -n {sc_name} -c 1 --vo {vo_name} -m 'Create {sc_name}'")
        try:
            # Read
            sc_created = cta_cli.get_single_ls_item("sc ls", lambda x: x["name"] == sc_name)
            assert int(sc_created["nbCopies"]) == 1
            assert sc_created["vo"] == vo_name
            assert sc_created["comment"] == f"Create {sc_name}"

            # Update
            cta_cli.exec(f"cta-admin sc ch -n {sc_name} -c 2 -m 'Update {sc_name}'")
            sc_updated = cta_cli.get_single_ls_item("sc ls", lambda x: x["name"] == sc_name)
            assert_dict_equals(sc_updated, sc_created, ["nbCopies", "comment", "lastModificationLog"])
            assert int(sc_updated["nbCopies"]) == 2
            assert sc_updated["comment"] == f"Update {sc_name}"
        finally:
            # Delete
            cta_cli.exec(f"cta-admin sc rm -n {sc_name}")

    assert ls_before == cta_cli.execWithOutput("cta-admin --json sc ls")


# -------------------------------------------------------------------------------------------------
# Requests - sq, re
# -------------------------------------------------------------------------------------------------


@pytest.mark.eos
def test_cta_admin_show_queue(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name

    ls_before = cta_cli.execWithOutput("cta-admin --json sq")
    # Ensure we didn't have anything in the queue at this point
    ls_before_json = json.loads(ls_before)
    assert len(ls_before_json) == 0, "There are still files left in the queue"

    env.cta_cli[0].set_all_drives_down(wait=True)

    # Trigger an archive request
    file_path = "/eos/ctaeos/cta/cta_admin_sq_testfile_" + str(uuid.uuid4())[:8]
    env.disk_client[0].generate_and_archive_file(disk_instance_name, destination_path=file_path, wait=False)

    # Verify there is at least one ARCHIVE_FOR_USER entry
    sq_out = cta_cli.execWithOutput("cta-admin --json sq")
    sq_json = json.loads(sq_out)
    assert len(sq_json) == 1
    assert sq_json[0]["mountType"] == "ARCHIVE_FOR_USER"
    assert int(sq_json[0]["queuedFiles"]) == 1
    assert int(sq_json[0]["curMounts"]) == 0
    assert int(sq_json[0]["curFiles"]) == 0
    assert int(sq_json[0]["curBytes"]) == 0

    # Bring drives back up
    env.cta_cli[0].set_all_drives_up(wait=True)
    # Wait for file to be archived
    env.disk_client[0].wait_for_file_archival(disk_instance_name, path=file_path)

    # sq has a bit of a delay
    wait_for_condition(lambda: cta_cli.execWithOutput("cta-admin --json sq") == "[]")

    ls_after = cta_cli.execWithOutput("cta-admin --json sq")
    assert ls_before == ls_after

    # Cleanup
    env.disk_client[0].delete_file(disk_instance_name, path=file_path)


@pytest.mark.eos
def test_cta_admin_repack(env):
    cta_cli: CtaCliHost = env.cta_cli[0]
    disk_instance_name: str = env.disk_instance[0].instance_name

    vo_name = "vo_repack"
    pl_name = "test_cta_admin_repack_pl"
    ll_name = "test_cta_admin_repack_ll"
    tp_name = "test_cta_admin_repack_tp"
    vid = "REP1000"
    ls_before = cta_cli.execWithOutput("cta-admin re ls")

    # Note that the goal is not to do an actual repack workflow (that is what the repack tests are for)
    # This is why the repack expand and repack reporting routines should be disabled for this to (reliably) pass
    with TempVirtualOrganization(cta_cli, vo_name, disk_instance_name, "--isrepackvo true"), TempMountPolicy(
        cta_cli, "repack_ctasystest"
    ), TempPhysicalLibrary(cta_cli, pl_name), TempLogicalLibrary(cta_cli, ll_name, pl_name), TempTapePool(
        cta_cli, tp_name, vo_name
    ), TempTape(
        cta_cli, vid, ll_name, tp_name
    ):
        cta_cli.exec(f"cta-admin ta ch -v {vid} -f true")
        ta_full = cta_cli.get_single_ls_item("ta ls --all", lambda x: x["vid"] == vid)
        assert ta_full["full"]
        assert ta_full["state"] == "ACTIVE"

        cta_cli.exec(f"cta-admin ta ch -v {vid} -s REPACKING -r 'Test repack'")
        wait_for_condition(lambda: is_in_repacking_state(cta_cli, vid))

        # Create
        cta_cli.exec(
            f"cta-admin re add -v {vid} -m -u repack_ctasystest -b root://{disk_instance_name}//eos/ctaeos/cta"
        )
        try:
            # Read
            re_created = cta_cli.get_single_ls_item("re ls", lambda x: x["vid"] == vid)
            assert re_created["repackBufferUrl"] == f"root://{disk_instance_name}//eos/ctaeos/cta"
            assert re_created["tapepool"] == tp_name
        finally:
            # Delete
            cta_cli.exec(f"cta-admin re rm -v {vid}")

    ls_after = cta_cli.execWithOutput("cta-admin re ls")
    assert ls_before == ls_after
