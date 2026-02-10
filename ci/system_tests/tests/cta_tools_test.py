# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

import pytest
import time

from ..helpers.hosts.cta_frontend_host import CtaFrontendHost

#####################################################################################################################
# Tests
#####################################################################################################################


@pytest.mark.eos
def test_hosts_present_tools(env):
    assert len(env.cta_frontend) > 0


# -------------------------------------------------------------------------------------------------
# Misc - v
# -------------------------------------------------------------------------------------------------


@pytest.mark.eos
def test_cta_admin_version(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    out = cta_frontend.execWithOutput("cta-admin --json v")
    assert out.strip() != ""


# -------------------------------------------------------------------------------------------------
# Users - ad, vo
# -------------------------------------------------------------------------------------------------


def test_cta_admin_admin(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    ls_before = cta_frontend.execWithOutput("cta-admin ad ls")
    cta_frontend.exec("cta-admin ad add -u test_user -m 'Add test user'")
    cta_frontend.exec("cta-admin ad ch -u test_user -m 'Change test_user'")
    cta_frontend.exec("cta-admin ad rm -u test_user")
    ls_after = cta_frontend.execWithOutput("cta-admin ad ls")
    assert ls_before == ls_after


def test_cta_admin_virtual_organization(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    ls_before = cta_frontend.execWithOutput("cta-admin vo ls")
    cta_frontend.exec("cta-admin vo add --vo 'vo_cta' --rmd 1 --wmd 1 --di 'ctaeos' -m 'Add vo_cta'")
    cta_frontend.exec("cta-admin vo ch --vo 'vo_cta' --wmd 2 --mfs 100 --isrepackvo false")
    cta_frontend.exec("cta-admin vo rm --vo 'vo_cta'")
    ls_after = cta_frontend.execWithOutput("cta-admin vo ls")
    assert ls_before == ls_after


# -------------------------------------------------------------------------------------------------
# Disk - di, dis, ds
# -------------------------------------------------------------------------------------------------


@pytest.mark.eos
def test_cta_admin_disk_instance(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    ls_before = cta_frontend.execWithOutput("cta-admin di ls")
    cta_frontend.exec("cta-admin di add -n ctaeos2 -m 'Add di ctaeos2'")
    cta_frontend.exec("cta-admin di ch -n ctaeos2 -m 'Change di ctaeos2'")
    cta_frontend.exec("cta-admin di rm -n ctaeos2")
    ls_after = cta_frontend.execWithOutput("cta-admin di ls")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_disk_instance_space(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    disk_instance_name: str = env.disk_instance[0].instance_name

    ls_before = cta_frontend.execWithOutput("cta-admin dis ls")
    cta_frontend.exec(
        f"cta-admin dis add -n ctaeos_dis --di {disk_instance_name} -i 10 -u eosSpace:default -m 'Add dis ctaeos_dis'"
    )
    cta_frontend.exec(f"cta-admin dis ch -n ctaeos_dis --di {disk_instance_name} -i 100 -m 'Change dis ctaeos_dis'")
    cta_frontend.exec(f"cta-admin dis rm -n ctaeos_dis --di {disk_instance_name}")
    ls_after = cta_frontend.execWithOutput("cta-admin dis ls")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_disk_system(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    disk_instance_name: str = env.disk_instance[0].instance_name

    cta_frontend.exec(
        f"cta-admin dis add -n ctaeos_dis --di {disk_instance_name} -i 10 -u eosSpace:default -m 'Add dis ctaeos_dis'"
    )

    ls_before = cta_frontend.execWithOutput("cta-admin ds ls")
    cta_frontend.exec(
        f"cta-admin ds add -n ctaeos_ds --di {disk_instance_name} --dis ctaeos_dis "
        f"-r root://{disk_instance_name}//eos/ctaeos/cta/ -f 10000 -s 10 -m 'Adding ds ctaeos_ds'"
    )
    cta_frontend.exec("cta-admin ds ch -n ctaeos_ds -s 100")
    cta_frontend.exec("cta-admin ds rm -n ctaeos_ds")
    ls_after = cta_frontend.execWithOutput("cta-admin ds ls")
    assert ls_before == ls_after

    cta_frontend.exec(f"cta-admin dis rm -n ctaeos_dis --di {disk_instance_name}")


# -------------------------------------------------------------------------------------------------
# Tape - ta, tf, tp, dr, pl, ll, mt, rtf
# -------------------------------------------------------------------------------------------------


@pytest.mark.eos
def test_cta_admin_tape(env, cta_admin_ctx):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    lls: List[str] = cta_admin_ctx["lls"]
    assert len(lls) >= 2, "Need at least 2 logical libraries for tape test."
    ll = lls[1]

    ls_before = cta_frontend.execWithOutput("cta-admin ta ls --all")

    cta_frontend.exec("cta-admin pl add --name phys1 --ma man --mo mod --npcs 3 --npds 4")
    cta_frontend.exec(f"cta-admin ll ch --name {ll} --pl phys1")

    cta_frontend.exec(
        f"cta-admin ta add -v ULT9999 --mt LTO8 --ve vendor -l {ll} -t ctasystest -f true --purchaseorder order1"
    )
    cta_frontend.exec("cta-admin ta reclaim -v ULT9999")
    cta_frontend.exec("cta-admin ta ch -v ULT9999 -s 'REPACKING' -r 'Test admin-cta ta ch'")
    cta_frontend.exec("cta-admin ta ch -v ULT9999 --purchaseorder 'order2' -r 'Test admin-cta ta ch'")
    cta_frontend.exec("cta-admin ta rm -v ULT9999")

    cta_frontend.exec(f"cta-admin ll ch --name {ll} --pl ''")
    cta_frontend.exec("cta-admin pl rm --name phys1")

    ls_after = cta_frontend.execWithOutput("cta-admin ta ls --all")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_tape_file(env, cta_admin_ctx):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    vids: List[str] = cta_admin_ctx["vids"]
    assert vids, "No tape VIDs available to test tape file commands."

    # Archive one file (as in bash)
    env.eos_client[0].exec("echo 'foo' > /root/testfile")
    env.eos_client[0].exec(f"xrdcp /root/testfile root://{disk_instance_name}//eos/ctaeos/cta/testfile")

    # Find a VID with an entry in tf ls
    vid_in_use = ""
    for vid in vids:
        out = cta_frontend.execWithOutput(f"cta-admin --json tf ls -v {vid} | jq -r '.[]' || true").strip()
        if out:
            vid_in_use = vid
            break
    assert vid_in_use, "Could not find a VID with tape files after archiving."

    # Snapshot for tf is vid-specific
    ls_before = cta_frontend.execWithOutput(f"cta-admin tf ls -v {vid_in_use}")

    # Extract archiveId for the file
    a_id = cta_frontend.execWithOutput(
        f"cta-admin --json tf ls -v {vid_in_use} | jq -r '.[] | .af | .archiveId' | head -n1"
    ).strip()
    assert a_id, "Failed to extract archiveId from tf ls output."

    # Removing should fail (single copy)
    rc = cta_frontend.exec(f"cta-admin tf rm -v {vid_in_use} -i ctaeos -I {a_id} -r Test")
    assert rc != 0, "tf rm unexpectedly succeeded; expected failure for single-copy file."

    # Cleanup EOS
    cta_frontend.exec(f"eos root://{disk_instance_name} rm /eos/ctaeos/cta/testfile || true")

    ls_after = cta_frontend.execWithOutput(f"cta-admin tf ls -v {vid_in_use}")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_tape_pool(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    ls_before = cta_frontend.execWithOutput("cta-admin tp ls")
    cta_frontend.exec("cta-admin tp add -n 'cta_admin_test' --vo vo -p 0 -m 'Test tp cmd'")
    cta_frontend.exec("cta-admin tp ch -n 'cta_admin_test' -k encrypt_key_name")
    cta_frontend.exec("cta-admin tp rm -n 'cta_admin_test'")
    ls_after = cta_frontend.execWithOutput("cta-admin tp ls")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_drive(env, cta_admin_ctx):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    lls: List[str] = cta_admin_ctx["lls"]
    assert len(lls) >= 2, "Need at least 2 logical libraries for drive test."

    env.cta_cli[0].set_all_drives_down(wait=True)

    drive = dr_down[0]
    ll = lls[1]

    ls_before = cta_frontend.execWithOutput("cta-admin dr ls")

    cta_frontend.exec("cta-admin pl add --name phys1 --ma man --mo mod --npcs 3 --npds 4")
    cta_frontend.exec(f"cta-admin ll ch --name {ll} --pl phys1")

    cta_frontend.exec(f"cta-admin dr up {drive} -r 'cta-admin systest up'")
    cta_frontend.exec(f"cta-admin dr down {drive} -r 'cta-admin systest down'")
    cta_frontend.exec(f"cta-admin dr ch {drive} -m 'cta-admin test ch'")
    cta_frontend.exec(f"cta-admin dr rm {drive}")

    cta_frontend.exec(f"cta-admin ll ch --name {ll} --pl ''")
    cta_frontend.exec("cta-admin pl rm --name phys1")

    ls_after = cta_frontend.execWithOutput("cta-admin dr ls")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_physical_library(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]

    ls_before = cta_frontend.execWithOutput("cta-admin pl ls")

    cta_frontend.exec(
        "cta-admin pl add --name 'cta_adm_systest' --manufacturer 'manA' --model 'modA' --location 'locA' "
        "--type 'typeA' --guiurl 'urlA' --webcamurl 'urlA' "
        "--nbphysicalcartridgeslots 4 --nbavailablecartridgeslots 3 --nbphysicaldriveslots 2 --comment 'commentA'"
    )
    cta_frontend.exec(
        "cta-admin pl ch --name 'cta_adm_systest' --location 'locB' "
        "--guiurl 'urlB' --webcamurl 'urlB' "
        "--nbphysicalcartridgeslots 4 --nbavailablecartridgeslots 3 --nbphysicaldriveslots 2 --comment 'commentB'"
    )

    # Duplicate add should fail
    rc = cta_frontend.exec(
        "cta-admin pl add --name 'cta_adm_systest' --manufacturer 'manA' --model 'modA' --location 'locA' "
        "--type 'typeA' --guiurl 'urlA' --webcamurl 'urlA' "
        "--nbphysicalcartridgeslots 4 --nbavailablecartridgeslots 3 --nbphysicaldriveslots 2 --comment 'commentA'"
    )
    assert rc != 0, "Expected duplicate physical library add to fail."

    cta_frontend.exec("cta-admin pl ch --name 'cta_adm_systest' --disabled true --dr 'disabled_reason_provided'")
    cta_frontend.exec("cta-admin pl rm --name 'cta_adm_systest'")

    ls_after = cta_frontend.execWithOutput("cta-admin pl ls")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_logical_library(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]

    ls_before = cta_frontend.execWithOutput("cta-admin ll ls")

    cta_frontend.exec("cta-admin pl add --name phys1 --ma man --mo mod --npcs 3 --npds 4")
    cta_frontend.exec("cta-admin pl add --name phys2 --ma man --mo mod --npcs 3 --npds 4")

    cta_frontend.exec("cta-admin ll add -n 'cta_adm_systest' -d false --pl phys1 -m 'cta-admin systest add'")
    cta_frontend.exec("cta-admin ll add -n 'cta_adm_systest2' -d false --pl phys1 -m 'cta-admin systest add'")
    cta_frontend.exec("cta-admin ll ch -n 'cta_adm_systest' -d true --pl phys2 --dr 'cta-admin systest ch'")
    cta_frontend.exec("cta-admin ll rm -n cta_adm_systest")
    cta_frontend.exec("cta-admin ll rm -n cta_adm_systest2")

    cta_frontend.exec("cta-admin pl rm --name phys1")
    cta_frontend.exec("cta-admin pl rm --name phys2")

    ls_after = cta_frontend.execWithOutput("cta-admin ll ls")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_media_type(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    ls_before = cta_frontend.execWithOutput("cta-admin mt ls")
    cta_frontend.exec(
        "cta-admin mt add -n 'CTAADMSYSTEST' -t 12345C -c 5000000000000 -p 50 -m 'cta-admin systest add cartridge'"
    )
    cta_frontend.exec("cta-admin mt ch -n 'CTAADMSYSTEST' -w 10 -m 'cta-admin systest ch'")
    cta_frontend.exec("cta-admin mt rm -n 'CTAADMSYSTEST'")
    ls_after = cta_frontend.execWithOutput("cta-admin mt ls")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_recycle_tape_file_ls(env, cta_admin_ctx):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    vids: List[str] = cta_admin_ctx["vids"]
    assert vids, "Need at least one VID for rtf ls test."

    # Bash uses ${vid} found during tf section. Here we re-use vids[0] as a baseline.
    # If you need the exact 'vid in use' from tf test, you'd need shared state between tests.
    vid = vids[0]
    out = cta_frontend.execWithOutput(
        f"cta-admin --json rtf ls -v {vid} | jq -r '.[] | select(.vid==\"{vid}\") | .vid' | wc -l"
    )
    # Keep semantics close to bash: expect 1
    assert int(out.strip()) == 1


# -------------------------------------------------------------------------------------------------
# Workflow - amr, gmr, rmr, ar, mp, sc
# -------------------------------------------------------------------------------------------------


@pytest.mark.eos
def test_cta_admin_activity_mount_rule(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    ls_before = cta_frontend.execWithOutput("cta-admin amr ls")
    cta_frontend.exec(
        "cta-admin amr add -i ctaeos -n powerusers --ar ^T1Reprocess -u repack_ctasystest -m 'cta-admin systest add amr'"
    )
    cta_frontend.exec("cta-admin amr ch -i ctaeos -n powerusers --ar ^T1Reprocess -m 'cta-admin systest ch amr'")
    cta_frontend.exec("cta-admin amr rm -i ctaeos -n powerusers --ar ^T1Reprocess")
    ls_after = cta_frontend.execWithOutput("cta-admin amr ls")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_group_mount_rule(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    ls_before = cta_frontend.execWithOutput("cta-admin gmr ls")
    cta_frontend.exec("cta-admin gmr add -i ctaeos -n eosusers2 -u ctasystest -m 'cta-admin systest add gmr'")
    cta_frontend.exec("cta-admin gmr ch -i ctaeos -n eosusers2 -u repack_ctasystest")
    cta_frontend.exec("cta-admin gmr rm -i ctaeos -n eosusers2")
    ls_after = cta_frontend.execWithOutput("cta-admin gmr ls")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_requester_mount_rule(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    ls_before = cta_frontend.execWithOutput("cta-admin rmr ls")
    cta_frontend.exec("cta-admin rmr add -i ctaeos -n adm2 -u ctasystest -m 'cta-admin systest add rmr'")
    cta_frontend.exec("cta-admin rmr ch -i ctaeos -n adm2 -u repack_ctasystest -m 'cta-admin systest ch rmr'")
    cta_frontend.exec("cta-admin rmr rm -i ctaeos -n adm2")
    ls_after = cta_frontend.execWithOutput("cta-admin rmr ls")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_archive_route(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    ls_before = cta_frontend.execWithOutput("cta-admin ar ls")
    cta_frontend.exec(
        "cta-admin ar add -s ctaStorageClass -c 2 --art DEFAULT -t ctasystest_A -m 'cta-admin systest add'"
    )
    cta_frontend.exec("cta-admin ar ch -s ctaStorageClass -c 2 --art DEFAULT -t ctasystest_B -m 'cta-admin systest ch'")
    cta_frontend.exec("cta-admin ar rm -s ctaStorageClass -c 2 --art DEFAULT")
    ls_after = cta_frontend.execWithOutput("cta-admin ar ls")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_mount_policy(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    ls_before = cta_frontend.execWithOutput("cta-admin mp ls")
    cta_frontend.exec("cta-admin mp add -n ctasystest2 --ap 3 --aa 2 --rp 2 --ra 1 -m 'cta-admin systest add'")
    cta_frontend.exec("cta-admin mp ch -n ctasystest2 --ap 4 -m 'cta-admin systest ch'")
    cta_frontend.exec("cta-admin mp rm -n ctasystest2")
    ls_after = cta_frontend.execWithOutput("cta-admin mp ls")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_storage_class(env):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    ls_before = cta_frontend.execWithOutput("cta-admin sc ls")
    cta_frontend.exec("cta-admin sc add -n ctaStorageClass_4_copy -c 1 --vo vo -m 'cta-admin systest add'")
    cta_frontend.exec("cta-admin sc ch -n ctaStorageClass_4_copy -c 2 -m 'cta-admin systest ch'")
    cta_frontend.exec("cta-admin sc rm -n ctaStorageClass_4_copy")
    ls_after = cta_frontend.execWithOutput("cta-admin sc ls")
    assert ls_before == ls_after


# -------------------------------------------------------------------------------------------------
# Requests - sq, re
# -------------------------------------------------------------------------------------------------


@pytest.mark.eos
def test_cta_admin_show_queue(env, cta_admin_ctx):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    disk_instance_name: str = env.disk_instance[0].instance_name

    # Snapshot is plain output (bash uses start=$(admin_cta sq))
    ls_before = cta_frontend.execWithOutput("cta-admin sq")

    env.cta_cli[0].set_all_drives_down(wait=True)

    # Trigger an archive request
    # TODO: create file
    # TODO: create method to archive a random file and receive the path
    env.eos_client[0].exec(f"xrdcp /root/testfile root://{disk_instance_name}//eos/ctaeos/cta/testfile")
    # TODO: wait for it to be queued

    # Verify there is at least one ARCHIVE_FOR_USER entry (no wait loop per request)
    cnt = cta_frontend.execWithOutput(
        r"cta-admin --json sq | jq -r '.[] | select(.mountType==\"ARCHIVE_FOR_USER\") | .mountType' | wc -l"
    ).strip()
    assert int(cnt) >= 1, "No ARCHIVE_FOR_USER request found in queue."

    # Bring drives back up
    env.cta_cli[0].set_all_drives_up(wait=True)
    # Wait for file to be archived

    ls_after = cta_frontend.execWithOutput("cta-admin sq")
    assert ls_before == ls_after


@pytest.mark.eos
def test_cta_admin_repack(env, cta_admin_ctx):
    cta_frontend: CtaFrontendHost = env.cta_frontend[0]
    disk_instance_name: str = env.disk_instance[0].instance_name
    vids: List[str] = cta_admin_ctx["vids"]
    assert vids, "Need at least one VID for repack test."

    vid = vids[0]

    ls_before = cta_frontend.execWithOutput("cta-admin re ls")

    cta_frontend.exec(f"cta-admin ta ch -v {vid} -f true")
    time.sleep(3)

    cta_frontend.exec(f"cta-admin ta ch -v {vid} -s REPACKING -r 'Test repack'")
    time.sleep(2)
    cta_frontend.exec(f"cta-admin ta ch -v {vid} -s REPACKING -r 'Test repack'")
    time.sleep(2)

    # Wait loop in bash verifies state == REPACKING; user asked to skip wait-until conditions.
    # So we proceed directly.

    # Add/remove repack request
    cta_frontend.exec(
        f"cta-admin re add -v {vid} -m -u repack_ctasystest -b root://{disk_instance_name}//eos/ctaeos/cta"
    )
    cta_frontend.exec(f"cta-admin re rm -v {vid}")

    ls_after = cta_frontend.execWithOutput("cta-admin re ls")
    assert ls_before == ls_after
