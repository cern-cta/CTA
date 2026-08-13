# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# =========================================================================
#  EOS initialisation
# =========================================================================


from pathlib import Path

from system_tests.helpers.hosts import EosMgmHost
from system_tests.helpers.test_env import TestEnv


def test_eos_version(eos_mgm: EosMgmHost) -> None:
    eos_mgm.exec("eos version")


def test_general_settings(eos_mgm: EosMgmHost) -> None:
    eos_mgm.exec("eos vid enable unix")
    eos_mgm.exec("eos vid enable https")
    eos_mgm.exec("eos space set default on")
    eos_mgm.exec("eos space config default space.filearchivedgc=on")
    eos_mgm.exec("eos space config default space.wfe=on")
    eos_mgm.exec("eos space config default space.wfe.ntx=100")
    eos_mgm.exec("eos space config default taperestapi.status=on")
    eos_mgm.exec("eos space config default taperestapi.stage=on")
    eos_mgm.exec("eos space config default space.scanrate=0")
    eos_mgm.exec("eos space config default space.scaninterval=0")
    eos_mgm.exec("eos space config default space.token.generation=1")
    eos_mgm.exec("eos attr -r set default=replica /eos")
    eos_mgm.exec("eos attr -r set sys.forced.nstripes=1 /eos")
    tape_fs_id = 65535
    eos_mgm.exec("eos space define tape", throw_on_failure=False)
    eos_mgm.exec(f"eos fs add -m {tape_fs_id} tape localhost:1234 /does_not_exist tape", throw_on_failure=False)


# This function sets the SciToken add-on convenience in EOS, allowing our test scripts to acquire test SciTokens
# from EOS and use them to test the staging token capabilities (and others).
def test_scitokens_addon_on_eos(eos_mgm: EosMgmHost, remote_scripts_dir: Path) -> None:
    eos_mgm.copy_to(remote_scripts_dir / "eos_mgm" / "eos-jwk-https", Path("/sbin"), permissions="755")

    # Setup a local jwk file and start the jwk daemon (in the background)
    # Kill any previous daemon if it exists
    eos_mgm.exec("eos scitoken create-keys --keyid ctaeos > /etc/xrootd/ctaeos.jwk")
    eos_mgm.exec("pkill -f '/sbin/[e]os-jwk-https' || true")
    eos_mgm.exec("nohup eos daemon jwk /etc/xrootd/ctaeos.jwk >/tmp/eos-jwk.log 2>&1 </dev/null &")

    print("Checking SciTokens add-on is fully running")
    # EOS should be able to generate SciTokens now.
    scitoken_base64 = eos_mgm.exec(
        "eos scitoken create --expires $(($(date +%s) + 60)) "
        "--issuer https://localhost:4443 --keyid ctaeos --profile wlcg "
        "--claim scope=storage.read:/eos/ --claim sub=test",
        capture_output=True,
    ).stdout.strip()
    assert scitoken_base64, "SciToken generation returned an empty token"


def test_add_users(eos_mgm: EosMgmHost) -> None:
    # We don't really care if these already exist
    eos_mgm.exec("groupadd --gid 1100 eosusers", throw_on_failure=False)
    eos_mgm.exec("groupadd --gid 1200 powerusers", throw_on_failure=False)
    eos_mgm.exec("groupadd --gid 1300 ctaadmins", throw_on_failure=False)
    eos_mgm.exec("groupadd --gid 1400 eosadmins", throw_on_failure=False)
    eos_mgm.exec("useradd --uid 11001 --gid 1100 user1", throw_on_failure=False)
    eos_mgm.exec("useradd --uid 11002 --gid 1100 user2", throw_on_failure=False)
    eos_mgm.exec("useradd --uid 12001 --gid 1200 poweruser1", throw_on_failure=False)
    eos_mgm.exec("useradd --uid 12002 --gid 1200 poweruser2", throw_on_failure=False)
    eos_mgm.exec("useradd --uid 13001 --gid 1300 ctaadmin1", throw_on_failure=False)
    eos_mgm.exec("useradd --uid 13002 --gid 1300 ctaadmin2", throw_on_failure=False)
    eos_mgm.exec("useradd --uid 14001 --gid 1400 eosadmin1", throw_on_failure=False)
    eos_mgm.exec("useradd --uid 14002 --gid 1400 eosadmin2", throw_on_failure=False)
    eosadmin1_id = eos_mgm.exec_with_output("id -u eosadmin1")
    eosadmin2_id = eos_mgm.exec_with_output("id -u eosadmin2")
    eos_mgm.exec(f"eos vid set membership {eosadmin1_id} +sudo")
    eos_mgm.exec(f"eos vid set membership {eosadmin2_id} +sudo")


def test_create_wf_directory(eos_mgm: EosMgmHost, eos_workflow_dir: Path) -> None:
    eos_mgm.exec(f"eos mkdir -p {eos_workflow_dir}")
    eos_mgm.exec(f'eos attr set sys.workflow.sync::create.default="proto" {eos_workflow_dir}')
    eos_mgm.exec(f'eos attr set sys.workflow.sync::closew.default="proto" {eos_workflow_dir}')
    eos_mgm.exec(f'eos attr set sys.workflow.sync::archived.default="proto" {eos_workflow_dir}')
    eos_mgm.exec(f'eos attr set sys.workflow.sync::archive_failed.default="proto" {eos_workflow_dir}')
    eos_mgm.exec(f'eos attr set sys.workflow.sync::prepare.default="proto" {eos_workflow_dir}')
    eos_mgm.exec(f'eos attr set sys.workflow.sync::abort_prepare.default="proto" {eos_workflow_dir}')
    eos_mgm.exec(f'eos attr set sys.workflow.sync::evict_prepare.default="proto" {eos_workflow_dir}')
    eos_mgm.exec(f'eos attr set sys.workflow.sync::closew.retrieve_written="proto" {eos_workflow_dir}')
    eos_mgm.exec(f'eos attr set sys.workflow.sync::retrieve_failed.default="proto" {eos_workflow_dir}')
    eos_mgm.exec(f'eos attr set sys.workflow.sync::delete.default="proto" {eos_workflow_dir}')


def test_delete_cta_directory(eos_mgm: EosMgmHost, cta_dir: Path) -> None:
    # Cleanup a possibly existing directory
    eos_mgm.force_remove_directory(cta_dir)


def test_create_cta_directory(
    eos_mgm: EosMgmHost, cta_dir: Path, eos_workflow_dir: Path, cta_storage_class: str
) -> None:
    eos_mgm.exec(f"eos mkdir -p {cta_dir}")
    # Must be writable by eosusers and powerusers
    # but as there is no sticky bit in eos, we need to remove deletion for non owner to eosusers members
    # this is achieved through the ACLs.
    # ACLs in EOS are evaluated when unix permissions are failing, hence the 555 unix permission.
    eos_mgm.exec(f"eos chmod 555 {cta_dir}")
    eos_mgm.exec(f"eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+dp,u:poweruser2:rwx+dp,z:'!'u'!'d {cta_dir}")
    eos_mgm.exec(f"eos attr set sys.archive.storage_class={cta_storage_class} {cta_dir}")
    eos_mgm.exec(f"eos attr link {eos_workflow_dir} {cta_dir}")


# Identities and numeric IDs are defined in setup_eos_test.py
# - ctaadmin2 is the CTA administrator used by client-side CTA commands (see client_helper.sh and test_add_admins)
#   This admin should eventually be removed as the client pod will no longer be able to run CTA admin commands directly
# - eosadmin1 is an EOS sudo user used for namespace administration, token generation, and privileged eviction
# - poweruser1 has explicit read/write/prepare/delete rights and is used for staging, releasing, and cleanup
# - user1 is the regular eosusers archive writer; it can create files but is explicitly denied deletion
def test_kinit_eos_clients(env: TestEnv, krb5_realm: str) -> None:
    client_users = ["eosadmin1", "ctaadmin2", "poweruser1", "user1"]
    for eos_client in env.eos_client:
        for user in client_users:
            eos_client.exec(f"mkdir -p /tmp/{user}")
            eos_client.exec(f"KRB5CCNAME=/tmp/{user}/krb5cc_0  kinit -kt /root/{user}.keytab {user}@{krb5_realm}")

        # Set the default to be user1
        # This is so that we can play around with xrdcp manually without always having to specify the user
        eos_client.exec("kinit -kt /root/user1.keytab user1@TEST.CTA")
