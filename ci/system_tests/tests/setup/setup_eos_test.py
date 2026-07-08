# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

#####################################################################################################################
# EOS initialisation
#####################################################################################################################


def test_eos_version(eos_mgm):
    eos_mgm.exec("eos version")


def test_general_settings(eos_mgm):
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
    TAPE_FS_ID = 65535
    eos_mgm.exec("eos space define tape", throw_on_failure=False)
    eos_mgm.exec(f"eos fs add -m {TAPE_FS_ID} tape localhost:1234 /does_not_exist tape", throw_on_failure=False)


def test_add_users(eos_mgm):
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


def test_create_wf_directory(eos_mgm, eos_workflow_dir):
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


def test_delete_cta_directory(eos_mgm, cta_dir) -> None:
    # Cleanup a possibly existing directory
    eos_mgm.force_remove_directory(cta_dir)


def test_create_cta_directory(eos_mgm, cta_dir, eos_workflow_dir, cta_storage_class) -> None:
    eos_mgm.exec(f"eos mkdir -p {cta_dir}")
    # Must be writable by eosusers and powerusers
    # but as there is no sticky bit in eos, we need to remove deletion for non owner to eosusers members
    # this is achieved through the ACLs.
    # ACLs in EOS are evaluated when unix permissions are failing, hence the 555 unix permission.
    eos_mgm.exec(f"eos chmod 555 {cta_dir}")
    eos_mgm.exec(f"eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+dp,u:poweruser2:rwx+dp,z:'!'u'!'d {cta_dir}")
    eos_mgm.exec(f"eos attr set sys.archive.storage_class={cta_storage_class} {cta_dir}")
    eos_mgm.exec(f"eos attr link {eos_workflow_dir} {cta_dir}")
