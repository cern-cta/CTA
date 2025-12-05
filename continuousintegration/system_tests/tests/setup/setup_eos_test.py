import pytest


#####################################################################################################################
# Prerequisites
#####################################################################################################################


@pytest.mark.eos
def test_hosts_present_eos_setup(env):
    # Need at least a disk instance and a client
    assert len(env.eos_client) > 0, "To setup EOS, there must be at least one EOS client"
    assert len(env.eos_mgm) > 0, "To setup EOS, there must be at least one EOS MGM"


#####################################################################################################################
# EOS initialisation
#####################################################################################################################


@pytest.mark.eos
def test_eos_version(env):
    env.eos_mgm[0].exec("eos version")


@pytest.mark.eos
def test_general_settings(env):
    env.eos_mgm[0].exec("eos vid enable unix")
    env.eos_mgm[0].exec("eos vid enable https")
    env.eos_mgm[0].exec("eos space set default on")
    env.eos_mgm[0].exec("eos space config default space.filearchivedgc=on")
    env.eos_mgm[0].exec("eos space config default space.wfe=on")
    env.eos_mgm[0].exec("eos space config default space.wfe.ntx=100")
    env.eos_mgm[0].exec("eos space config default taperestapi.status=on")
    env.eos_mgm[0].exec("eos space config default taperestapi.stage=on")
    env.eos_mgm[0].exec("eos space config default space.scanrate=0")
    env.eos_mgm[0].exec("eos space config default space.scaninterval=0")
    env.eos_mgm[0].exec("eos space config default space.token.generation=1")
    env.eos_mgm[0].exec("eos space config default space.scanrate=0")
    env.eos_mgm[0].exec("eos space config default space.scaninterval=0")
    env.eos_mgm[0].exec("eos attr -r set default=replica /eos")
    env.eos_mgm[0].exec("eos attr -r set sys.forced.nstripes=1 /eos")


@pytest.mark.eos
def test_add_users(env):
    # We don't really care if these already exist
    env.eos_mgm[0].exec("groupadd --gid 1100 eosusers", throw_on_failure=False)
    env.eos_mgm[0].exec("groupadd --gid 1200 powerusers", throw_on_failure=False)
    env.eos_mgm[0].exec("groupadd --gid 1300 ctaadmins", throw_on_failure=False)
    env.eos_mgm[0].exec("groupadd --gid 1400 eosadmins", throw_on_failure=False)
    env.eos_mgm[0].exec("useradd --uid 11001 --gid 1100 user1", throw_on_failure=False)
    env.eos_mgm[0].exec("useradd --uid 11002 --gid 1100 user2", throw_on_failure=False)
    env.eos_mgm[0].exec("useradd --uid 12001 --gid 1200 poweruser1", throw_on_failure=False)
    env.eos_mgm[0].exec("useradd --uid 12002 --gid 1200 poweruser2", throw_on_failure=False)
    env.eos_mgm[0].exec("useradd --uid 13001 --gid 1300 ctaadmin1", throw_on_failure=False)
    env.eos_mgm[0].exec("useradd --uid 13002 --gid 1300 ctaadmin2", throw_on_failure=False)
    env.eos_mgm[0].exec("useradd --uid 14001 --gid 1400 eosadmin1", throw_on_failure=False)
    env.eos_mgm[0].exec("useradd --uid 14002 --gid 1400 eosadmin2", throw_on_failure=False)
    eosadmin1_id = env.eos_mgm[0].execWithOutput("id -u eosadmin1")
    eosadmin2_id = env.eos_mgm[0].execWithOutput("id -u eosadmin2")
    env.eos_mgm[0].exec(f"eos vid set membership {eosadmin1_id} +sudo")
    env.eos_mgm[0].exec(f"eos vid set membership {eosadmin2_id} +sudo")


@pytest.mark.eos
def test_create_wf_directory(env):
    base_dir: str = env.eos_mgm[0].base_dir_path
    workflow_dir: str = f"{base_dir}/proc/cta/workflow"
    env.eos_mgm[0].exec(f"eos mkdir -p {workflow_dir}")
    env.eos_mgm[0].exec(f'eos attr set sys.workflow.sync::create.default="proto" {workflow_dir}')
    env.eos_mgm[0].exec(f'eos attr set sys.workflow.sync::closew.default="proto" {workflow_dir}')
    env.eos_mgm[0].exec(f'eos attr set sys.workflow.sync::archived.default="proto" {workflow_dir}')
    env.eos_mgm[0].exec(f'eos attr set sys.workflow.sync::archive_failed.default="proto" {workflow_dir}')
    env.eos_mgm[0].exec(f'eos attr set sys.workflow.sync::prepare.default="proto" {workflow_dir}')
    env.eos_mgm[0].exec(f'eos attr set sys.workflow.sync::abort_prepare.default="proto" {workflow_dir}')
    env.eos_mgm[0].exec(f'eos attr set sys.workflow.sync::evict_prepare.default="proto" {workflow_dir}')
    env.eos_mgm[0].exec(f'eos attr set sys.workflow.sync::closew.retrieve_written="proto" {workflow_dir}')
    env.eos_mgm[0].exec(f'eos attr set sys.workflow.sync::retrieve_failed.default="proto" {workflow_dir}')
    env.eos_mgm[0].exec(f'eos attr set sys.workflow.sync::delete.default="proto" {workflow_dir}')


@pytest.mark.eos
def test_create_archive_directory(env) -> None:
    base_dir: str = env.eos_mgm[0].base_dir_path
    archive_dir: str = f"{base_dir}/cta"
    # TODO: can we somehow pass values from one test to the next?
    # Ideally I don't want to redefine this all over the place
    workflow_dir: str = f"{base_dir}/proc/cta/workflow"
    env.eos_mgm[0].exec(f"eos mkdir -p {archive_dir}")
    # Must be writable by eosusers and powerusers
    # but as there is no sticky bit in eos, we need to remove deletion for non owner to eosusers members
    # this is achieved through the ACLs.
    # ACLs in EOS are evaluated when unix permissions are failing, hence the 555 unix permission.
    env.eos_mgm[0].exec(f"eos chmod 555 {archive_dir}")
    env.eos_mgm[0].exec(
        f"eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+dp,u:poweruser2:rwx+dp,z:'!'u'!'d {archive_dir}"
    )
    # For now storage class is hardcoded
    env.eos_mgm[0].exec(f"eos attr set sys.archive.storage_class=ctaStorageClass {archive_dir}")
    env.eos_mgm[0].exec(f"eos attr link {workflow_dir} {archive_dir}")


@pytest.mark.eos
def test_kinit_eos_client(env, krb5_realm):
    for eos_client in env.eos_client:
        eos_client.exec(f"kinit -kt /root/user1.keytab user1@{krb5_realm}")
