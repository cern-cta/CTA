import uuid
import base64
import pytest
import time
import json
import glob

@pytest.mark.order("first")
def test_setup_client_scripts_and_certs(env):
    env.client[0].copyTo("system_tests/scripts/client/", "/root/", permissions="+x")
    env.client[0].exec("mv /root/client/* /root && rm -d /root/client")
    env.eosmgm[0].copyFrom("etc/grid-security/certificates/", "/tmp/certificates/")
    env.client[0].copyTo("/tmp/certificates/", "/etc/grid-security/")
    env.execLocal("rm -rf /tmp/certificates")

@pytest.mark.order("second")
def test_setup_client(env):
    # Probably same for this one
    file_count=5000
    file_size_kb=15
    processes_count = 20

    # TODO: refactor this so that these magical flags make sense
    env.client[0].exec(f"/root/client_setup.sh -n {file_count} -s {file_size_kb} -p {processes_count} -d {env.eos_preprod_dir} -v -r -c xrd")

def test_http_rest_api(env):
    env.client[0].exec("/root/test_rest_api.sh")

def test_archive_metadata(env):
    metadata = base64.b64encode(b"{\"scheduling_hints\": \"test 4\"}")
    env.eosmgm[0].copyTo("system_tests/scripts/eos/grep_eosreport_for_archive_metadata.sh", "/root/", permissions="+x")
    env.client[0].exec(f"/root/test_archive_metadata.sh {metadata}")
    env.eosmgm[0].exec(f"/root/grep_eosreport_for_archive_metadata.sh {metadata}")

def test_immutable_files(env):
    test_dir = f"{env.eos_preprod_dir}/{uuid.uuid4()}"
    env.client[0].exec(f". /root/client_env && echo yes | cta-immutable-file-test root://{env.disk_instance_name}/{test_dir}/immutable_file")
    # Cleanup
    env.eosmgm[0].exec(f"eos rm -rf {test_dir}")

def test_simple_archive_retrieve(env):
    env.client[0].exec(f". /root/client_env && /root/test_simple_ar.sh")

# TODO: some of these tests are dependent on eachother
def test_archive(env):
    env.client[0].exec(f". /root/client_env && /root/test_archive.sh")
    # Allow FST and MGM communication to settle
    # TODO: replace by something more deterministic
    time.sleep(10)

def test_retrieve(env):
    env.client[0].exec(f". /root/client_env && /root/test_retrieve.sh")

def test_evict(env):
    env.client[0].exec(f". /root/client_env && /root/test_simple_evict.sh")

@pytest.mark.skip(reason="currently not working with the -e flag set")
def test_abort_prepare(env):
    env.client[0].exec(f". /root/client_env && /root/test_abort_prepare.sh")

def test_delete(env):
    env.client[0].exec(f". /root/client_env && /root/test_delete.sh")

def test_archive_retrieve_timestamps_are_correct(env):
    env.client[0].exec(f". /root/client_env && /root/test_timestamp.sh")

def test_multiple_retrieve(env):
    env.client[0].exec(f"/root/test_multiple_retrieve.sh")

@pytest.mark.skip(reason="currently not working with the -e flag set")
def test_idempotent_prepare(env):
    # ${CTA_TEST_NO_P_DIR} must be writable by eosusers and powerusers
    # but not allow prepare requests.
    # this is achieved through the ACLs.
    no_prepare_dir=f"{env.eos_preprod_dir}/no_prepare"
    env.eosmgm[0].exec(f"eos mkdir {no_prepare_dir}")
    env.eosmgm[0].exec(f"eos attr set sys.acl=g:eosusers:rwx!d,u:poweruser1:rwx+d,u:poweruser2:rwx+d,z:'!'u'!'d {no_prepare_dir}")

    env.client[0].exec(f"/root/test_idempotent_prepare.sh")

    # Cleanup
    env.eosmgm[0].exec(f"eos rm -rF {no_prepare_dir}")

def test_delete_on_closew_error(env):
    env.client[0].exec(f"/root/test_delete_on_closew_error.sh")

def test_archive_zero_length_file(env):
    env.client[0].exec(f"/root/test_archive_zero_length_file.sh")

def test_evict_before_archive_completed(env):
    env.client[0].exec(f"/root/test_evict_before_archive_completed.sh")

def test_stagerrm(env):
    env.client[0].exec(f"/root/test_stagerrm.sh")

def test_evict(env):
    env.client[0].exec(f"/root/test_evict.sh")

def test_taped_log_rotation(env):
    env.ctataped[0].exec(f"/root/test_refresh_log_fd.sh.sh")

def test_retrieve_cleanup(env):
    num_copies = 3
    copy_dirs = []
    for i in range(num_copies):
        multi_copy_dir = f"{env.eos_preprod_dir}/dir_{i}_copy"
        env.eosmgm[0].exec(f"eos mkdir {multi_copy_dir}")
        env.eosmgm[0].exec(f"eos attr set sys.archive.storage_class=ctaStorageClass_1_copy {multi_copy_dir}")
        copy_dirs.append(multi_copy_dir)


    tapes = json.loads(env.ctacli[0].execWithOutput("cta-admin --json tape ls --all"))
    non_full_tapes = [tape["vid"] for tape in tapes if not tape.get("full", True)]
    assert len(non_full_tapes) >= num_copies
    for i in range(num_copies):
        letter = chr(65 + i) # 0 = A, 1 = B, etc
        env.ctacli[0].exec(f"cta-admin tape ch --vid {non_full_tapes[0]} --tapepool ctasystest_{letter}")

    env.client[0].exec(f"/root/test_retrieve_cleanup.sh")

    # Cleanup
    for dir in copy_dirs:
        env.eosmgm[0].exec(f"eos rm -rf {dir}")


def test_eosdf_runs_correctly(env):
    eosdf_buffer_basedir = f"{env.eos_base_dir}/eosdf"
    env.eosmgm[0].exec(f"eos mkdir {eosdf_buffer_basedir}")
    env.eosmgm[0].exec(f"eos chmod 1777 {eosdf_buffer_basedir}")

    env.client[0].exec(f". /root/client_env && /root/test_eosdf.sh")
    taped_log_file_loc = env.ctataped[0].taped_log_file_location()
    env.ctataped[0].exec(f"grep -q 'unable to get the free disk space with the script' {taped_log_file_loc}")

    # Cleanup
    env.eosmgm[0].exec(f"eos rm -rf {eosdf_buffer_basedir}")

def test_eosdf_runs_correctly_without_script_present(env):
    eosdf_buffer_basedir = f"{env.eos_base_dir}/eosdf"
    env.eosmgm[0].exec(f"eos mkdir {eosdf_buffer_basedir}")
    env.eosmgm[0].exec(f"eos chmod 1777 {eosdf_buffer_basedir}")
    env.ctataped[0].exec("mv /usr/bin/cta-eosdf.sh /usr/bin/eosdf_newname.sh")

    env.client[0].exec(f". /root/client_env && /root/test_eosdf.sh")
    taped_log_file_loc = env.ctataped[0].taped_log_file_location()
    env.ctataped[0].exec(f"grep -q 'No such file or directory' {taped_log_file_loc}")

    # Cleanup
    env.ctataped[0].exec("mv /usr/bin/eosdf_newname.sh /usr/bin/cta-eosdf.sh")
    env.eosmgm[0].exec(f"eos rm -rf {eosdf_buffer_basedir}")

def test_eosdf_runs_correctly_with_incorrect_permissions(env):
    eosdf_buffer_basedir = f"{env.eos_base_dir}/eosdf"
    env.eosmgm[0].exec(f"eos mkdir {eosdf_buffer_basedir}")
    env.eosmgm[0].exec(f"eos chmod 1777 {eosdf_buffer_basedir}")
    env.ctataped[0].exec("chmod -x /usr/bin/cta-eosdf.sh")

    env.client[0].exec(f". /root/client_env && /root/test_eosdf.sh")
    taped_log_file_loc = env.ctataped[0].taped_log_file_location()
    env.ctataped[0].exec(f"grep -q 'Permission denied' {taped_log_file_loc}")

    # Cleanup
    env.ctataped[0].exec("chmod +x /usr/bin/cta-eosdf.sh")
    env.eosmgm[0].exec(f"eos rm -rf {eosdf_buffer_basedir}")

def test_eosdf_runs_correctly_with_eos_unreachable(env):
    eosdf_buffer_basedir = f"{env.eos_base_dir}/eosdf"
    env.eosmgm[0].exec(f"eos mkdir {eosdf_buffer_basedir}")
    env.eosmgm[0].exec(f"eos chmod 1777 {eosdf_buffer_basedir}")
    env.ctataped[0].exec(r"sed -i 's|root://\$diskInstance|root://nonexistentinstance|g' /usr/bin/cta-eosdf.sh")

    env.client[0].exec(f". /root/client_env && /root/test_eosdf.sh")
    taped_log_file_loc = env.ctataped[0].taped_log_file_location()
    env.ctataped[0].exec(f"grep -q 'Permission denied' {taped_log_file_loc}")

    # Cleanup
    env.ctataped[0].exec(r"sed -i 's|root://nonexistentinstance|root://\$diskInstance|g' /usr/bin/cta-eosdf.sh")
    env.eosmgm[0].exec(f"eos rm -rf {eosdf_buffer_basedir}")
