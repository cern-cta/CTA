import time

# For now simple global variables
# Should go into env at some point
file_count=2000
file_size_kb=10
processes_count=20

def test_setup_client_gfal_xrootd(env):
    env.client[0].exec("dnf install -y python3-gfal2-util")
    env.client[0].exec("dnf install -y gfal2-plugin-xrootd")
    env.client[0].exec(f"/root/client_setup.sh -n {file_count} -s {file_size_kb} -p {processes_count} -d {env.eos_preprod_dir} -r -c gfal2 -Z root")

def test_archive_gfal_xrootd(env):
    env.client[0].exec(". /root/client_env && /root/test_archive.sh")
    # TODO: replace by something more deterministic
    time.sleep(10)

def test_retrieve_gfal_xrootd(env):
    env.client[0].exec(". /root/client_env && /root/test_retrieve.sh")

def test_evict_gfal_xrootd(env):
    env.client[0].exec(". /root/client_env && /root/test_simple_evict.sh")

def test_delete_gfal_xrootd(env):
    env.client[0].exec(". /root/client_env && /root/test_delete.sh")

def test_setup_client_gfal_https(env):
    env.client[0].exec("dnf install -y gfal2-plugin-http")
    env.client[0].exec(f"/root/client_setup.sh -n {file_count} -s {file_size_kb} -p {processes_count} -d {env.eos_preprod_dir} -r -c gfal2 -Z https")
    # Enable insecure certs for gfal2
    env.client[0].exec("sed -i 's/INSECURE=false/INSECURE=true/g' /etc/gfal2.d/http_plugin.conf")

def test_archive_gfal_https(env):
    env.client[0].exec(". /root/client_env && /root/test_archive.sh")
    # TODO: replace by something more deterministic
    time.sleep(10)

def test_retrieve_gfal_https(env):
    env.client[0].exec(". /root/client_env && /root/test_retrieve.sh")

def test_evict_gfal_https(env):
    env.client[0].exec(". /root/client_env && /root/test_simple_evict.sh")

def test_delete_gfal_https(env):
    env.client[0].exec(". /root/client_env && /root/test_delete.sh")

def test_gfal_activity(env):
    env.client[0].exec(". /root/client_env && /root/test_activity_gfal.sh")

def test_xrootd_activity(env):
    env.client[0].exec(". /root/client_env && /root/test_activity_xrootd.sh")

def test_eosreport_activity(env):
    env.eosmgm[0].copyTo("system_tests/scripts/eos/test_activity_eosreport.sh", "/root/", permissions="+x")
    env.eosmgm[0].exec("/root/test_activity_eosreport.sh")
