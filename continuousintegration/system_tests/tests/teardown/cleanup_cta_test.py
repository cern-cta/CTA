#####################################################################################################################
# CTA Teardown
#####################################################################################################################


def test_cleanup_catalogue(env):
    schema_version = env.cta_frontend[0].get_schema_version()
    env.cta_frontend[0].exec("echo yes | cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf")
    env.cta_frontend[0].exec(f"cta-catalogue-schema-create -v {schema_version} /etc/cta/cta-catalogue.conf")


def test_restart_cta_taped(env):
    # As the drives need to register themselves in the catalogue, they cannot survive a catalogue wipe without a restart
    hosts = env.cta_taped
    for host in hosts:
        host.restart(wait_for_restart=False)
    for host in hosts:
        host.wait_for_host_up()


def test_delete_test_scripts(env):
    # Don't need to do this for taped as these already restarted
    hosts = env.disk_client + env.cta_cli + env.cta_frontend + env.disk_instance
    for host in hosts:
        host.exec("rm -rf /test/ 2>/dev/null || true")
