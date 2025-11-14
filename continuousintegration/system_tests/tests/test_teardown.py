def test_cleanup_eos(env):
    env.eosmgm[0].exec("eos rm -rF --no-confirmation /eos/ctaeos/cta/* 2>/dev/null || true")
    env.eosmgm[0].exec("eos rm -rF --no-confirmation /eos/ctaeos/preprod/* 2>/dev/null || true")
    env.eosmgm[0].exec("rm -rf /test/ 2>/dev/null || true")

def test_cleanup_catalogue(env):
    schema_version = env.ctafrontend[0].get_schema_version()
    env.ctafrontend[0].exec("echo yes | cta-catalogue-schema-drop /etc/cta/cta-catalogue.conf")
    env.ctafrontend[0].exec(f"cta-catalogue-schema-create -v {schema_version} /etc/cta/cta-catalogue.conf")

def test_restart_cta_taped(env):
    # As the drives need to register themselves in the catalogue, they cannot survive a catalogue wipe without a restart
    hosts = env.ctataped + env.ctarmcd
    for host in hosts:
        host.restart()
    for host in hosts:
        host.wait_for_host_up()
