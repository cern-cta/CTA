def test_install_systest_roms(env):
    env.ctarmcd[0].exec("dnf -y install cta-integrationtests")

def test_read_osm_tapes(env):
    env.ctarmcd[0].copyTo("system_tests/scripts/rmcd/", "/root/", permissions="+x")
    env.ctarmcd[0].exec("mv /root/rmcd/* /root && rm -d /root/rmcd")
    drive_device = env.ctataped[0].drive_device()
    env.ctarmcd[0].exec(f"./root/read_osm_tape.sh {drive_device}")

def test_osm_reader(env):
    drive_name = env.ctataped[0].drive_name()
    drive_device = env.ctataped[0].drive_device()
    env.ctarmcd[0].exec(f"cta-osmReaderTest {drive_name} {drive_device}")
