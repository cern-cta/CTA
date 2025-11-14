from ...helpers.tape import list_all_tapes_in_libraries, unload_tapes

def test_reset_tapes(env):
    for ctarmcd in env.ctarmcd:
        unload_tapes(ctarmcd)

def test_reset_drive_devices(env):
    for ctataped in env.ctataped:
        ctataped.exec(f"sg_turs {ctataped.drive_device()} 2>&1 > /dev/null || true")

def test_label_tapes(env):
    tapes: list[str] = list_all_tapes_in_libraries(env.ctarmcd)
    for tape in tapes:
        env.ctataped[0].exec(f"cta-tape-label --vid {tape} --force")

def test_set_all_drives_up(env):
    env.ctacli[0].set_all_drives_up()
