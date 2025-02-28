def test_set_one_drive_down(env):
    # The cta-admin tests rely on the second drive being down (blegh; something to change in the future)
    env.ctacli[0].set_drive_down(env.ctataped[1].drive_name())

# For now we don't refactor/split the test_cta_admin.sh script yet
# That will come in stage 2 of the system test refactor
def test_cta_admin(env):
    env.client[0].exec("/root/test_cta_admin.sh")
