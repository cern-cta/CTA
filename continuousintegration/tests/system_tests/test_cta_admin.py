# For now we don't refactor/split the test_cta_admin.sh script yet
# That will come in stage 2 of the system test refactor
def test_cta_admin(env):
    env.client[0].exec("/root/test_cta_admin.sh")
