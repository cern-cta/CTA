# For now we don't refactor/split the test_cta_admin.sh script yet
# That will come in stage 2 of the system test refactor
def test_repack(env):
    env.execLocal(f"cd system_tests/scripts/repack && ./test_repack.sh -n {env.namespace}")
