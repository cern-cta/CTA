def test_setup_helm_repos(env):
    env.execLocal("helm repo add bitnami https://charts.bitnami.com/bitnami")
    env.execLocal("helm repo add dcache https://gitlab.desy.de/api/v4/projects/7648/packages/helm/test")
    env.execLocal("helm repo update")

# Feel free to split this up further and introduce variables
# For now I have just copied verbatim what was in the script up to this point
def test_helm_install_auth(env):
    env.execLocal(f"helm install -n {env.namespace} --replace --wait --timeout 10m0s --set auth.username=dcache --set auth.password=let-me-in --set auth.database=chimera chimera bitnami/postgresql --version=12.12.10")
    env.execLocal(f"helm install -n {env.namespace} --replace --wait --timeout 10m0s cells bitnami/zookeeper")
    env.execLocal(f"helm install -n {env.namespace} --replace --wait --timeout 10m0s --set externalZookeeper.servers=cells-zookeeper --set kraft.enabled=false billing bitnami/kafka --version 23.0.7")
    env.execLocal(f"helm install -n {env.namespace} --debug --replace --wait --timeout 10m0s --set image.tag=9.2.22 --set dcache.hsm.enabled=true store dcache/dcache")

def test_helm_debug(env):
    print("DEBUG INFO... statefulsets")
    env.execLocal(f"kubectl -n {env.namespace} get statefulsets")
    print("DEBUG INFO... descibe statefulset")
    env.execLocal(f"kubectl -n {env.namespace} describe statefulset")
    print("DEBUG INFO... descibe pod")
    env.execLocal(f"kubectl -n {env.namespace} describe pod store-dcache-door")
