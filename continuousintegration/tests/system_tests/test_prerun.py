import json

def test_xrootd_tcp_capabilities_on_fsts(env):
    ###
    # TPC capabilities
    #
    # if the installed xrootd version is not the same as the one used to compile eos
    # Third Party Copy can be broken
    #
    # Valid check:
    # [root@eosctafst0017 ~]# xrdfs
    # root://pps-ngtztag6p5ht-minion-2.cern.ch:1101 query config tpc
    # 1
    #
    # invalid check:
    # [root@ctaeos /]# xrdfs root://ctaeos.toto.svc.cluster.local:1095 query config tpc
    # tpc
    fst_hosts = env.eosmgm[0].execWithOutput(r"eos node ls -m | grep status=online | sed -e 's/.*hostport=//;s/ .*//' ").splitlines()
    for fst_host in fst_hosts:
        env.eosmgm[0].exec(f"xrdfs root://{fst_host} query config tpc | grep -q 1")

def test_xrootd_api_fts_compliance(env):
    ###
    # xrootd API FTS compliance
    #
    # see CTA#1256
    # write 3 files and xrdfs query them in reverse order with duplicates
    test_dir=f"{env.eos_base_dir}/tmp"
    env.eosmgm[0].exec(f"eos mkdir {test_dir} && eos chmod 777 {test_dir}")
    files_to_write=[f"{test_dir}/{file}" for file in ["test1", "test2", "test3"]]
    for test_file in files_to_write:
        env.eosmgm[0].exec(f"eos touch {test_file}")

    # Reverse order with duplicates
    files_to_prepare=[f"{test_dir}/{file}" for file in ["test3", "test2", "test1", "test3"]]
    json_response = env.eosmgm[0].execWithOutput(f"xrdfs root://localhost query prepare 0 {' '.join(files_to_prepare)}")
    prepare_responses = json.loads(json_response)["responses"]
    # xrdfs query prepare 3 2 1 3 must answer 3 2 1 3
    for response, prepare_file in zip(prepare_responses, files_to_prepare):
        assert response["path"] == prepare_file, "path in xrd prepare response is not correct or not in the correct order"

    env.eosmgm[0].exec(f"eos rm -rF --no-confirmation {test_dir}")
