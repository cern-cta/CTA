CSEC_MECHLIST=CSEC_Mechlist

ROOT_DIR=../..
NEW_INCLUDE_DIR=$(ROOT_DIR)/h
TEST_INCLUDE_DIR=$(ROOT_DIR)/test/vmgr
EXECUTABLES= \
  vmgrquerytape \
  vmgrquerytape_trunk_r21843 \
  vmgrenterdenmap_trunk_r21843 \
  vmgrlistdenmap_trunk_r21843 \
  vmgrentertape_trunk_r21843 \
  vmgrlisttape_trunk_r21843 \
  vmgrmodifytape_trunk_r21843 \
  vmgrentermodel_trunk_r21843 \
  vmgrlistmodel_trunk_r21843 \
  vmgrenterlibrary_trunk_r21843 \
  vmgrlistlibrary_trunk_r21843 \
  vmgrmodifylibrary_trunk_r21843 \
  vmgrenterdgnmap_trunk_r21843 \
  vmgrlistdgnmap_trunk_r21843 \
  vmgrenterpool_trunk_r21843 \
  vmgrlistpool_trunk_r21843 \
  vmgrmodifypool_trunk_r21843 \
  vmgrsettag_trunk_r21843 \
  vmgrgettag_trunk_r21843 \
  vmgrdeletedenmap_trunk_r21843 \
  vmgrdeletedgnmap_trunk_r21843 \
  vmgrdeletelibrary_trunk_r21843 \
  vmgrdeletemodel_trunk_r21843 \
  vmgrdeletepool_trunk_r21843 \
  vmgrdeletetape_trunk_r21843 \
  vmgrdeltag_trunk_r21843 \
  reclaim_trunk_r21843 \
  printlistpackingfactors

all: $(EXECUTABLES)

vmgrquerytape: \
  vmgrquerytape.o \
  vmgr_querytape_byte_u64.o \
  vmgr_apiinit.o \
  Cglobals.o \
  send2vmgr.o \
  getconfent.o \
  Cnetdb.o \
  vmgr_errmsg.o \
  socket.o \
  serror.o
	gcc -g -o $@ $^

vmgrquerytape_trunk_r21843: \
  vmgrquerytape_trunk_r21843.o \
  vmgr_querytape_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o
	gcc -g -o $@ $^

vmgrenterdenmap_trunk_r21843: \
  vmgrenterdenmap_trunk_r21843.o \
  vmgr_enterdenmap_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrlistdenmap_trunk_r21843: \
  vmgrlistdenmap_trunk_r21843.o \
  vmgr_listdenmap_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrentertape_trunk_r21843: \
  vmgrentertape_trunk_r21843.o \
  vmgr_entertape_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrlisttape_trunk_r21843: \
  vmgrlisttape_trunk_r21843.o \
  vmgr_listtape_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrmodifytape_trunk_r21843: \
  vmgrmodifytape_trunk_r21843.o \
  vmgr_modifytape_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrentermodel_trunk_r21843: \
  vmgrentermodel_trunk_r21843.o \
  vmgr_entermodel_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrlistmodel_trunk_r21843: \
  vmgrlistmodel_trunk_r21843.o \
  vmgr_listmodel_trunk_r21843.o \
  vmgr_querymodel_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrenterlibrary_trunk_r21843: \
  vmgrenterlibrary_trunk_r21843.o \
  vmgr_enterlibrary_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrlistlibrary_trunk_r21843: \
  vmgrlistlibrary_trunk_r21843.o \
  vmgr_listlibrary_trunk_r21843.o \
  vmgr_querylibrary_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrmodifylibrary_trunk_r21843: \
  vmgrmodifylibrary_trunk_r21843.o \
  vmgr_modifylibrary_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrenterdgnmap_trunk_r21843: \
  vmgrenterdgnmap_trunk_r21843.o \
  vmgr_enterdgnmap_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrlistdgnmap_trunk_r21843: \
  vmgrlistdgnmap_trunk_r21843.o \
  vmgr_listdgnmap_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrenterpool_trunk_r21843: \
  vmgrenterpool_trunk_r21843.o \
  vmgr_enterpool_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrlistpool_trunk_r21843: \
  vmgrlistpool_trunk_r21843.o \
  vmgr_listpool_trunk_r21843.o \
  vmgr_querypool_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrmodifypool_trunk_r21843: \
  vmgrmodifypool_trunk_r21843.o \
  vmgr_modifypool_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrsettag_trunk_r21843: \
  vmgrsettag_trunk_r21843.o \
  vmgr_settag_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrgettag_trunk_r21843: \
  vmgrgettag_trunk_r21843.o \
  vmgr_gettag_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrdeletedenmap_trunk_r21843: \
  vmgrdeletedenmap_trunk_r21843.o \
  vmgr_deletedenmap_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrdeletedgnmap_trunk_r21843: \
  vmgrdeletedgnmap_trunk_r21843.o \
  vmgr_deletedgnmap_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrdeletelibrary_trunk_r21843: \
  vmgrdeletelibrary_trunk_r21843.o \
  vmgr_deletelibrary_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrdeletemodel_trunk_r21843: \
  vmgrdeletemodel_trunk_r21843.o \
  vmgr_deletemodel_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrdeletepool_trunk_r21843: \
  vmgrdeletepool_trunk_r21843.o \
  vmgr_deletepool_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

vmgrdeletetape_trunk_r21843: \
  vmgrdeletetape_trunk_r21843.o \
  vmgr_deletetape_trunk_r21843.o \
  vmgr_querytape_trunk_r21843.o \
  Cns_listtape_trunk_r21843.o \
  Cns_apiinit_trunk_r21843.o \
  Cns_auth_trunk_r21843.o \
  send2nsd_trunk_r21843.o \
  marshall_trunk_r21843.o \
  Cns_errmsg_trunk_r21843.o \
  socket_timeout_trunk_r21843.o \
  Csnprintf_trunk_r21843.o \
  Csec_api_trunk_r21843.o \
  Cmutex_trunk_r21843.o \
  Csec_common_trunk_r21843.o \
  Csec_protocol_policy_trunk_r21843.o \
  Csec_errmsg_trunk_r21843.o \
  Csec_api_loader_trunk_r21843.o \
  Cdomainname_trunk_r21843.o \
  Csec_apiinit_trunk_r21843.o \
  Cpwd_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -ldl -g -o $@ $^

vmgrdeltag_trunk_r21843: \
  vmgrdeltag_trunk_r21843.o \
  vmgr_deltag_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -g -o $@ $^

reclaim_trunk_r21843: \
  reclaim_trunk_r21843.o \
  vmgr_reclaim_trunk_r21843.o \
  vmgr_querytape_trunk_r21843.o \
  Cns_getpath_trunk_r21843.o \
  Cns_listtape_trunk_r21843.o \
  Cns_unlink_trunk_r21843.o \
  Cns_apiinit_trunk_r21843.o \
  Cns_auth_trunk_r21843.o \
  Cns_selectsrvr_trunk_r21843.o \
  send2nsd_trunk_r21843.o \
  marshall_trunk_r21843.o \
  Cns_errmsg_trunk_r21843.o \
  socket_timeout_trunk_r21843.o \
  Csnprintf_trunk_r21843.o \
  Csec_api_trunk_r21843.o \
  Cmutex_trunk_r21843.o \
  Csec_common_trunk_r21843.o \
  Csec_protocol_policy_trunk_r21843.o \
  Csec_errmsg_trunk_r21843.o \
  Csec_api_loader_trunk_r21843.o \
  Cdomainname_trunk_r21843.o \
  Csec_apiinit_trunk_r21843.o \
  Cpwd_trunk_r21843.o \
  vmgr_apiinit_trunk_r21843.o \
  Cglobals_trunk_r21843.o \
  send2vmgr_trunk_r21843.o \
  getconfent_trunk_r21843.o \
  Cnetdb_trunk_r21843.o \
  vmgr_errmsg_trunk_r21843.o \
  socket_trunk_r21843.o \
  serror_trunk_r21843.o \
  Cgetopt_trunk_r21843.o \
  u64subr_trunk_r21843.o
	gcc -ldl -g -o $@ $^

printlistpackingfactors: \
  printlistpackingfactors.o
	gcc -ldl -g -o $@ $^

vmgrquerytape.o: vmgrquerytape.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

vmgrquerytape_trunk_r21843.o: vmgrquerytape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrenterdenmap_trunk_r21843.o: vmgrenterdenmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrlistdenmap_trunk_r21843.o: vmgrlistdenmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrentertape_trunk_r21843.o: vmgrentertape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrlisttape_trunk_r21843.o: vmgrlisttape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrmodifytape_trunk_r21843.o: vmgrmodifytape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrentermodel_trunk_r21843.o: vmgrentermodel_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrlistmodel_trunk_r21843.o: vmgrlistmodel_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrenterlibrary_trunk_r21843.o: vmgrenterlibrary_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrlistlibrary_trunk_r21843.o: vmgrlistlibrary_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrmodifylibrary_trunk_r21843.o: vmgrmodifylibrary_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrenterdgnmap_trunk_r21843.o: vmgrenterdgnmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrlistdgnmap_trunk_r21843.o: vmgrlistdgnmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrenterpool_trunk_r21843.o: vmgrenterpool_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrlistpool_trunk_r21843.o: vmgrlistpool_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrmodifypool_trunk_r21843.o: vmgrmodifypool_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrsettag_trunk_r21843.o: vmgrsettag_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrgettag_trunk_r21843.o: vmgrgettag_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrdeletedenmap_trunk_r21843.o: vmgrdeletedenmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrdeletedgnmap_trunk_r21843.o: vmgrdeletedgnmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrdeletelibrary_trunk_r21843.o: vmgrdeletelibrary_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrdeletemodel_trunk_r21843.o: vmgrdeletemodel_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrdeletepool_trunk_r21843.o: vmgrdeletepool_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrdeletetape_trunk_r21843.o: vmgrdeletetape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrdeltag_trunk_r21843.o: vmgrdeltag_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

printlistpackingfactors.o: printlistpackingfactors.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_querytape_byte_u64.o: $(ROOT_DIR)/vmgr/vmgr_querytape_byte_u64.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

vmgr_querytape_trunk_r21843.o: vmgr_querytape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_enterdenmap_trunk_r21843.o: vmgr_enterdenmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_listdenmap_trunk_r21843.o: vmgr_listdenmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_entertape_trunk_r21843.o: vmgr_entertape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_listtape_trunk_r21843.o: vmgr_listtape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_modifytape_trunk_r21843.o: vmgr_modifytape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_entermodel_trunk_r21843.o: vmgr_entermodel_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_listmodel_trunk_r21843.o: vmgr_listmodel_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_querymodel_trunk_r21843.o: vmgr_querymodel_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_enterlibrary_trunk_r21843.o: vmgr_enterlibrary_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_listlibrary_trunk_r21843.o: vmgr_listlibrary_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_querylibrary_trunk_r21843.o: vmgr_querylibrary_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_modifylibrary_trunk_r21843.o: vmgr_modifylibrary_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_enterpool_trunk_r21843.o: vmgr_enterpool_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_listpool_trunk_r21843.o: vmgr_listpool_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_querypool_trunk_r21843.o: vmgr_querypool_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_modifypool_trunk_r21843.o: vmgr_modifypool_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_settag_trunk_r21843.o: vmgr_settag_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_gettag_trunk_r21843.o: vmgr_gettag_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_deletedenmap_trunk_r21843.o: vmgr_deletedenmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_deletedgnmap_trunk_r21843.o: vmgr_deletedgnmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_deletelibrary_trunk_r21843.o: vmgr_deletelibrary_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_deletemodel_trunk_r21843.o: vmgr_deletemodel_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_deletepool_trunk_r21843.o: vmgr_deletepool_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_deletetape_trunk_r21843.o: vmgr_deletetape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_reclaim_trunk_r21843.o: vmgr_reclaim_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Cns_listtape_trunk_r21843.o: Cns_listtape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Cns_getpath_trunk_r21843.o: Cns_getpath_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Cns_unlink_trunk_r21843.o: Cns_unlink_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Cns_apiinit_trunk_r21843.o: Cns_apiinit_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Cns_auth_trunk_r21843.o: Cns_auth_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Cns_selectsrvr_trunk_r21843.o: Cns_selectsrvr_trunk_r21843.c
	gcc -c -g -o $@ -DCNSHOSTPFX=\"cns\" -DSCNSHOSTPFX=\"scns\" -I $(TEST_INCLUDE_DIR) $^

send2nsd_trunk_r21843.o: send2nsd_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Csec_api_trunk_r21843.o: Csec_api_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Cmutex_trunk_r21843.o: Cmutex_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Csec_common_trunk_r21843.o: Csec_common_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Csec_protocol_policy_trunk_r21843.o: Csec_protocol_policy_trunk_r21843.c
	gcc -c -g -o $@ -DCSEC_DEFAULT_MECHS="\"$(CSEC_MECHLIST)\"" -I $(TEST_INCLUDE_DIR) $^

Csec_errmsg_trunk_r21843.o: Csec_errmsg_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Csec_api_loader_trunk_r21843.o: Csec_api_loader_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Cdomainname_trunk_r21843.o: Cdomainname_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Csec_apiinit_trunk_r21843.o: Csec_apiinit_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Cpwd_trunk_r21843.o: Cpwd_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_apiinit_trunk_r21843.o: vmgr_apiinit_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_apiinit.o: $(ROOT_DIR)/vmgr/vmgr_apiinit.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

marshall_trunk_r21843.o: marshall_trunk_r21843.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

Cns_errmsg_trunk_r21843.o: Cns_errmsg_trunk_r21843.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

socket_timeout_trunk_r21843.o: socket_timeout_trunk_r21843.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

Csnprintf_trunk_r21843.o: Csnprintf_trunk_r21843.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

Cglobals.o: $(ROOT_DIR)/common/Cglobals.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

Cglobals_trunk_r21843.o: Cglobals_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

send2vmgr.o: $(ROOT_DIR)/vmgr/send2vmgr.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

send2vmgr_trunk_r21843.o: send2vmgr_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

getconfent.o: $(ROOT_DIR)/common/getconfent.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

getconfent_trunk_r21843.o: getconfent_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Cnetdb.o: $(ROOT_DIR)/common/Cnetdb.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

Cnetdb_trunk_r21843.o: Cnetdb_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_errmsg.o: $(ROOT_DIR)/vmgr/vmgr_errmsg.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

vmgr_errmsg_trunk_r21843.o: vmgr_errmsg_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

socket.o: $(ROOT_DIR)/common/socket.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

socket_trunk_r21843.o: socket_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

serror.o: $(ROOT_DIR)/common/serror.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

serror_trunk_r21843.o: serror_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

Cgetopt.o: $(ROOT_DIR)/common/Cgetopt.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

Cgetopt_trunk_r21843.o: Cgetopt_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

u64subr_trunk_r21843.o: u64subr_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

clean:
	rm -f *.o $(EXECUTABLES)
