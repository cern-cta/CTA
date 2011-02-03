ROOT_DIR=../..
NEW_INCLUDE_DIR=$(ROOT_DIR)/h
TEST_INCLUDE_DIR=$(ROOT_DIR)/test/vmgr
EXECUTABLES= \
  vmgrquerytape \
  vmgrquerytape_trunk_r21843 \
  vmgrenterdenmap_trunk_r21843 \
  vmgrlistdenmap_trunk_r21843 \
  vmgrlisttape_trunk_r21843 \
  vmgrentermodel_trunk_r21843 \
  vmgrlistmodel_trunk_r21843

all: $(EXECUTABLES)

vmgrquerytape: \
  vmgrquerytape.o\
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
  vmgrquerytape_trunk_r21843.o\
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
  vmgrenterdenmap_trunk_r21843.o\
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
  vmgrlistdenmap_trunk_r21843.o\
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

vmgrlisttape_trunk_r21843: \
  vmgrlisttape_trunk_r21843.o\
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

vmgrentermodel_trunk_r21843: \
  vmgrentermodel_trunk_r21843.o\
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
  vmgrlistmodel_trunk_r21843.o\
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

vmgrquerytape.o: vmgrquerytape.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

vmgrquerytape_trunk_r21843.o: vmgrquerytape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrenterdenmap_trunk_r21843.o: vmgrenterdenmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrlistdenmap_trunk_r21843.o: vmgrlistdenmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrlisttape_trunk_r21843.o: vmgrlisttape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrentermodel_trunk_r21843.o: vmgrentermodel_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgrlistmodel_trunk_r21843.o: vmgrlistmodel_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_querytape_byte_u64.o: $(ROOT_DIR)/vmgr/vmgr_querytape_byte_u64.c
	gcc -c -g -o $@ -I $(NEW_INCLUDE_DIR) $^

vmgr_querytape_trunk_r21843.o: vmgr_querytape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_enterdenmap_trunk_r21843.o: vmgr_enterdenmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_listdenmap_trunk_r21843.o: vmgr_listdenmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_listtape_trunk_r21843.o: vmgr_listtape_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_entermodel_trunk_r21843.o: vmgr_entermodel_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_listmodel_trunk_r21843.o: vmgr_listmodel_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_querymodel_trunk_r21843.o: vmgr_querymodel_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_apiinit_trunk_r21843.o: vmgr_apiinit_trunk_r21843.c
	gcc -c -g -o $@ -I $(TEST_INCLUDE_DIR) $^

vmgr_apiinit.o: $(ROOT_DIR)/vmgr/vmgr_apiinit.c
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
