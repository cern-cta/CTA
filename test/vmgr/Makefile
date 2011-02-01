ROOT_DIR=../..
INCLUDE_DIR=$(ROOT_DIR)/h

all: \
  vmgrquerytape \
  vmgrquerytape_trunk_r21843 \
  vmgrenterdenmap_trunk_r21843

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
  vmgr_apiinit.o \
  Cglobals.o \
  send2vmgr.o \
  getconfent.o \
  Cnetdb.o \
  vmgr_errmsg.o \
  socket.o \
  serror.o
	gcc -g -o $@ $^

vmgrenterdenmap_trunk_r21843: \
  vmgrenterdenmap_trunk_r21843.o\
  vmgr_enterdenmap_trunk_r21843.o \
  vmgr_apiinit.o \
  Cglobals.o \
  send2vmgr.o \
  getconfent.o \
  Cnetdb.o \
  vmgr_errmsg.o \
  socket.o \
  serror.o \
  Cgetopt.o \
  u64subr.o
	gcc -g -o $@ $^

vmgrquerytape.o: vmgrquerytape.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

vmgrquerytape_trunk_r21843.o: vmgrquerytape_trunk_r21843.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

vmgrenterdenmap_trunk_r21843.o: vmgrenterdenmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

vmgr_querytape_byte_u64.o: $(ROOT_DIR)/vmgr/vmgr_querytape_byte_u64.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

vmgr_querytape_trunk_r21843.o: vmgr_querytape_trunk_r21843.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

vmgr_enterdenmap_trunk_r21843.o: vmgr_enterdenmap_trunk_r21843.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

vmgr_apiinit.o: $(ROOT_DIR)/vmgr/vmgr_apiinit.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

Cglobals.o: $(ROOT_DIR)/common/Cglobals.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

send2vmgr.o: $(ROOT_DIR)/vmgr/send2vmgr.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

getconfent.o: $(ROOT_DIR)/common/getconfent.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

Cnetdb.o: $(ROOT_DIR)/common/Cnetdb.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

vmgr_errmsg.o: $(ROOT_DIR)/vmgr/vmgr_errmsg.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

socket.o: $(ROOT_DIR)/common/socket.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

serror.o: $(ROOT_DIR)/common/serror.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

Cgetopt.o: $(ROOT_DIR)/common/Cgetopt.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

u64subr.o: $(ROOT_DIR)/common/u64subr.c
	gcc -c -g -o $@ -I $(INCLUDE_DIR) $^

clean:
	rm -f vmgrquerytape vmgrquerytape_trunk_r21843 vmgrenterdenmap_trunk_r21843 *.o
