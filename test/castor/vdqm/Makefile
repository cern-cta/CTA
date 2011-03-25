EXECUTABLES=setvdqmunitstatus
ROOT=../../..
INCLUDE_OPS=-I$(ROOT) -I$(ROOT)/h
GCC_COMMON_OPS=-g -pthread

all: setvdqmunitstatus

clean:
	rm -f $(EXECUTABLES) *.o

setvdqmunitstatus: \
  setvdqmunitstatus.o \
  vdqmapi.o \
  trace.o \
  serror.o \
  Cglobals.o \
  getconfent.o \
  socket.o \
  Cnetdb.o \
  Cpwd.o \
  vdqm_SendRecv.o \
  socket_timeout.o \
  remote.o \
  marshall.o \
  log.o
	gcc $(GCC_COMMON_OPS) -lpthread -o $@ $^

setvdqmunitstatus.o: setvdqmunitstatus.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

vdqmapi.o: $(ROOT)/vdqm/vdqmapi.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

trace.o: $(ROOT)/common/trace.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

serror.o: $(ROOT)/common/serror.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

Cglobals.o: $(ROOT)/common/Cglobals.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

getconfent.o: $(ROOT)/common/getconfent.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

socket.o: $(ROOT)/common/socket.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

Cnetdb.o: $(ROOT)/common/Cnetdb.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

Cpwd.o: $(ROOT)/common/Cpwd.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

vdqm_SendRecv.o: $(ROOT)/vdqm/vdqm_SendRecv.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

socket_timeout.o: $(ROOT)/common/socket_timeout.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

remote.o: $(ROOT)/common/remote.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

marshall.o: $(ROOT)/common/marshall.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^

log.o: $(ROOT)/common/log.c
	gcc $(GCC_COMMON_OPS) $(INCLUDE_OPS) -c -o $@ $^
