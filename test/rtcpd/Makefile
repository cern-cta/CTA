ROOT_DIR=../..
INCLUDE_OPS=-I$(ROOT_DIR) -I$(ROOT_DIR)/h
EXECUTABLES=sendrtcopyjob
COMMON_OPS=-g -pthread

all: $(EXECUTABLES)

clean:
	rm -f $(EXECUTABLES) *.o

sendrtcopyjob: \
  sendrtcopyjob.o \
  TestRemoteCopyConnection.o \
  socket_timeout.o \
  serror.o \
  Cglobals.o \
  Exception.o \
  socket.o \
  AbstractSocket.o \
  AbstractTCPSocket.o \
  Cnetdb.o \
  BaseObject.o \
  StreamAddress.o \
  Services.o \
  BaseAddress.o \
  getconfent.o \
  Internal.o \
  Cmutex.o \
  Factories.o \
  Constants.o \
  Cdlopen.o \
  ObjectSet.o \
  Csnprintf.o \
  Cuuid.o
	g++ $(COMMON_OPS) -lpthread -ldl -luuid -o $@ $^

sendrtcopyjob.o: sendrtcopyjob.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

TestRemoteCopyConnection.o: TestRemoteCopyConnection.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

socket_timeout.o: $(ROOT_DIR)/common/socket_timeout.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

serror.o: $(ROOT_DIR)/common/serror.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cglobals.o: $(ROOT_DIR)/common/Cglobals.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Exception.o: $(ROOT_DIR)/castor/exception/Exception.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

socket.o: $(ROOT_DIR)/common/socket.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

AbstractSocket.o: $(ROOT_DIR)/castor/io/AbstractSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

AbstractTCPSocket.o: $(ROOT_DIR)/castor/io/AbstractTCPSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cnetdb.o: $(ROOT_DIR)/common/Cnetdb.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

BaseObject.o: $(ROOT_DIR)/castor/BaseObject.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

StreamAddress.o: $(ROOT_DIR)/castor/io/StreamAddress.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Services.o: $(ROOT_DIR)/castor/Services.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

BaseAddress.o: $(ROOT_DIR)/castor/BaseAddress.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

getconfent.o: $(ROOT_DIR)/common/getconfent.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Internal.o: $(ROOT_DIR)/castor/exception/Internal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cmutex.o: $(ROOT_DIR)/common/Cmutex.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Factories.o: $(ROOT_DIR)/castor/Factories.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Constants.o: $(ROOT_DIR)/castor/Constants.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cdlopen.o: $(ROOT_DIR)/common/Cdlopen.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ObjectSet.o: $(ROOT_DIR)/castor/ObjectSet.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Csnprintf.o: $(ROOT_DIR)/common/Csnprintf.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cuuid.o: $(ROOT_DIR)/common/Cuuid.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^
