UNAME := $(shell uname)

ifeq ($(UNAME), Darwin)
UUID_LIB=
else
UUID_LIB=-luuid
endif

ROOT_DIR=../..
INCLUDE_OPS=-I$(ROOT_DIR) -I$(ROOT_DIR)/h -I/opt/local/var/macports/software/cppunit/1.12.1_0/opt/local/include
LIB_DIR_OPS=-L/opt/local/var/macports/software/cppunit/1.12.1_0/opt/local/lib
EXECUTABLES=testtapebridge sendflushedtotape
COMMON_OPS=-g -pthread

all: $(EXECUTABLES)

clean:
	rm -f $(EXECUTABLES) *.o

testtapebridge: \
  testTapeBridgeMain.o \
  tapebridge_marshall.o \
  Cglobals.o \
  marshall.o \
  legacymsg_TapeBridgeMarshal.o \
  legacymsg_CommonMarshal.o \
  exception_Exception.o \
  exception_Internal.o
	g++ $(COMMON_OPS) $(LIB_DIR_OPS) -lpthread -lcppunit -o $@ $^

sendflushedtotape: \
  sendFlushedToTapeMain.o \
  tapebridge_marshall.o \
  Cglobals.o \
  marshall.o \
  tapebridge_sendTapeBridgeFlushedToTape.o \
  common_socket_timeout.o \
  common_socket.o \
  common_serror.o
	g++ $(COMMON_OPS) $(LIB_DIR_OPS) -lpthread -lcppunit -o $@ $^

testTapeBridgeMain.o: \
  testTapeBridgeMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testTapeBridgeMain.cpp: \
  MarshallTapeBridgeClientInfoTest.hpp \
  MarshallTapeBridgeFlushedToTapeTest.hpp \
  SendTapeBridgeFlushedToTapeTest.hpp
	touch $@

sendFlushedToTapeMain.o: \
  sendFlushedToTapeMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_sendTapeBridgeFlushedToTape.o: \
  $(ROOT_DIR)/tapebridge/tapebridge_sendTapeBridgeFlushedToTape.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_marshall.o: $(ROOT_DIR)/tapebridge/tapebridge_marshall.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

$(ROOT_DIR)/tapebridge/tapebridge_marshall.c: \
  $(ROOT_DIR)/h/tapebridge_marshall.h
	touch $@

Cglobals.o: $(ROOT_DIR)/common/Cglobals.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

marshall.o: $(ROOT_DIR)/common/marshall.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_TapeBridgeMarshal.o: \
  $(ROOT_DIR)/castor/tape/legacymsg/TapeBridgeMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_CommonMarshal.o: \
  $(ROOT_DIR)/castor/tape/legacymsg/CommonMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_Exception.o: $(ROOT_DIR)/castor/exception/Exception.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_Internal.o: $(ROOT_DIR)/castor/exception/Internal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_socket_timeout.o: $(ROOT_DIR)/common/socket_timeout.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_socket.o: $(ROOT_DIR)/common/socket.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_serror.o: $(ROOT_DIR)/common/serror.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^
