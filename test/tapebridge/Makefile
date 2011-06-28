UNAME := $(shell uname)

ifeq ($(UNAME), Darwin)
UUID_LIB=
else
UUID_LIB=-luuid
endif

ROOT_DIR=../..
INCLUDE_OPS=-I$(ROOT_DIR) -I$(ROOT_DIR)/h -I/opt/local/var/macports/software/cppunit/1.12.1_0/opt/local/include
LIB_DIR_OPS=-L/opt/local/var/macports/software/cppunit/1.12.1_0/opt/local/lib
EXECUTABLES=testtapebridge
COMMON_OPS=-g -pthread

all: $(EXECUTABLES)

clean:
	rm -f $(EXECUTABLES) *.o

testtapebridge: \
  testTapeBridgeMain.o \
  tapebridge_marshall.o \
  castor_Constants.o \
  common_Cglobals.o \
  common_Csnprintf.o \
  common_Cuuid.o \
  common_getconfent.o \
  common_marshall.o \
  common_socket_timeout.o \
  common_socket.o \
  common_serror.o \
  dlf_Dlf.o \
  dlf_lib.o \
  legacymsg_TapeBridgeMarshal.o \
  legacymsg_CommonMarshal.o \
  exception_Communication.o \
  exception_Exception.o \
  exception_Internal.o \
  exception_InvalidArgument.o \
  exception_InvalidConfigEntry.o \
  exception_InvalidConfiguration.o \
  exception_NoEntry.o \
  exception_NoPortInRange.o \
  exception_OutOfMemory.o \
  exception_PermissionDenied.o \
  exception_TapeNetAcceptInterrupted.o \
  exception_TimeOut.o \
  exception_TooBig.o \
  net_net.o \
  tapebridge_LegacyTxRx.o \
  tapebridge_LogHelper.o \
  tapebridge_recvTapeBridgeFlushedToTapeAck.o \
  tapebridge_sendTapeBridgeFlushedToTape.o \
  test_exception.o \
  utils_SmartFd.o \
  utils_SmartFILEPtr.o \
  utils_utils.o
	g++ $(COMMON_OPS) $(LIB_DIR_OPS) -lpthread -lcppunit $(UUID_LIB) -o $@ $^

testTapeBridgeMain.o: \
  testTapeBridgeMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testTapeBridgeMain.cpp: \
  MarshallTapeBridgeClientInfo2Test.hpp \
  MarshallTapeBridgeFlushedToTapeTest.hpp \
  RecvTapeBridgeFlushedToTapeAckTest.hpp \
  SendTapeBridgeFlushedToTapeTest.hpp
	touch $@

tapebridge_recvTapeBridgeFlushedToTapeAck.o: \
  $(ROOT_DIR)/tapebridge/tapebridge_recvTapeBridgeFlushedToTapeAck.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_LegacyTxRx.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/LegacyTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_LogHelper.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/LogHelper.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_sendTapeBridgeFlushedToTape.o: \
  $(ROOT_DIR)/tapebridge/tapebridge_sendTapeBridgeFlushedToTape.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_marshall.o: $(ROOT_DIR)/tapebridge/tapebridge_marshall.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

$(ROOT_DIR)/tapebridge/tapebridge_marshall.c: \
  $(ROOT_DIR)/h/tapebridge_marshall.h
	touch $@

castor_Constants.o: $(ROOT_DIR)/castor/Constants.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_getconfent.o: $(ROOT_DIR)/common/getconfent.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cglobals.o: $(ROOT_DIR)/common/Cglobals.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Csnprintf.o: $(ROOT_DIR)/common/Csnprintf.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cuuid.o: $(ROOT_DIR)/common/Cuuid.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_marshall.o: $(ROOT_DIR)/common/marshall.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_socket_timeout.o: $(ROOT_DIR)/common/socket_timeout.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_socket.o: $(ROOT_DIR)/common/socket.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_serror.o: $(ROOT_DIR)/common/serror.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

dlf_Dlf.o: $(ROOT_DIR)/castor/dlf/Dlf.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

dlf_lib.o: $(ROOT_DIR)/dlf/dlf_lib.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_Exception.o: $(ROOT_DIR)/castor/exception/Exception.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_NoEntry.o: $(ROOT_DIR)/castor/exception/NoEntry.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_InvalidArgument.o: $(ROOT_DIR)/castor/exception/InvalidArgument.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_Communication.o: $(ROOT_DIR)/castor/exception/Communication.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_Internal.o: $(ROOT_DIR)/castor/exception/Internal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_InvalidConfigEntry.o: \
  $(ROOT_DIR)/castor/exception/InvalidConfigEntry.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_InvalidConfiguration.o: \
   $(ROOT_DIR)/castor/exception/InvalidConfiguration.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_TimeOut.o: $(ROOT_DIR)/castor/exception/TimeOut.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_TooBig.o: $(ROOT_DIR)/castor/exception/TooBig.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_NoPortInRange.o: $(ROOT_DIR)/castor/exception/NoPortInRange.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_TapeNetAcceptInterrupted.o: \
  $(ROOT_DIR)/castor/exception/TapeNetAcceptInterrupted.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_PermissionDenied.o: \
  $(ROOT_DIR)/castor/exception/PermissionDenied.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_OutOfMemory.o: \
  $(ROOT_DIR)/castor/exception/OutOfMemory.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_TapeBridgeMarshal.o: \
  $(ROOT_DIR)/castor/tape/legacymsg/TapeBridgeMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_CommonMarshal.o: \
  $(ROOT_DIR)/castor/tape/legacymsg/CommonMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

net_net.o: $(ROOT_DIR)/castor/tape/net/net.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

test_exception.o: test_exception.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

utils_SmartFd.o: $(ROOT_DIR)/castor/tape/utils/SmartFd.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

utils_SmartFILEPtr.o: $(ROOT_DIR)/castor/tape/utils/SmartFILEPtr.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

utils_utils.o: $(ROOT_DIR)/castor/tape/utils/utils.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^
