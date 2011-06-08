ROOT_DIR=../..
INCLUDE_OPS=-I$(ROOT_DIR) -I$(ROOT_DIR)/h
EXECUTABLES=sendrtcopyjob testrtcpd
COMMON_OPS=-Werror -Wno-long-long -pedantic -Wall -g -pthread

all: $(EXECUTABLES)

clean:
	rm -f $(EXECUTABLES) *.o

sendrtcopyjob: \
  sendrtcopyjob.o \
  TestRemoteCopyConnection.o \
  castor_BaseObject.o \
  castor_Services.o \
  castor_BaseAddress.o \
  castor_Factories.o \
  castor_Constants.o \
  castor_ObjectSet.o \
  common_Cnetdb.o \
  common_getconfent.o \
  common_Cmutex.o \
  common_Cdlopen.o \
  common_socket.o \
  common_socket_timeout.o \
  common_serror.o \
  common_Cglobals.o \
  common_Csnprintf.o \
  common_Cuuid.o \
  exception_Exception.o \
  exception_Internal.o \
  io_AbstractSocket.o \
  io_AbstractTCPSocket.o \
  io_StreamAddress.o
	g++ $(COMMON_OPS) -lpthread -ldl -luuid -o $@ $^

testrtcpd: \
  testRtcpdMain.o \
  common_Cglobals.o \
  common_marshall.o \
  common_Cmutex.o \
  common_Cthread.o \
  common_serror.o \
  common_log.o \
  common_Cuuid.o \
  common_Csnprintf.o \
  common_Cnetdb.o \
  common_getconfent.o \
  common_socket.o \
  common_socket_timeout.o \
  common_trace.o \
  common_remote.o \
  common_Cpwd.o \
  dlf_dlf_lib.o \
  rtcp_marshallVdqmClientInfoMsg.o \
  rtcp_log.o \
  rtcpd_SignalFilePositioned.o \
  rtcp_SendRecv.o \
  tape_tplogger.o \
  tape_tplogger_messages.o \
  vdqm_vdqmapi.o \
  vdqm_vdqm_SendRecv.o
	g++ $(COMMON_OPS) -lpthread -luuid -lcppunit -o $@ $^

sendrtcopyjob.o: sendrtcopyjob.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

TestRemoteCopyConnection.o: TestRemoteCopyConnection.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_Exception.o: $(ROOT_DIR)/castor/exception/Exception.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_Internal.o: $(ROOT_DIR)/castor/exception/Internal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_Services.o: $(ROOT_DIR)/castor/Services.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_BaseAddress.o: $(ROOT_DIR)/castor/BaseAddress.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_Factories.o: $(ROOT_DIR)/castor/Factories.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_Constants.o: $(ROOT_DIR)/castor/Constants.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_BaseObject.o: $(ROOT_DIR)/castor/BaseObject.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_ObjectSet.o: $(ROOT_DIR)/castor/ObjectSet.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_socket.o: $(ROOT_DIR)/common/socket.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_socket_timeout.o: $(ROOT_DIR)/common/socket_timeout.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_serror.o: $(ROOT_DIR)/common/serror.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cglobals.o: $(ROOT_DIR)/common/Cglobals.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cnetdb.o: $(ROOT_DIR)/common/Cnetdb.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_getconfent.o: $(ROOT_DIR)/common/getconfent.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cmutex.o: $(ROOT_DIR)/common/Cmutex.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cdlopen.o: $(ROOT_DIR)/common/Cdlopen.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Csnprintf.o: $(ROOT_DIR)/common/Csnprintf.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cuuid.o: $(ROOT_DIR)/common/Cuuid.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_marshall.o: $(ROOT_DIR)/common/marshall.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cthread.o: $(ROOT_DIR)/common/Cthread.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_log.o: $(ROOT_DIR)/common/log.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_trace.o: $(ROOT_DIR)/common/trace.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_remote.o: $(ROOT_DIR)/common/remote.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cpwd.o: $(ROOT_DIR)/common/Cpwd.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

dlf_dlf_lib.o: $(ROOT_DIR)/dlf/dlf_lib.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_AbstractSocket.o: $(ROOT_DIR)/castor/io/AbstractSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_AbstractTCPSocket.o: $(ROOT_DIR)/castor/io/AbstractTCPSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamAddress.o: $(ROOT_DIR)/castor/io/StreamAddress.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testRtcpdMain.o: testRtcpdMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testRtcpdMain.cpp: RtcpdTest.hpp
	touch $@

rtcp_marshallVdqmClientInfoMsg.o: \
  $(ROOT_DIR)/rtcopy/rtcp_marshallVdqmClientInfoMsg.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_tapeIOthread.o: $(ROOT_DIR)/rtcopy/rtcpd_tapeIOthread.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_SignalFilePositioned.o: $(ROOT_DIR)/rtcopy/rtcpd_SignalFilePositioned.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_log.o: $(ROOT_DIR)/rtcopy/rtcp_log.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_SendRecv.o: $(ROOT_DIR)/rtcopy/rtcp_SendRecv.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_tplogger.o: $(ROOT_DIR)/tape/tplogger.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

tape_tplogger_messages.o: $(ROOT_DIR)/tape/tplogger_messages.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

vdqm_vdqmapi.o: $(ROOT_DIR)/vdqm/vdqmapi.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

vdqm_vdqm_SendRecv.o: $(ROOT_DIR)/vdqm/vdqm_SendRecv.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^
