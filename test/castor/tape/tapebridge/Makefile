ROOT_DIR=../../../..
INCLUDE_OPS=-I$(ROOT_DIR) -I$(ROOT_DIR)/h
EXECUTABLES=testtapebridge tapebridgedaemontester tapebridged
COMMON_OPS=-Werror -Wall -pedantic -Wno-long-long -g -pthread

all: $(EXECUTABLES)

clean:
	rm -f $(EXECUTABLES) *.o

testtapebridge: \
  testTapeBridgeMain.o \
  BridgeClientInfoSender.o \
  VmgrTxRx.o \
  VmgrMarshal.o \
  legacymsg_CommonMarshal.o \
  legacymsg_RtcpMarshal.o \
  tape_utils.o \
  Cuuid.o \
  getconfent.o \
  exception_Exception.o \
  exception_InvalidArgument.o \
  exception_Communication.o \
  exception_Internal.o \
  exception_InvalidConfigEntry.o \
  exception_InvalidConfiguration.o \
  exception_TimeOut.o \
  exception_TooBig.o \
  exception_NoPortInRange.o \
  exception_TapeNetAcceptInterrupted.o \
  exception_PermissionDenied.o \
  exception_OutOfMemory.o \
  exception_NoEntry.o \
  AbstractSocket.o \
  AbstractTCPSocket.o \
  ClientSocket.o \
  socket_timeout.o \
  Cglobals.o \
  socket.o \
  serror.o \
  BaseAddress.o \
  BaseObject.o \
  Cmutex.o \
  Services.o \
  Factories.o \
  Cdlopen.o \
  Csnprintf.o \
  Cnetdb.o \
  StreamAddress.o \
  SmartFd.o \
  SmartFILEPtr.o \
  tape_net.o \
  ObjectSet.o \
  bridge_LegacyTxRx.o \
  bridge_LogHelper.o \
  Dlf.o \
  dlf_lib.o \
  Constants.o \
  tapebridge_marshall.o \
  tapebridge_sendTapeBridgeAck.o \
  marshall.o \
  test_exception.o \
  vdqmapi.o \
  vdqm_SendRecv.o \
  log.o \
  remote.o \
  trace.o \
  Cpwd.o \
  rtcp_recvRtcpHdr.o \
  rtcpd_GetClientInfo.o \
  rtcpd_SendAckToVdqmOrTapeBridge.o \
  rtcpd_GetClientInfoMsg.o \
  rtcp_marshallVdqmClientInfoMsg.o \
  rtcp_Listen.o \
  rtcp_log.o \
  rtcp_SendRecv.o
	g++ $(COMMON_OPS) -lpthread -luuid -lcppunit -o $@ $^

tapebridgedaemontester: \
  tapebridgedaemontester.o \
  bridge_LegacyTxRx.o \
  exception_Exception.o \
  exception_InvalidArgument.o \
  exception_Communication.o \
  exception_Internal.o \
  exception_InvalidConfigEntry.o \
  exception_InvalidConfiguration.o \
  exception_TimeOut.o \
  exception_TooBig.o \
  exception_NoPortInRange.o \
  exception_TapeNetAcceptInterrupted.o \
  exception_PermissionDenied.o \
  exception_OutOfMemory.o \
  exception_NoEntry.o \
  gateway_FileToMigrateRequest.o \
  gateway_FileToMigrate.o \
  gateway_FileMigratedNotification.o \
  gateway_FileToRecallRequest.o \
  gateway_FileToRecall.o \
  gateway_FileRecalledNotification.o \
  gateway_BaseFileInfo.o \
  gateway_VolumeRequest.o \
  gateway_Volume.o \
  gateway_GatewayMessage.o \
  gateway_PositionCommandCode.o \
  gateway_PingNotification.o \
  gateway_DumpParametersRequest.o \
  gateway_DumpParameters.o \
  gateway_DumpNotification.o \
  gateway_FileToMigrate.o \
  gateway_NoMoreFiles.o \
  gateway_EndNotification.o \
  gateway_EndNotificationErrorReport.o \
  gateway_NotificationAcknowledge.o \
  gateway_ClientType.o \
  gateway_VolumeMode.o \
  io_StreamCnvSvc.o \
  io_StreamVolumeRequestCnv.o \
  io_StreamVolumeCnv.o \
  io_StreamBaseCnv.o \
  castor_BaseCnvSvc.o \
  castor_Converters.o \
  castor_BaseSvc.o \
  castor_ObjectCatalog.o \
  RemoteCopyConnection.o \
  rtcpd_GetClientInfo.o \
  rtcpd_GetClientInfoMsg.o \
  rtcpd_SendAckToVdqmOrTapeBridge.o \
  rtcp_marshallVdqmClientInfoMsg.o \
  rtcp_recvRtcpHdr.o \
  tapebridge_marshall.o \
  tapebridge_sendTapeBridgeAck.o \
  legacymsg_CommonMarshal.o \
  legacymsg_RtcpMarshal.o \
  tape_net.o \
  tape_utils.o \
  vdqm_SendRecv.o \
  marshall.o \
  SmartFd.o \
  SmartFILEPtr.o \
  Cuuid.o \
  log.o \
  remote.o \
  vdqmapi.o \
  trace.o \
  Cpwd.o \
  rtcp_Listen.o \
  rtcp_log.o \
  rtcp_SendRecv.o \
  AbstractSocket.o \
  AbstractTCPSocket.o \
  ClientSocket.o \
  socket_timeout.o \
  socket.o \
  serror.o \
  BaseAddress.o \
  BaseObject.o \
  ObjectSet.o \
  StreamAddress.o \
  Cnetdb.o \
  Cmutex.o \
  Services.o \
  Factories.o \
  Cdlopen.o \
  Cglobals.o \
  getconfent.o \
  Dlf.o \
  dlf_lib.o \
  Csnprintf.o \
  Constants.o \
  test_exception.o
	g++ $(COMMON_OPS) -lpthread -luuid -lcppunit -o $@ $^

tapebridged: \
  TapeBridgeMain.o \
  TapeBridgeDaemon.o \
  bridge_BoolFunctor.o \
  BridgeProtocolEngine.o \
  BridgeSocketCatalogue.o \
  bridge_ClientTxRx.o \
  bridge_DlfMessageStrings.o \
  bridge_LegacyTxRx.o \
  bridge_LogHelper.o \
  io_StreamCnvSvc.o \
  io_StreamVolumeRequestCnv.o \
  io_StreamVolumeCnv.o \
  io_StreamBaseCnv.o \
  castor_BaseCnvSvc.o \
  castor_Converters.o \
  castor_BaseSvc.o \
  castor_ObjectCatalog.o \
  VdqmRequestHandler.o \
  server_TCPListenerThreadPool.o \
  server_UDPListenerThreadPool.o \
  server_ListenerThreadPool.o \
  server_DynamicThreadPool.o \
  server_SignalThreadPool.o \
  server_BaseThreadPool.o \
  server_BaseDaemon.o \
  server_BaseServer.o \
  server_Queue.o \
  server_Mutex.o \
  server_ThreadNotification.o \
  server_NotifierThread.o \
  gateway_FileToMigrateRequest.o \
  gateway_FileToMigrate.o \
  gateway_FileMigratedNotification.o \
  gateway_FileToRecallRequest.o \
  gateway_FileToRecall.o \
  gateway_FileRecalledNotification.o \
  gateway_BaseFileInfo.o \
  gateway_VolumeRequest.o \
  gateway_Volume.o \
  gateway_GatewayMessage.o \
  gateway_PositionCommandCode.o \
  gateway_PingNotification.o \
  gateway_DumpParametersRequest.o \
  gateway_DumpParameters.o \
  gateway_DumpNotification.o \
  gateway_FileToMigrate.o \
  gateway_NoMoreFiles.o \
  gateway_EndNotification.o \
  gateway_EndNotificationErrorReport.o \
  gateway_NotificationAcknowledge.o \
  gateway_ClientType.o \
  gateway_VolumeMode.o \
  RtcpTxRx.o \
  VmgrTxRx.o \
  BridgeClientInfoSender.o \
  dlf_Param.o \
  legacymsg_CommonMarshal.o \
  legacymsg_RtcpMarshal.o \
  VmgrMarshal.o \
  tape_utils.o \
  Cuuid.o \
  Cthread.o \
  Cgrp.o \
  Cgetopt.o \
  getconfent.o \
  exception_Exception.o \
  exception_InvalidArgument.o \
  exception_Communication.o \
  exception_Internal.o \
  exception_InvalidConfigEntry.o \
  exception_InvalidConfiguration.o \
  exception_TimeOut.o \
  exception_TooBig.o \
  exception_NoPortInRange.o \
  exception_TapeNetAcceptInterrupted.o \
  exception_PermissionDenied.o \
  exception_OutOfMemory.o \
  exception_NoEntry.o \
  AbstractSocket.o \
  AbstractTCPSocket.o \
  ClientSocket.o \
  ServerSocket.o \
  UDPSocket.o \
  socket_timeout.o \
  Cglobals.o \
  socket.o \
  serror.o \
  BaseAddress.o \
  BaseObject.o \
  Cmutex.o \
  Services.o \
  Factories.o \
  Cdlopen.o \
  Csnprintf.o \
  Cnetdb.o \
  StreamAddress.o \
  SmartFd.o \
  SmartFILEPtr.o \
  ObjectSet.o \
  tape_net.o \
  Dlf.o \
  dlf_lib.o \
  Constants.o \
  tapebridge_marshall.o \
  tapebridge_sendTapeBridgeAck.o \
  marshall.o \
  rtcpd_SendAckToVdqmOrTapeBridge.o \
  vdqmapi.o \
  vdqm_SendRecv.o \
  log.o \
  remote.o \
  trace.o \
  Cpwd.o \
  rtcpd_GetClientInfoMsg.o \
  rtcp_marshallVdqmClientInfoMsg.o \
  rtcp_recvRtcpHdr.o \
  rtcpd_GetClientInfo.o \
  rtcp_Listen.o \
  rtcp_log.o \
  rtcp_SendRecv.o \
  metrics_MetricsCollector.o \
  metrics_Histogram.o \
  metrics_Counter.o \
  metrics_InternalCounter.o \
  metrics_UpdateThread.o \
  Cmutex.o \
  castor_System.o
	g++ $(COMMON_OPS) -lpthread -luuid -lcppunit -o $@ $^

testTapeBridgeMain.o: \
  testTapeBridgeMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testTapeBridgeMain.cpp: BridgeClientInfoSenderTest.hpp VmgrTxRxTest.hpp
	touch $@

BridgeClientInfoSender.o: \
 $(ROOT_DIR)/castor/tape/tapebridge/BridgeClientInfoSender.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

$(ROOT_DIR)/castor/tape/tapebridge/BridgeClientInfoSender.cpp: \
  $(ROOT_DIR)/castor/tape/tapebridge/BridgeClientInfoSender.hpp
	touch $@

VmgrTxRx.o: $(ROOT_DIR)/castor/tape/tapebridge/VmgrTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

VmgrMarshal.o: $(ROOT_DIR)/castor/tape/legacymsg/VmgrMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_utils.o: $(ROOT_DIR)/castor/tape/utils/utils.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

SmartFd.o: $(ROOT_DIR)/castor/tape/utils/SmartFd.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

SmartFILEPtr.o: $(ROOT_DIR)/castor/tape/utils/SmartFILEPtr.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cuuid.o: $(ROOT_DIR)/common/Cuuid.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cgrp.o: $(ROOT_DIR)/common/Cgrp.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cthread.o: $(ROOT_DIR)/common/Cthread.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cgetopt.o: $(ROOT_DIR)/common/Cgetopt.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

getconfent.o: $(ROOT_DIR)/common/getconfent.c
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

exception_InvalidConfigEntry.o: $(ROOT_DIR)/castor/exception/InvalidConfigEntry.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

exception_InvalidConfiguration.o: $(ROOT_DIR)/castor/exception/InvalidConfiguration.cpp
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

AbstractSocket.o: $(ROOT_DIR)/castor/io/AbstractSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

AbstractTCPSocket.o: $(ROOT_DIR)/castor/io/AbstractTCPSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ClientSocket.o: $(ROOT_DIR)/castor/io/ClientSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ServerSocket.o: $(ROOT_DIR)/castor/io/ServerSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

UDPSocket.o: $(ROOT_DIR)/castor/io/UDPSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

socket_timeout.o: $(ROOT_DIR)/common/socket_timeout.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cglobals.o: $(ROOT_DIR)/common/Cglobals.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

socket.o: $(ROOT_DIR)/common/socket.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

serror.o: $(ROOT_DIR)/common/serror.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

BaseAddress.o: $(ROOT_DIR)/castor/BaseAddress.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

BaseObject.o: $(ROOT_DIR)/castor/BaseObject.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cmutex.o: $(ROOT_DIR)/common/Cmutex.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Services.o: $(ROOT_DIR)/castor/Services.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Factories.o: $(ROOT_DIR)/castor/Factories.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cdlopen.o: $(ROOT_DIR)/common/Cdlopen.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Csnprintf.o: $(ROOT_DIR)/common/Csnprintf.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cnetdb.o: $(ROOT_DIR)/common/Cnetdb.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

StreamAddress.o: $(ROOT_DIR)/castor/io/StreamAddress.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ObjectSet.o: $(ROOT_DIR)/castor/ObjectSet.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_net.o: $(ROOT_DIR)/castor/tape/net/net.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_CommonMarshal.o: $(ROOT_DIR)/castor/tape/legacymsg/CommonMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_RtcpMarshal.o: $(ROOT_DIR)/castor/tape/legacymsg/RtcpMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Dlf.o: $(ROOT_DIR)/castor/dlf/Dlf.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

dlf_lib.o: $(ROOT_DIR)/dlf/dlf_lib.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Constants.o: $(ROOT_DIR)/castor/Constants.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_marshall.o: $(ROOT_DIR)/tapebridge/tapebridge_marshall.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

$(ROOT_DIR)/tapebridge/tapebridge_marshall.c: \
  $(ROOT_DIR)/h/tapebridge_marshall.h
	touch $@

tapebridge_sendTapeBridgeAck.o: \
  $(ROOT_DIR)/tapebridge/tapebridge_sendTapeBridgeAck.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

$(ROOT_DIR)/tapebridge/tapebridge_sendTapeBridgeAck.c: \
  $(ROOT_DIR)/h/tapebridge_sendTapeBridgeAck.h
	touch $@

marshall.o: $(ROOT_DIR)/common/marshall.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

test_exception.o: test_exception.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

test_exception.cpp: test_exception.hpp
	touch $@

rtcpd_SendAckToVdqmOrTapeBridge.o: \
  $(ROOT_DIR)/rtcopy/rtcpd_SendAckToVdqmOrTapeBridge.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

$(ROOT_DIR)/rtcopy/rtcpd_SendAckToVdqmOrTapeBridge.c: \
  $(ROOT_DIR)/h/rtcpd_SendAckToVdqmOrTapeBridge.h
	touch $@

vdqmapi.o: $(ROOT_DIR)/vdqm/vdqmapi.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

vdqm_SendRecv.o: $(ROOT_DIR)/vdqm/vdqm_SendRecv.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

log.o: $(ROOT_DIR)/common/log.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

remote.o: $(ROOT_DIR)/common/remote.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

trace.o: $(ROOT_DIR)/common/trace.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

Cpwd.o: $(ROOT_DIR)/common/Cpwd.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_GetClientInfoMsg.o: \
  $(ROOT_DIR)/rtcopy/rtcpd_GetClientInfoMsg.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

$(ROOT_DIR)/rtcopy/rtcpd_GetClientInfoMsg.c: \
  $(ROOT_DIR)/h/rtcpd_GetClientInfoMsg.h
	touch $@

rtcp_marshallVdqmClientInfoMsg.o: \
  $(ROOT_DIR)/rtcopy/rtcp_marshallVdqmClientInfoMsg.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

$(ROOT_DIR)/rtcopy/rtcp_marshallVdqmClientInfoMsg.c: \
  $(ROOT_DIR)/h/rtcp_marshallVdqmClientInfoMsg.h
	touch $@

rtcp_recvRtcpHdr.o: $(ROOT_DIR)/rtcopy/rtcp_recvRtcpHdr.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

$(ROOT_DIR)/rtcopy/rtcp_recvRtcpHdr.c: \
  $(ROOT_DIR)/h/rtcp_recvRtcpHdr.h
	touch $@

rtcpd_GetClientInfo.o: $(ROOT_DIR)/rtcopy/rtcpd_GetClientInfo.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -DNOMORETAPES=\"/var/run/nomoretapes\" -o $@ $^

rtcp_Listen.o: $(ROOT_DIR)/rtcopy/rtcp_Listen.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_log.o: $(ROOT_DIR)/rtcopy/rtcp_log.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_SendRecv.o: $(ROOT_DIR)/rtcopy/rtcp_SendRecv.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridgedaemontester.o: tapebridgedaemontester.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

RemoteCopyConnection.o: $(ROOT_DIR)/castor/vdqm/RemoteCopyConnection.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

TapeBridgeMain.o: $(ROOT_DIR)/castor/tape/tapebridge/TapeBridgeMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

TapeBridgeDaemon.o: $(ROOT_DIR)/castor/tape/tapebridge/TapeBridgeDaemon.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

bridge_BoolFunctor.o: $(ROOT_DIR)/castor/tape/tapebridge/BoolFunctor.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

BridgeProtocolEngine.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/BridgeProtocolEngine.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

BridgeSocketCatalogue.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/BridgeSocketCatalogue.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

bridge_ClientTxRx.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/ClientTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

bridge_DlfMessageStrings.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/DlfMessageStrings.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

bridge_LegacyTxRx.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/LegacyTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

bridge_LogHelper.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/LogHelper.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

VdqmRequestHandler.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/VdqmRequestHandler.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

dlf_Param.o: \
  $(ROOT_DIR)/castor/dlf/Param.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

RtcpTxRx.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/RtcpTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_FileToRecallRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileToRecallRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_FileToRecall.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileToRecall.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_FileRecalledNotification.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileRecalledNotification.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_VolumeRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/VolumeRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_Volume.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/Volume.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_GatewayMessage.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/GatewayMessage.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_FileToMigrateRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileToMigrateRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_FileToMigrate.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileToMigrate.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_FileMigratedNotification.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileMigratedNotification.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_NoMoreFiles.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/NoMoreFiles.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_BaseFileInfo.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/BaseFileInfo.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_ClientType.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/ClientType.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_VolumeMode.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/VolumeMode.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_DumpParametersRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/DumpParametersRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_DumpParameters.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/DumpParameters.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_DumpNotification.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/DumpNotification.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_PositionCommandCode.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/PositionCommandCode.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_PingNotification.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/PingNotification.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_EndNotification.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/EndNotification.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_EndNotificationErrorReport.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/EndNotificationErrorReport.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

gateway_NotificationAcknowledge.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/NotificationAcknowledge.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

server_TCPListenerThreadPool.o: \
  $(ROOT_DIR)/castor/server/TCPListenerThreadPool.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

server_UDPListenerThreadPool.o: \
  $(ROOT_DIR)/castor/server/UDPListenerThreadPool.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

server_ListenerThreadPool.o: \
  $(ROOT_DIR)/castor/server/ListenerThreadPool.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

server_DynamicThreadPool.o: \
  $(ROOT_DIR)/castor/server/DynamicThreadPool.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

server_SignalThreadPool.o: \
  $(ROOT_DIR)/castor/server/SignalThreadPool.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

server_BaseThreadPool.o: \
  $(ROOT_DIR)/castor/server/BaseThreadPool.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

server_BaseDaemon.o: \
  $(ROOT_DIR)/castor/server/BaseDaemon.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

server_BaseServer.o: \
  $(ROOT_DIR)/castor/server/BaseServer.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

server_Queue.o: \
  $(ROOT_DIR)/castor/server/Queue.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

server_Mutex.o: \
  $(ROOT_DIR)/castor/server/Mutex.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

server_ThreadNotification.o: \
  $(ROOT_DIR)/castor/server/ThreadNotification.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

server_NotifierThread.o: \
  $(ROOT_DIR)/castor/server/NotifierThread.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

metrics_MetricsCollector.o: \
  $(ROOT_DIR)/castor/metrics/MetricsCollector.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

metrics_Histogram.o: \
  $(ROOT_DIR)/castor/metrics/Histogram.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

metrics_Counter.o: \
  $(ROOT_DIR)/castor/metrics/Counter.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

metrics_InternalCounter.o: \
  $(ROOT_DIR)/castor/metrics/InternalCounter.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

metrics_UpdateThread.o: \
  $(ROOT_DIR)/castor/metrics/UpdateThread.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_System.o: $(ROOT_DIR)/castor/System.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -DSTAGERSUPERUSER=\"stage\" -DSTAGERSUPERGROUP=\"st\" -o $@ $^

io_StreamCnvSvc.o: $(ROOT_DIR)/castor/io/StreamCnvSvc.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamVolumeRequestCnv.o: $(ROOT_DIR)/castor/io/StreamVolumeRequestCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamVolumeCnv.o:$(ROOT_DIR)/castor/io/StreamVolumeCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamBaseCnv.o: $(ROOT_DIR)/castor/io/StreamBaseCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_BaseCnvSvc.o: $(ROOT_DIR)/castor/BaseCnvSvc.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_Converters.o: $(ROOT_DIR)/castor/Converters.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_BaseSvc.o: $(ROOT_DIR)/castor/BaseSvc.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_ObjectCatalog.o: $(ROOT_DIR)/castor/ObjectCatalog.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^
