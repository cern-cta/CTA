UNAME := $(shell uname)

ifeq ($(UNAME), Darwin)
UUID_LIB=
MACPORTS_CPPUNIT_INCLUDE_OP=-I/opt/local/include
MACPORTS_CPPUNIT_LIB_OP=-L/opt/local/lib
else
UUID_LIB=-luuid
MACPORTS_CPPUNIT_INCLUDE_OP=
MACPORTS_CPPUNIT_LIB_OP=
endif

ROOT_DIR=../../../..
INCLUDE_OPS=-I$(ROOT_DIR) -I$(ROOT_DIR)/h $(MACPORTS_CPPUNIT_INCLUDE_OP)
LIB_DIR_OPS=$(MACPORTS_CPPUNIT_LIB_OP)
EXECUTABLES=tapebridgedaemontester tapebridged
COMMON_OPS=-Werror -Wall -pedantic -Wno-long-long -g -pthread

all: $(EXECUTABLES)

clean:
	rm -f $(EXECUTABLES) *.o

tapebridgedaemontester: \
  tapebridgedaemontester.o \
  exception_Communication.o \
  exception_Exception.o \
  exception_Internal.o \
  exception_InvalidArgument.o \
  exception_InvalidConfigEntry.o \
  exception_InvalidConfiguration.o \
  exception_NoEntry.o \
  exception_NoPortInRange.o \
  exception_NoValue.o \
  exception_OutOfMemory.o \
  exception_PermissionDenied.o \
  exception_TapeNetAcceptInterrupted.o \
  exception_TimeOut.o \
  exception_TooBig.o \
  tapegateway_BaseFileInfo.o \
  tapegateway_BaseFileInfoStruct.o \
  tapegateway_ClientType.o \
  tapegateway_DumpNotification.o \
  tapegateway_DumpParameters.o \
  tapegateway_DumpParametersRequest.o \
  tapegateway_EndNotification.o \
  tapegateway_EndNotificationErrorReport.o \
  tapegateway_EndNotificationFileErrorReport.o \
  tapegateway_FilesListRequest.o \
  tapegateway_FileErrorReportStruct.o \
  tapegateway_FileMigratedNotificationStruct.o \
  tapegateway_FileMigrationReportList.o \
  tapegateway_FileRecalledNotificationStruct.o \
  tapegateway_FileRecallReportList.o \
  tapegateway_FilesToMigrateList.o \
  tapegateway_FilesToMigrateListRequest.o \
  tapegateway_FilesToRecallList.o \
  tapegateway_FilesToRecallListRequest.o \
  tapegateway_FileToMigrateStruct.o \
  tapegateway_FileToRecallStruct.o \
  tapegateway_GatewayMessage.o \
  tapegateway_PositionCommandCode.o \
  tapegateway_PingNotification.o \
  tapegateway_NoMoreFiles.o \
  tapegateway_NotificationAcknowledge.o \
  tapegateway_Volume.o \
  tapegateway_VolumeMode.o \
  tapegateway_VolumeRequest.o \
  io_StreamBaseCnv.o \
  io_StreamCnvSvc.o \
  io_StreamEndNotificationCnv.o \
  io_StreamEndNotificationErrorReportCnv.o \
  io_StreamEndNotificationFileErrorReportCnv.o \
  io_StreamFilesListRequestCnv.o \
  io_StreamFileErrorReportStructCnv.o \
  io_StreamFileMigratedNotificationStructCnv.o \
  io_StreamFileMigrationReportListCnv.o \
  io_StreamFileRecalledNotificationStructCnv.o \
  io_StreamFileRecallReportListCnv.o \
  io_StreamFilesToMigrateListCnv.o \
  io_StreamFilesToMigrateListRequestCnv.o \
  io_StreamFilesToRecallListCnv.o \
  io_StreamFilesToRecallListRequestCnv.o \
  io_StreamFileToMigrateStructCnv.o \
  io_StreamFileToRecallStructCnv.o \
  io_StreamNoMoreFilesCnv.o \
  io_StreamNotificationAcknowledgeCnv.o \
  io_StreamPtrCnv.o \
  io_StreamVolumeRequestCnv.o \
  io_StreamVolumeCnv.o \
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
  tapebridge_LegacyTxRx.o \
  tapebridge_marshall.o \
  tapebridge_sendTapeBridgeClientInfo2Ack.o \
  tapebridge_sendTapeBridgeFlushedToTape.o \
  tapebridge_SystemFileCloser.o \
  tapebridge_tapeFlushModeToStr.o \
  legacymsg_CommonMarshal.o \
  legacymsg_RtcpMarshal.o \
  legacymsg_TapeBridgeMarshal.o \
  legacymsg_VmgrMarshal.o \
  tape_net.o \
  tape_utils.o \
  vdqm_SendRecv.o \
  marshall.o \
  utils_SmartFd.o \
  utils_SmartFILEPtr.o \
  vdqmapi.o \
  common_Cpwd.o \
  common_Cuuid.o \
  common_getconfent.o \
  common_log.o \
  common_remote.o \
  common_trace.o \
  common_u64subr.o \
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
  Dlf.o \
  dlf_lib.o \
  Csnprintf.o \
  Constants.o \
  test_exception.o
	g++ $(COMMON_OPS) $(LIB_DIR_OPS) -lpthread $(UUID_LIB) -lcppunit -o $@ $^

tapebridged: \
  castor_BaseCnvSvc.o \
  castor_Converters.o \
  castor_BaseSvc.o \
  castor_ObjectCatalog.o \
  common_Cgetopt.o \
  common_Cgrp.o \
  common_Cpwd.o \
  common_Cuuid.o \
  common_Cthread.o \
  common_getconfent.o \
  common_log.o \
  common_remote.o \
  common_trace.o \
  common_u64subr.o \
  dlf_Param.o \
  exception_Communication.o \
  exception_Exception.o \
  exception_Internal.o \
  exception_InvalidArgument.o \
  exception_InvalidConfigEntry.o \
  exception_InvalidConfiguration.o \
  exception_NoEntry.o \
  exception_NoPortInRange.o \
  exception_NoValue.o \
  exception_OutOfMemory.o \
  exception_PermissionDenied.o \
  exception_TapeNetAcceptInterrupted.o \
  exception_TimeOut.o \
  exception_TooBig.o \
  io_StreamBaseCnv.o \
  io_StreamCnvSvc.o \
  io_StreamEndNotificationCnv.o \
  io_StreamEndNotificationErrorReportCnv.o \
  io_StreamEndNotificationFileErrorReportCnv.o \
  io_StreamFilesListRequestCnv.o \
  io_StreamFileErrorReportStructCnv.o \
  io_StreamFileMigratedNotificationStructCnv.o \
  io_StreamFileMigrationReportListCnv.o \
  io_StreamFileRecalledNotificationStructCnv.o \
  io_StreamFileRecallReportListCnv.o \
  io_StreamFilesToMigrateListCnv.o \
  io_StreamFilesToMigrateListRequestCnv.o \
  io_StreamFilesToRecallListCnv.o \
  io_StreamFilesToRecallListRequestCnv.o \
  io_StreamFileToMigrateStructCnv.o \
  io_StreamFileToRecallStructCnv.o \
  io_StreamNoMoreFilesCnv.o \
  io_StreamNotificationAcknowledgeCnv.o \
  io_StreamPtrCnv.o \
  io_StreamVolumeRequestCnv.o \
  io_StreamVolumeCnv.o \
  legacymsg_CommonMarshal.o \
  legacymsg_RtcpMarshal.o \
  legacymsg_TapeBridgeMarshal.o \
  legacymsg_VmgrMarshal.o \
  metrics_MetricsCollector.o \
  metrics_Histogram.o \
  metrics_Counter.o \
  metrics_InternalCounter.o \
  metrics_UpdateThread.o \
  rtcp_marshallVdqmClientInfoMsg.o \
  rtcp_recvRtcpHdr.o \
  rtcp_Listen.o \
  rtcp_log.o \
  rtcp_SendRecv.o \
  rtcpd_GetClientInfo.o \
  rtcpd_GetClientInfoMsg.o \
  rtcpd_SendAckToVdqmOrTapeBridge.o \
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
  tape_utils.o \
  tapebridge_sendTapeBridgeClientInfo2Ack.o \
  tapebridge_BridgeClientInfo2Sender.o \
  tapebridge_BridgeProtocolEngine.o \
  tapebridge_BridgeSocketCatalogue.o \
  tapebridge_BulkRequestConfigParams.o \
  tapebridge_ClientTxRx.o \
  tapebridge_ConfigParams.o \
  tapebridge_DlfMessageStrings.o \
  tapebridge_FileToMigrate.o \
  tapebridge_FileToRecall.o \
  tapebridge_GetMoreWorkConnection.o \
  tapebridge_LegacyTxRx.o \
  tapebridge_LogHelper.o \
  tapebridge_marshall.o \
  tapebridge_MigrationReportConnection.o \
  tapebridge_PendingMigrationsStore.o \
  tapebridge_RtcpTxRx.o \
  tapebridge_SessionError.o \
  tapebridge_SystemFileCloser.o \
  tapebridge_TapeBridgeMain.o \
  tapebridge_TapeBridgeDaemon.o \
  tapebridge_TapeFlushConfigParams.o \
  tapebridge_tapeFlushModeToStr.o \
  tapebridge_VdqmRequestHandler.o \
  tapebridge_VmgrTxRx.o \
  tapegateway_BaseFileInfo.o \
  tapegateway_BaseFileInfoStruct.o \
  tapegateway_ClientType.o \
  tapegateway_DumpNotification.o \
  tapegateway_DumpParameters.o \
  tapegateway_DumpParametersRequest.o \
  tapegateway_EndNotification.o \
  tapegateway_EndNotificationErrorReport.o \
  tapegateway_EndNotificationFileErrorReport.o \
  tapegateway_FileErrorReportStruct.o \
  tapegateway_FileMigratedNotificationStruct.o \
  tapegateway_FileMigrationReportList.o \
  tapegateway_FileRecalledNotificationStruct.o \
  tapegateway_FileRecallReportList.o \
  tapegateway_FilesListRequest.o \
  tapegateway_FilesToMigrateList.o \
  tapegateway_FilesToMigrateListRequest.o \
  tapegateway_FilesToRecallList.o \
  tapegateway_FilesToRecallListRequest.o \
  tapegateway_FileToMigrateStruct.o \
  tapegateway_FileToRecallStruct.o \
  tapegateway_GatewayMessage.o \
  tapegateway_PositionCommandCode.o \
  tapegateway_PingNotification.o \
  tapegateway_NoMoreFiles.o \
  tapegateway_NotificationAcknowledge.o \
  tapegateway_Volume.o \
  tapegateway_VolumeMode.o \
  tapegateway_VolumeRequest.o \
  utils_BoolFunctor.o \
  utils_SmartFd.o \
  utils_SmartFILEPtr.o \
  vdqm_SendRecv.o \
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
  ObjectSet.o \
  tape_net.o \
  Dlf.o \
  dlf_lib.o \
  Constants.o \
  marshall.o \
  vdqmapi.o \
  Cmutex.o \
  castor_System.o
	g++ $(COMMON_OPS) -lpthread $(UUID_LIB) -ldl -o $@ $^

tapebridge_BridgeClientInfo2Sender.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/BridgeClientInfo2Sender.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

$(ROOT_DIR)/castor/tape/tapebridge/BridgeClientInfo2Sender.cpp: \
  $(ROOT_DIR)/castor/tape/tapebridge/BridgeClientInfo2Sender.hpp
	touch $@

tapebridge_SessionError.o: $(ROOT_DIR)/castor/tape/tapebridge/SessionError.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_SystemFileCloser.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/SystemFileCloser.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_tapeFlushModeToStr.o: \
  $(ROOT_DIR)/tapebridge/tapebridge_tapeFlushModeToStr.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_VmgrTxRx.o: $(ROOT_DIR)/castor/tape/tapebridge/VmgrTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_CommonMarshal.o: $(ROOT_DIR)/castor/tape/legacymsg/CommonMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_RtcpMarshal.o: $(ROOT_DIR)/castor/tape/legacymsg/RtcpMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_TapeBridgeMarshal.o: \
  $(ROOT_DIR)/castor/tape/legacymsg/TapeBridgeMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_VmgrMarshal.o: $(ROOT_DIR)/castor/tape/legacymsg/VmgrMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_utils.o: $(ROOT_DIR)/castor/tape/utils/utils.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

utils_SmartFd.o: $(ROOT_DIR)/castor/tape/utils/SmartFd.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

utils_SmartFILEPtr.o: $(ROOT_DIR)/castor/tape/utils/SmartFILEPtr.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

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

exception_NoValue.o: $(ROOT_DIR)/castor/exception/NoValue.cpp
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

tapebridge_sendTapeBridgeClientInfo2Ack.o: \
  $(ROOT_DIR)/tapebridge/tapebridge_sendTapeBridgeClientInfo2Ack.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

$(ROOT_DIR)/tapebridge/tapebridge_sendTapeBridgeClientInfo2Ack.c: \
  $(ROOT_DIR)/h/tapebridge_sendTapeBridgeClientInfo2Ack.h
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

common_Cuuid.o: $(ROOT_DIR)/common/Cuuid.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cgetopt.o: $(ROOT_DIR)/common/Cgetopt.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cgrp.o: $(ROOT_DIR)/common/Cgrp.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cpwd.o: $(ROOT_DIR)/common/Cpwd.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cthread.o: $(ROOT_DIR)/common/Cthread.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_getconfent.o: $(ROOT_DIR)/common/getconfent.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_log.o: $(ROOT_DIR)/common/log.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_remote.o: $(ROOT_DIR)/common/remote.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_trace.o: $(ROOT_DIR)/common/trace.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_u64subr.o: $(ROOT_DIR)/common/u64subr.c
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

tapebridge_TapeBridgeMain.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/TapeBridgeMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_TapeBridgeDaemon.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/TapeBridgeDaemon.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

utils_BoolFunctor.o: $(ROOT_DIR)/castor/tape/utils/BoolFunctor.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_BridgeProtocolEngine.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/BridgeProtocolEngine.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_BridgeSocketCatalogue.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/BridgeSocketCatalogue.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_BulkRequestConfigParams.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/BulkRequestConfigParams.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_ClientTxRx.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/ClientTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_ConfigParams.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/ConfigParams.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_DlfMessageStrings.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/DlfMessageStrings.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_FileToMigrate.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/FileToMigrate.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_FileToRecall.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/FileToRecall.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_GetMoreWorkConnection.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/GetMoreWorkConnection.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_LegacyTxRx.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/LegacyTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_LogHelper.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/LogHelper.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_MigrationReportConnection.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/MigrationReportConnection.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_PendingMigrationsStore.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/PendingMigrationsStore.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_RtcpTxRx.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/RtcpTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_TapeFlushConfigParams.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/TapeFlushConfigParams.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_VdqmRequestHandler.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/VdqmRequestHandler.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_sendTapeBridgeFlushedToTape.o: \
  $(ROOT_DIR)/tapebridge/tapebridge_sendTapeBridgeFlushedToTape.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

dlf_Param.o: \
  $(ROOT_DIR)/castor/dlf/Param.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_BaseFileInfo.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/BaseFileInfo.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_BaseFileInfoStruct.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/BaseFileInfoStruct.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_ClientType.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/ClientType.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_DumpNotification.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/DumpNotification.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_DumpParameters.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/DumpParameters.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_DumpParametersRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/DumpParametersRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_EndNotification.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/EndNotification.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_EndNotificationErrorReport.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/EndNotificationErrorReport.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_EndNotificationFileErrorReport.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/EndNotificationFileErrorReport.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileErrorReportStruct.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileErrorReportStruct.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileMigratedNotificationStruct.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileMigratedNotificationStruct.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileMigrationReportList.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileMigrationReportList.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileRecalledNotificationStruct.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileRecalledNotificationStruct.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileRecallReportList.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileRecallReportList.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FilesListRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FilesListRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FilesToMigrateList.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FilesToMigrateList.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FilesToMigrateListRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FilesToMigrateListRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileToMigrateStruct.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileToMigrateStruct.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileToRecallStruct.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileToRecallStruct.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FilesToRecallList.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FilesToRecallList.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FilesToRecallListRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FilesToRecallListRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_GatewayMessage.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/GatewayMessage.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_NoMoreFiles.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/NoMoreFiles.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_NotificationAcknowledge.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/NotificationAcknowledge.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_PingNotification.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/PingNotification.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_PositionCommandCode.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/PositionCommandCode.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_Volume.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/Volume.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_VolumeMode.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/VolumeMode.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_VolumeRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/VolumeRequest.cpp
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

io_StreamEndNotificationCnv.o: \
  $(ROOT_DIR)/castor/io/StreamEndNotificationCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamEndNotificationErrorReportCnv.o: \
  $(ROOT_DIR)/castor/io/StreamEndNotificationErrorReportCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamEndNotificationFileErrorReportCnv.o: \
  $(ROOT_DIR)/castor/io/StreamEndNotificationFileErrorReportCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamFilesListRequestCnv.o: \
  $(ROOT_DIR)/castor/io/StreamFilesListRequestCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamFileErrorReportStructCnv.o: \
  $(ROOT_DIR)/castor/io/StreamFileErrorReportStructCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamFileMigratedNotificationStructCnv.o: \
  $(ROOT_DIR)/castor/io/StreamFileMigratedNotificationStructCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamFileMigrationReportListCnv.o: \
  $(ROOT_DIR)/castor/io/StreamFileMigrationReportListCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamFileRecalledNotificationStructCnv.o: \
  $(ROOT_DIR)/castor/io/StreamFileRecalledNotificationStructCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamFileRecallReportListCnv.o: \
  $(ROOT_DIR)/castor/io/StreamFileRecallReportListCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamFilesToMigrateListCnv.o: \
  $(ROOT_DIR)/castor/io/StreamFilesToMigrateListCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamFilesToMigrateListRequestCnv.o: \
  $(ROOT_DIR)/castor/io/StreamFilesToMigrateListRequestCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamFilesToRecallListCnv.o: \
  $(ROOT_DIR)/castor/io/StreamFilesToRecallListCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamFilesToRecallListRequestCnv.o: \
  $(ROOT_DIR)/castor/io/StreamFilesToRecallListRequestCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamFileToMigrateStructCnv.o: \
  $(ROOT_DIR)/castor/io/StreamFileToMigrateStructCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamFileToRecallStructCnv.o: \
  $(ROOT_DIR)/castor/io/StreamFileToRecallStructCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamNotificationAcknowledgeCnv.o: \
  $(ROOT_DIR)/castor/io/StreamNotificationAcknowledgeCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamNoMoreFilesCnv.o: \
  $(ROOT_DIR)/castor/io/StreamNoMoreFilesCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamPingNotificationCnv.o: \
  $(ROOT_DIR)/castor/io/StreamPingNotificationCnv.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamPtrCnv.o: $(ROOT_DIR)/castor/io/StreamPtrCnv.cpp
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
