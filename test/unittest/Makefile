UNAME := $(shell uname)

ifeq ($(UNAME), Darwin)
UUID_LIB=
else
UUID_LIB=-luuid
endif

ROOT_DIR=../..
INCLUDE_OPS=-I$(ROOT_DIR) -I$(ROOT_DIR)/h -I/opt/local/var/macports/software/cppunit/1.12.1_0/opt/local/include
LIB_DIR_OPS=-L/opt/local/var/macports/software/cppunit/1.12.1_0/opt/local/lib
EXECUTABLES=rununittests
CSEC_MECHLIST=KRB5 GSI ID
COMMON_OPS=-DCSEC_DEFAULT_MECHS="\"$(CSEC_MECHLIST)\"" -DSTAGERSUPERUSER=\"stage\" -DSTAGERSUPERGROUP=\"st\" -DSTAGEMAP=\"/etc/castor/stagemap.conf\" -DCNS_HOST=\"castorns\" -DSCNS_HOST=\"scnsdev\" -DCNSHOSTPFX=\"cns\" -DSCNSHOSTPFX=\"scns\" -DACCTFILE=\"/var/log/castor/sacct\" -DCNS_ROOT=\"/castor\" -DNOMORETAPES=\"/var/run/nomoretapes\" -DTAPELOGFILE=\"/var/log/castor/taped_legacy.log\" -DSCSIINC=\"scsi/sg.h\" -DNOMORERFIO=\"/var/run/nomorerfio\" -DRFIOLOGFILE=\"/var/log/castor/rfiod.log\" -D_LARGEFILE64_SOURCE -Werror -Wno-long-long -pedantic -Wall -g -pthread

all: $(EXECUTABLES)

clean:
	rm -f $(EXECUTABLES) *.o

rununittests: \
  runUnitTestsMain.o \
  castor_BaseAddress.o \
  client_BaseClient.o \
  castor_BaseObject.o \
  castor_Constants.o \
  castor_Factories.o \
  castor_ObjectSet.o \
  castor_Services.o \
  castor_System.o \
  castor_IClient.o \
  castor_MessageAck.o \
  client_VectorResponseHandler.o \
  common_Cdlopen.o \
  common_Cdomainname.o \
  common_Cgetopt.o \
  common_Cglobals.o \
  common_Cgrp.o \
  common_Cmutex.o \
  common_Cnetdb.o \
  common_Cpool.o \
  common_Cpwd.o \
  common_Csecloader.o \
  common_Csnprintf.o \
  common_Cthread.o \
  common_Cuuid.o \
  common_getacctent.o \
  common_getconfent.o \
  common_getifnam.o \
  common_log.o \
  common_marshall.o \
  common_serror.o \
  common_remote.o \
  common_setnetio.o \
  common_socket.o \
  common_socket_timeout.o \
  common_trace.o \
  common_u64subr.o \
  common_wsacct.o \
  dlf_Dlf.o \
  dlf_Param.o \
  dlf_dlf_lib.o \
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
  io_AbstractSocket.o \
  io_AbstractTCPSocket.o \
  io_AuthClientSocket.o \
  io_ClientSocket.o \
  io_ServerSocket.o \
  io_StreamAddress.o \
  legacymsg_CommonMarshal.o \
  legacymsg_RtcpMarshal.o \
  legacymsg_TapeBridgeMarshal.o \
  legacymsg_VmgrMarshal.o \
  ns_Cns_selectsrvr.o \
  ns_Cns_opendir.o \
  ns_Cns_closedir.o \
  ns_Cns_stat.o \
  ns_Cns_readdir.o \
  ns_Cns_readdirx.o \
  ns_Cns_rename.o \
  ns_Cns_rewinddir.o \
  ns_Cns_rmdir.o \
  ns_Cns_unlink.o \
  ns_Cns_access.o \
  ns_Cns_chdir.o \
  ns_Cns_chmod.o \
  ns_Cns_chown.o \
  ns_Cns_creat.o \
  ns_Cns_getcwd.o \
  ns_Cns_mkdir.o \
  ns_Cns_apiinit.o \
  ns_Cns_aborttrans.o \
  ns_Cns_auth.o \
  ns_send2nsd.o \
  ns_Cns_errmsg.o \
  ns_Cns_getpath.o \
  rfio_xyread.o \
  rfio_error.o \
  rfio_mstat.o \
  rfio_connect.o \
  rfio_open.o \
  rfio_open64.o \
  rfio_xyopen.o \
  rfio_close.o \
  rfio_xyclose.o \
  rfio_switch_req.o \
  rfio_read.o \
  rfio_read64.o \
  rfio_fio.o \
  rfio_lun2fn.o \
  rfio_write.o \
  rfio_write64.o \
  rfio_xywrite.o \
  rfio_rfio_rfilefdt.o \
  rfio_lseek.o \
  rfio_lseek64.o \
  rfio_stream.o \
  rfio_stream64.o \
  rfio_setopt.o \
  rfio_misc.o \
  rfio_parse.o \
  rfio_rfio_HsmIf.o \
  rfio_RfioTURL.o \
  rh_FileResponse.o \
  rh_IOResponse.o \
  rh_Response.o \
  rh_Client.o \
  rtcopy_client_rtcp_log.o \
  rtcopy_client_rtcp_SendRecv.o \
  rtcopy_rtcp_CallVMGR.o \
  rtcopy_rtcp_InitNW.o \
  rtcopy_rtcp_Listen.o \
  rtcopy_rtcp_marshallVdqmClientInfoMsg.o \
  rtcopy_rtcp_recvRtcpHdr.o \
  rtcopy_rtcp_RetvalSHIFT.o \
  rtcopy_rtcpc_BuildReq.o \
  rtcopy_rtcpd_ConvertData.o \
  rtcopy_rtcpd_GetClientInfo.o \
  rtcopy_rtcpd_GetClientInfoMsg.o \
  rtcopy_rtcpd_SendAckToVdqmOrTapeBridge.o \
  security_Csec_api_loader.o \
  security_Csec_api.o \
  security_Csec_errmsg.o \
  security_Csec_apiinit.o \
  security_Csec_common.o \
  security_Csec_protocol_policy.o \
  stager_mapper.o \
  stager_errmsg.o \
  stager_client_commandline.o \
  stager_client_api_common.o \
  stager_client_api_open.o \
  stager_client_api_get.o \
  stager_client_api_put.o \
  stager_client_api_update.o \
  stager_DiskServerStatusCode.o \
  stager_FileSystemStatusCodes.o \
  stager_StagePutRequest.o \
  stager_StagePutDoneRequest.o \
  stager_StagePrepareToPutRequest.o \
  stager_StagePrepareToUpdateRequest.o \
  stager_StageGetRequest.o \
  stager_StagePrepareToGetRequest.o \
  stager_FileRequest.o \
  stager_Request.o \
  stager_SubRequest.o \
  stager_SubRequestStatusCodes.o \
  stager_SubRequestGetNextStatusCodes.o \
  stager_StageUpdateRequest.o \
  tape_asc2ebc.o \
  tape_chkdirw.o \
  tape_Ctape_kill.o \
  tape_Ctape_errmsg.o \
  tape_Ctape_mount.o \
  tape_Ctape_position.o \
  tape_Ctape_info.o \
  tape_Ctape_devinfo.o \
  tape_Ctape_drvinfo.o \
  tape_Ctape_rls.o \
  tape_Ctape_reserve.o \
  tape_cvtden.o \
  tape_ebc2asc.o \
  tape_findpgrp.o \
  tape_initlabel.o \
  tape_net.o \
  tape_send2tpd.o \
  tape_usrmsg.o \
  tape_utils.o \
  tapebridge_BoolFunctor.o \
  tapebridge_BridgeClientInfo2Sender.o \
  tapebridge_BridgeProtocolEngine.o \
  tapebridge_BridgeSocketCatalogue.o \
  tapebridge_ClientTxRx.o \
  tapebridge_DlfMessageStrings.o \
  tapebridge_FailedToCopyTapeFile.o \
  tapebridge_LegacyTxRx.o \
  tapebridge_LogHelper.o \
  tapebridge_marshall.o \
  tapebridge_PendingMigrationsStore.o \
  tapebridge_recvTapeBridgeFlushedToTapeAck.o \
  tapebridge_RtcpTxRx.o \
  tapebridge_sendTapeBridgeClientInfo2Ack.o \
  tapebridge_sendTapeBridgeFlushedToTape.o \
  tapebridge_TapeFlushConfigParams.o \
  tapebridge_tapeFlushModeToStr.o \
  tapebridge_VmgrTxRx.o \
  tapegateway_FileToMigrateRequest.o \
  tapegateway_FileToMigrate.o \
  tapegateway_FileMigratedNotification.o \
  tapegateway_FileToRecallRequest.o \
  tapegateway_FileToRecall.o \
  tapegateway_FileRecalledNotification.o \
  tapegateway_BaseFileInfo.o \
  tapegateway_VolumeRequest.o \
  tapegateway_Volume.o \
  tapegateway_GatewayMessage.o \
  tapegateway_PositionCommandCode.o \
  tapegateway_PingNotification.o \
  tapegateway_DumpParametersRequest.o \
  tapegateway_DumpParameters.o \
  tapegateway_DumpNotification.o \
  tapegateway_FileToMigrate.o \
  tapegateway_NoMoreFiles.o \
  tapegateway_EndNotification.o \
  tapegateway_EndNotificationErrorReport.o \
  tapegateway_EndNotificationFileErrorReport.o \
  tapegateway_NotificationAcknowledge.o \
  tapegateway_ClientType.o \
  tapegateway_VolumeMode.o \
  test_exception.o \
  upv_Cupv_check.o \
  upv_send2Cupv.o \
  upv_Cupv_errmsg.o \
  upv_Cupv_apiinit.o \
  utils_SmartFd.o \
  utils_SmartFILEPtr.o \
  vdqm_vdqmapi.o \
  vdqm_vdqm_SendRecv.o \
  vmgr_vmgrcheck.o \
  vmgr_vmgr_errmsg.o \
  vmgr_vmgr_apiinit.o \
  vmgr_vmgr_querypool.o \
  vmgr_send2vmgr.o \
  vmgr_vmgr_querytape_byte_u64.o
	g++ $(COMMON_OPS) $(LIB_DIR_OPS) -lpthread $(UUID_LIB) -lcppunit -lz -o $@ $^

runUnitTestsMain.o: runUnitTestsMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

runUnitTestsMain.cpp: \
  castor/tape/tapebridge/BridgeClientInfo2SenderTest.hpp \
  castor/tape/tapebridge/TapeFlushConfigParamsTest.hpp \
  castor/tape/tapebridge/PendingMigrationsStoreTest.hpp \
  castor/tape/tapebridge/VmgrTxRxTest.hpp \
  castor/tape/utils/IndexedContainerTest.hpp \
  castor/tape/utils/UtilsTest.hpp \
  rtcopy/RtcopyTest.hpp \
  tapebridge/MarshallTapeBridgeClientInfo2Test.hpp \
  tapebridge/MarshallTapeBridgeFlushedToTapeTest.hpp \
  tapebridge/RecvTapeBridgeFlushedToTapeAckTest.hpp \
  tapebridge/SendTapeBridgeFlushedToTapeTest.hpp
	touch $@

castor_Services.o: $(ROOT_DIR)/castor/Services.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_MessageAck.o: $(ROOT_DIR)/castor/MessageAck.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_IClient.o: $(ROOT_DIR)/castor/IClient.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

castor_System.o: $(ROOT_DIR)/castor/System.cpp
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

client_VectorResponseHandler.o: $(ROOT_DIR)/castor/client/VectorResponseHandler.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

client_BaseClient.o: $(ROOT_DIR)/castor/client/BaseClient.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Csecloader.o: $(ROOT_DIR)/common/Csecloader.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

common_getifnam.o: $(ROOT_DIR)/common/getifnam.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cdomainname.o: $(ROOT_DIR)/common/Cdomainname.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cdlopen.o: $(ROOT_DIR)/common/Cdlopen.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cgrp.o: $(ROOT_DIR)/common/Cgrp.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_setnetio.o: $(ROOT_DIR)/common/setnetio.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cpool.o: $(ROOT_DIR)/common/Cpool.c
	gcc -pthread -g -c $(INCLUDE_OPS) -o $@ $^

common_getacctent.o: $(ROOT_DIR)/common/getacctent.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_socket.o: $(ROOT_DIR)/common/socket.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_socket_timeout.o: $(ROOT_DIR)/common/socket_timeout.c
	gcc -pthread -g -c $(INCLUDE_OPS) -o $@ $^

common_serror.o: $(ROOT_DIR)/common/serror.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cglobals.o: $(ROOT_DIR)/common/Cglobals.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cnetdb.o: $(ROOT_DIR)/common/Cnetdb.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_getconfent.o: $(ROOT_DIR)/common/getconfent.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cgetopt.o: $(ROOT_DIR)/common/Cgetopt.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cmutex.o: $(ROOT_DIR)/common/Cmutex.c
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

common_u64subr.o: $(ROOT_DIR)/common/u64subr.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

common_wsacct.o: $(ROOT_DIR)/common/wsacct.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

dlf_Dlf.o: $(ROOT_DIR)/castor/dlf/Dlf.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

dlf_Param.o: $(ROOT_DIR)/castor/dlf/Param.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

dlf_dlf_lib.o: $(ROOT_DIR)/dlf/dlf_lib.c
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

io_AbstractSocket.o: $(ROOT_DIR)/castor/io/AbstractSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_AbstractTCPSocket.o: $(ROOT_DIR)/castor/io/AbstractTCPSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_AuthClientSocket.o: $(ROOT_DIR)/castor/io/AuthClientSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_ClientSocket.o: $(ROOT_DIR)/castor/io/ClientSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_ServerSocket.o: $(ROOT_DIR)/castor/io/ServerSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamAddress.o: $(ROOT_DIR)/castor/io/StreamAddress.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_CommonMarshal.o: \
  $(ROOT_DIR)/castor/tape/legacymsg/CommonMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_RtcpMarshal.o: $(ROOT_DIR)/castor/tape/legacymsg/RtcpMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_TapeBridgeMarshal.o: \
  $(ROOT_DIR)/castor/tape/legacymsg/TapeBridgeMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

legacymsg_VmgrMarshal.o: $(ROOT_DIR)/castor/tape/legacymsg/VmgrMarshal.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_selectsrvr.o: $(ROOT_DIR)/ns/Cns_selectsrvr.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_opendir.o: $(ROOT_DIR)/ns/Cns_opendir.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_closedir.o: $(ROOT_DIR)/ns/Cns_closedir.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_stat.o: $(ROOT_DIR)/ns/Cns_stat.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_readdir.o: $(ROOT_DIR)/ns/Cns_readdir.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_readdirx.o: $(ROOT_DIR)/ns/Cns_readdirx.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_rename.o: $(ROOT_DIR)/ns/Cns_rename.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_rewinddir.o: $(ROOT_DIR)/ns/Cns_rewinddir.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_rmdir.o: $(ROOT_DIR)/ns/Cns_rmdir.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_unlink.o: $(ROOT_DIR)/ns/Cns_unlink.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_access.o: $(ROOT_DIR)/ns/Cns_access.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_chdir.o: $(ROOT_DIR)/ns/Cns_chdir.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_chmod.o: $(ROOT_DIR)/ns/Cns_chmod.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_chown.o: $(ROOT_DIR)/ns/Cns_chown.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_creat.o: $(ROOT_DIR)/ns/Cns_creat.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_getcwd.o: $(ROOT_DIR)/ns/Cns_getcwd.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_mkdir.o: $(ROOT_DIR)/ns/Cns_mkdir.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_apiinit.o: $(ROOT_DIR)/ns/Cns_apiinit.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_aborttrans.o: $(ROOT_DIR)/ns/Cns_aborttrans.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_auth.o: $(ROOT_DIR)/ns/Cns_auth.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_send2nsd.o: $(ROOT_DIR)/ns/send2nsd.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_errmsg.o: $(ROOT_DIR)/ns/Cns_errmsg.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

ns_Cns_getpath.o: $(ROOT_DIR)/ns/Cns_getpath.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_misc.o: $(ROOT_DIR)/rfio/misc.c
	gcc -g -pthread -DCNS_ROOT=\"/castor\" -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_parse.o: $(ROOT_DIR)/rfio/parse.c
	gcc -g -pthread -DCNS_ROOT=\"/castor\" -c $(INCLUDE_OPS) -o $@ $^

rfio_RfioTURL.o: $(ROOT_DIR)/rfio/RfioTURL.c
	gcc -g -pthread -DCNS_ROOT=\"/castor\" -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_rfio_HsmIf.o: $(ROOT_DIR)/rfio/rfio_HsmIf.c
	gcc -g -pthread -DCNS_ROOT=\"/castor\" -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_setopt.o: $(ROOT_DIR)/rfio/setopt.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_switch_req.o: $(ROOT_DIR)/rfio/switch_req.c
	gcc -g -pthread -DCNS_ROOT=\"/castor\" -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_xyread.o: $(ROOT_DIR)/rfio/xyread.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_error.o: $(ROOT_DIR)/rfio/error.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_mstat.o: $(ROOT_DIR)/rfio/mstat.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_lseek.o: $(ROOT_DIR)/rfio/lseek.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_lseek64.o: $(ROOT_DIR)/rfio/lseek64.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_stream.o: $(ROOT_DIR)/rfio/stream.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -DNOMORERFIO=\"/var/run/nomorerfio\" -c $(INCLUDE_OPS) -o $@ $^

rfio_stream64.o: $(ROOT_DIR)/rfio/stream64.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_connect.o: $(ROOT_DIR)/rfio/connect.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_open.o: $(ROOT_DIR)/rfio/open.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rfio_open64.o: $(ROOT_DIR)/rfio/open64.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_xyopen.o: $(ROOT_DIR)/rfio/xyopen.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_close.o: $(ROOT_DIR)/rfio/close.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rfio_xyclose.o: $(ROOT_DIR)/rfio/xyclose.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_read.o: $(ROOT_DIR)/rfio/read.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rfio_read64.o: $(ROOT_DIR)/rfio/read64.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_fio.o: $(ROOT_DIR)/rfio/fio.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_lun2fn.o: $(ROOT_DIR)/rfio/lun2fn.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_write.o: $(ROOT_DIR)/rfio/write.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rfio_write64.o: $(ROOT_DIR)/rfio/write64.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rfio_xywrite.o: $(ROOT_DIR)/rfio/xywrite.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rfio_rfio_rfilefdt.o: $(ROOT_DIR)/rfio/rfio_rfilefdt.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rh_Client.o: $(ROOT_DIR)/castor/rh/Client.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rh_FileResponse.o: $(ROOT_DIR)/castor/rh/FileResponse.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rh_IOResponse.o: $(ROOT_DIR)/castor/rh/IOResponse.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rh_Response.o: $(ROOT_DIR)/castor/rh/Response.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpapi.o: $(ROOT_DIR)/rtcopy/rtcpapi.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcp_accounting.o: $(ROOT_DIR)/rtcopy/rtcp_accounting.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcp_RetvalSHIFT.o: $(ROOT_DIR)/rtcopy/rtcp_RetvalSHIFT.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcp_InitNW.o: $(ROOT_DIR)/rtcopy/rtcp_InitNW.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_Disk.o: $(ROOT_DIR)/rtcopy/rtcpd_Disk.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_tpdump.o: $(ROOT_DIR)/rtcopy/rtcpd_tpdump.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_SendAckToVdqmOrTapeBridge.o: \
  $(ROOT_DIR)/rtcopy/rtcpd_SendAckToVdqmOrTapeBridge.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_SelfMonitor.o: $(ROOT_DIR)/rtcopy/rtcpd_SelfMonitor.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_Tape.o: $(ROOT_DIR)/rtcopy/rtcpd_Tape.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_TapeIO.o: $(ROOT_DIR)/rtcopy/rtcpd_TapeIO.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_GetClientInfoMsg.o: $(ROOT_DIR)/rtcopy/rtcpd_GetClientInfoMsg.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_Ctape.o: $(ROOT_DIR)/rtcopy/rtcpd_Ctape.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_ClientListen.o: $(ROOT_DIR)/rtcopy/rtcpd_ClientListen.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_GetClientInfo.o: $(ROOT_DIR)/rtcopy/rtcpd_GetClientInfo.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcp_marshallVdqmClientInfoMsg.o: \
  $(ROOT_DIR)/rtcopy/rtcp_marshallVdqmClientInfoMsg.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcp_CallVMGR.o: $(ROOT_DIR)/rtcopy/rtcp_CallVMGR.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcp_CheckReq.o: $(ROOT_DIR)/rtcopy/rtcp_CheckReq.c
	gcc $(COMMON_OPS) -DRTCP_SERVER -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcp_Listen.o: $(ROOT_DIR)/rtcopy/rtcp_Listen.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcp_recvRtcpHdr.o: $(ROOT_DIR)/rtcopy/rtcp_recvRtcpHdr.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpc_BuildReq.o: $(ROOT_DIR)/rtcopy/rtcpc_BuildReq.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_tapeIOthread.o: $(ROOT_DIR)/rtcopy/rtcpd_tapeIOthread.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_SignalFilePositioned.o: \
  $(ROOT_DIR)/rtcopy/rtcpd_SignalFilePositioned.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_rtcpd_MainCntl.o: $(ROOT_DIR)/rtcopy/rtcpd_MainCntl.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_client_rtcp_log.o: $(ROOT_DIR)/rtcopy/rtcp_log.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_server_rtcp_log.o: $(ROOT_DIR)/rtcopy/rtcp_log.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -DRTCP_SERVER -o $@ $^

rtcopy_client_rtcp_SendRecv.o: $(ROOT_DIR)/rtcopy/rtcp_SendRecv.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcopy_server_rtcp_SendRecv.o: $(ROOT_DIR)/rtcopy/rtcp_SendRecv.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -DRTCP_SERVER -o $@ $^

rtcopy_rtcpd_ConvertData.o: $(ROOT_DIR)/rtcopy/rtcpd_ConvertData.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -DRTCP_SERVER -o $@ $^

security_Csec_protocol_policy.o: $(ROOT_DIR)/security/Csec_protocol_policy.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

security_Csec_api_loader.o: $(ROOT_DIR)/security/Csec_api_loader.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

security_Csec_api.o: $(ROOT_DIR)/security/Csec_api.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

security_Csec_errmsg.o: $(ROOT_DIR)/security/Csec_errmsg.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

security_Csec_apiinit.o: $(ROOT_DIR)/security/Csec_apiinit.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

security_Csec_common.o: $(ROOT_DIR)/security/Csec_common.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_mapper.o: $(ROOT_DIR)/client/src/stager/stager_mapper.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_errmsg.o: $(ROOT_DIR)/client/src/stager/stager_errmsg.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_client_commandline.o: \
  $(ROOT_DIR)/client/src/stager/stager_client_commandline.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_client_api_common.o: \
  $(ROOT_DIR)/client/src/stager/stager_client_api_common.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_client_api_open.o: \
  $(ROOT_DIR)/client/src/stager/stager_client_api_open.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_client_api_get.o: \
  $(ROOT_DIR)/client/src/stager/stager_client_api_get.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_client_api_put.o: \
  $(ROOT_DIR)/client/src/stager/stager_client_api_put.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_client_api_update.o: \
  $(ROOT_DIR)/client/src/stager/stager_client_api_update.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_DiskServerStatusCode.o: \
  $(ROOT_DIR)/castor/stager/DiskServerStatusCode.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_FileSystemStatusCodes.o: \
  $(ROOT_DIR)/castor/stager/FileSystemStatusCodes.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_StagePutRequest.o: $(ROOT_DIR)/castor/stager/StagePutRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_StagePutDoneRequest.o: \
  $(ROOT_DIR)/castor/stager/StagePutDoneRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_StagePrepareToPutRequest.o: \
  $(ROOT_DIR)/castor/stager/StagePrepareToPutRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_StagePrepareToUpdateRequest.o: \
  $(ROOT_DIR)/castor/stager/StagePrepareToUpdateRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_StageGetRequest.o: $(ROOT_DIR)/castor/stager/StageGetRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_StagePrepareToGetRequest.o: \
  $(ROOT_DIR)/castor/stager/StagePrepareToGetRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_FileRequest.o: $(ROOT_DIR)/castor/stager/FileRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_Request.o: $(ROOT_DIR)/castor/stager/Request.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_StageUpdateRequest.o: $(ROOT_DIR)/castor/stager/StageUpdateRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_SubRequest.o: $(ROOT_DIR)/castor/stager/SubRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_SubRequestStatusCodes.o: \
  $(ROOT_DIR)/castor/stager/SubRequestStatusCodes.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_SubRequestGetNextStatusCodes.o: \
  $(ROOT_DIR)/castor/stager/SubRequestGetNextStatusCodes.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_usrlbl.o: $(ROOT_DIR)/tape/usrlbl.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

tape_asc2ebc.o: $(ROOT_DIR)/tape/asc2ebc.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_chkdirw.o: $(ROOT_DIR)/tape/chkdirw.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_Ctape_kill.o: $(ROOT_DIR)/tape/Ctape_kill.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_Ctape_errmsg.o: $(ROOT_DIR)/tape/Ctape_errmsg.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_Ctape_dmpfil.o: $(ROOT_DIR)/tape/Ctape_dmpfil.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_Ctape_mount.o: $(ROOT_DIR)/tape/Ctape_mount.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_Ctape_position.o: $(ROOT_DIR)/tape/Ctape_position.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_Ctape_devinfo.o: $(ROOT_DIR)/tape/Ctape_devinfo.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_Ctape_drvinfo.o: $(ROOT_DIR)/tape/Ctape_drvinfo.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_Ctape_info.o: $(ROOT_DIR)/tape/Ctape_info.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_Ctape_rls.o: $(ROOT_DIR)/tape/Ctape_rls.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_Ctape_reserve.o: $(ROOT_DIR)/tape/Ctape_reserve.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_cvtden.o: $(ROOT_DIR)/tape/cvtden.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_ebc2asc.o: $(ROOT_DIR)/tape/ebc2asc.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_findpgrp.o: $(ROOT_DIR)/tape/findpgrp.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_getcompstat.o: $(ROOT_DIR)/tape/getcompstat.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

tape_net.o: $(ROOT_DIR)/castor/tape/net/net.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_initlabel.o: $(ROOT_DIR)/tape/initlabel.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_locate.o: $(ROOT_DIR)/tape/locate.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_readlbl.o: $(ROOT_DIR)/tape/readlbl.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_rwndtape.o: $(ROOT_DIR)/tape/rwndtape.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_send2tpd.o: $(ROOT_DIR)/tape/send2tpd.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_sendrep.o: $(ROOT_DIR)/tape/sendrep.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_sendscsicmd.o: $(ROOT_DIR)/tape/sendscsicmd.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_skiptape.o: $(ROOT_DIR)/tape/skiptape.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_tperror.o: $(ROOT_DIR)/tape/tperror.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_tplogger.o: $(ROOT_DIR)/tape/tplogger.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

tape_tplogger_messages.o: $(ROOT_DIR)/tape/tplogger_messages.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_tplogit.o: $(ROOT_DIR)/tape/tplogit.c
	gcc $(COMMON_OPS) -DTAPELOGFILE=\"/var/log/castor/taped_legacy.log\" -c $(INCLUDE_OPS) -o $@ $^

tape_usrmsg.o: $(ROOT_DIR)/tape/usrmsg.c
	gcc $(COMMON_OPS) -DNOTRACE -c $(INCLUDE_OPS) -o $@ $^

tape_utils.o: $(ROOT_DIR)/castor/tape/utils/utils.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_writelbl.o: $(ROOT_DIR)/tape/writelbl.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_wrttpmrk.o: $(ROOT_DIR)/tape/wrttpmrk.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_BoolFunctor.o: $(ROOT_DIR)/castor/tape/tapebridge/BoolFunctor.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_BridgeClientInfo2Sender.o: \
 $(ROOT_DIR)/castor/tape/tapebridge/BridgeClientInfo2Sender.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_BridgeProtocolEngine.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/BridgeProtocolEngine.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_BridgeSocketCatalogue.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/BridgeSocketCatalogue.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_ClientTxRx.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/ClientTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_DlfMessageStrings.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/DlfMessageStrings.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_FailedToCopyTapeFile.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/FailedToCopyTapeFile.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_LegacyTxRx.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/LegacyTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_LogHelper.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/LogHelper.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_marshall.o: $(ROOT_DIR)/tapebridge/tapebridge_marshall.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_PendingMigrationsStore.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/PendingMigrationsStore.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_recvTapeBridgeFlushedToTapeAck.o: \
  $(ROOT_DIR)/tapebridge/tapebridge_recvTapeBridgeFlushedToTapeAck.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_RtcpTxRx.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/RtcpTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_sendTapeBridgeClientInfo2Ack.o: \
  $(ROOT_DIR)/tapebridge/tapebridge_sendTapeBridgeClientInfo2Ack.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_sendTapeBridgeFlushedToTape.o: \
  $(ROOT_DIR)/tapebridge/tapebridge_sendTapeBridgeFlushedToTape.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_TapeBridgeDaemon.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/TapeBridgeDaemon.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_TapeBridgeMain.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/TapeBridgeMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_tapeFlushModeToStr.o: \
  $(ROOT_DIR)/tapebridge/tapebridge_tapeFlushModeToStr.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_TapeFlushConfigParams.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/TapeFlushConfigParams.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_VmgrTxRx.o: $(ROOT_DIR)/castor/tape/tapebridge/VmgrTxRx.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileToRecallRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileToRecallRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileToRecall.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileToRecall.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileRecalledNotification.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileRecalledNotification.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_VdqmRequestHandler.o: \
  $(ROOT_DIR)/castor/tape/tapebridge/VdqmRequestHandler.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_VolumeRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/VolumeRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_Volume.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/Volume.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_GatewayMessage.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/GatewayMessage.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileToMigrateRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileToMigrateRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileToMigrate.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileToMigrate.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_FileMigratedNotification.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/FileMigratedNotification.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_NoMoreFiles.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/NoMoreFiles.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_BaseFileInfo.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/BaseFileInfo.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_ClientType.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/ClientType.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^
tapegateway_VolumeMode.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/VolumeMode.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_DumpParametersRequest.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/DumpParametersRequest.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_DumpParameters.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/DumpParameters.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_DumpNotification.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/DumpNotification.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_PositionCommandCode.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/PositionCommandCode.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapegateway_PingNotification.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/PingNotification.cpp
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

tapegateway_NotificationAcknowledge.o: \
  $(ROOT_DIR)/castor/tape/tapegateway/NotificationAcknowledge.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

test_exception.o: test_exception.cpp
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

upv_Cupv_check.o: $(ROOT_DIR)/upv/Cupv_check.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

upv_send2Cupv.o: $(ROOT_DIR)/upv/send2Cupv.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

upv_Cupv_errmsg.o: $(ROOT_DIR)/upv/Cupv_errmsg.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

upv_Cupv_apiinit.o: $(ROOT_DIR)/upv/Cupv_apiinit.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

utils_SmartFd.o: $(ROOT_DIR)/castor/tape/utils/SmartFd.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

utils_SmartFILEPtr.o: $(ROOT_DIR)/castor/tape/utils/SmartFILEPtr.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

vdqm_vdqmapi.o: $(ROOT_DIR)/vdqm/vdqmapi.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

vdqm_vdqm_SendRecv.o: $(ROOT_DIR)/vdqm/vdqm_SendRecv.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

vmgr_vmgrcheck.o: $(ROOT_DIR)/vmgr/vmgrcheck.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

vmgr_vmgr_errmsg.o: $(ROOT_DIR)/vmgr/vmgr_errmsg.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

vmgr_vmgr_apiinit.o: $(ROOT_DIR)/vmgr/vmgr_apiinit.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

vmgr_vmgr_querypool.o: $(ROOT_DIR)/vmgr/vmgr_querypool.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

vmgr_send2vmgr.o: $(ROOT_DIR)/vmgr/send2vmgr.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

vmgr_vmgr_querytape_byte_u64.o: $(ROOT_DIR)/vmgr/vmgr_querytape_byte_u64.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^
