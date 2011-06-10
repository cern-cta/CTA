ROOT_DIR=../..
INCLUDE_OPS=-I$(ROOT_DIR) -I$(ROOT_DIR)/h
EXECUTABLES=sendrtcopyjob testrtcpd testrtcpdtape
COMMON_OPS=-DCNS_HOST=\"castorns\" -DSCNS_HOST=\"scnsdev\" -DCNSHOSTPFX=\"cns\" -DSCNSHOSTPFX=\"scns\" -DACCTFILE=\"/var/log/castor/sacct\" -DCNS_ROOT=\"/castor\" -DNOMORETAPES=\"/var/run/nomoretapes\" -DTAPELOGFILE=\"/var/log/castor/taped_legacy.log\" -DSCSIINC=\"/usr/include/scsi/sg.h\" -DNOMORERFIO=\"/var/run/nomorerfio\" -DRFIOLOGFILE=\"/var/log/castor/rfiod.log\" -D_LARGEFILE64_SOURCE -Werror -Wno-long-long -pedantic -Wall -g -pthread

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
  rtcp_client_log.o \
  rtcp_client_SendRecv.o \
  tape_tplogger.o \
  tape_tplogger_messages.o \
  vdqm_vdqmapi.o \
  vdqm_vdqm_SendRecv.o
	g++ $(COMMON_OPS) -lpthread -luuid -lcppunit -o $@ $^

testrtcpdtape: \
  testRtcpdTapeMain.o \
  testJid.o \
  common_getifnam.o \
  common_Cpool.o \
  common_getacctent.o \
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
  common_u64subr.o \
  common_wsacct.o \
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
  rfio_xyread.o \
  rfio_error.o \
  rfio_mstat.o \
  rfio_connect.o \
  rfio_open.o \
  rfio_open64.o \
  rfio_xyopen.o \
  rfio_close.o \
  rfio_xyclose.o \
  rfio_read.o \
  rfio_write.o \
  rfio_xywrite.o \
  rfio_rfio_rfilefdt.o \
  rfio_lseek64.o \
  rfio_setopt.o \
  rfio_parse.o \
  rfio_rfio_HsmIf.o \
  rtcpapi.o \
  rtcp_accounting.o \
  rtcp_marshallVdqmClientInfoMsg.o \
  rtcp_server_log.o \
  rtcp_server_SendRecv.o \
  rtcp_CheckReq.o \
  rtcp_Listen.o \
  rtcp_CallVMGR.o \
  rtcp_recvRtcpHdr.o \
  rtcp_RetvalSHIFT.o \
  rtcp_InitNW.o \
  rtcpc_BuildReq.o \
  rtcpd_Disk.o \
  rtcpd_tpdump.o \
  rtcpd_GetClientInfoMsg.o \
  rtcpd_MainCntl.o \
  rtcpd_TapeIO.o \
  rtcpd_Ctape.o \
  rtcpd_SelfMonitor.o \
  rtcpd_GetClientInfoMsg.o \
  rtcpd_ConvertData.o \
  rtcpd_ClientListen.o \
  rtcpd_GetClientInfo.o \
  rtcpd_SendAckToVdqmOrTapeBridge.o \
  stager_client_api_open.o \
  stager_client_api_get.o \
  stager_client_api_put.o \
  stager_client_api_update.o \
  tape_cvtden.o \
  tape_tplogger.o \
  tape_Ctape_kill.o \
  tape_Ctape_errmsg.o \
  tape_Ctape_dmpfil.o \
  tape_Ctape_mount.o \
  tape_Ctape_position.o \
  tape_Ctape_info.o \
  tape_Ctape_devinfo.o \
  tape_Ctape_drvinfo.o \
  tape_Ctape_rls.o \
  tape_Ctape_reserve.o \
  tape_asc2ebc.o \
  tape_ebc2asc.o \
  tape_send2tpd.o \
  tape_tplogger_messages.o \
  tape_initlabel.o \
  tape_findpgrp.o \
  tape_chkdirw.o \
  tape_locate.o \
  tape_tperror.o \
  tape_tplogit.o \
  tape_skiptape.o \
  tape_usrlbl.o \
  tape_writelbl.o \
  tape_wrttpmrk.o \
  tape_rwndtape.o \
  tape_readlbl.o \
  tape_sendscsicmd.o \
  tape_usrmsg.o \
  tape_sendrep.o \
  tape_getcompstat.o \
  tapebridge_sendTapeBridgeAck.o \
  tapebridge_marshall.o \
  test_rtcpd_Tape.o \
  upv_Cupv_check.o \
  upv_send2Cupv.o \
  upv_Cupv_errmsg.o \
  upv_Cupv_apiinit.o \
  vdqm_vdqmapi.o \
  vdqm_vdqm_SendRecv.o \
  vmgr_vmgrcheck.o \
  vmgr_vmgr_errmsg.o \
  vmgr_vmgr_apiinit.o \
  vmgr_vmgr_querypool.o \
  vmgr_send2vmgr.o \
  vmgr_vmgr_querytape_byte_u64.o
	g++ $(COMMON_OPS) -lpthread -luuid -lcppunit -lz -o $@ $^

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

common_getifnam.o: $(ROOT_DIR)/common/getifnam.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

common_Cpool.o: $(ROOT_DIR)/common/Cpool.c
	gcc -pthread -g -c $(INCLUDE_OPS) -o $@ $^

common_getacctent.o: $(ROOT_DIR)/common/getacctent.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

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

common_u64subr.o: $(ROOT_DIR)/common/u64subr.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

common_wsacct.o: $(ROOT_DIR)/common/wsacct.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

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

io_AbstractSocket.o: $(ROOT_DIR)/castor/io/AbstractSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_AbstractTCPSocket.o: $(ROOT_DIR)/castor/io/AbstractTCPSocket.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

io_StreamAddress.o: $(ROOT_DIR)/castor/io/StreamAddress.cpp
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

testRtcpdMain.o: testRtcpdMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testRtcpdMain.cpp: RtcpdTest.hpp
	touch $@

testRtcpdTapeMain.o: testRtcpdTapeMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testRtcpdTapeMain.cpp: RtcpdTapeTest.hpp
	touch $@

testJid.o: testJid.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_parse.o: $(ROOT_DIR)/rfio/parse.c
	gcc -g -pthread -DCNS_ROOT=\"/castor\" -c $(INCLUDE_OPS) -o $@ $^

rfio_rfio_HsmIf.o: $(ROOT_DIR)/rfio/rfio_HsmIf.c
	gcc -g -pthread -DCNS_ROOT=\"/castor\" -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_setopt.o: $(ROOT_DIR)/rfio/setopt.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_xyread.o: $(ROOT_DIR)/rfio/xyread.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_error.o: $(ROOT_DIR)/rfio/error.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_mstat.o: $(ROOT_DIR)/rfio/mstat.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_lseek64.o: $(ROOT_DIR)/rfio/lseek64.c
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

rfio_write.o: $(ROOT_DIR)/rfio/write.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rfio_xywrite.o: $(ROOT_DIR)/rfio/xywrite.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rfio_rfio_rfilefdt.o: $(ROOT_DIR)/rfio/rfio_rfilefdt.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rtcpapi.o: $(ROOT_DIR)/rtcopy/rtcpapi.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_accounting.o: $(ROOT_DIR)/rtcopy/rtcp_accounting.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_RetvalSHIFT.o: $(ROOT_DIR)/rtcopy/rtcp_RetvalSHIFT.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_InitNW.o: $(ROOT_DIR)/rtcopy/rtcp_InitNW.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_Disk.o: $(ROOT_DIR)/rtcopy/rtcpd_Disk.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rtcpd_tpdump.o: $(ROOT_DIR)/rtcopy/rtcpd_tpdump.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_SendAckToVdqmOrTapeBridge.o: $(ROOT_DIR)/rtcopy/rtcpd_SendAckToVdqmOrTapeBridge.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_SelfMonitor.o: $(ROOT_DIR)/rtcopy/rtcpd_SelfMonitor.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_GetClientInfoMsg.o: $(ROOT_DIR)/rtcopy/rtcpd_GetClientInfoMsg.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_Ctape.o: $(ROOT_DIR)/rtcopy/rtcpd_Ctape.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rtcpd_ClientListen.o: $(ROOT_DIR)/rtcopy/rtcpd_ClientListen.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_GetClientInfo.o: $(ROOT_DIR)/rtcopy/rtcpd_GetClientInfo.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_marshallVdqmClientInfoMsg.o: \
  $(ROOT_DIR)/rtcopy/rtcp_marshallVdqmClientInfoMsg.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_CallVMGR.o: $(ROOT_DIR)/rtcopy/rtcp_CallVMGR.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_CheckReq.o: $(ROOT_DIR)/rtcopy/rtcp_CheckReq.c
	gcc $(COMMON_OPS) -DRTCP_SERVER -c $(INCLUDE_OPS) -o $@ $^

rtcp_Listen.o: $(ROOT_DIR)/rtcopy/rtcp_Listen.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_recvRtcpHdr.o: $(ROOT_DIR)/rtcopy/rtcp_recvRtcpHdr.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpc_BuildReq.o: $(ROOT_DIR)/rtcopy/rtcpc_BuildReq.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_tapeIOthread.o: $(ROOT_DIR)/rtcopy/rtcpd_tapeIOthread.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_SignalFilePositioned.o: $(ROOT_DIR)/rtcopy/rtcpd_SignalFilePositioned.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_MainCntl.o: $(ROOT_DIR)/rtcopy/rtcpd_MainCntl.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcpd_TapeIO.o: $(ROOT_DIR)/rtcopy/rtcpd_TapeIO.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rtcp_client_log.o: $(ROOT_DIR)/rtcopy/rtcp_log.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_server_log.o: $(ROOT_DIR)/rtcopy/rtcp_log.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -DRTCP_SERVER -o $@ $^

rtcp_client_SendRecv.o: $(ROOT_DIR)/rtcopy/rtcp_SendRecv.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_server_SendRecv.o: $(ROOT_DIR)/rtcopy/rtcp_SendRecv.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -DRTCP_SERVER -o $@ $^

rtcpd_ConvertData.o: $(ROOT_DIR)/rtcopy/rtcpd_ConvertData.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -DRTCP_SERVER -o $@ $^

test_rtcpd_Tape.o: test_rtcpd_Tape.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_sendTapeBridgeAck.o: $(ROOT_DIR)/tapebridge/tapebridge_sendTapeBridgeAck.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tapebridge_marshall.o: $(ROOT_DIR)/tapebridge/tapebridge_marshall.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_usrlbl.o: $(ROOT_DIR)/tape/usrlbl.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

tape_wrttpmrk.o: $(ROOT_DIR)/tape/wrttpmrk.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

tape_writelbl.o: $(ROOT_DIR)/tape/writelbl.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

tape_findpgrp.o: $(ROOT_DIR)/tape/findpgrp.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_asc2ebc.o: $(ROOT_DIR)/tape/asc2ebc.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_ebc2asc.o: $(ROOT_DIR)/tape/ebc2asc.c
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

tape_tplogger.o: $(ROOT_DIR)/tape/tplogger.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

tape_tplogger_messages.o: $(ROOT_DIR)/tape/tplogger_messages.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_cvtden.o: $(ROOT_DIR)/tape/cvtden.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_initlabel.o: $(ROOT_DIR)/tape/initlabel.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_send2tpd.o: $(ROOT_DIR)/tape/send2tpd.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_chkdirw.o: $(ROOT_DIR)/tape/chkdirw.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_locate.o: $(ROOT_DIR)/tape/locate.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_tperror.o: $(ROOT_DIR)/tape/tperror.c
	gcc $(COMMON_OPS) -DNOTRACE -c $(INCLUDE_OPS) -o $@ $^

tape_tplogit.o: $(ROOT_DIR)/tape/tplogit.c
	gcc $(COMMON_OPS) -DTAPELOGFILE=\"/var/log/castor/taped_legacy.log\" -c $(INCLUDE_OPS) -o $@ $^

tape_skiptape.o: $(ROOT_DIR)/tape/skiptape.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_getcompstat.o: $(ROOT_DIR)/tape/getcompstat.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

tape_rwndtape.o: $(ROOT_DIR)/tape/rwndtape.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_usrmsg.o: $(ROOT_DIR)/tape/usrmsg.c
	gcc $(COMMON_OPS) -DNOTRACE -c $(INCLUDE_OPS) -o $@ $^

tape_sendrep.o: $(ROOT_DIR)/tape/sendrep.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_sendscsicmd.o: $(ROOT_DIR)/tape/sendscsicmd.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

tape_readlbl.o: $(ROOT_DIR)/tape/readlbl.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

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

upv_Cupv_check.o: $(ROOT_DIR)/upv/Cupv_check.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

upv_send2Cupv.o: $(ROOT_DIR)/upv/send2Cupv.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

upv_Cupv_errmsg.o: $(ROOT_DIR)/upv/Cupv_errmsg.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

upv_Cupv_apiinit.o: $(ROOT_DIR)/upv/Cupv_apiinit.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

stager_client_api_open.o: $(ROOT_DIR)/client/src/stager/stager_client_api_open.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_client_api_get.o: $(ROOT_DIR)/client/src/stager/stager_client_api_get.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_client_api_put.o: $(ROOT_DIR)/client/src/stager/stager_client_api_put.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

stager_client_api_update.o: $(ROOT_DIR)/client/src/stager/stager_client_api_update.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^
