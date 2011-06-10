ROOT_DIR=../..
INCLUDE_OPS=-I$(ROOT_DIR) -I$(ROOT_DIR)/h
EXECUTABLES=sendrtcopyjob testrtcpd testrtcpdtape
COMMON_OPS=-DNOMORETAPES=\"/var/run/nomoretapes\" -DTAPELOGFILE=\"/var/log/castor/taped_legacy.log\" -DSCSIINC=\"/usr/include/scsi/sg.h\" -DNOMORERFIO=\"/var/run/nomorerfio\" -DRFIOLOGFILE=\"/var/log/castor/rfiod.log\" -D_LARGEFILE64_SOURCE -Werror -Wno-long-long -pedantic -Wall -g -pthread

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
  dlf_dlf_lib.o \
  rfio_xyread.o \
  rfio_error.o \
  rfio_mstat.o \
  rfio_connect.o \
  rfio_xyclose.o \
  rfio_read.o \
  rtcpapi.o \
  rtcp_accounting.o \
  rtcp_marshallVdqmClientInfoMsg.o \
  rtcp_server_log.o \
  rtcp_server_SendRecv.o \
  rtcp_CheckReq.o \
  rtcp_Listen.o \
  rtcp_CallVMGR.o \
  rtcp_recvRtcpHdr.o \
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

testRtcpdTapeMain.o: testRtcpdTapeMain.cpp
	g++ $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

testRtcpdTapeMain.cpp: RtcpdTapeTest.hpp
	touch $@

testJid.o: testJid.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_xyread.o: $(ROOT_DIR)/rfio/xyread.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_error.o: $(ROOT_DIR)/rfio/error.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_mstat.o : $(ROOT_DIR)/rfio/mstat.c
	gcc -g -pthread -D_LARGEFILE64_SOURCE -c $(INCLUDE_OPS) -o $@ $^

rfio_connect.o: $(ROOT_DIR)/rfio/connect.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_xyclose.o: $(ROOT_DIR)/rfio/xyclose.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rfio_read.o: $(ROOT_DIR)/rfio/read.c
	gcc -g -pthread -c $(INCLUDE_OPS) -o $@ $^

rtcpapi.o: $(ROOT_DIR)/rtcopy/rtcpapi.c
	gcc $(COMMON_OPS) -c $(INCLUDE_OPS) -o $@ $^

rtcp_accounting.o: $(ROOT_DIR)/rtcopy/rtcp_accounting.c
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
