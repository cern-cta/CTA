#                 rtcopy/CMakeLists.txt
#
# This file is part of the Castor project.
# See http://castor.web.cern.ch/castor
#
# Copyright (C) 2003  CERN
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#
# Steven.Murray@cern.ch Eric.Cano@cern.ch
#
cmake_minimum_required (VERSION 2.6)

################################################################################
# Rules to build and install remote tape copy daemon
################################################################################
set_property (SOURCE
  rtcp_SendRecv.c
  APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
set(RTCPD_SRC_FILES rtcpd.c rtcp_CallVMGR.c rtcp_CheckReq.c rtcp_Listen.c
  rtcp_SendRecv.c rtcp_log.c rtcpd_ClientListen.c rtcpd_Ctape.c rtcpd_Disk.c
  rtcpd_MainCntl.c rtcpd_SelfMonitor.c rtcpd_Tape.c rtcpd_TapeIO.c
  rtcp_InitNW.c rtcpc_BuildReq.c rtcpapi.c
  rtcpd_tpdump.c rtcp_recvRtcpHdr.c 
  rtcp_marshallVdqmClientInfoMsg.c rtcpd_SendAckToVdqmOrTapeBridge.c 
  rtcpd_GetClientInfoMsg.c rtcpd_GetClientInfo.c rtcp_tellClient.c
  rtcpdIsConfiguredToReuseMount.c)
add_executable (rtcpd ${RTCPD_SRC_FILES})
# Relax compilation of old code full of pointer casting. Funnily enough,
# the compiler only complains in -O2 mode (RelWithDebInfo in cmake).
set_property(SOURCE rtcp_log.c rtcpd_Ctape.c APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
target_link_libraries (rtcpd castorcommon castorvmgr castorvdqm castortape 
  castortapebridge castorrfio castorsecurity castorrfio -lz)
install (TARGETS rtcpd DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallInitScript(rtcpd)
CastorInstallSysconfigExample(rtcpd)
CastorInstallLogrotate(castor-rtcopy-server)
CastorInstallExeManPage(rtcpd)
CastorInstallDir(/var/lib/castor)
