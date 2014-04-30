#
#                      castor/legacymsg/CMakeLists.txt
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
# Steven.Murray@cern.ch
#
cmake_minimum_required (VERSION 2.6)

################################################################################
# Rules to build and install libcastortapelegacymsg.so
################################################################################
set (TAPE_LEGACYMSG_LIB_SRC_FILES
  CommonMarshal.cpp
  CupvCheckMsgBody.cpp
  CupvMarshal.cpp
  CupvProxy.cpp
  CupvProxyTcpIp.cpp
  GiveOutpMsgBody.cpp
  MessageHeader.cpp
  RmcAcsMntMsgBody.cpp
  RmcMarshal.cpp
  RmcMountMsgBody.cpp
  RmcProxy.cpp
  RmcProxyFactory.cpp
  RmcProxyTcpIp.cpp
  RmcProxyTcpIpFactory.cpp
  RmcUnmountMsgBody.cpp
  RtcpDumpTapeRqstMsgBody.cpp
  RtcpErrorAppendix.cpp
  RtcpFileRqst.cpp
  RtcpJobReplyMsgBody.cpp
  RtcpJobRqstMsgBody.cpp
  RtcpMarshal.cpp
  RtcpSegmentAttributes.cpp
  RtcpTapeRqstMsgBody.cpp
  ScsiLibrarySlot.cpp
  SetVidRequestMsgBody.cpp
  TapeBridgeMarshal.cpp
  TapeConfigRequestMsgBody.cpp
  TapeStatDriveEntry.cpp
  TapeMarshal.cpp
  TapeserverProxy.cpp
  TapeserverProxyFactory.cpp
  TapeserverProxyTcpIp.cpp
  TapeserverProxyTcpIpFactory.cpp
  TapeStatRequestMsgBody.cpp
  TapeStatReplyMsgBody.cpp
  VdqmDrvRqstMsgBody.cpp
  VdqmMarshal.cpp
  VdqmProxy.cpp
  VdqmProxyFactory.cpp
  VdqmProxyTcpIp.cpp
  VdqmProxyTcpIpFactory.cpp
  VmgrProxy.cpp
  VmgrProxyFactory.cpp
  VmgrProxyTcpIp.cpp
  VmgrProxyTcpIpFactory.cpp
  VmgrMarshal.cpp
  VmgrTapeInfoMsgBody.cpp
  VmgrTapeInfoRqstMsgBody.cpp)
add_library (castortapelegacymsg SHARED ${TAPE_LEGACYMSG_LIB_SRC_FILES})
CastorSetLibraryVersions (castortapelegacymsg)
target_link_libraries (castortapelegacymsg castorclient castorcommon
  castortapeutils)
install (TARGETS castortapelegacymsg LIBRARY DESTINATION ${CASTOR_DEST_LIB_DIR}
  NAMELINK_SKIP)
