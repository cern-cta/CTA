#
#                      castor/tape/tapegateway/CMakeLists.txt
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
# Rules to build and install tapegatewayd
################################################################################
set (TAPEGATEWAY_SRC_FILES
  ScopedTransaction.cpp
  TapeGatewayDlfMessageStrings.cpp
  daemon/NsTapeGatewayHelper.cpp
  daemon/TapeGatewayDaemon.cpp
  daemon/TapeGatewayMain.cpp
  daemon/TapeMigrationMountLinkerThread.cpp 
  daemon/VdqmRequestsCheckerThread.cpp 
  daemon/VdqmRequestsProducerThread.cpp
  daemon/VdqmTapeGatewayHelper.cpp 
  daemon/VmgrTapeGatewayHelper.cpp 
  daemon/WorkerThread.cpp
  daemon/ora/OraTapeGatewaySvc.cpp
)
add_executable (tapegatewayd ${TAPEGATEWAY_SRC_FILES})
set_target_properties (tapegatewayd PROPERTIES
  COMPILE_FLAGS ${ORACLE_CPPFLAGS}
  LINK_FLAGS "-L${ORACLE_LIBDIR} -locci -lclntsh")
target_link_libraries (tapegatewayd castorclient castorcnvs castorcommon
  castorcommonora castorns castortapegatewayprotocol castortapeutils castorvdqm
  castorvmgr)
install (TARGETS tapegatewayd DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallAdmManPage (tapegatewayd)
CastorInstallLogrotate (castor-tapegateway-server)
CastorInstallSysconfigExample (tapegatewayd)
CastorInstallInitScript (tapegatewayd)
