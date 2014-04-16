#
#                      castor/stager/daemon/CMakeLists.txt
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
# Rules to build and install stagerd
################################################################################
set (STAGERD_SRC_FILES
  StagerDaemon.cpp
  PutDoneHandler.cpp
  ReplyHelper.cpp
  RequestHelper.cpp
  RmHandler.cpp
  SetGCWeightHandler.cpp
  RequestHandler.cpp
  BaseRequestSvcThread.cpp
  StageRequestSvcThread.cpp
  BulkStageReqSvcThread.cpp
  JobRequestSvcThread.cpp
  QueryRequestSvcThread.cpp
  GcSvcThread.cpp
  ErrorSvcThread.cpp
  JobSvcThread.cpp
  LoggingThread.cpp
  NsOverride.cpp)
add_executable (stagerd ${STAGERD_SRC_FILES})
set_target_properties (stagerd PROPERTIES
  COMPILE_FLAGS ${ORACLE_CPPFLAGS}
  LINK_FLAGS "-L${ORACLE_LIBDIR} -locci -lclntsh")
target_link_libraries (stagerd castorclient castorcnvs castorcommon castorns
  castorupv castordlf castorcommonora)
install (TARGETS stagerd DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallAdmManPage (stagerd)
CastorInstallLogRotate (castor-stager-server)
CastorInstallSysConfigExample (stagerd)
CastorInstallInitScript (stagerd)
