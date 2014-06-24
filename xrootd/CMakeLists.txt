#
#                      xrootd/CMakeLists.txt
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

find_package (xrootd REQUIRED)

include_directories (${XROOTD_INCLUDE_DIR} ${XROOTD_PRIVATE_INCLUDE_DIR})

################################################################################
# Rules to build and install libXrdxCastor2Fs.so
################################################################################
set (XRDXCASTOR2FS_LIB_SRC_FILES
  XrdxCastor2Fs.cpp
  XrdxCastor2FsFile.cpp
  XrdxCastor2FsDirectory.cpp
  XrdxCastor2FsConfig.cpp
  XrdxCastor2FsStats.cpp
  XrdxCastor2ServerAcc.cpp
  XrdxCastor2Stager.cpp
  XrdxCastor2Proc.cpp
  XrdxCastorLogging.cpp
  XrdxCastorTiming.cpp
  XrdxCastorClient.cpp)
add_library (XrdxCastor2Fs SHARED ${XRDXCASTOR2FS_LIB_SRC_FILES})
CastorSetLibraryVersions (XrdxCastor2Fs)
set_property (TARGET XrdxCastor2Fs
  APPEND PROPERTY COMPILE_DEFINITIONS _FILE_OFFSET_BITS=64)
target_link_libraries (XrdxCastor2Fs castorclient castorcommon castorns crypto
  ${XROOTD_XRDOFS_LIB} ${XROOTD_XRDUTILS_LIB})
install (TARGETS XrdxCastor2Fs DESTINATION ${CASTOR_DEST_LIB_DIR})

################################################################################
# Rules to build and install libXrdxCastor2ServerAcc.so
################################################################################
set (XRDXCASTOR2SERVERACC_LIB_SRC_FILES
  XrdxCastor2ServerAcc.cpp
  XrdxCastorLogging.cpp
  XrdxCastorTiming.cpp)
add_library (XrdxCastor2ServerAcc SHARED ${XRDXCASTOR2SERVERACC_LIB_SRC_FILES})
CastorSetLibraryVersions (XrdxCastor2ServerAcc)
target_link_libraries (XrdxCastor2ServerAcc crypto ${UUID_LIB} ${XROOTD_XRDUTILS_LIB})
install (TARGETS XrdxCastor2ServerAcc DESTINATION ${CASTOR_DEST_LIB_DIR})

################################################################################
# Rules to build and install libXrdxCastor2Ofs.so
################################################################################
set (XRDXCASTOR2OFS_LIB_SRC_FILES
  XrdxCastor2Ofs.cpp
  XrdxCastor2OfsProc.cpp
  XrdxCastorLogging.cpp
  XrdxCastorTiming.cpp)
set_property(SOURCE XrdxCastor2Ofs.cpp
  PROPERTY COMPILE_FLAGS -Wno-write-strings)
add_library (XrdxCastor2Ofs SHARED ${XRDXCASTOR2OFS_LIB_SRC_FILES})
CastorSetLibraryVersions (XrdxCastor2Ofs)
set_property (TARGET XrdxCastor2Ofs
  APPEND PROPERTY COMPILE_DEFINITIONS _FILE_OFFSET_BITS=64)
target_link_libraries (XrdxCastor2Ofs crypto ${XROOTD_XRDCL_LIB}
  ${XROOTD_XRDCLIENT_LIB} ${XROOTD_XRDOFS_LIB} ${UUID_LIB} z)
install (TARGETS XrdxCastor2Ofs DESTINATION ${CASTOR_DEST_LIB_DIR})

################################################################################
# Rules to install the x2cp and x2proc scripts
################################################################################
CastorInstallScript (x2cp)
CastorInstallScript (x2proc)

################################################################################
# Rules to install xrd cron job
################################################################################
CastorInstallEtcCronD (xrd-logs)

################################################################################
# Rules to install example configuration files
################################################################################
CastorInstallEtcExample (xrd.cf.server)
CastorInstallEtcExample (xrd.cf.manager)
CastorInstallSysconfigExample (xrootd.server)
CastorInstallSysconfigExample (xrootd.manager)

################################################################################
# Rules to install the log, spool and run directories of xrootd
################################################################################
CastorInstallDir(/var/log/castor)
CastorInstallDir(/var/spool/castor)
CastorInstallDir(/var/log/xrootd/manager)
CastorInstallDir(/var/log/xrootd/server)
CastorInstallDir(/var/log/xrootd/manager/proc)
CastorInstallDir(/var/log/xrootd/server/proc)
CastorInstallDir(/var/spool/xrootd/admin)
CastorInstallDir(/var/spool/xrootd/core)
CastorInstallDir(/var/spool/xrootd/manager)
CastorInstallDir(/var/spool/xrootd/server)
CastorInstallDir(/var/run/xrootd)
