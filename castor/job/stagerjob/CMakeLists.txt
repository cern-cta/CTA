#
#                      castor/job/stagerjob/CMakeLists.txt
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

find_package (ceph REQUIRED)
include_directories(${RADOS_INCLUDE_DIR})
set(STAGERJOB_EXE_SRC_FILES
  StagerJob.cpp
  InputArguments.cpp
  BasePlugin.cpp
  RawMoverPlugin.cpp
  InstrumentedMoverPlugin.cpp
  RfioPlugin.cpp
  RootPlugin.cpp
  XRootPlugin.cpp
  GridFTPPlugin.cpp)
add_executable(stagerjob ${STAGERJOB_EXE_SRC_FILES})
target_link_libraries(stagerjob castorcommon castordlf castorclient castorrfio
  ${RADOS_LIBS} castorcephposix castorserver)
install(TARGETS stagerjob DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallLogrotate(castor-job)
