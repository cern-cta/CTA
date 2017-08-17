# The CERN Tape Archive(CTA) project
# Copyright(C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

cmake_minimum_required (VERSION 2.6)

list (APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
find_package (cryptopp REQUIRED)
find_package (xrootdclient REQUIRED)
include_directories (${XROOTD_INCLUDE_DIR} ${CMAKE_SOURCE_DIR})

add_executable (cta CTACmdMain.cpp Configuration.cpp)
target_link_libraries (cta ${XROOTD_XRDCL_LIB} cryptopp)
install (TARGETS cta DESTINATION usr/bin)
INSTALL (FILES cta-cli.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)

include_directories (${CMAKE_SOURCE_DIR}/tapeserver/)

#
# eosctastub is a drop-in replacement for "cta archive|retrieve|delete"
#

# Get some extra debug messages on stdout
#add_definitions(-DXRDSSI_DEBUG)

# XRootD must be compiled with the SSI extensions
find_package(xrootd REQUIRED)
find_package(Protobuf3 REQUIRED)
find_package(cryptopp REQUIRED)

include_directories(${PROTOBUF3_INCLUDE_DIRS} ${XROOTD_INCLUDE_DIR} ${XROOTD_INCLUDE_DIR}/private ../xroot_ssi_pb)
add_executable(eoscta_stub EosCtaStub.cpp)
target_link_libraries(eoscta_stub cryptopp XrdCtaMessages ${PROTOBUF3_LIBRARIES} XrdSsi-4 XrdSsiLib)
install(TARGETS eoscta_stub DESTINATION usr/bin)

#
# cta_admin is a drop-in replacement for "cta <admin_command>"
#

add_executable(cta_admin CtaAdminCmdMain.cpp CtaAdminCmd.cpp)
target_link_libraries(cta_admin ${PROTOBUF3_LIBRARIES} XrdSsi-4 XrdSsiLib)
install(TARGETS cta_admin DESTINATION usr/bin)

