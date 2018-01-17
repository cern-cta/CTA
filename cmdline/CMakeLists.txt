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

find_package(xrootdclient REQUIRED)
find_package(xrootd REQUIRED)
find_package(Protobuf3 REQUIRED)

# XRootD SSI
include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_INCLUDE_DIR}/private)

# XRootD SSI Protocol Buffer bindings
include_directories(${XRD_SSI_PB_DIR}/include ${XRD_SSI_PB_DIR}/eos_cta/include)

# Compiled protocol buffers
include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

#
# cta-admin <admin_command> is the SSI version of "cta <admin_command>"
#
add_executable(cta-admin CtaAdminCmd.cpp CtaAdminCmdParse.cpp Configuration.cpp)
target_link_libraries(cta-admin XrdSsiPbEosCta XrdSsi-4 XrdSsiLib)
set_property (TARGET cta-admin APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
# Get some extra debug messages on stdout
#target_compile_definitions(cta-admin PUBLIC XRDSSI_DEBUG)
install(TARGETS cta-admin DESTINATION usr/bin)

#
# Old cta is scheduled for deletion
#
add_executable(cta.deprecated CTACmdMain.cpp Configuration.cpp)
target_link_libraries(cta.deprecated ${XROOTD_XRDCL_LIB} cryptopp)
install(TARGETS cta.deprecated DESTINATION usr/bin)
INSTALL(FILES cta-cli.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)

