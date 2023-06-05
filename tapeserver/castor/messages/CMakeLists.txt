# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

cmake_minimum_required (VERSION 3.17)

include_directories(/usr/include/shift)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_BINARY_DIR}/tapeserver)

find_package(Protobuf3 REQUIRED)

file(GLOB ProtoFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
PROTOBUF3_GENERATE_CPP(ProtoSources ProtoHeaders ${ProtoFiles})

include_directories (${PROTOBUF3_INCLUDE_DIRS})
add_library(ctamessagesprotobuf
  ${ProtoSources})

add_library(ctamessagesutils SHARED
  TapeserverProxyDummy.cpp)
set_property(TARGET ctamessagesutils PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctamessagesutils PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctamessagesutils DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

set(CtaMessageSrcs
  Constants.cpp
  ../../../tapeserver/daemon/TapedProxy.cpp
)

add_library(ctamessages SHARED
  ${CtaMessageSrcs})
set_property(TARGET ctamessages PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctamessages PROPERTY   VERSION "${CTA_LIBVERSION}")

install (TARGETS ctamessages DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_dependencies(ctamessages ctamessagesprotobuf)

target_link_libraries(ctamessages
  ctamessagesprotobuf
  ctamediachanger
  ctatapesession
  ${PROTOBUF3_LIBRARIES}
)
