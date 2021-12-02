# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2015-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.

cmake_minimum_required (VERSION 3.17)

include_directories(/usr/include/shift)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_BINARY_DIR}/tapeserver)

find_package(openssl REQUIRED)
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
  Frame.cpp
  messages.cpp
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
  ssl)
