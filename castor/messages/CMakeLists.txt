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
# @author Castor Dev team, castor-dev@cern.ch
#
cmake_minimum_required (VERSION 2.6)

find_program(PROTOC NAMES protoc)
if(${PROTOC} STREQUAL "PROTOC-NOTFOUND")
  message(FATAL_ERROR "Failed to find protoc")
endif(${PROTOC} STREQUAL "PROTOC-NOTFOUND")
message(STATUS "Found protoc: ${PROTOC}")

file(GLOB PROTO_PATHS "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")

set(PROTO_TGTS)
set(PROTO_H_FILES)
set(PROTO_CC_FILES)
foreach(PROTO_PATH ${PROTO_PATHS})
  get_filename_component(PROTO_TGT ${PROTO_PATH} NAME_WE)
  add_custom_target(${PROTO_TGT}
    COMMAND ${PROTOC} -I ${CMAKE_CURRENT_SOURCE_DIR} ${PROTO_PATH} --cpp_out ${CMAKE_CURRENT_BINARY_DIR})
  list(APPEND PROTO_TGTS ${PROTO_TGT})
  set(PROTO_H_FILE "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_TGT}.pb.h")
  set(PROTO_CC_FILE "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_TGT}.pb.h")
  list(APPEND PROTO_H_FILES "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_TGT}.pb.h")
  list(APPEND PROTO_CC_FILES "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_TGT}.pb.cc")
endforeach(PROTO_PATH)

set_source_files_properties(${PROTO_H_FILES} PROPERTIES GENERATED TRUE)
set_source_files_properties(${PROTO_CC_FILES} PROPERTIES GENERATED TRUE)

add_library(castormessages SHARED
  ${PROTO_CC_FILES}
  AcsProxy.cpp
  AcsProxyDummy.cpp
  AcsProxyZmq.cpp
  Constants.cpp
  Frame.cpp
  messages.cpp
  Mutex.cpp
  SmartZmqContext.cpp
  TapeserverProxy.cpp
  TapeserverProxyDummy.cpp
  TapeserverProxyZmq.cpp
  ZmqMsg.cpp
  ZmqSocket.cpp
  ZmqSocketMT.cpp
  ZmqSocketST.cpp
)

target_link_libraries(castormessages
  castorlog
  castormediachanger
  protobuf
  ssl
  zmq)
add_dependencies(castormessages ${PROTO_TGTS})
install (TARGETS castormessages DESTINATION ${CASTOR_DEST_LIB_DIR})
