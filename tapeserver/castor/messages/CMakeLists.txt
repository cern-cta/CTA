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

include_directories(/usr/include/shift)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_BINARY_DIR}/tapeserver)

find_package(Protobuf)

file(GLOB ProtoFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
PROTOBUF_GENERATE_CPP(ProtoSources ProtoHeaders ${ProtoFiles})

add_library(ctamessagesprotobuf
  ${ProtoSources})

add_library(ctamessagesutils SHARED
  AcsProxyDummy.cpp
  TapeserverProxyDummy.cpp)

set(CtaMessageSrcs
  AcsProxy.cpp
  AcsProxyZmq.cpp
  Constants.cpp
  Frame.cpp
  messages.cpp
  Mutex.cpp
  SmartZmqContext.cpp
  TapeserverProxy.cpp
  TapeserverProxyZmq.cpp
  ZmqMsg.cpp
  ZmqSocket.cpp
  ZmqSocketMT.cpp
  ZmqSocketST.cpp
)

add_library(ctamessages SHARED
  ${CtaMessageSrcs})

add_dependencies(ctamessages ctamessagesprotobuf)

target_link_libraries(ctamessages
  ctamessagesprotobuf
  ctalog
  ctamediachanger
  ctautils
  protobuf
  ssl
  zmq)

add_library(ctamessagesunittests SHARED
  MessagesTest.cpp
  MutexLockerTest.cpp
  MutexTest.cpp)
