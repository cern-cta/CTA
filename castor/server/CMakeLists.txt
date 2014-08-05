#
#         castor/server/CMakeLists.txt
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
# Rules to build and install libcastorserver.so
################################################################################
set (SERVER_LIB_SRC_FILES
  AuthListenerThreadPool.cpp
  BaseThreadPool.cpp
  CMakeLists.txt
  Daemon.cpp
  DbAlertedThreadPool.cpp
  DynamicThreadPool.cpp
  ForkedProcessPool.cpp
  ../io/StreamThreadNotificationCnv.cpp
  ListenerThreadPool.cpp
  metrics/Counter.cpp
  metrics/InternalCounter.cpp
  metrics/Histogram.cpp
  metrics/MetricsCollector.cpp
  metrics/UpdateThread.cpp
  metrics/ObjTypeCounter.cpp
  MultiThreadedDaemon.cpp
  AllInOneLockingUtility.cpp
  NotifierThread.cpp
  ProcessCap.cpp
  Queue.cpp
  Mutex.cpp
  SelectProcessThread.cpp
  SignalThreadPool.cpp
  Semaphores.cpp
  SmartCap.cpp
  TCPListenerThreadPool.cpp
  ThreadNotification.cpp
  UDPListenerThreadPool.cpp
  Threading.cpp ChildProcess.cpp
  )
add_library (castorserver SHARED ${SERVER_LIB_SRC_FILES})
CastorSetLibraryVersions (castorserver)
target_link_libraries (castorserver cap castorclient castorcommon)
install (TARGETS castorserver LIBRARY DESTINATION ${CASTOR_DEST_LIB_DIR}
  NAMELINK_SKIP)

install(FILES
  metrics/Counter.hpp
  metrics/Histogram.hpp
  metrics/MetricsCollector.hpp
  DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/server/metrics
  PERMISSIONS ${CASTOR_HEADER_PERMS})
