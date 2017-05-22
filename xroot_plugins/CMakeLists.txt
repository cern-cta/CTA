# The CERN Tape Archive (CTA) project
# Copyright (C) 2015  CERN
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
find_package (xrootd REQUIRED)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_PRIVATE_INCLUDE_DIR})
include_directories(${CMAKE_BINARY_DIR})
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

add_library (XrdCtaOfs MODULE XrdCtaFilesystem.cpp XrdCtaFile.cpp XrdCtaDir.cpp)
target_link_libraries (XrdCtaOfs ctacatalogue ctaeosmessages ctascheduler ctacommon protobuf ctaobjectstore cryptopp)

INSTALL (TARGETS XrdCtaOfs DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
INSTALL (FILES xrootd-cta.cfg DESTINATION /etc/xrootd/)
INSTALL (FILES cta-frontend.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)
install (FILES cta-frontend.logrotate DESTINATION /etc/logrotate.d RENAME cta-frontend)

set (OPAQUE_QUERY_CMD_SRC_FILES
  CmdLineTool.cpp
  OpaqueQueryCmd.cpp
  OpaqueQueryCmdLineArgs.cpp
  OpaqueQueryCmdMain.cpp)

add_executable (cta-xrootd_plugins-opaque-query ${OPAQUE_QUERY_CMD_SRC_FILES})
add_dependencies (cta-xrootd_plugins-opaque-query generate_notification.pb.h)
target_link_libraries (cta-xrootd_plugins-opaque-query ctacommon)

set (WRITE_NOTIFICATION_MSG_CMD_SRC_FILES
  CmdLineTool.cpp
  WriteNotificationMsgCmd.cpp
  WriteNotificationMsgCmdLineArgs.cpp
  WriteNotificationMsgCmdMain.cpp)

add_executable (cta-xrootd_plugins-write-notification-msg ${WRITE_NOTIFICATION_MSG_CMD_SRC_FILES})
add_dependencies (cta-xrootd_plugins-write-notification-msg generate_notification.pb.h)
target_link_libraries (cta-xrootd_plugins-write-notification-msg ctacommon ctaeosmessages)
