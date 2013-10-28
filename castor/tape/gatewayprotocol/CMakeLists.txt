#
#                      castor/tape/gatewayprotocol/CMakeLists.txt
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
# Rules to build and install libcastortapegatewayprotocol.so
################################################################################
set (TAPEGATEWAYPROTOCOL_LIB_SRC_FILES
  ../tapegateway/BaseFileInfoStruct.cpp
  ../tapegateway/ClientType.cpp
  ../tapegateway/DumpNotification.cpp
  ../tapegateway/DumpParameters.cpp
  ../tapegateway/DumpParametersRequest.cpp
  ../tapegateway/EndNotification.cpp
  ../tapegateway/EndNotificationErrorReport.cpp
  ../tapegateway/FileErrorReportStruct.cpp
  ../tapegateway/FileMigratedNotificationStruct.cpp
  ../tapegateway/FileMigrationReportList.cpp
  ../tapegateway/FileRecalledNotificationStruct.cpp
  ../tapegateway/FileRecallReportList.cpp
  ../tapegateway/FilesListRequest.cpp
  ../tapegateway/FilesToMigrateList.cpp
  ../tapegateway/FilesToMigrateListRequest.cpp
  ../tapegateway/FilesToRecallList.cpp
  ../tapegateway/FilesToRecallListRequest.cpp
  ../tapegateway/FileToMigrateStruct.cpp
  ../tapegateway/FileToRecallStruct.cpp
  ../tapegateway/GatewayMessage.cpp
  ../tapegateway/NoMoreFiles.cpp
  ../tapegateway/NotificationAcknowledge.cpp
  ../tapegateway/PingNotification.cpp
  ../tapegateway/PositionCommandCode.cpp
  ../tapegateway/Volume.cpp
  ../tapegateway/VolumeMode.cpp
  ../tapegateway/VolumeRequest.cpp
  ../../io/StreamDumpNotificationCnv.cpp
  ../../io/StreamDumpParametersCnv.cpp
  ../../io/StreamDumpParametersRequestCnv.cpp
  ../../io/StreamEndNotificationCnv.cpp
  ../../io/StreamEndNotificationErrorReportCnv.cpp
  ../../io/StreamFileErrorReportStructCnv.cpp
  ../../io/StreamFileMigratedNotificationStructCnv.cpp
  ../../io/StreamFileMigrationReportListCnv.cpp
  ../../io/StreamFileRecalledNotificationStructCnv.cpp
  ../../io/StreamFileRecallReportListCnv.cpp
  ../../io/StreamFilesListRequestCnv.cpp
  ../../io/StreamFilesToMigrateListCnv.cpp
  ../../io/StreamFilesToMigrateListRequestCnv.cpp
  ../../io/StreamFilesToRecallListCnv.cpp
  ../../io/StreamFilesToRecallListRequestCnv.cpp
  ../../io/StreamFileToMigrateStructCnv.cpp
  ../../io/StreamFileToRecallStructCnv.cpp
  ../../io/StreamNoMoreFilesCnv.cpp
  ../../io/StreamNotificationAcknowledgeCnv.cpp
  ../../io/StreamPingNotificationCnv.cpp
  ../../io/StreamVolumeCnv.cpp
  ../../io/StreamVolumeRequestCnv.cpp)
add_library (castortapegatewayprotocol SHARED
  ${TAPEGATEWAYPROTOCOL_LIB_SRC_FILES})
set_target_properties (castortapegatewayprotocol
  PROPERTIES SOVERSION ${MAJOR_CASTOR_VERSION})
target_link_libraries (castortapegatewayprotocol castorclient castorcommon)
install (TARGETS castortapegatewayprotocol DESTINATION ${CASTOR_DEST_LIB_DIR})
