#
#                      castor/CMakeLists.txt
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

if (${COMPILE_SERVER} STREQUAL "1")
  add_subdirectory (db)
  add_subdirectory (gc)
  add_subdirectory (rh)
  add_subdirectory (job)
  add_subdirectory (scheduler)
  add_subdirectory (stager/daemon)
  add_subdirectory (tape)
  add_subdirectory (vdqm)
endif (${COMPILE_SERVER} STREQUAL "1")

################################################################################
# Rules to build and install libcastorclient.so
################################################################################
set (CLIENT_LIB_SRC_FILES
  BaseAddress.cpp
  VectorAddress.cpp
  BaseCnvSvc.cpp
  BaseObject.cpp
  BaseSvc.cpp
  Constants.cpp
  Converters.cpp
  Factories.cpp
  IObject.cpp
  IClient.cpp
  IClientFactory.cpp
  MessageAck.cpp
  ObjectCatalog.cpp
  ObjectSet.cpp
  Services.cpp
  System.cpp
  bwlist/Privilege.cpp
  bwlist/ChangePrivilege.cpp
  bwlist/ListPrivileges.cpp
  bwlist/ListPrivilegesResponse.cpp
  bwlist/BWUser.cpp
  bwlist/RequestType.cpp
  client/BaseClient.cpp
  client/BasicResponseHandler.cpp
  client/VectorResponseHandler.cpp
  dlf/Dlf.cpp
  dlf/IPAddress.cpp
  dlf/Message.cpp
  dlf/Param.cpp
  dlf/TimeStamp.cpp
  io/AbstractSocket.cpp
  io/AbstractTCPSocket.cpp
  io/AuthServerSocket.cpp
  io/AuthClientSocket.cpp
  io/IpAndPort.cpp
  io/UDPSocket.cpp
  io/PipeSocket.cpp
  io/ClientSocket.cpp
  io/io.cpp
  io/PollEventHandler.cpp
  io/PollReactor.cpp
  io/PollReactorImpl.cpp
  io/ServerSocket.cpp
  io/StreamAddress.cpp
  io/StreamBaseCnv.cpp
  io/StreamBaseAddressCnv.cpp
  io/StreamClientCnv.cpp
  io/StreamStartResponseCnv.cpp
  io/StreamCnvSvc.cpp
  io/StreamDiskCopyForRecallCnv.cpp
  io/StreamDiskCopyInfoCnv.cpp
  io/StreamEndResponseCnv.cpp
  io/StreamFileResponseCnv.cpp
  io/StreamFiles2DeleteCnv.cpp
  io/StreamFilesDeletedCnv.cpp
  io/StreamNsFilesDeletedCnv.cpp
  io/StreamNsFilesDeletedResponseCnv.cpp
  io/StreamFilesDeletionFailedCnv.cpp
  io/StreamGetUpdateStartRequestCnv.cpp
  io/StreamGetUpdateStartResponseCnv.cpp
  io/StreamGCFilesResponseCnv.cpp
  io/StreamGCLocalFileCnv.cpp
  io/StreamGCFileCnv.cpp
  io/StreamGetUpdateDoneCnv.cpp
  io/StreamGetUpdateFailedCnv.cpp
  io/StreamMessageAckCnv.cpp
  io/StreamMoverCloseRequestCnv.cpp
  io/StreamPtrCnv.cpp
  io/StreamPutFailedCnv.cpp
  io/StreamPutStartRequestCnv.cpp
  io/StreamQueryParameterCnv.cpp
  io/StreamBasicResponseCnv.cpp
  io/StreamSetFileGCWeightCnv.cpp
  io/StreamStringResponseCnv.cpp
  io/StreamSubRequestCnv.cpp
  io/StreamStageAbortRequestCnv.cpp
  io/StreamStageFileQueryRequestCnv.cpp
  io/StreamStageGetRequestCnv.cpp
  io/StreamStagePrepareToGetRequestCnv.cpp
  io/StreamStagePrepareToPutRequestCnv.cpp
  io/StreamStagePrepareToUpdateRequestCnv.cpp
  io/StreamStagePutDoneRequestCnv.cpp
  io/StreamStagePutRequestCnv.cpp
  io/StreamStageRmRequestCnv.cpp
  io/StreamStageUpdateRequestCnv.cpp
  io/StreamThreadNotificationCnv.cpp
  io/StreamStgFilesDeletedCnv.cpp
  io/StreamStgFilesDeletedResponseCnv.cpp
  io/StreamAbortResponseCnv.cpp
  io/StreamFileQryResponseCnv.cpp
  io/StreamFirstByteWrittenCnv.cpp
  io/StreamIOResponseCnv.cpp
  io/StreamDiskPoolQueryResponseCnv.cpp
  io/StreamFileSystemDescriptionCnv.cpp
  io/StreamDiskServerDescriptionCnv.cpp
  io/StreamDiskPoolQueryCnv.cpp
  io/StreamDiskPoolQueryCnvOld.cpp
  io/StreamVersionQueryCnv.cpp
  io/StreamVersionResponseCnv.cpp
  io/StreamPrivilegeCnv.cpp
  io/StreamListPrivilegesCnv.cpp
  io/StreamListPrivilegesResponseCnv.cpp
  io/StreamChangePrivilegeCnv.cpp
  io/StreamBWUserCnv.cpp
  io/StreamRequestTypeCnv.cpp
  io/StreamNsFileIdCnv.cpp
  log/Logger.cpp
  log/LoggerImplementation.cpp
  log/StringLogger.cpp
  log/LogContext.cpp
  log/Message.cpp
  log/Param.cpp
  metrics/Counter.cpp
  metrics/InternalCounter.cpp
  metrics/Histogram.cpp
  metrics/MetricsCollector.cpp
  metrics/UpdateThread.cpp
  metrics/ObjTypeCounter.cpp
  query/DiskPoolQueryResponse.cpp
  query/FileSystemDescription.cpp
  query/DiskServerDescription.cpp
  query/DiskPoolQuery.cpp
  query/DiskPoolQueryType.cpp
  query/VersionQuery.cpp
  query/VersionResponse.cpp
  replier/ClientConnection.cpp
  replier/RequestReplier.cpp
  rh/Client.cpp
  rh/StartResponse.cpp
  rh/EndResponse.cpp
  rh/FileResponse.cpp
  rh/GetUpdateStartResponse.cpp
  rh/GCFilesResponse.cpp
  rh/Response.cpp
  rh/StringResponse.cpp
  rh/AbortResponse.cpp
  rh/FileQryResponse.cpp
  rh/IOResponse.cpp
  rh/BasicResponse.cpp
  server/AuthListenerThreadPool.cpp
  server/BaseThreadPool.cpp
  server/Daemon.cpp
  server/DynamicThreadPool.cpp
  server/ListenerThreadPool.cpp
  server/MultiThreadedDaemon.cpp
  server/TCPListenerThreadPool.cpp
  server/UDPListenerThreadPool.cpp
  server/ForkedProcessPool.cpp
  server/SignalThreadPool.cpp
  server/DbAlertedThreadPool.cpp
  server/Mutex.cpp
  server/NotifierThread.cpp
  server/SelectProcessThread.cpp
  server/ThreadNotification.cpp
  server/Queue.cpp
  stager/CastorFile.cpp
  stager/DiskCopyInfo.cpp
  stager/DiskCopyForRecall.cpp
  stager/DiskCopyStatusCodes.cpp
  stager/DiskFile.cpp
  stager/DiskServerStatusCode.cpp
  stager/FileSystemStatusCodes.cpp
  stager/DLFInit.cpp
  stager/FileClass.cpp
  stager/FileRequest.cpp
  stager/ErrorFileRequest.cpp
  stager/Files2Delete.cpp
  stager/FilesDeleted.cpp
  stager/NsFilesDeleted.cpp
  stager/NsFilesDeletedResponse.cpp
  stager/StgFilesDeleted.cpp
  stager/StgFilesDeletedResponse.cpp
  stager/FilesDeletionFailed.cpp
  stager/FirstByteWritten.cpp
  stager/GetUpdateDone.cpp
  stager/GetUpdateFailed.cpp
  stager/GetUpdateStartRequest.cpp
  stager/GCLocalFile.cpp
  stager/GCFile.cpp
  stager/GCFileList.cpp
  stager/MoverCloseRequest.cpp
  stager/PutStartRequest.cpp
  stager/QryRequest.cpp
  stager/PutFailed.cpp
  stager/Request.cpp
  stager/RequestQueryType.cpp
  stager/RemoteGCSvc.cpp
  stager/RemoteJobSvc.cpp
  stager/IJobSvcCInt.cpp
  stager/StartRequest.cpp
  stager/SvcClass.cpp
  stager/QueryParameter.cpp
  stager/SetFileGCWeight.cpp
  stager/StageAbortRequest.cpp
  stager/StageFileQueryRequest.cpp
  stager/StageGetRequest.cpp
  stager/StagePrepareToGetRequest.cpp
  stager/StagePrepareToPutRequest.cpp
  stager/StagePrepareToUpdateRequest.cpp
  stager/StageRepackRequest.cpp
  stager/StageRepackRequestStatusCodes.cpp
  stager/StagePutDoneRequest.cpp
  stager/StagePutRequest.cpp
  stager/StageQueryResult.cpp
  stager/StageRmRequest.cpp
  stager/StageUpdateRequest.cpp
  stager/SubRequest.cpp
  stager/SubRequestStatusCodes.cpp
  stager/SubRequestGetNextStatusCodes.cpp
  stager/TapeVid.cpp
  stager/NsFileId.cpp
  stager/BulkRequestResult.cpp
  stager/FileResult.cpp
  utils/SmartFd.cpp
  utils/SmartFILEPtr.cpp
  utils/utils.cpp
  vdqm/ClientIdentification.cpp
  vdqm/TapeDrive.cpp
  vdqm/DeviceGroupName.cpp
  vdqm/TapeAccessSpecification.cpp
  vdqm/TapeDriveCompatibility.cpp
  vdqm/TapeDriveDedication.cpp
  vdqm/TapeDriveStatusCodes.cpp
  vdqm/TapeRequest.cpp
  vdqm/TapeRequestStatusCodes.cpp
  vdqm/TapeServer.cpp
  vdqm/TapeServerStatusCodes.cpp
  vdqm/VdqmTape.cpp
  ../client/src/stager/stager_client_api_common.cpp
  ../client/src/stager/stager_client_commandline.cpp
  ../client/src/stager/stager_client_changePrivilege.c
  ../client/src/stager/stager_client_api_get.cpp
  ../client/src/stager/stager_client_api_put.cpp
  ../client/src/stager/stager_client_api_update.cpp
  ../client/src/stager/stager_client_api_open.cpp
  ../client/src/stager/stager_client_api_query.cpp
  ../client/src/stager/stager_client_api_rm.cpp
  ../client/src/stager/stager_client_api_setFileGCWeight.cpp
  ../client/src/stager/stager_client_api_version.cpp
  ../client/src/stager/stager_client_api_changePrivilege.cpp
  ../client/src/stager/stager_client_api_listPrivileges.cpp
  ../client/src/stager/stager_errmsg.c
  ../client/src/stager/stager_mapper.c)
add_library (castorclient SHARED ${CLIENT_LIB_SRC_FILES})
# Relax compilation of old code full of pointer casting. Funnily enough,
# the compiler only complains in -O2 mode (RelWithDebInfo in cmake).
set_property(SOURCE ../client/src/stager/stager_errmsg.c APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
CastorSetLibraryVersions (castorclient)
target_link_libraries (castorclient castordlf)
install (TARGETS castorclient DESTINATION ${CASTOR_DEST_LIB_DIR})

if (${COMPILE_SERVER} STREQUAL "1")
  CastorInstallExample (castor.conf /etc/castor)
  CastorInstallExample (rsyslog.conf.client /etc/castor)
  CastorInstallExample (rsyslog.conf.server /etc/castor)

  ################################################################################
  # Installation of header files for package castor-build-headers
  ################################################################################
  
  # Required by xrootd-xcastor2fs and castor-srmv2
  install(FILES
    BaseObject.hpp
    Constants.hpp
    IObject.hpp
    IService.hpp
    Services.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    client/BaseClient.hpp
    client/IResponseHandler.hpp
    client/VectorResponseHandler.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/client
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    dlf/Dlf.hpp
    dlf/IPAddress.hpp
    dlf/Message.hpp
    dlf/Param.hpp
    dlf/TimeStamp.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/dlf
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    exception/AcceptConnectionInterrupted.hpp
    exception/AlreadyInitialized.hpp
    exception/Backtrace.hpp
    exception/BadAlloc.hpp
    exception/CommandLineNotParsed.hpp
    exception/Communication.hpp
    exception/Exception.hpp
    exception/Internal.hpp
    exception/InvalidArgument.hpp
    exception/InvalidNbArguments.hpp
    exception/NoEntry.hpp
    exception/NotAnOwner.hpp
    exception/OutOfMemory.hpp
    exception/SQLError.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/exception
    PERMISSIONS ${CASTOR_HEADER_PERMS})

  install(FILES
    io/ServerSocket.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/io
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    rh/Client.hpp
    rh/FileResponse.hpp
    rh/IOResponse.hpp
    rh/Response.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/rh
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    stager/DiskCopyStatusCodes.hpp
    stager/FileRequest.hpp
    stager/ICommonSvc.hpp
    stager/Request.hpp
    stager/StageGetRequest.hpp
    stager/StagePrepareToGetRequest.hpp
    stager/StagePrepareToPutRequest.hpp
    stager/StagePutRequest.hpp
    stager/SubRequestGetNextStatusCodes.hpp
    stager/SubRequest.hpp
    stager/SubRequestStatusCodes.hpp
    stager/TapeVid.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/stager
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  # Required by xrootd-xcastor2fs
  install(FILES  io/AbstractSocket.hpp
    io/AbstractTCPSocket.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/io
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    stager/IJobSvc.hpp
    stager/StagePrepareToUpdateRequest.hpp
    stager/StageUpdateRequest.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/stager
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  # Required by castor-srmv2
  install(FILES
    BaseAddress.hpp
    BaseCnvSvc.hpp
    BaseSvc.hpp
    CnvFactory.hpp
    Converters.hpp
    Factories.hpp
    IAddress.hpp
    IClient.hpp
    ICnvFactory.hpp
    ICnvSvc.hpp
    IConverter.hpp
    IFactory.hpp
    ISvcFactory.hpp
    MessageAck.hpp
    ObjectSet.hpp
    SvcFactory.hpp
    System.hpp
    VectorAddress.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    db/DbBaseObj.hpp
    db/DbCnvSvc.hpp
    db/DbParamsSvc.hpp
    db/IDbResultSet.hpp
    db/IDbStatement.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/db
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    db/cnv/DbBaseCnv.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/db/cnv
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    db/ora/OraCnvSvc.hpp
    db/ora/OraCommonSvc.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/db/ora
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    io/ClientSocket.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/io
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    metrics/Counter.hpp
    metrics/Histogram.hpp
    metrics/MetricsCollector.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/metrics
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    query/DiskPoolQueryType.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/query
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    rh/FileQryResponse.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/rh
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    server/BaseThreadPool.hpp
    server/DynamicThreadPool.hpp
    server/IThread.hpp
    server/ListenerThreadPool.hpp
    server/Mutex.hpp
    server/NotifierThread.hpp
    server/Queue.hpp
    server/SelectProcessThread.hpp
    server/SignalThreadPool.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/server
    PERMISSIONS ${CASTOR_HEADER_PERMS})
  
  install(FILES
    stager/NsFileId.hpp
    stager/QryRequest.hpp
    stager/QueryParameter.hpp
    stager/RequestQueryType.hpp
    stager/StageAbortRequest.hpp
    stager/StageFileQueryRequest.hpp
    stager/StageRmRequest.hpp
    stager/SvcClass.hpp
    DESTINATION ${CASTOR_DEST_CPP_HEADERS_DIR}/stager
    PERMISSIONS ${CASTOR_HEADER_PERMS})
endif (${COMPILE_SERVER} STREQUAL "1")
