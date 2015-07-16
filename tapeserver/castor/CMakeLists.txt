cmake_minimum_required (VERSION 2.6)

add_subdirectory (legacymsg)
add_subdirectory (mediachanger)
add_subdirectory (utils)
add_subdirectory (messages)
add_subdirectory (log)
add_subdirectory (server)
add_subdirectory (tape)

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
  io/ServerSocket.cpp
  io/StreamAddress.cpp
  io/StreamBaseCnv.cpp
  io/StreamBaseAddressCnv.cpp
  io/StreamClientCnv.cpp
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
  io/StreamGCFilesResponseCnv.cpp
  io/StreamGCLocalFileCnv.cpp
  io/StreamGCFileCnv.cpp
  io/StreamMessageAckCnv.cpp
  io/StreamPtrCnv.cpp
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
  io/StreamStagePutDoneRequestCnv.cpp
  io/StreamStagePutRequestCnv.cpp
  io/StreamStageRmRequestCnv.cpp
  io/StreamStgFilesDeletedCnv.cpp
  io/StreamStgFilesDeletedResponseCnv.cpp
  io/StreamAbortResponseCnv.cpp
  io/StreamFileQryResponseCnv.cpp
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

add_library (castorclient SHARED
  ${CLIENT_LIB_SRC_FILES})

# Relax compilation of old code full of pointer casting. Funnily enough,
# the compiler only complains in -O2 mode (RelWithDebInfo in cmake).
set_property(SOURCE ../client/src/stager/stager_errmsg.c APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
CastorSetLibraryVersions (castorclient)
target_link_libraries (castorclient castordlf castorutils)
