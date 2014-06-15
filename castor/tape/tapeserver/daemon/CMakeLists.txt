find_package( ZLIB REQUIRED )

add_library(castorTapeServerDaemon 
  ../client/ClientProxy.cpp 
  CapabilityUtils.cpp
  CapabilityUtilsImpl.cpp
  LabelSession.cpp
  RecallTaskInjector.cpp 
  MigrationReportPacker.cpp 
  RecallReportPacker.cpp
  DiskWriteThreadPool.cpp
  DiskReadThreadPool.cpp
  TapeWriteTask.cpp
  DiskReadTask.cpp
  DiskWriteTask.cpp
  MigrationTaskInjector.cpp
  MigrationMemoryManager.cpp
  RecallMemoryManager.cpp
  MountSession.cpp
  AdminAcceptHandler.cpp
  AdminConnectionHandler.cpp
  MountSessionAcceptHandler.cpp
  MountSessionConnectionHandler.cpp
  DriveCatalogue.cpp
  DriveCatalogueEntry.cpp
  ../drive/Drive.cpp
  TapeDaemonMain.cpp
  VdqmAcceptHandler.cpp
  VdqmConnectionHandler.cpp
  GlobalStatusReporter.cpp
  SmartCap.cpp)

add_library(castorTapeServerDaemonTestDummies
  ../client/ClientSimulator.cpp 
  ../../tpcp/TpcpCommand.cpp
  ../../tpcp/StreamOperators.cpp
  ../../tpcp/Helper.cpp
  ../../tpcp/TapeFseqRange.cpp)

add_executable(tapeserverd TapeDaemon.cpp)

find_library (LIBCAP_LIB cap)
if (LIBCAP_LIB)
  message(STATUS "Found the libcap library")
  set (LIBCAPUNITTEST_SRC_FILES
    ../castor/tape/tapeserver/daemon/SmartCapTest.cpp)
else (LIBCAP_LIB)
  message(STATUS "Failed to find libcap library")
endif (LIBCAP_LIB)

target_link_libraries(tapeserverd
  castorTapeServerDaemon
  Exception
  SCSI
  System
  Utils
  File
  castorcommon
  castorclient
  castortapegatewayprotocol
  castortapelegacymsg
  castorTapeServerThreading
  ${LIBCAP_LIB}
  ${ZLIB_LIBRARIES})

install (TARGETS tapeserverd DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallAdmManPage (tapeserverd)
CastorInstallLogrotate (castor-tapeserver-server)
CastorInstallSysconfigExample (tapeserverd)
CastorInstallInitScript (tapeserverd)
CastorInstallUdevRule (99-tape.rules)
