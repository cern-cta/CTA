find_package( ZLIB REQUIRED )

add_library(castorTapeServerDaemon 
  AdminAcceptHandler.cpp
  AdminConnectionHandler.cpp
  ../client/ClientProxy.cpp 
  CapabilityUtils.cpp
  CapabilityUtilsImpl.cpp
  DiskReadThreadPool.cpp
  DiskReadTask.cpp
  DiskWriteTask.cpp
  DiskWriteThreadPool.cpp
  DriveCatalogue.cpp
  DriveCatalogueEntry.cpp
  DriveCatalogueSession.cpp
  DriveCatalogueLabelSession.cpp
  DriveCatalogueTransferSession.cpp
  ../drive/Drive.cpp
  GlobalStatusReporter.cpp
  LabelCmdAcceptHandler.cpp
  LabelCmdConnectionHandler.cpp
  LabelSession.cpp
  MigrationMemoryManager.cpp
  MigrationReportPacker.cpp 
  MigrationTaskInjector.cpp
  MountSession.cpp
  MountSessionAcceptHandler.cpp
  RecallMemoryManager.cpp
  RecallTaskInjector.cpp 
  RecallReportPacker.cpp
  SmartCap.cpp
  TapeMessageHandler.cpp
  TapeDaemonMain.cpp
  TapeWriteTask.cpp
  TaskWatchDog.cpp
  VdqmAcceptHandler.cpp
  VdqmConnectionHandler.cpp)

target_link_libraries(castorTapeServerDaemon castorMessages castortapereactor)

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
  castorlegacymsg
  castortapegatewayprotocol
  castortapereactor
  castorTapeServerThreading
  castortapeutils
  ${LIBCAP_LIB}
  ${ZLIB_LIBRARIES}
  castorMessages)

install (TARGETS tapeserverd DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallAdmManPage (tapeserverd)
CastorInstallLogrotate (castor-tapeserver-server)
CastorInstallSysconfigExample (tapeserverd)
CastorInstallInitScript (tapeserverd)
CastorInstallUdevRule (99-tape.rules)
