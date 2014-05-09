find_package( ZLIB REQUIRED )

add_library(castorTapeServerDaemon 
  ../client/ClientProxy.cpp 
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
  MountSessionAcceptHandler.cpp
  DriveCatalogue.cpp
  ../drive/Drive.cpp
  TapeDaemonMain.cpp
  VdqmAcceptHandler.cpp
  VdqmConnectionHandler.cpp
  GlobalStatusReporter.cpp)

add_library(castorTapeServerDaemonTestDummies
  ../client/ClientSimulator.cpp 
  ../../tpcp/TpcpCommand.cpp
  ../../tpcp/StreamOperators.cpp
  ../../tpcp/Helper.cpp
  ../../tpcp/TapeFseqRange.cpp)

add_executable(tapeserverd TapeDaemon.cpp)

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
  ${ZLIB_LIBRARIES})

install (TARGETS tapeserverd DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallAdmManPage (tapeserverd)
CastorInstallLogrotate (castor-tapeserver-server)
CastorInstallSysconfigExample (tapeserverd)
CastorInstallInitScript (tapeserverd)