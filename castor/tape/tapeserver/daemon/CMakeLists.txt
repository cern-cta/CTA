find_package( ZLIB REQUIRED )

add_library(castorTapeServerDaemon 
  AdminAcceptHandler.cpp
  AdminConnectionHandler.cpp
  ../client/ClientProxy.cpp 
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
  TapeServerReporter.cpp
  LabelCmdAcceptHandler.cpp
  LabelCmdConnectionHandler.cpp
  LabelSession.cpp
  CleanerSession.cpp
  MigrationMemoryManager.cpp
  MigrationReportPacker.cpp 
  MigrationTaskInjector.cpp
  DataTransferSession.cpp
  ProcessForker.cpp
  ProcessForkerProxy.cpp
  ProcessForkerMsgType.cpp
  ProcessForkerProxySocket.cpp
  ProcessForkerUtils.cpp
  RecallMemoryManager.cpp
  RecallTaskInjector.cpp 
  RecallReportPacker.cpp
  TapeMessageHandler.cpp
  TapeDaemonMain.cpp
  TapeWriteSingleThread.cpp
  TapeWriteTask.cpp
  TaskWatchDog.cpp
  VdqmAcceptHandler.cpp
  VdqmConnectionHandler.cpp)

target_link_libraries(castorTapeServerDaemon castormessages castortapereactor)

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
  castorlegacymsg
  castorserver
  castortapegatewayprotocol
  castortapereactor
  castorTapeServerThreading
  castortapeutils
  ${LIBCAP_LIB}
  ${ZLIB_LIBRARIES}
  castormessages)

install (TARGETS tapeserverd DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallAdmManPage (tapeserverd)
CastorInstallLogrotate (castor-tapeserver-server)
CastorInstallSysconfigExample (tapeserverd)
CastorInstallInitScript (tapeserverd)
CastorInstallUdevRule (99-tape.rules)
