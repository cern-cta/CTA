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
  DriveCatalogueCleanerSession.cpp
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
  ProcessForkerConnectionHandler.cpp
  ProcessForkerProxy.cpp
  ProcessForkerProxyDummy.cpp
  ProcessForkerProxySocket.cpp
  ProcessForkerUtils.cpp
  RecallMemoryManager.cpp
  RecallTaskInjector.cpp 
  RecallReportPacker.cpp
  TapeMessageHandler.cpp
  TapeDaemonMain.cpp
  TapeReadSingleThread.cpp
  TapeWriteSingleThread.cpp
  TapeWriteTask.cpp
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
  SCSI
  System
  Utils
  File
  TapeDrive
  castorcommon
  castorclient
  castorlegacymsg
  castorserver
  castortapegatewayprotocol
  castortapereactor
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
