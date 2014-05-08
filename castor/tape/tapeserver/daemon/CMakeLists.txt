add_executable(tapeserverd
  AdminAcceptHandler.cpp
  MountSessionAcceptHandler.cpp
  DebugMountSessionForVdqmProtocol.cpp
  DriveCatalogue.cpp
  TapeDaemon.cpp
  TapeDaemonMain.cpp
  VdqmAcceptHandler.cpp
  VdqmConnectionHandler.cpp)

target_link_libraries(tapeserverd
  Exception
  SCSI
  System
  Utils
  File
  castorcommon
  castorclient
  castortapegatewayprotocol
  castortapelegacymsg)

install (TARGETS tapeserverd DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallAdmManPage (tapeserverd)
CastorInstallLogrotate (castor-tapeserver-server)
CastorInstallSysconfigExample (tapeserverd)
CastorInstallInitScript (tapeserverd)

add_library(tapeserver 
  ../client/ClientProxy.cpp 
  MountSession.cpp 
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
  GlobalStatusReporter.cpp) 


add_library(tapeserverdTest 
  ../client/ClientSimulator.cpp 
  ../../tpcp/TpcpCommand.cpp
  ../../tpcp/StreamOperators.cpp
  ../../tpcp/Helper.cpp
  ../../tpcp/TapeFseqRange.cpp)
