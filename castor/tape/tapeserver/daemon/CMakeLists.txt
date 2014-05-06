add_executable(tapeserverd
  AdminAcceptHandler.cpp
  MountSessionAcceptHandler.cpp
  DebugMountSessionForVdqmProtocol.cpp
  DriveCatalogue.cpp
  TapeDaemon.cpp
  LabelSession.cpp
  ../drive/Drive.cpp
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

add_executable(tapeserver-mm 
  tapeserver-mm.cpp 
  RecallTaskInjector.cpp
  ../client/ClientProxy.cpp
  MigrationReportPacker.cpp
  DiskWriteThreadPool.cpp
  DiskReadThreadPool.cpp
  TapeWriteTask.cpp
  DiskReadTask.cpp
  DiskWriteTask.cpp
  MigrationTaskInjector.cpp)

find_package( ZLIB REQUIRED )

target_link_libraries(tapeserver-mm 
TapeDrive Exception SCSI System Utils File 
castorcommon castorclient castorTapeServerThreading castortapeutils 
castortapegatewayprotocol tapeserver ${ZLIB_LIBRARIES} )

add_library(tapeserver 
  ../client/ClientProxy.cpp 
  MountSession.cpp
  LabelSession.cpp
  RecallTaskInjector.cpp 
  MigrationReportPacker.cpp 
  RecallReportPacker.cpp
  DiskWriteThreadPool.cpp
  DiskReadThreadPool.cpp
  TapeWriteTask.cpp
  DiskReadTask.cpp
  DiskWriteTask.cpp
  MigrationTaskInjector.cpp) 


add_library(tapeserverdTest 
  ../client/ClientSimulator.cpp 
  ../../tpcp/TpcpCommand.cpp
  ../../tpcp/StreamOperators.cpp
  ../../tpcp/Helper.cpp
  ../../tpcp/TapeFseqRange.cpp)
