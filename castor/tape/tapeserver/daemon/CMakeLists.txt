add_executable(tapeserverd
  DriveCatalogue.cpp
  TapeDaemon.cpp
  TapeDaemonMain.cpp
  Vdqm.cpp
  VdqmAcceptHandler.cpp
  VdqmConnectionHandler.cpp
  VdqmImpl.cpp)

target_link_libraries(tapeserverd
  Exception
  SCSI
  System
  Utils
  File
  castorcommon
  castorclient
  castortapelegacymsg)

install (TARGETS tapeserverd DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallAdmManPage (tapeserverd)
CastorInstallLogrotate (castor-tapeserver-server)
CastorInstallSysconfigExample (tapeserverd)
CastorInstallInitScript (tapeserverd)

add_executable(tapeserver-mm 
  tapeserver-mm.cpp 
  RecallTaskInjector.cpp
  ClientProxy.cpp
  MigrationReportPacker.cpp
  RecallReportPacker.cpp)

target_link_libraries(tapeserver-mm 
TapeDrive Exception SCSI System Utils File 
castorcommon castorclient castorTapeServerThreading castortapeutils 
castortapegatewayprotocol)

add_library(tapeserver 
  ClientProxy.cpp 
  MountSession.cpp 
  RecallTaskInjector.cpp 
  ClientProxy.cpp 
  MigrationReportPacker.cpp 
  RecallReportPacker.cpp)

add_library(tapeserverdTest 
  ClientSimulator.cpp 
  ../../tpcp/TpcpCommand.cpp
  ../../tpcp/StreamOperators.cpp
  ../../tpcp/Helper.cpp
  ../../tpcp/TapeFseqRange.cpp)
