add_executable(tapeserverd
  DriveCatalogue.cpp
  TapeDaemon.cpp
  TapeDaemonMain.cpp
  Vdqm.cpp
  VdqmAcceptHandler.cpp
  AdminAcceptHandler.cpp
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
  ../client/ClientProxy.cpp
  MigrationReportPacker.cpp
  RecallReportPacker.cpp)

target_link_libraries(tapeserver-mm 
TapeDrive Exception SCSI System Utils File 
castorcommon castorclient castorTapeServerThreading castortapeutils 
castortapegatewayprotocol)

add_library(tapeserver 
  ../client/ClientProxy.cpp 
  MountSession.cpp 
  RecallTaskInjector.cpp 
  MigrationReportPacker.cpp 
  RecallReportPacker.cpp)

add_library(tapeserverdTest 
  ../client/ClientSimulator.cpp 
  ../../tpcp/TpcpCommand.cpp
  ../../tpcp/StreamOperators.cpp
  ../../tpcp/Helper.cpp
  ../../tpcp/TapeFseqRange.cpp)
