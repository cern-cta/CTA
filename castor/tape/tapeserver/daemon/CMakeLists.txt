add_executable(tapeserverd
  AdminAcceptHandler.cpp
  DebugMountSessionForVdqmProtocol.cpp
  DriveCatalogue.cpp
  Rmc.cpp
  RmcImpl.cpp
  TapeDaemon.cpp
  TapeDaemonMain.cpp
  Vdqm.cpp
  VdqmAcceptHandler.cpp
  VdqmConnectionHandler.cpp
  VdqmImpl.cpp
  Vmgr.cpp
  VmgrImpl.cpp)

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
  Exception.cpp
  ClientProxy.cpp)

target_link_libraries(tapeserver-mm 
TapeDrive Exception SCSI System Utils File 
castorcommon castorclient castorTapeServerThreading castortapeutils 
castortapegatewayprotocol)

add_library(tapeserver ClientProxy.cpp MountSession.cpp)

add_library(tapeserverdTest 
  ClientSimulator.cpp 
  ../../tpcp/TpcpCommand.cpp
  ../../tpcp/StreamOperators.cpp
  ../../tpcp/Helper.cpp
  ../../tpcp/TapeFseqRange.cpp)
