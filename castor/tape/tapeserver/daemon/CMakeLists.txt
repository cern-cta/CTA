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

add_executable(tapeserver-mm tapeserver-mm.cpp)

target_link_libraries(tapeserver-mm TapeDrive Exception SCSI System Utils File castorcommon castorclient castorTapeServerThreading)

add_library(tapeserver ClientInterface.cpp MountSession.cpp)

add_library(tapeserverdTest 
  ClientSimulator.cpp 
  ../../tpcp/TpcpCommand.cpp
  ../../tpcp/StreamOperators.cpp
  ../../tpcp/Helper.cpp
  ../../tpcp/TapeFseqRange.cpp)
