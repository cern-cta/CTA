add_executable(tapeserverd TapeDaemon.cpp TapeDaemonMain.cpp)

target_link_libraries(tapeserverd Exception SCSI System Utils File castorcommon
castorclient)

add_library(tapeserver ClientInterface.cpp)

add_library(tapeserverdTest 
  ClientSimulator.cpp 
  ../../tpcp/TpcpCommand.cpp
  ../../tpcp/StreamOperators.cpp
  ../../tpcp/Helper.cpp
  ../../tpcp/TapeFseqRange.cpp)
