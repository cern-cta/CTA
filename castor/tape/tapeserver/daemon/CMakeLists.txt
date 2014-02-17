add_executable(tapeserverd tapeserverd.cpp)

target_link_libraries(tapeserverd Exception SCSI System Utils File castorcommon
castorclient)

add_library(tapeserver clientInterface.cpp)

add_library(tapeserverdTest 
  clientSimulator.cpp 
  ../../tpcp/TpcpCommand.cpp
  ../../tpcp/StreamOperators.cpp
  ../../tpcp/Helper.cpp
  ../../tpcp/TapeFseqRange.cpp)