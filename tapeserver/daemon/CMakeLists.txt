cmake_minimum_required (VERSION 2.6)

find_package(Protobuf REQUIRED)

PROTOBUF_GENERATE_CPP(WDMsgSources WDMsgHeaders WatchdogMessage.proto)

SET_SOURCE_FILES_PROPERTIES(${WDMsgHeaders} PROPERTIES HEADER_FILE_ONLY TRUE)
SET_SOURCE_FILES_PROPERTIES(DriveHandler.cpp PROPERTIES OBJECT_DEPENDS ${WDMsgHeaders})

add_library(ctatapedaemon
  ${WDMsgSources}
  CommandLineParams.cpp
  ConfigurationFile.cpp
  DriveHandler.cpp
  SignalHandler.cpp
  SourcedParameter.cpp
  SubprocessHandler.cpp
  ProcessManager.cpp
  TapedConfiguration.cpp
  TapeDaemon.cpp
  TapedProxy.cpp
  TpconfigLine.cpp
  Tpconfig.cpp)

add_library(ctadaemonunittests SHARED
  ConfigurationFileTests.cpp
  TapedConfigurationTests.cpp
  TpconfigTests.cpp)

add_library(ctadaemonunittests-multiprocess SHARED
  ProcessManagerTests.cpp
  SignalHandlerTests.cpp)

target_link_libraries(ctadaemonunittests
  ctatapedaemon
  unitTestHelper)

target_link_libraries(ctadaemonunittests-multiprocess
  ctatapedaemon
  unitTestHelper)

install(TARGETS ctadaemonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install(TARGETS ctadaemonunittests-multiprocess DESTINATION usr/${CMAKE_INSTALL_LIBDIR})