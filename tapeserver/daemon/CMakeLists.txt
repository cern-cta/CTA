cmake_minimum_required (VERSION 2.6)

add_library(ctatapedaemon
  CommandLineParams.cpp
  ConfigurationFile.cpp
  SourcedParameter.cpp
  TapedConfiguration.cpp
  TapeDaemon.cpp
  TpconfigLine.cpp
  TpconfigLines.cpp)

add_library(ctadaemonunittests SHARED
  ConfigurationFileTests.cpp
  TapedConfigurationTests.cpp)

target_link_libraries(ctadaemonunittests
  ctatapedaemon
  unitTestHelper)

install(TARGETS ctadaemonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})