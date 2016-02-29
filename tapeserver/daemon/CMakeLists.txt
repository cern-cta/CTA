cmake_minimum_required (VERSION 2.6)

add_library(ctatapedaemon
  CommandLineParams.cpp
  ConfigurationFile.cpp
  GlobalConfiguration.cpp
  TapeDaemon.cpp
  TpconfigLine.cpp
  TpconfigLines.cpp)

add_library(ctadaemonunittests SHARED
  ConfigurationFileTests.cpp)

target_link_libraries(ctadaemonunittests
  ctatapedaemon
  unitTestHelper)