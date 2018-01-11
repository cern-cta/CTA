cmake_minimum_required (VERSION 2.6)

find_package(Protobuf3 REQUIRED)

include_directories(${PROJECT_BINARY_DIR}/tapeserver)

PROTOBUF3_GENERATE_CPP(WDMsgSources WDMsgHeaders WatchdogMessage.proto)

SET_SOURCE_FILES_PROPERTIES(${WDMsgHeaders} PROPERTIES HEADER_FILE_ONLY TRUE)
SET_SOURCE_FILES_PROPERTIES(DriveHandler.cpp PROPERTIES OBJECT_DEPENDS ${WDMsgHeaders})

include_directories(${PROTOBUF3_INCLUDE_DIRS})
add_library(ctatapedaemon
  ${WDMsgSources}
  CommandLineParams.cpp
  DriveHandler.cpp
  DriveHandlerProxy.cpp
  GarbageCollectorHandler.cpp
  SignalHandler.cpp
  SourcedParameter.cpp
  SubprocessHandler.cpp
  ProcessManager.cpp
  TapedConfiguration.cpp
  TapeDaemon.cpp
  TapedProxy.cpp
  TpconfigLine.cpp
  Tpconfig.cpp)

if(CMAKE_COMPILER_IS_GNUCC)
  if(GCC_VERSION_GE_4_8_0)
    foreach(CTATAPESERVERDAEMON_LIBRARY_SRC
      DriveHandler.cpp)
      set_property(SOURCE ${CTATAPESERVERDAEMON_LIBRARY_SRC}
        PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
    endforeach(CTATAPESERVERDAEMON_LIBRARY_SRC)
  endif(GCC_VERSION_GE_4_8_0)
endif(CMAKE_COMPILER_IS_GNUCC)

target_link_libraries(ctatapedaemon
  ctatapesession
  ctaTapeServerDaemon
  SCSI
  TapeDrive
  File)

add_library(ctadaemonunittests SHARED
  ConfigurationFileTests.cpp
  TapedConfigurationTests.cpp
  TpconfigTests.cpp
  SourcedParameterTests.cpp)
set_property(TARGET ctadaemonunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadaemonunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

add_library(ctadaemonunittests-multiprocess SHARED
  ProcessManagerTests.cpp
  SignalHandlerTests.cpp)
set_property(TARGET ctadaemonunittests-multiprocess PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadaemonunittests-multiprocess PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctadaemonunittests
  ctatapedaemon
  unitTestHelper)

target_link_libraries(ctadaemonunittests-multiprocess
  ctatapedaemon
  unitTestHelper)

install(TARGETS ctadaemonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install(TARGETS ctadaemonunittests-multiprocess DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

install (FILES cta-taped.conf.example
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
