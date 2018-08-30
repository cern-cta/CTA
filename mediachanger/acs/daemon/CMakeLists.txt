cmake_minimum_required (VERSION 2.6)

find_package(Protobuf3 REQUIRED)
find_package(openssl REQUIRED)
find_package(zeromq REQUIRED)

include_directories (${PROTOBUF3_INCLUDE_DIRS})


PROTOBUF3_GENERATE_CPP(WDMsgSources WDMsgHeaders WatchdogMessage.proto)

SET_SOURCE_FILES_PROPERTIES(${WDMsgHeaders} PROPERTIES HEADER_FILE_ONLY TRUE)


set (ACS_DAEMON_SRC_FILES
  AcsDaemon.cpp
  AcsDaemonMain.cpp
  AcsdConfiguration.cpp
  AcsRequest.cpp
  AcsPendingRequests.cpp
  AcsdCmdLine.cpp
  AcsDismountTape.cpp
  AcsMessageHandler.cpp
  AcsMountTapeReadWrite.cpp 
  AcsMountTapeReadOnly.cpp
  AcsForceDismountTape.cpp
  Constants.cpp
  AcsRequestDismountTape.cpp
)
add_executable (cta-acsd ${ACS_DAEMON_SRC_FILES})

set_target_properties (cta-acsd PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)

target_link_libraries(
  cta-acsd
  ctacommon
  ctareactor 
  cta-acs
  ctamediachanger 
  zmq
  ${STK_LIBRARIES})

set_property (TARGET cta-acsd APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

install (FILES cta-acsd.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)
install (TARGETS cta-acsd DESTINATION /usr/bin)
install (FILES cta-acsd.logrotate DESTINATION /etc/logrotate.d RENAME cta-acsd)
install (FILES cta-acsd.service DESTINATION /etc/systemd/system)

add_library (ctamediachangeracsdaemonunittests SHARED AcsdCmdLineTest.cpp AcsdCmdLine.cpp)
set_property(TARGET ctamediachangeracsdaemonunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctamediachangeracsdaemonunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctamediachangeracsdaemonunittests
 ctamediachangerunittests)

install (TARGETS ctamediachangeracsdaemonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})



