cmake_minimum_required (VERSION 2.6)

find_package(Protobuf3 REQUIRED)
find_package(openssl REQUIRED)
find_package(zeromq REQUIRED)

#file(GLOB ProtoFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
#PROTOBUF3_GENERATE_CPP(ProtoSources ProtoHeaders ${ProtoFiles})
include_directories (${PROTOBUF3_INCLUDE_DIRS})
#add_library(ctamediachangeracsdaemonprotobuf ${ProtoSources})


PROTOBUF3_GENERATE_CPP(WDMsgSources WDMsgHeaders WatchdogMessage.proto)

SET_SOURCE_FILES_PROPERTIES(${WDMsgHeaders} PROPERTIES HEADER_FILE_ONLY TRUE)


set (ACS_DAEMON_SRC_FILES
  AcsDaemon.cpp
  AcsDaemonMain.cpp
  AcsdConfiguration.cpp
  AcsPendingRequests.cpp
  AcsdCmdLine.cpp
  AcsDismountTape.cpp
  AcsMessageHandler.cpp
  AcsMountTapeReadWrite.cpp 
  AcsMountTapeReadOnly.cpp
  AcsForceDismountTape.cpp
  Constants.cpp
  #AcsRequestDismountTape.cpp
  #AcsLibraryInteraction.cpp
)
add_executable (cta-acsd ${ACS_DAEMON_SRC_FILES})

set_target_properties (cta-acsd PROPERTIES
  COMPILE_FLAGS -I/usr/include/CDK
  COMPILE_DEFINITIONS LINUX)

target_link_libraries(
  cta-acsd
  ctacommon
  ctareactor 
 CtaAcsDaemon
 ctamediachanger 
 # ctamediachangeracsdaemonprotobuf 
  zmq
  ${STK_LIBRARIES})

add_library (ctamediachangeracsdaemonunittests SHARED AcsdCmdLineTest.cpp AcsdCmdLine.cpp)
set_property(TARGET ctamediachangeracsdaemonunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctamediachangeracsdaemonunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctamediachangeracsdaemonunittests
 ctamediachangerunittests)

install (TARGETS ctamediachangeracsdaemonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

