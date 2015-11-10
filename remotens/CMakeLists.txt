cmake_minimum_required (VERSION 2.6)

set (CTA_REMOTENS_SRC_FILES
  EosNS.cpp
  MockRemoteNS.cpp
  RemoteNS.cpp
  RemoteNSDispatcher.cpp)

add_library (ctaremotens SHARED
  ${CTA_REMOTENS_SRC_FILES})

find_package (xrootd REQUIRED)
include_directories (${XROOTD_INCLUDE_DIR} ${XROOTD_PRIVATE_INCLUDE_DIR} ${CMAKE_SOURCE_DIR})

target_link_libraries (ctaremotens ${XROOTD_XRDCL_LIB} ctacommon cryptopp)
install (TARGETS ctaremotens DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_library (ctaremotensunittests SHARED
  MockRemoteNS.cpp
  MockRemoteFullFS.cpp
  MockRemoteNSTest.cpp
  RemoteNSDispatcherTest.cpp 
  RemoteNSFactory.cpp)

target_link_libraries(ctaremotensunittests
  ctaremotens)
