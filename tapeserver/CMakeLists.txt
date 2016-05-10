cmake_minimum_required (VERSION 2.6)

# Old CASTOR's tapeserverd daemon code
add_subdirectory (castor)
add_subdirectory (test)

# CTA's cta-taped code
add_subdirectory (daemon)
# The tape session's threads are in a separate directory
add_subdirectory (session)

add_executable (cta-taped cta-taped.cpp)
target_link_libraries(cta-taped
  ctatapedaemon ctacommon protobuf)
install (TARGETS cta-taped DESTINATION usr/bin)

# CTA's cta-taped system tests.
add_library(cta-tapedSystemTests SHARED
  cta-tapedSystemtests.cpp)

target_link_libraries(cta-tapedSystemTests
  systemTestHelper
  unitTestHelper
  ctacommon)

install(TARGETS cta-tapedSystemTests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})