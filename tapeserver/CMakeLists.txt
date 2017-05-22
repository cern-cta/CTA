cmake_minimum_required (VERSION 2.6)

# Old CASTOR's tapeserverd daemon code
add_subdirectory (castor)

# CTA's cta-taped code
add_subdirectory (daemon)
add_subdirectory (session)
# The tape session's threads are in a separate directory (session, but compiled
# from the previous one to create a single library).

add_executable (cta-taped cta-taped.cpp)
target_link_libraries(cta-taped
  ctatapedaemon ctacommon protobuf)
install (TARGETS cta-taped DESTINATION usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-taped.1cta DESTINATION /usr/share/man/man1)

# CTA's cta-taped system tests.
add_library(cta-tapedSystemTests SHARED
  cta-tapedSystemtests.cpp)

target_link_libraries(cta-tapedSystemTests
  unitTestHelper
  ctacommon)

install(TARGETS cta-tapedSystemTests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
