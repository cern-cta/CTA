cmake_minimum_required (VERSION 2.6)

# Old CASTOR's tapeserverd daemon code
add_subdirectory (castor)

# CTA's cta-taped code
add_subdirectory (daemon)
add_subdirectory (session)
# The tape session's threads are in a separate directory (session, but compiled
# from the previous one to create a single library).

include_directories (${PROTOBUF3_INCLUDE_DIRS})
add_executable (cta-taped cta-taped.cpp)
find_package(Protobuf3 REQUIRED)
target_link_libraries(cta-taped
  ctatapedaemon ctacommon ${PROTOBUF3_LIBRARIES})
set_property (TARGET cta-taped APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
if (OCCI_SUPPORT)
  set_property (TARGET cta-taped APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif (OCCI_SUPPORT)
install (TARGETS cta-taped DESTINATION usr/bin)
install (FILES TPCONFIG.example DESTINATION /etc/cta)
install (FILES cta-taped.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-taped.logrotate DESTINATION /etc/logrotate.d RENAME cta-taped)
install (FILES cta-taped.sysconfig DESTINATION /etc/sysconfig RENAME cta-taped)
install (FILES cta-taped.service DESTINATION /etc/systemd/system)

# CTA's cta-taped system tests.
add_library(cta-tapedSystemTests SHARED
  cta-tapedSystemtests.cpp)
set_property(TARGET cta-tapedSystemTests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET cta-tapedSystemTests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(cta-tapedSystemTests
  unitTestHelper
  ctacommon)

install(TARGETS cta-tapedSystemTests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
