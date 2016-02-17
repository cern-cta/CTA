cmake_minimum_required (VERSION 2.6)

list (APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
find_package (xrootd REQUIRED)
include_directories (${XROOTD_INCLUDE_DIR} ${XROOTD_PRIVATE_INCLUDE_DIR} ${CMAKE_SOURCE_DIR})

add_executable (cta CTACmdMain.cpp CTACmd.cpp)
target_link_libraries (cta ${XROOTD_XRDCL_LIB} ctacommon cryptopp ctautils)

include_directories (${CMAKE_SOURCE_DIR}/tapeserver/)
add_executable (ctaAddAdminUser CTAAddAdminUser.cpp )
target_link_libraries (ctaAddAdminUser ctacommon castorlog
  ctautils protobuf ctascheduler ctanameserver)

install (TARGETS cta ctaAddAdminUser DESTINATION usr/bin)

