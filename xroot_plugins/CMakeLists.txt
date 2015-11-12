cmake_minimum_required (VERSION 2.6)

list (APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
find_package (xrootd REQUIRED)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_PRIVATE_INCLUDE_DIR})
include_directories(${CMAKE_BINARY_DIR})
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

add_library (XrdCtaOfs MODULE XrdCtaFilesystem.cpp XrdCtaFile.cpp)
target_link_libraries (XrdCtaOfs ctascheduler ctacommon ctanameserver ctaremotens protobuf CTAObjectStore cryptopp)

INSTALL (TARGETS XrdCtaOfs DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
INSTALL (FILES xrd.cf.cta DESTINATION ${CMAKE_INSTALL_SYSCONFDIR})