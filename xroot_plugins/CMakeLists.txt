cmake_minimum_required (VERSION 2.6)

list (APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
find_package (xrootd REQUIRED)
include_directories (${CMAKE_SOURCE_DIR})
include_directories (${XROOTD_INCLUDE_DIR} ${XROOTD_PRIVATE_INCLUDE_DIR})

add_library (XrdProFst MODULE XrdProFilesystem.cpp XrdProFile.cpp)
target_link_libraries (XrdProFst ctascheduler ctacommon ctanameserver ctaremotens)
