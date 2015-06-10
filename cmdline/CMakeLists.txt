cmake_minimum_required (VERSION 2.6)

list (APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
find_package (xrootd REQUIRED)
include_directories (${XROOTD_INCLUDE_DIR} ${XROOTD_PRIVATE_INCLUDE_DIR} ${CMAKE_SOURCE_DIR})

add_executable (CTA_cmd CTACmdMain.cpp CTACmd.cpp)
target_link_libraries (CTA_cmd ${XROOTD_XRDCL_LIB})

add_executable (CTA_copycmd CTACopyCmdMain.cpp CTACopyCmd.cpp)
target_link_libraries (CTA_copycmd ${XROOTD_XRDCL_LIB})

add_executable (CTA_dircmd CTADirCmdMain.cpp CTADirCmd.cpp)
target_link_libraries (CTA_dircmd ${XROOTD_XRDCL_LIB})
