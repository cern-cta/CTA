#
#  Copyright (C) 1993-2013 by CERN/IT/DSS
#  All rights reserved
#
# CMakeLists.txt for the CASTOR project.
#

###########################################################################
# In source build forbidding
###########################################################################
CMAKE_MINIMUM_REQUIRED (VERSION 2.6)
SET(CMAKE_DISABLE_SOURCE_CHANGES ON)
SET(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

###########################################################################
# project definition section
###########################################################################
PROJECT(castor)
LIST(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
INCLUDE(cmake/CASTORVersion.cmake)
FIND_PACKAGE(Oracle)

###########################################################################
# Configure header file
###########################################################################
CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/h/patchlevel.h.in ${CMAKE_BINARY_DIR}/h/patchlevel.h)
INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/h)

###########################################################################
# Include the subdirectories of the project
###########################################################################
add_subdirectory(tape)

###########################################################################
# Packaging step (replacing the maketar)
# See http://www.vtk.org/Wiki/CMakeUserUseRPMTools
###########################################################################
SET(CPACK_SOURCE_PACKAGE_FILE_NAME "${PROJECT_NAME}-${CASTOR_VERSION}-${CASTOR_RELEASE}")
INCLUDE(CPack)
INCLUDE(cmake/UseRPMTools.cmake)
IF(RPMTools_FOUND)
  EXECUTE_PROCESS(COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/${PROJECT_NAME}.spec.in.head  ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in)
  EXECUTE_PROCESS(COMMAND ./makespec.in.sh ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in
       WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
  RPMTools_ADD_RPM_TARGETS(${PROJECT_NAME} ${CMAKE_BINARY_DIR}/${PROJECT_NAME}.spec.in)
ENDIF(RPMTools_FOUND)