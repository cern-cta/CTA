cmake_minimum_required (VERSION 2.6)
project (castor)

set (MAJOR_CASTOR_VERSION  2.1)
set (MINOR_CASTOR_VERSION 14.2)
set (CASTOR_DEST_BIN_DIR /usr/bin)
set (CASTOR_DEST_LIB_DIR /usr/lib64)

# All common variables and values to be used by all CMakeLists.txt files should
# be specified here
set (CMAKE_C_FLAGS
  "-pthread -fPIC -Wall -Wextra -Werror -Wno-unused-parameter")
set (CMAKE_CXX_FLAGS ${CMAKE_C_FLAGS})
set_property ( DIRECTORY PROPERTY COMPILE_DEFINITIONS _LARGEFILE64_SOURCE)

# Do not add rpath information to compiled executables
set (CMAKE_SKIP_RPATH TRUE)

add_subdirectory (common)
add_subdirectory (tape)
