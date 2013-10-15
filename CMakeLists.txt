cmake_minimum_required (VERSION 2.6)
project (castor)

set (MAJOR_CASTOR_VERSION  2.1)
set (MINOR_CASTOR_VERSION 14.2)

# All common variables and values to be used by all CMakeLists.txt files should
# be specified here
set (CMAKE_C_FLAGS "-fPIC -Wall -Wextra -Werror -Wno-unused-parameter")
set (CMAKE_CXX_FLAGS "-fPIC -Wall -Wextra -Werror -Wno-unused-parameter")
set_property ( DIRECTORY PROPERTY COMPILE_DEFINITIONS _LARGEFILE64_SOURCE)

add_subdirectory (common)
add_subdirectory (tape)
