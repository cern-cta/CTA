cmake_minimum_required (VERSION 2.6)

set (CMAKE_C_FLAGS "-fPIC -Wall -Wextra -Werror -Wno-unused-parameter")
set (CMAKE_CXX_FLAGS "-fPIC -Wall -Wextra -Werror -Wno-unused-parameter")
set_property (
  DIRECTORY
  PROPERTY COMPILE_DEFINITIONS _LARGEFILE64_SOURCE)

add_subdirectory (tape)
