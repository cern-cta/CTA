cmake_minimum_required(VERSION 3.9)
project(Demo_cpp_plugin)
set(CMAKE_CXX_STANDARD 17)     
set(CMAKE_VERBOSE_MAKEFILE ON)
include_directories(..)

#========== Targets Configurations ============#

# >>> ---- Target: main ----------------
add_executable(main main.cpp)
target_include_directories(main PUBLIC ".")
# Note: the library ldl is necessary for the UNIX APIs dlopen,
# dlclose, ... and so on
if(UNIX)
  target_link_libraries(main PRIVATE dl)
  set_target_properties(main PROPERTIES SUFFIX ".bin")
endif()  


# >>> ---- Target: PluginOracle ----------------
add_library(PluginOracle SHARED PluginOracle.cpp)
target_include_directories(PluginOracle PUBLIC ".")
set_target_properties(PluginOracle PROPERTIES
  PREFIX ""
  )

# >>> ---- Target: PluginPostgreSQL ----------------
add_library(PluginPostgreSQL SHARED PluginPostgreSQL.cpp)
target_include_directories(PluginPostgreSQL PUBLIC ".")
set_target_properties(PluginPostgreSQL PROPERTIES
  PREFIX ""
  )

# If not set the install directory, attemp set the install directory
# CMAKE_INSTALL_PREFIX to the directory ./bin 
if(NOT DEFINED CMAKE_INSTALL_PREFIX OR CMAKE_INSTALL_PREFIX MATCHES "")
  set(CMAKE_INSTALL_PREFIX  ${CMAKE_CURRENT_LIST_DIR}/bin)
endif()

install(TARGETS main PluginOracle PluginPostgreSQL DESTINATION ".")
