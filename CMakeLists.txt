cmake_minimum_required (VERSION 2.6)

project(cta)

# Explicitly setting the C and C++ compiler flags for the RelWithDebInfo build
# in order to prevent the -O2 flag from being used.
set(CMAKE_C_FLAGS_RELWITHDEBINFO "-g")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g")

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

add_subdirectory(libs)
add_subdirectory(unit_tests)
add_subdirectory(xroot_clients)
add_subdirectory(xroot_plugins)
add_subdirectory(objectstore)
