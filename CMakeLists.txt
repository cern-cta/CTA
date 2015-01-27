cmake_minimum_required (VERSION 2.6)

project(cta)

# Explicitly setting the C and C++ compiler flags for the RelWithDebInfo build
# in order to prevent the -O2 flag from being used.
set(CMAKE_C_FLAGS_RELWITHDEBINFO "-g")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g")

add_subdirectory(libs)
add_subdirectory(unit_tests)
add_subdirectory(xroot_clients)
add_subdirectory(xroot_plugins)
