add_library(System Wrapper.cpp FileWrappers.cpp)
set_property(SOURCE Wrapper.cpp
  APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)