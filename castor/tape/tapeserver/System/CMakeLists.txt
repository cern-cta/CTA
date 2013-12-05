add_library(System Wrapper.cpp FileWrappers.cpp)
set_property(SOURCE Wrapper.cpp
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)