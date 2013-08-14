add_library(System Wrapper.cc FileWrappers.cc)
set_property(SOURCE Wrapper.cc
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)