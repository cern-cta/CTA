cmake_minimum_required (VERSION 2.6)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)

add_library (ctaexceptionunittests SHARED
  ExceptionTest.cpp)
set_property(TARGET ctaexceptionunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaexceptionunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctaexceptionunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
