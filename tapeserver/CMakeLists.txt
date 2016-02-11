cmake_minimum_required (VERSION 2.6)

# Old CASTOR's tapeserverd daemon code
add_subdirectory (castor)
add_subdirectory (test)

# CTA's cta-taped code
add_subdirectory (daemon)

add_executable (cta-taped cta-taped.cpp)
target_link_libraries(cta-taped
  ctatapedaemon ctacommon protobuf)
