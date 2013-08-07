add_library(TapeDrive Drive.cc)
set_property(SOURCE Drive.cc
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)

add_executable(TapeDriveInfo TapeDriveInfo.cc)
set_property(SOURCE TapeDriveInfo.cc
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)

target_link_libraries(TapeDriveInfo Exception SCSI System Utils ${GTEST_LIBRARY} gmock pthread)
install(TARGETS TapeDriveInfo
  RUNTIME DESTINATION bin)
