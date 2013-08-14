add_library(TapeDrive Drive.cc)
set_property(SOURCE Drive.cc
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)

add_executable(TapeDriveInfo TapeDriveInfo.cc)

target_link_libraries(TapeDriveInfo TapeDrive Exception SCSI System Utils ${GTEST_LIBRARY} gmock pthread)
install(TARGETS TapeDriveInfo
  RUNTIME DESTINATION bin)
