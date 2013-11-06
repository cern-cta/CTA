add_library(TapeDrive Drive.cc)
set_property(SOURCE Drive.cc
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)

add_executable(TapeDriveReadWriteTest TapeDriveReadWriteTest.cc)

target_link_libraries(TapeDriveReadWriteTest TapeDrive Exception SCSI System Utils ${GTEST_LIBRARY} gmock pthread)
install(TARGETS TapeDriveReadWriteTest
  RUNTIME DESTINATION bin)
