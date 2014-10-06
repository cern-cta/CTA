add_library(TapeDrive DriveGeneric.cpp FakeDrive.cpp)
set_property(SOURCE DriveGeneric.cpp PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
target_link_libraries(TapeDrive SCSI)

add_executable(TapeDriveReadWriteTest
  TapeDriveReadWriteTest.cpp)
target_link_libraries(TapeDriveReadWriteTest
  TapeDrive
  castormediachangerclient
  SCSI
  System
  Utils
  castorcommon
  castorclient
  ${GTEST_LIBRARY}
  gmock
  pthread)
