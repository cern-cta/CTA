add_library(TapeDrive Drive.cc)

add_executable(TapeDriveInfo TapeDriveInfo.cc)
target_link_libraries(TapeDriveInfo Exception SCSI System Utils ${GTEST_LIBRARY} gmock pthread)
install(TARGETS TapeDriveInfo
  RUNTIME DESTINATION bin)
