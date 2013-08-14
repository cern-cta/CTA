add_executable(unitTest unitTest.cc
  ../SCSI/DeviceTest.cc
  ../SCSI/StructuresTest.cc
  ../Utils/RegexTest.cc
  ../Drive/DriveTest.cc
  ../Exception/ExceptionTest.cc
)

target_link_libraries(unitTest TapeDrive Exception SCSI System Utils ${GTEST_LIBRARY} gmock pthread)

install(TARGETS unitTest
  RUNTIME DESTINATION bin)


