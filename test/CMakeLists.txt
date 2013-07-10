add_executable(unitTest unitTest.cc
  ../SCSI/DeviceTest.cc
  ../SCSI/StructuresTest.cc
  ../Utils/RegexTest.cc
  ../Drive/DriveTest.cc
)
target_link_libraries(unitTest Exception SCSI System Utils ${GTEST_LIBRARY} gmock pthread)

install(TARGETS unitTest
  RUNTIME DESTINATION bin)

# TODO: consider removing.
add_test("Unit_tests" unitTest)

