add_executable(unitTest unitTest.cc ../SCSI/DeviceTest.cc ../Utils/RegexTest.cc)
target_link_libraries(unitTest Exception SCSI System Utils ${GTEST_LIBRARY} gmock pthread)

install(TARGETS unitTest
  RUNTIME DESTINATION bin)

add_test("Unit_tests" unitTest)
