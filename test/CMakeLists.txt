add_executable(unitTest unitTest.cc ../SCSI/DeviceTest.cc)
target_link_libraries(unitTest SCSI ${GTEST_LIBRARY} gmock pthread)
install(TARGETS unitTest
  RUNTIME DESTINATION bin)

add_test("Unit_tests" unitTest)
