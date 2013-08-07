add_executable(unitTest unitTest.cc
  ../SCSI/DeviceTest.cc
  ../SCSI/StructuresTest.cc
  ../Utils/RegexTest.cc
  ../Drive/DriveTest.cc
  ../Exception/ExceptionTest.cc
)
set_property(SOURCE ../SCSI/DeviceTest.cc ../SCSI/StructuresTest.cc ../Drive/DriveTest.cc
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)
target_link_libraries(unitTest Exception SCSI System Utils ${GTEST_LIBRARY} gmock pthread)

install(TARGETS unitTest
  RUNTIME DESTINATION bin)

# TODO: consider removing.
add_test("Unit_tests" unitTest)

