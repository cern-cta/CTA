add_executable(unitTest unitTest.cc
  ../SCSI/DeviceTest.cc
  ../SCSI/StructuresTest.cc
  ../Utils/RegexTest.cc
  ../Drive/DriveTest.cc
  ../Exception/ExceptionTest.cc
  ../File/StructuresTest.cc
)
set_property(SOURCE ../SCSI/StructuresTest.cc
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)

target_link_libraries(unitTest TapeDrive Exception SCSI System Utils File ${GTEST_LIBRARY} gmock pthread)

install(TARGETS unitTest
  RUNTIME DESTINATION bin)

install(FILES
SystemTest/device.conf
SystemTest/library_contents.10
SystemTest/library_contents.30
SystemTest/mhvtl.conf
DESTINATION /etc/mhvtl)

install(FILES
SystemTest/tape-server-system-test.sh
DESTINATION bin)


