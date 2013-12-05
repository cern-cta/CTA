add_executable(unitTest unitTest.cpp
  ../SCSI/DeviceTest.cpp
  ../SCSI/StructuresTest.cpp
  ../Utils/RegexTest.cpp
  ../Drive/DriveTest.cpp
  ../Exception/ExceptionTest.cpp
  ../File/StructuresTest.cpp
)
set_property(SOURCE ../SCSI/StructuresTest.cpp
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


