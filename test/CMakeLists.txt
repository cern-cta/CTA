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


