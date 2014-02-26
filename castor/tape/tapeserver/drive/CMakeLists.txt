add_library(TapeDrive Drive.cpp)
set_property(SOURCE Drive.cpp PROPERTY COMPILE_FLAGS -fno-strict-aliasing)

add_executable(TapeDriveReadWriteTest TapeDriveReadWriteTest.cpp)
target_link_libraries(TapeDriveReadWriteTest TapeDrive Exception SCSI System Utils castorcommon castorclient ${GTEST_LIBRARY} gmock pthread)
