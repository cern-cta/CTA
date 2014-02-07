add_library(File File.cpp Structures.cpp)
add_executable(castor-tape-AULFileFormatTest FileFormatTest.cpp)

target_link_libraries(castor-tape-AULFileFormatTest File TapeDrive Exception SCSI System Utils 
  castorcommon castorclient ${GTEST_LIBRARY} gmock pthread)

install(TARGETS castor-tape-AULFileFormatTest
  RUNTIME DESTINATION bin)
