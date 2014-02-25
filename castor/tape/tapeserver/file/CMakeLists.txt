add_library(File File.cpp Structures.cpp)
add_executable(castor-tape-AULFileFormatTest FileFormatTest.cpp)
add_executable(castor-tape-TapeCopyTest TapeCopyTest.cpp)

target_link_libraries(castor-tape-AULFileFormatTest File TapeDrive Exception SCSI System Utils castorcommon castorclient ${GTEST_LIBRARY} gmock pthread)
target_link_libraries(castor-tape-TapeCopyTest File TapeDrive Exception SCSI System Utils castorcommon castorclient ${GTEST_LIBRARY} gmock pthread)

install(TARGETS castor-tape-AULFileFormatTest RUNTIME DESTINATION bin)
install(TARGETS castor-tape-TapeCopyTest RUNTIME DESTINATION bin)