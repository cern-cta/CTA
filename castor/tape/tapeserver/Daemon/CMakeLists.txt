add_executable(tapeserverd tapeserverd.cpp)

target_link_libraries(tapeserverd Exception SCSI System Utils File castorcommon)