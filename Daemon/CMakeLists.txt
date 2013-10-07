add_executable(tapeserverd tapeserverd.cc)

target_link_libraries(tapeserverd Exception SCSI System Utils File)