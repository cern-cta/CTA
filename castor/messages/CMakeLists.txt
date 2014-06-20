add_library(castorMessages SHARED
  Header.pb.cc
  Heartbeat.pb.cc
  NotifyDrive.pb.cc
)

target_link_libraries(castorMessages protobuf)