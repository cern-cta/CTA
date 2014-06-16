add_library(castorMessages 
  Header.pb.cc
  Heartbeat.pb.cc
)

target_link_libraries(castorMessages protobuf)