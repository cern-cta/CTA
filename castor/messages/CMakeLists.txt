add_library(castorMessages SHARED
  Header.pb.cc
  Heartbeat.pb.cc
  messages.cpp
  NotifyDrive.pb.cc
  TapeserverProxy.cpp
  TapeserverProxyDummy.cpp
  TapeserverProxyDummyFactory.cpp
  TapeserverProxyFactory.cpp
  TapeserverProxyZmq.cpp
  TapeserverProxyZmqFactory.cpp
)

target_link_libraries(castorMessages protobuf)
