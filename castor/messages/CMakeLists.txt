add_library(castormessages SHARED
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

target_link_libraries(castormessages castorzmqwrapper protobuf)
install (TARGETS castormessages DESTINATION ${CASTOR_DEST_LIB_DIR})
