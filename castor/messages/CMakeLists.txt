add_library(castormessages SHARED
  ForkCleaner.pb.cc
  ForkDataTransfer.pb.cc
  ForkLabel.pb.cc
  Header.pb.cc
  Heartbeat.pb.cc
  messages.cpp
  NotifyDrive.pb.cc
  ReplyContainer.cpp
  StopProcessForker.pb.cc
  TapeserverProxy.cpp
  TapeserverProxyDummy.cpp
  TapeserverProxyDummyFactory.cpp
  TapeserverProxyFactory.cpp
  TapeserverProxyZmq.cpp
  TapeserverProxyZmqFactory.cpp
)

target_link_libraries(castormessages protobuf ssl)
install (TARGETS castormessages DESTINATION ${CASTOR_DEST_LIB_DIR})
