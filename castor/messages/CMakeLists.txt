add_library(castormessages SHARED
  Exception.pb.cc
  ForkCleaner.pb.cc
  ForkDataTransfer.pb.cc
  ForkLabel.pb.cc
  ForkSucceeded.pb.cc
  Frame.cpp
  Header.pb.cc
  Heartbeat.pb.cc
  messages.cpp
  MigrationJobFromTapeGateway.pb.cc
  MigrationJobFromWriteTp.pb.cc
  NbFilesOnTape.pb.cc
  ProcessCrashed.pb.cc
  ProcessExited.pb.cc
  RecallJobFromReadTp.pb.cc
  RecallJobFromTapeGateway.pb.cc
  ReturnValue.pb.cc
  StopProcessForker.pb.cc
  TapeMountedForMigration.pb.cc
  TapeMountedForRecall.pb.cc
  TapeserverProxy.cpp
  TapeserverProxyDummy.cpp
  TapeserverProxyDummyFactory.cpp
  TapeserverProxyFactory.cpp
  TapeserverProxyZmq.cpp
  TapeserverProxyZmqFactory.cpp
  TapeUnmountStarted.pb.cc
  TapeUnmounted.pb.cc
  ZmqMsg.cpp
  ZmqSocket.cpp
  ZmqSocketMT.cpp
  ZmqSocketST.cpp
)

target_link_libraries(castormessages protobuf ssl)
install (TARGETS castormessages DESTINATION ${CASTOR_DEST_LIB_DIR})
