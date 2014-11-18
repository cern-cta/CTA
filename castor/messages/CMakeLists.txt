add_library(castormessages SHARED
  AcsDismountTape.pb.cc
  AcsMountTapeReadOnly.pb.cc
  AcsMountTapeReadWrite.pb.cc
  AddLogParams.pb.cc
  DeleteLogParams.pb.cc
  Exception.pb.cc
  ForkCleaner.pb.cc
  ForkDataTransfer.pb.cc
  ForkLabel.pb.cc
  ForkSucceeded.pb.cc
  Header.pb.cc
  Heartbeat.pb.cc
  LabelError.pb.cc
  MigrationJobFromTapeGateway.pb.cc
  MigrationJobFromWriteTp.pb.cc
  NbFilesOnTape.pb.cc
  ProcessCrashed.pb.cc
  ProcessExited.pb.cc
  RecallJobFromReadTp.pb.cc
  RecallJobFromTapeGateway.pb.cc
  RecallJob.pb.cc
  ReturnValue.pb.cc
  StopProcessForker.pb.cc
  TapeMountedForMigration.pb.cc
  TapeMountedForRecall.pb.cc
  TapeUnmounted.pb.cc
  TapeUnmountStarted.pb.cc

  AcsProxy.cpp
  AcsProxyDummy.cpp
  AcsProxyZmq.cpp
  Constants.cpp
  Frame.cpp
  messages.cpp
  SmartZmqContext.cpp
  TapeserverProxy.cpp
  TapeserverProxyDummy.cpp
  TapeserverProxyZmq.cpp
  ZmqMsg.cpp
  ZmqSocket.cpp
  ZmqSocketMT.cpp
  ZmqSocketST.cpp
)

target_link_libraries(castormessages castormediachangerclient protobuf ssl zmq)
install (TARGETS castormessages DESTINATION ${CASTOR_DEST_LIB_DIR})
