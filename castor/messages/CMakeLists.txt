find_package(Protobuf)

file(GLOB ProtoFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
PROTOBUF_GENERATE_CPP(ProtoSources ProtoHeaders ${ProtoFiles})

add_library(castormessages SHARED
  ${ProtoSources}
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
