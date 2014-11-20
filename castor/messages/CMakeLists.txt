find_package(Protobuf)

file(GLOB ProtoFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
PROTOBUF_GENERATE_CPP(ProtoSources ProtoHeaders ${ProtoFiles})

add_library(castormessagesprotobuf
  ${ProtoSources})

set(CastorMessageSrcs
  AcsProxy.cpp
  AcsProxyDummy.cpp
  AcsProxyZmq.cpp
  Constants.cpp
  Frame.cpp
  messages.cpp
  Mutex.cpp
  SmartZmqContext.cpp
  TapeserverProxy.cpp
  TapeserverProxyDummy.cpp
  TapeserverProxyZmq.cpp
  ZmqMsg.cpp
  ZmqSocket.cpp
  ZmqSocketMT.cpp
  ZmqSocketST.cpp
)

add_library(castormessages SHARED
  ${CastorMessageSrcs})

add_dependencies(castormessages castormessagesprotobuf)

target_link_libraries(castormessages
  castormessagesprotobuf
  castorlog
  castormediachanger
  protobuf
  ssl
  zmq)
install (TARGETS castormessages DESTINATION ${CASTOR_DEST_LIB_DIR})
