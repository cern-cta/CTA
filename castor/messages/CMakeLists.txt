find_program(PROTOC NAMES protoc)
if(${PROTOC} STREQUAL "PROTOC-NOTFOUND")
  message(FATAL_ERROR "Failed to find protoc")
endif(${PROTOC} STREQUAL "PROTOC-NOTFOUND")
message(STATUS "Found protoc: ${PROTOC}")

file(GLOB PROTO_PATHS "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")

set(PROTO_TGTS)
set(PROTO_H_FILES)
set(PROTO_CC_FILES)
foreach(PROTO_PATH ${PROTO_PATHS})
  get_filename_component(PROTO_TGT ${PROTO_PATH} NAME_WE)
  add_custom_target(${PROTO_TGT} ALL
    COMMAND ${PROTOC} -I ${CMAKE_CURRENT_SOURCE_DIR} ${PROTO_PATH} --cpp_out ${CMAKE_CURRENT_BINARY_DIR})
  list(APPEND PROTO_TGTS ${PROTO_TGT})
  set(PROTO_H_FILE "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_TGT}.pb.h")
  set(PROTO_CC_FILE "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_TGT}.pb.h")
  list(APPEND PROTO_H_FILES "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_TGT}.pb.h")
  list(APPEND PROTO_CC_FILES "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_TGT}.pb.cc")
endforeach(PROTO_PATH)

set_source_files_properties(${PROTO_H_FILES} PROPERTIES GENERATED TRUE)
set_source_files_properties(${PROTO_CC_FILES} PROPERTIES GENERATED TRUE)

add_library(castormessages SHARED
  ${PROTO_CC_FILES}
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
add_dependencies(castormessages ${PROTO_TGTS})
install (TARGETS castormessages DESTINATION ${CASTOR_DEST_LIB_DIR})
