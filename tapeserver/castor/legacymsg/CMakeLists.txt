cmake_minimum_required (VERSION 2.6)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver/h)

add_library (ctalegacymsg
  TapeLabelRqstMsgBody.cpp
  RmcProxyTcpIp.cpp
  RmcMarshal.cpp
  GenericMarshal.cpp
  MessageHeader.cpp
  legacymsg.cpp
  TapeConfigRequestMsgBody.cpp
  TapeMarshal.cpp
  CommonMarshal.cpp
  TapeStatReplyMsgBody.cpp
  TapeStatRequestMsgBody.cpp
  GenericErrorReplyMsgBody.cpp
  GenericReplyMsgBody.cpp
  TapeStatDriveEntry.cpp
  RmcProxy.cpp
  RmcMountMsgBody.cpp
  RmcUnmountMsgBody.cpp
  RmcProxyDummy.cpp)

add_library (ctalegacymsgunittests SHARED
  CommonMarshalTest.cpp
  GenericMarshalTest.cpp
  TapeMarshalTest.cpp)
target_link_libraries (ctalegacymsgunittests
  castorclient
  ctamessages)
