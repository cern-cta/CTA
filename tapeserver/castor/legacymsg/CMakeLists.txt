cmake_minimum_required (VERSION 2.6)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver/h)

add_library (ctalegacymsg
  TapeLabelRqstMsgBody.cpp
  MessageHeader.cpp
  TapeConfigRequestMsgBody.cpp
  TapeStatReplyMsgBody.cpp
  TapeStatRequestMsgBody.cpp
  GenericErrorReplyMsgBody.cpp
  GenericReplyMsgBody.cpp
  TapeStatDriveEntry.cpp)
