add_library(castorZmqWrapper SHARED
  ZmqWrapper.cpp
)

target_link_libraries(castorZmqWrapper zmq)
