add_library(castorZmqWrapper SHARED
  castorZmqWrapper.cpp
)

target_link_libraries(castorZmqWrapper zmq)
