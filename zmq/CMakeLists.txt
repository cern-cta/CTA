add_library(castorzmqwrapper SHARED
  ZmqWrapper.cpp
)

target_link_libraries(castorzmqwrapper zmq)
install (TARGETS castorzmqwrapper DESTINATION ${CASTOR_DEST_LIB_DIR})
