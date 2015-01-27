#pragma once

#include "ObjectStores.hpp"
#include "ContextHandle.hpp"

class Agent {
public:
  Agent(ObjectStore & os): m_objectStore(os) {};
  ~Agent() {
    for (size_t i=0; i < c_handleCount; i++) {
      m_contexts[i].release();
    }
  }
  void act() {
    
  }
private:
  ObjectStore & m_objectStore;
  static const size_t c_handleCount = 100;
  ContextHandleImplementation<myOS> m_contexts[c_handleCount];
  ContextHandleImplementation<myOS> getFreeContext() {
    for (size_t i=0; i < c_handleCount; i++) {
      if (!m_contexts[i].isSet())
        return m_contexts[i];
    }
    throw cta::exception::Exception("Could not find free context slot");
  }
};