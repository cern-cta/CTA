#pragma once

#include "ObjectStores.hpp"

template <class C>
class ObjectOps {
public:
  ObjectOps(ObjectStore & os, const std::string & name):m_name(name),
    m_objectStore(os) {}
  
  void updateFromObjectStore (C & val, ContextHandle & context) {
    m_objectStore.lockShared(m_name, context);
    std::string reStr = m_objectStore.read(m_name);
    m_objectStore.unlock(m_name, context);
    val.ParseFromString(reStr);
  }
  
  void lockExclusiveAndRead (C & val, ContextHandle & context) {
    m_objectStore.lockExclusive(m_name, context);
    // Re-read to get latest version (lock upgrade could be useful here)
    std::string reStr = m_objectStore.read(m_name);
    cta::objectstore::RootEntry res;
    val.ParseFromString(reStr);
  }
  
  void write (C & val) {
    m_objectStore.atomicOverwrite(m_name, val.SerializeAsString());
  }
  
  void unlock (ContextHandle & context) {
    // release the lock, and return the register name
    m_objectStore.unlock(m_name, context);
  }
  
  template <class C2>
  void writeChild (const std::string & name, C2 & val) {
    m_objectStore.atomicOverwrite(name, val.SerializeAsString());
  }
private:
  std::string m_name;
  ObjectStore & m_objectStore;
};