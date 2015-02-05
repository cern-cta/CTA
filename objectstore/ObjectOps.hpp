#pragma once

#include "ObjectStores.hpp"

namespace cta { namespace objectstore {

template <class C>
class ObjectOps {
public:
  ObjectOps(ObjectStore & os, const std::string & name):m_nameSet(true), m_name(name),
    m_objectStore(os) {}
  
  ObjectOps(ObjectStore & os): m_nameSet(false), m_objectStore(os) {}
  
  class NameNotSet: public cta::exception::Exception {
  public:
    NameNotSet(const std::string & w): cta::exception::Exception(w) {}
  };
  
  void setName(const std::string & name) {
    m_name = name;
    m_nameSet = true;
  }
  
  void updateFromObjectStore (C & val, ContextHandle & context) {
    if(!m_nameSet) throw NameNotSet("In ObjectOps<>::updateFromObjectStore: name not set");
    m_objectStore.lockShared(m_name, context);
    std::string reStr = m_objectStore.read(m_name);
    m_objectStore.unlock(m_name, context);
    val.ParseFromString(reStr);
  }
  
  void lockExclusiveAndRead (C & val, ContextHandle & context, std::string where) {
    if(!m_nameSet) throw NameNotSet("In ObjectOps<>::updateFromObjectStore: name not set");
    m_objectStore.lockExclusive(m_name, context, where);
    // Re-read to get latest version (lock upgrade could be useful here)
    std::string reStr = m_objectStore.read(m_name);
    if (reStr.size())
    val.ParseFromString(reStr);
  }
  
  void write (C & val) {
    if(!m_nameSet) throw NameNotSet("In ObjectOps<>::updateFromObjectStore: name not set");
    m_objectStore.atomicOverwrite(m_name, val.SerializeAsString());
  }
  
  void unlock (ContextHandle & context, std::string where="") {
    if(!m_nameSet) throw NameNotSet("In ObjectOps<>::updateFromObjectStore: name not set");
    // release the lock, and return the register name
    m_objectStore.unlock(m_name, context, where);
  }
  
  void remove () {
    if(!m_nameSet) throw NameNotSet("In ObjectOps<>::remove: name not set");
    // remove the object
    m_objectStore.remove(m_name);
  }
  
  template <class C2>
  void writeChild (const std::string & name, C2 & val) {
    m_objectStore.create(name, val.SerializeAsString());
  }
  
  void removeOther(const std::string & name) {
    m_objectStore.remove(name);
  }
  
  std::string selfName() {
    if(!m_nameSet) throw NameNotSet("In ObjectOps<>::updateFromObjectStore: name not set");
    return m_name;
  }
  
  ObjectStore & objectStore() {
    return m_objectStore;
  }
  
private:
  bool m_nameSet;
  std::string m_name;
  ObjectStore & m_objectStore;
};

}}