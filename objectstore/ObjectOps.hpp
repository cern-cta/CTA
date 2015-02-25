#pragma once

#include "Backend.hpp"
#include "exception/Exception.hpp"
#include <memory>

namespace cta { namespace objectstore {

template <class C>
class ObjectOps {
public:
  ObjectOps(Backend & os, const std::string & name):m_nameSet(true), m_name(name),
    m_objectStore(os) {}
  
  ObjectOps(Backend & os): m_nameSet(false), m_objectStore(os) {}
  
  class NameNotSet: public cta::exception::Exception {
  public:
    NameNotSet(const std::string & w): cta::exception::Exception(w) {}
  };
  
  class AlreadyLocked: public cta::exception::Exception {
  public:
    AlreadyLocked(const std::string & w): cta::exception::Exception(w) {}
  };
  
  void setName(const std::string & name) {
    m_name = name;
    m_nameSet = true;
  }
  
  void updateFromObjectStore (C & val) {
    if(!m_nameSet) throw NameNotSet("In ObjectOps<>::updateFromObjectStore: name not set");
    std::auto_ptr<Backend::ScopedLock> lock (m_objectStore.lockShared(m_name));
    std::string reStr = m_objectStore.read(m_name);
    lock->release();
    val.ParseFromString(reStr);
  }
  
  void lockExclusiveAndRead (C & val) {
    if(!m_nameSet) throw NameNotSet("In ObjectOps<>::updateFromObjectStore: name not set");
    if(NULL != m_writeLock.get()) throw AlreadyLocked("In ObjectOps<>::updateFromObjectStore: already lcoked for write");
    m_writeLock.reset(m_objectStore.lockExclusive(m_name));
    // Re-read to get latest version (lock upgrade could be useful here)
    std::string reStr = m_objectStore.read(m_name);
    if (reStr.size())
    val.ParseFromString(reStr);
  }
  
  void write (C & val) {
    if(!m_nameSet) throw NameNotSet("In ObjectOps<>::updateFromObjectStore: name not set");
    m_objectStore.atomicOverwrite(m_name, val.SerializeAsString());
  }
  
  void unlock () {
    if(!m_nameSet) throw NameNotSet("In ObjectOps<>::updateFromObjectStore: name not set");
    // release the lock, and return the register name
    m_writeLock.reset(NULL);
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
  
  Backend & objectStore() {
    return m_objectStore;
  }
  
private:
  bool m_nameSet;
  std::string m_name;
  Backend & m_objectStore;
  std::auto_ptr<Backend::ScopedLock> m_writeLock;  
};

}}