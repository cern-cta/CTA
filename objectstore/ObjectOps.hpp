#pragma once

#include "Backend.hpp"
#include "exception/Exception.hpp"
#include "objectstore/cta.pb.h"
#include <memory>
#include <stdint.h>

namespace cta { namespace objectstore {

class ObjectOpsBase {
  friend class ScopedLock;
  friend class ScopedSharedLock;
  friend class ScopedExclusiveLock;
protected:
  ObjectOpsBase(Backend & os): m_nameSet(false), m_objectStore(os), 
    m_headerInterpreted(false), m_payloadInterpreted(false) {}
  
  class NameNotSet: public cta::exception::Exception {
  public:
    NameNotSet(const std::string & w): cta::exception::Exception(w) {}
  };
  
  class NotLocked: public cta::exception::Exception {
  public:
    NotLocked(const std::string & w): cta::exception::Exception(w) {}
  };
  
  class AlreadyLocked: public cta::exception::Exception {
  public:
    AlreadyLocked(const std::string & w): cta::exception::Exception(w) {}
  };
  
  class WrongType: public cta::exception::Exception {
  public:
    WrongType(const std::string & w): cta::exception::Exception(w) {}
  };
  
  class NotNew: public cta::exception::Exception {
  public:
    NotNew(const std::string & w): cta::exception::Exception(w) {}
  };
  
  class NotFetched: public cta::exception::Exception {
  public:
    NotFetched(const std::string & w): cta::exception::Exception(w) {}
  };
  
  void setName(const std::string & name) {
    m_name = name;
    m_nameSet = true;
  }
  
  std::string & getNameIfSet() {
    if (!m_nameSet) {
      throw NameNotSet("In ObjectOpsBase::getNameIfSet: name not set yet");
    }
    return m_name;
  }
protected:
  bool m_nameSet;
  std::string m_name;
  Backend & m_objectStore;
  serializers::ObjectHeader m_header;
  bool m_headerInterpreted;
  bool m_payloadInterpreted;
  bool m_locked;
  bool m_lockedForWrite;
  std::auto_ptr<Backend::ScopedLock> m_writeLock;
};

class ScopedLock {
public:
  void release() {
    m_lock.reset(NULL);
  }
  virtual ~ScopedLock() {
    m_objectOps.m_locked = false;
    m_objectOps.m_lockedForWrite = false;
  }
protected:
  ScopedLock(ObjectOpsBase & oo): m_objectOps(oo) {
    if (m_objectOps.m_locked)
      throw ObjectOpsBase::AlreadyLocked("In ScopedLock::ScopedLock: object already locked");
  }
  std::auto_ptr<Backend::ScopedLock> m_lock;
  ObjectOpsBase & m_objectOps;
};
  
class ScopedSharedLock: public ScopedLock {
public:
  ScopedSharedLock(ObjectOpsBase & oo): ScopedLock(oo) {
    m_lock.reset(m_objectOps.m_objectStore.lockShared(m_objectOps.getNameIfSet()));
    m_objectOps.m_locked = true;
  }
};

class ScopedExclusiveLock: public ScopedLock {
public:
  ScopedExclusiveLock(ObjectOpsBase & oo): ScopedLock(oo) {
    m_lock.reset(m_objectOps.m_objectStore.lockExclusive(m_objectOps.getNameIfSet()));
    m_objectOps.m_locked = true;
    m_objectOps.m_lockedForWrite = true;
  }
};

template <class C>
class ObjectOps: public ObjectOpsBase {
protected:
  ObjectOps(Backend & os, const std::string & name): ObjectOpsBase(os) {
    setName(name);
  }
  
  ObjectOps(Backend & os): ObjectOpsBase(os) {}
  
public:
  void fetch() {
    // Check that the object is locked, one way or another
    if(!m_locked)
      throw NotLocked("In ObjectOps::fetch(): object not locked");
    // Get the header from the object store
    getHeaderFromObjectStore();
    // Interpret the data
    getPayloadFromHeader();
  }
  
  void commit() {
    // Check that the object is locked for writing
    if (!m_locked || !m_lockedForWrite)
      throw NotLocked("In ObjectOps::commit(): object not locked for write");
    // Serialise the payload into the header
    m_header.set_payload(m_payload.SerializeAsString());
    // Write the payload
    m_objectStore.atomicOverwrite(getNameIfSet(), m_header.SerializeAsString());
  }
  
protected:
  void getHeaderFromObjectStore () {
    m_header.ParseFromString(m_objectStore.read(getNameIfSet()));
    if (m_header.type() != typeId) {
      std::stringstream err;
      err << "In ObjectOps::getHeaderFromObjectStore wrong object type: "
          << "found=" << m_header.type() << " expected=" << typeId;
      throw cta::exception::Exception(err.str());
    }
    m_headerInterpreted = true;
  }
  
  void getPayloadFromHeader () {
    m_payload.ParseFromString(m_header.payload());
    m_payloadInterpreted = true;
  }
  
public:
  
  void remove () {
    if (!m_lockedForWrite)
      throw NotLocked("In ObjectOps::remove: not locked for write");
    m_objectStore.remove(getNameIfSet());
  }
  
  /**
   * Fill up the header and object with its default contents
   */
  void initialize() {
    if (m_headerInterpreted)
      throw NotNew("In ObjectOps::initialize: trying to initialize an exitsting object");
    m_header.set_type(typeId);
  }
  
  void insert() {
    // Push the payload into the header and write the object
    // We don't require locking here, as the object does not exist
    // yet in the object store (and this is ensured by the )
    m_header.set_payload(m_payload.SerializeAsString());
    m_objectStore.create(getNameIfSet(), m_header.SerializeAsString());
  }
  
private:
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
  
protected:
  static const serializers::ObjectType typeId;
  C m_payload;
};

}}