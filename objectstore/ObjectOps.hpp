/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "Backend.hpp"
#include "common/exception/Exception.hpp"
#include "objectstore/cta.pb.h"
#include <memory>
#include <stdint.h>

namespace cta { namespace objectstore {

class ObjectOpsBase {
  friend class ScopedLock;
  friend class ScopedSharedLock;
  friend class ScopedExclusiveLock;
  friend class GenericObject;
protected:
  ObjectOpsBase(Backend & os): m_nameSet(false), m_objectStore(os), 
    m_headerInterpreted(false), m_payloadInterpreted(false),
    m_existingObject(false), m_locksCount(0),
    m_locksForWriteCount(0) {}
public:
  CTA_GENERATE_EXCEPTION_CLASS(AddressNotSet);
  CTA_GENERATE_EXCEPTION_CLASS(NotLocked);  
  CTA_GENERATE_EXCEPTION_CLASS(WrongType);
  CTA_GENERATE_EXCEPTION_CLASS(NotNewObject);
  CTA_GENERATE_EXCEPTION_CLASS(NewObject);
  CTA_GENERATE_EXCEPTION_CLASS(NotFetched);
  CTA_GENERATE_EXCEPTION_CLASS(NotInitialized);  
  CTA_GENERATE_EXCEPTION_CLASS(AddressAlreadySet);
  CTA_GENERATE_EXCEPTION_CLASS(InvalidAddress);
protected:
  void checkHeaderWritable() {
    if (!m_headerInterpreted) 
      throw NotFetched("In ObjectOps::checkHeaderWritable: header not yet fetched or initialized");
    checkWritable();
  }
  
  void checkHeaderReadable() {
    if (!m_headerInterpreted) 
      throw NotFetched("In ObjectOps::checkHeaderReadable: header not yet fetched or initialized");
    checkReadable();
  }
  
  void checkPayloadWritable() {
    if (!m_payloadInterpreted) 
      throw NotFetched("In ObjectOps::checkPayloadWritable: header not yet fetched or initialized");
    checkWritable();
  }
  
  void checkPayloadReadable() {
    if (!m_payloadInterpreted) 
      throw NotFetched("In ObjectOps::checkPayloadReadable: header not yet fetched or initialized");
    checkReadable();
  }
  
  void checkWritable() {
    if (m_existingObject && !m_locksForWriteCount)
      throw NotLocked("In ObjectOps::checkWritable: object not locked for write");
  }
  
  void checkReadable() {
    if (!m_locksCount)
      throw NotLocked("In ObjectOps::checkReadable: object not locked");
  }
  
public:
  
  void setAddress(const std::string & name) {
    if (m_nameSet)
      throw AddressAlreadySet("In ObjectOps::setName: trying to overwrite an already set name");
    if (name.empty())
      throw InvalidAddress("In ObjectOps::setName: empty name");
    m_name = name;
    m_nameSet = true;
  }
  
  std::string & getAddressIfSet() {
    if (!m_nameSet) {
      throw AddressNotSet("In ObjectOpsBase::getNameIfSet: name not set yet");
    }
    return m_name;
  }
  
  void remove () {
    checkWritable();
    m_objectStore.remove(getAddressIfSet());
    m_existingObject = false;
    m_headerInterpreted = false;
    m_payloadInterpreted = false;
  }
  
  void setOwner(const std::string & owner) {
    m_header.set_owner(owner);
  }
  
  std::string getOwner() {
    return m_header.owner();
  }
  
  void setBackupOwner(const std::string & owner) {
    m_header.set_backupowner(owner);
  }
  
  std::string getBackupOwner() {
    return m_header.backupowner();
  }

protected:
  bool m_nameSet;
  std::string m_name;
  Backend & m_objectStore;
  serializers::ObjectHeader m_header;
  bool m_headerInterpreted;
  bool m_payloadInterpreted;
  bool m_existingObject;
  int m_locksCount;
  int m_locksForWriteCount;
};

class ScopedLock {
public:
  void release() {
    checkLocked();
    releaseIfNeeded();
  }
  
  /** Move the locked object reference to a new one. This is done when the locked
   * object is a GenericObject and the caller instantiated a derived object from
   * it. The lock follows the move.
   * We check we move the lock from a Generic object (this is the only allowed
   * use case).
   * New object's locks are moved from the old one (referenced in the lock)
   */
  void transfer(ObjectOpsBase & newObject) {
    decltype(m_objectOps) oldObj(m_objectOps);
    m_objectOps = & newObject;
    // Transfer the locks from old to new object
    m_objectOps->m_locksCount = oldObj->m_locksCount;
    m_objectOps->m_locksForWriteCount = oldObj->m_locksForWriteCount;
    // The old object is not considered locked anymore and should be
    // discarded. A previous call the the new object's constructor should
    oldObj->m_locksCount =  0;
    oldObj->m_locksForWriteCount = 0;
  }
  
  virtual ~ScopedLock() {
    releaseIfNeeded();
  }
  
  CTA_GENERATE_EXCEPTION_CLASS(AlreadyLocked);
  CTA_GENERATE_EXCEPTION_CLASS(NotLocked);
  CTA_GENERATE_EXCEPTION_CLASS(MissingAddress);
  
protected:
  ScopedLock(): m_objectOps(NULL), m_locked(false) {}
  std::unique_ptr<Backend::ScopedLock> m_lock;
  ObjectOpsBase * m_objectOps;
  bool m_locked;
  void checkNotLocked() {
    if (m_locked)
      throw AlreadyLocked("In ScopedLock::checkNotLocked: trying to lock an already locked lock");
  }
  void checkLocked() {
    if (!m_locked)
      throw NotLocked("In ScopedLock::checkLocked: trying to unlock an unlocked lock");
  }
  void checkObjectAndAddressSet() {
    if (!m_objectOps) {
      throw MissingAddress("In ScopedLock::checkAddressSet: trying to lock a NULL object");
    } else if (!m_objectOps->m_nameSet || m_objectOps->m_name.empty()) {
      throw MissingAddress("In ScopedLock::checkAddressSet: trying to lock an object without address");
    }
  }
  virtual void releaseIfNeeded() {
    if(!m_locked) return;
    m_lock.reset(NULL);
    m_objectOps->m_locksCount--;
    m_locked = false;
  }
};
  
class ScopedSharedLock: public ScopedLock {
public:
  ScopedSharedLock() {}
  ScopedSharedLock(ObjectOpsBase & oo) {
    lock(oo);
  }
  void lock(ObjectOpsBase & oo) {
    checkNotLocked();
    m_objectOps  = & oo;
    checkObjectAndAddressSet();
    m_lock.reset(m_objectOps->m_objectStore.lockShared(m_objectOps->getAddressIfSet()));
    m_objectOps->m_locksCount++;
    m_locked = true;
  }
};

class ScopedExclusiveLock: public ScopedLock {
public:
  ScopedExclusiveLock() {}
  ScopedExclusiveLock(ObjectOpsBase & oo) {
    lock(oo);
  }
  void lock(ObjectOpsBase & oo) {
    checkNotLocked();
    m_objectOps = &oo;
    checkObjectAndAddressSet();
    m_lock.reset(m_objectOps->m_objectStore.lockExclusive(m_objectOps->getAddressIfSet()));
    m_objectOps->m_locksCount++;
    m_objectOps->m_locksForWriteCount++;
    m_locked = true;
  }
protected:
  void releaseIfNeeded() {
    if (!m_locked) return;
    ScopedLock::releaseIfNeeded();
    m_objectOps->m_locksForWriteCount--;
  }
};

template <class PayloadType, serializers::ObjectType PayloadTypeId>
class ObjectOps: public ObjectOpsBase {
protected:
  ObjectOps(Backend & os, const std::string & name): ObjectOpsBase(os) {
    setAddress(name);
  }
  
  ObjectOps(Backend & os): ObjectOpsBase(os) {}
  
public:
  void fetch() {
    // Check that the object is locked, one way or another
    if(!m_locksCount)
      throw NotLocked("In ObjectOps::fetch(): object not locked");
    m_existingObject = true;
    // Get the header from the object store
    getHeaderFromObjectStore();
    // Interpret the data
    getPayloadFromHeader();
  }
  
  void commit() {
    checkPayloadWritable();
    if (!m_existingObject) 
      throw NewObject("In ObjectOps::commit: trying to update a new object");
    // Serialise the payload into the header
    m_header.set_payload(m_payload.SerializeAsString());
    // Write the object
    m_objectStore.atomicOverwrite(getAddressIfSet(), m_header.SerializeAsString());
  }
  
  CTA_GENERATE_EXCEPTION_CLASS(WrongTypeForGarbageCollection);
  /**
   * This function should be overloaded in the inheriting classes
   */
  void garbageCollect(const std::string &presumedOwner) {
    throw WrongTypeForGarbageCollection("In ObjectOps::garbageCollect. "
      "This function should have been overloaded in derived class");
  }
  
protected:
  
  void getPayloadFromHeader () {
    m_payload.ParseFromString(m_header.payload());
    m_payloadInterpreted = true;
  }

  void getHeaderFromObjectStore () {
    m_header.ParseFromString(m_objectStore.read(getAddressIfSet()));
    if (m_header.type() != payloadTypeId) {
      std::stringstream err;
      err << "In ObjectOps::getHeaderFromObjectStore wrong object type: "
          << "found=" << m_header.type() << " expected=" << payloadTypeId;
      throw ObjectOpsBase::WrongType(err.str());
    }
    m_headerInterpreted = true;
  }
  
public:
  /**
   * Fill up the header and object with its default contents
   */
  void initialize() {
    if (m_headerInterpreted || m_existingObject)
      throw NotNewObject("In ObjectOps::initialize: trying to initialize an exitsting object");
    m_header.set_type(payloadTypeId);
    m_header.set_version(0);
    m_header.set_owner("");
    m_header.set_backupowner("");
    m_headerInterpreted = true;
  }
  
  void insert() {
    // Check that we are not dealing with an existing object
    if (m_existingObject)
      throw NotNewObject("In ObjectOps::insert: trying to insert an already exitsting object");
    // Check that the object is ready in memory
    if (!m_headerInterpreted || !m_payloadInterpreted)
      throw NotInitialized("In ObjectOps::insert: trying to insert an uninitialized object");
    // Push the payload into the header and write the object
    // We don't require locking here, as the object does not exist
    // yet in the object store (and this is ensured by the )
    m_header.set_payload(m_payload.SerializeAsString());
    m_objectStore.create(getAddressIfSet(), m_header.SerializeAsString());
    m_existingObject = true;
  }
  
  bool exists() {
    return m_objectStore.exists(getAddressIfSet());
  }
  
private:
  template <class ChildType>
  void writeChild (const std::string & name, ChildType & val) {
    m_objectStore.create(name, val.SerializeAsString());
  }
  
  void removeOther(const std::string & name) {
    m_objectStore.remove(name);
  }
  
  std::string selfName() {
    if(!m_nameSet) throw AddressNotSet("In ObjectOps<>::updateFromObjectStore: name not set");
    return m_name;
  }
  
  Backend & objectStore() {
    return m_objectStore;
  }
  
protected:
  static const serializers::ObjectType payloadTypeId = PayloadTypeId;
  PayloadType m_payload;
};

}}
