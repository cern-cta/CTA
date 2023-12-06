/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <memory>
#include <stdint.h>
#include <cryptopp/base64.h>
#include "Backend.hpp"
#include "objectstore/cta.pb.h"
#include "common/log/LogContext.hpp"

namespace cta {

namespace catalogue {
class Catalogue;
}

namespace objectstore {

class AgentReference;
class ScopedLock;
class ScopedExclusiveLock;
template<typename Q,typename C> struct ContainerTraits;

// Defined queue types
struct ArchiveQueue;
struct ArchiveQueueToTransferForUser;
struct ArchiveQueueToReportForUser;
struct ArchiveQueueToReportToRepackForFailure;
struct ArchiveQueueToReportToRepackForSuccess;
struct ArchiveQueueFailed;
struct ArchiveQueueToTransferForRepack;
struct RetrieveQueue;
struct RetrieveQueueToTransfer;
struct RetrieveQueueToReportForUser;
struct RetrieveQueueFailed;
struct RetrieveQueueToReportToRepackForSuccess;
struct RetrieveQueueToReportToRepackForFailure;
struct RetrieveQueueToTransferForRepack;
struct RepackQueue;
struct RepackQueuePending;
struct RepackQueueToExpand;
class ObjectOpsBase {
  friend class ScopedLock;
  friend class ScopedSharedLock;
  friend class ScopedExclusiveLock;
  friend class GenericObject;
  friend class Helpers;
  friend ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForUser>;
  friend ContainerTraits<ArchiveQueue,ArchiveQueueToReportForUser>;
  friend ContainerTraits<ArchiveQueue,ArchiveQueueFailed>;
  friend ContainerTraits<ArchiveQueue,ArchiveQueueToTransferForRepack>;
  friend ContainerTraits<ArchiveQueue,ArchiveQueueToReportToRepackForFailure>;
  friend ContainerTraits<ArchiveQueue,ArchiveQueueToReportToRepackForSuccess>;
  friend ContainerTraits<RetrieveQueue,RetrieveQueueToTransfer>;
  friend ContainerTraits<RetrieveQueue,RetrieveQueueToReportForUser>;
  friend ContainerTraits<RetrieveQueue,RetrieveQueueFailed>;
  friend ContainerTraits<RetrieveQueue,RetrieveQueueToReportToRepackForSuccess>;
  friend ContainerTraits<RetrieveQueue,RetrieveQueueToReportToRepackForFailure>;
  friend ContainerTraits<RetrieveQueue,RetrieveQueueToTransferForRepack>;
  friend ContainerTraits<RepackQueue,RepackQueuePending>;
  friend ContainerTraits<RepackQueue,RepackQueueToExpand>;
protected:
  explicit ObjectOpsBase(Backend& os): m_nameSet(false), m_objectStore(os), m_headerInterpreted(false),
    m_payloadInterpreted(false), m_existingObject(false), m_locksCount(0), m_locksForWriteCount(0) {}

  virtual ~ObjectOpsBase();
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
  CTA_GENERATE_EXCEPTION_CLASS(FailedToSerialize);
  CTA_GENERATE_EXCEPTION_CLASS(StillLocked);
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
    if (m_existingObject && !m_exclusiveLock && !m_lockForSubObject)
      throw exception::Exception("In ObjectOps::checkWritable: missing reference to exclusive lock");
  }

  void checkReadable() {
    // We could still read from a fresh, not yet inserted object.
    if (m_existingObject && (!m_locksCount && !m_noLock))
      throw NotLocked("In ObjectOps::checkReadable: object not locked");
  }

public:

  void setAddress(const std::string & name) {
    if (m_nameSet)
      throw AddressAlreadySet("In ObjectOps::setAddress(): trying to overwrite an already set name");
    if (name.empty())
      throw InvalidAddress("In ObjectOps::setAddress(): empty name");
    m_name = name;
    m_nameSet = true;
  }

  void resetAddress() {
    if (m_locksCount || m_locksForWriteCount) {
      throw StillLocked("In ObjectOps::resetAddress: reset the address of a locked object");
    }
    m_nameSet = false;
    m_name = "";
    m_headerInterpreted = false;
    m_payloadInterpreted = false;
    m_existingObject = false;
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

  void resetValues () {
    m_existingObject = false;
    m_headerInterpreted = false;
    m_payloadInterpreted = false;
  }

  virtual void setOwner(const std::string & owner) {
    checkHeaderWritable();
    m_header.set_owner(owner);
  }

  virtual std::string getOwner() {
    checkHeaderReadable();
    return m_header.owner();
  }

  void setBackupOwner(const std::string & owner) {
    checkHeaderWritable();
    m_header.set_backupowner(owner);
  }

  std::string getBackupOwner() {
    checkHeaderReadable();
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
  bool m_noLock = false;
  // When locked exclusively, we will keep a reference to the lock,
  // so we can propagate it to sub objects with minimal passing through.
  ScopedExclusiveLock * m_exclusiveLock = nullptr;
  // When being locked as a sub object, we will keep a reference to the lock
  // we are provided with. Likewise, the lock will update ourselves when released.
  ScopedLock * m_lockForSubObject = nullptr;
};

class ScopedLock {
public:
  void release() {
    checkLocked();
    releaseIfNeeded();
  }

  bool isLocked() {
    return m_locked;
  }

  /**
   * Virtual function (implemented differently in shared and exclusive locks),
   * marking the object as locked.
   * @param objectOps pointer to the ObjectOpsBase.
   */
  virtual void setObjectLocked(ObjectOpsBase * objectOps) = 0;

  /**
   * Virtual function (implemented differently in shared and exclusive locks),
   * marking the object as unlocked.
   * @param objectOps pointer to the ObjectOpsBase.
   */
  virtual void setObjectUnlocked(ObjectOpsBase * objectOps) = 0;

  /**
   * Expand the scope of the current lock to a sub object, which will also be covered
   * by this lock. This will allow the sub object to benefit from the same protection
   * from lack of proper locking. This feature is to be used with sharded objects.
   */
  void includeSubObject(ObjectOpsBase & subObject) {
    // To propagate a lock, we should have one to begin with.
    checkLocked();
    ObjectOpsBase * oob = & subObject;
    // Validate the sub object is defined.
    checkObjectAndAddressSet(oob);
    // Propagate the lock to the sub object (this is lock type dependant).
    setObjectLocked(oob);
    // Reference ourselves to the sub object so it can declare it destruction to us.
    oob->m_lockForSubObject = this;
    // Add a reference to the object.
    m_subObjectsOps.push_back(oob);
  }

  /** Move the locked object reference to a new one. This is done when the locked
   * object is a GenericObject and the caller instantiated a derived object from
   * it. The lock follows the move.
   * We check we move the lock from a Generic object (this is the only allowed
   * use case).
   * New object's locks are moved from the old one (referenced in the lock)
   */
  static void transfer(ObjectOpsBase & oldObject, ObjectOpsBase & newObject) {
    // Transfer the locks from old to new object
    newObject.m_locksCount = oldObject.m_locksCount;
    newObject.m_locksForWriteCount = oldObject.m_locksForWriteCount;
    newObject.m_exclusiveLock = oldObject.m_exclusiveLock;
    newObject.m_lockForSubObject = oldObject.m_lockForSubObject;
    newObject.m_noLock = oldObject.m_noLock;
    // The old object is not considered locked anymore and should be
    // discarded. A previous call the the new object's constructor should
    oldObject.m_locksCount =  0;
    oldObject.m_locksForWriteCount = 0;
    oldObject.m_exclusiveLock = nullptr;
    oldObject.m_lockForSubObject = nullptr;
    oldObject.m_noLock=false;
  }

  /**
   *
   * @param subObject
   */

  /**
   * Dereference a sub object at destruction time
   * @param subObject
   */
  void dereferenceSubObject(ObjectOpsBase & subObject) {
    m_subObjectsOps.remove(&subObject);
  }

  // Each child class will have to call releaseIfNeeded() in their own destructor,
  // as it relies on pure virtual members of this base class.
  virtual ~ScopedLock() = default;

  CTA_GENERATE_EXCEPTION_CLASS(AlreadyLocked);
  CTA_GENERATE_EXCEPTION_CLASS(NotLocked);
  CTA_GENERATE_EXCEPTION_CLASS(MissingAddress);

protected:
  ScopedLock(): m_objectOps(nullptr), m_locked(false) {}
  std::unique_ptr<Backend::ScopedLock> m_lock;
  ObjectOpsBase * m_objectOps;
  std::list <ObjectOpsBase *> m_subObjectsOps;
  bool m_locked;
  void checkNotLocked() {
    if (m_locked)
      throw AlreadyLocked("In ScopedLock::checkNotLocked: trying to lock an already locked lock");
  }
  void checkLocked() {
    if (!m_locked)
      throw NotLocked("In ScopedLock::checkLocked: trying to unlock an unlocked lock");
  }
  void checkObjectAndAddressSet(ObjectOpsBase * oob = nullptr) {
    // By default we deal with the main object.
    if (!oob) oob = m_objectOps;
    if (!oob) {
      throw MissingAddress("In ScopedLock::checkAddressSet: trying to lock a nullptr object");
    } else if (!oob->m_nameSet || oob->m_name.empty()) {
      throw MissingAddress("In ScopedLock::checkAddressSet: trying to lock an object without address");
    }
  }
  virtual void releaseIfNeeded() {
    if(!m_locked) return;
    m_lock.reset(nullptr);
    m_locked = false;
    setObjectUnlocked(m_objectOps);
    // Releasing a lock voids the object content in memory as stored object can now change.
    m_objectOps->m_payloadInterpreted = false;
    // Apply the same to sub objects
    for (auto & oob: m_subObjectsOps) {
      setObjectUnlocked(oob);
      oob->m_payloadInterpreted = false;
    }
  }
};

class ScopedSharedLock: public ScopedLock {
public:
  ScopedSharedLock() = default;
  explicit ScopedSharedLock(ObjectOpsBase& oo) {
    lock(oo);
  }

  void setObjectLocked(ObjectOpsBase* objectOps) override final {
    objectOps->m_locksCount++;
  }

  void setObjectUnlocked(ObjectOpsBase* objectOps) override final {
    objectOps->m_locksCount--;
  }

  void lock(ObjectOpsBase & oo) {
    checkNotLocked();
    m_objectOps  = & oo;
    checkObjectAndAddressSet();
    m_lock.reset(m_objectOps->m_objectStore.lockShared(m_objectOps->getAddressIfSet()));
    ScopedSharedLock::setObjectLocked(m_objectOps);
    m_locked = true;
  }

  virtual ~ScopedSharedLock() {
    releaseIfNeeded();
  }

};

class ScopedExclusiveLock: public ScopedLock {
public:
  ScopedExclusiveLock() = default;
  ScopedExclusiveLock(ObjectOpsBase & oo, uint64_t timeout_us = 0) {
    lock(oo, timeout_us);
  }

  void setObjectLocked(ObjectOpsBase* objectOps) override {
    objectOps->m_locksCount++;
    objectOps->m_locksForWriteCount++;
  }

  void setObjectUnlocked(ObjectOpsBase* objectOps) override {
    objectOps->m_locksCount--;
    objectOps->m_locksForWriteCount--;
  }

  void lock(ObjectOpsBase & oo, uint64_t timeout_us = 0) {
    checkNotLocked();
    m_objectOps = &oo;
    checkObjectAndAddressSet();
    m_lock.reset(m_objectOps->m_objectStore.lockExclusive(m_objectOps->getAddressIfSet(), timeout_us));
    ScopedExclusiveLock::setObjectLocked(m_objectOps);
    m_objectOps->m_exclusiveLock = this;
    m_locked = true;
  }

  /** Move the locked object reference to a new one. This is done when the locked
   * object is a GenericObject and the caller instantiated a derived object from
   * it. The lock follows the move.
   * We check we move the lock from a Generic object (this is the only allowed
   * use case).
   * New object's locks are moved from the old one (referenced in the lock)
   */
  void transfer(ObjectOpsBase & newObject) {
    // Sanity checks: we should be the lock for this object.
    if ((m_objectOps->m_exclusiveLock && m_objectOps->m_exclusiveLock != this) ||
            (m_objectOps->m_lockForSubObject && m_objectOps->m_lockForSubObject != this)) {
      std::stringstream err;
      err << "In ScopedExclusiveLock::transfer(): we should be this object's lock (and are not): "
          << std::hex << std::showbase << " exclusiveLock=" << m_objectOps->m_exclusiveLock
          << " lockForSubObject=" << m_objectOps->m_lockForSubObject
          << " this=" << this;
      throw exception::Exception (err.str());
    }
    ScopedLock::transfer(*m_objectOps, newObject);
  }

  virtual ~ScopedExclusiveLock() {
    releaseIfNeeded();
  }

};

template <class PayloadType, serializers::ObjectType PayloadTypeId>
class ObjectOps: public ObjectOpsBase {
protected:
  ObjectOps(Backend & os, const std::string & name): ObjectOpsBase(os) {
    setAddress(name);
  }

  explicit ObjectOps(Backend& os) : ObjectOpsBase(os) {}

  virtual ~ObjectOps() = default;

public:
  void fetch() {
    // Check that the object is locked, one way or another
    if(!m_locksCount)
      throw NotLocked("In ObjectOps::fetch(): object not locked");
    fetchBottomHalf();
  }

  void fetchNoLock() {
    m_noLock = true;
    fetchBottomHalf();
  }

  void fetchBottomHalf() {
    m_existingObject = true;
    // Get the header from the object store
    getHeaderFromObjectStore();
    // Interpret the data
    getPayloadFromHeader();
  }

  class AsyncLockfreeFetcher {
    friend class ObjectOps;
    explicit AsyncLockfreeFetcher(ObjectOps& obj): m_obj(obj) {}
  public:
    void wait() {
      // Current simplification: the parsing of the header/payload is synchronous.
      // This could be delegated to the backend.
      auto objData = m_asyncLockfreeFetcher->wait();
      m_obj.m_noLock = true;
      m_obj.m_existingObject = true;
      m_obj.getHeaderFromObjectData(objData);
      m_obj.getPayloadFromHeader();
    }
  private:
    ObjectOps & m_obj;
    std::unique_ptr<Backend::AsyncLockfreeFetcher> m_asyncLockfreeFetcher;
  };
  friend AsyncLockfreeFetcher;

  AsyncLockfreeFetcher * asyncLockfreeFetch () {
    std::unique_ptr<AsyncLockfreeFetcher> ret;
    ret.reset(new AsyncLockfreeFetcher(*this));
    ret->m_asyncLockfreeFetcher.reset(m_objectStore.asyncLockfreeFetch(getAddressIfSet()));
    return ret.release();
  }

  class AsyncInserter {
    friend class ObjectOps;
    explicit AsyncInserter(ObjectOps& obj) : m_obj(obj) {}
  public:
    void wait() {
      m_asyncCreator->wait();
      m_obj.m_existingObject = true;
    }
  private:
    ObjectOps & m_obj;
    std::unique_ptr<Backend::AsyncCreator> m_asyncCreator;
  };
  friend AsyncInserter;

  AsyncInserter * asyncInsert() {
    std::unique_ptr<AsyncInserter> ret;
    ret.reset(new AsyncInserter(*this));
    // Current simplification: the parsing of the header/payload is synchronous.
    // This could be delegated to the backend.
    // Check that we are not dealing with an existing object
    if (m_existingObject)
      throw NotNewObject("In ObjectOps::asyncInsert: trying to insert an already exitsting object");
    // Check that the object is ready in memory
    if (!m_headerInterpreted || !m_payloadInterpreted)
      throw NotInitialized("In ObjectOps::insert: trying to insert an uninitialized object");
    // Push the payload into the header and write the object
    // We don't require locking here, as the object does not exist
    // yet in the object store (and this is ensured by the )
    m_header.set_payload(m_payload.SerializeAsString());
    ret->m_asyncCreator.reset(m_objectStore.asyncCreate(getAddressIfSet(), m_header.SerializeAsString()));
    return ret.release();
  }

  virtual void commit() {
    checkPayloadWritable();
    if (!m_existingObject)
      throw NewObject("In ObjectOps::commit: trying to update a new object");
    // Serialise the payload into the header
    try {
      m_header.set_payload(m_payload.SerializeAsString());
    } catch (std::exception & stdex) {
      cta::exception::Exception ex(std::string("In ObjectOps::commit(): failed to serialize: ")+stdex.what());
      throw ex;
    }
    // Write the object
    m_objectStore.atomicOverwrite(getAddressIfSet(), m_header.SerializeAsString());
  }

  CTA_GENERATE_EXCEPTION_CLASS(WrongTypeForGarbageCollection);
  /**
   * This function should be overloaded in the inheriting classes
   */
  virtual void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) = 0;

protected:

  virtual void getPayloadFromHeader () {
    if (!m_payload.ParseFromString(m_header.payload())) {
      // Use the tolerant parser to assess the situation.
      m_header.ParsePartialFromString(m_header.payload());
      // Base64 encode the payload for diagnostics.
      const bool noNewLineInBase64Output = false;
      std::string payloadBase64;
      CryptoPP::StringSource ss1(m_header.payload(), true,
        new CryptoPP::Base64Encoder(
           new CryptoPP::StringSink(payloadBase64), noNewLineInBase64Output));
      throw cta::exception::Exception(std::string("In <ObjectOps") + typeid(PayloadType).name() +
              ">::getPayloadFromHeader(): could not parse payload: " + m_header.InitializationErrorString() +
              " size=" + std::to_string(m_header.payload().size()) + " data(b64)=\"" +
              payloadBase64 + "\"");
    }
    m_payloadInterpreted = true;
  }

  virtual void getHeaderFromObjectData(const std::string & objData) {
    if (!m_header.ParseFromString(objData)) {
      // Use the tolerant parser to assess the situation.
      m_header.ParsePartialFromString(objData);
      // Base64 encode the header for diagnostics.
      const bool noNewLineInBase64Output = false;
      std::string objDataBase64;
      CryptoPP::StringSource ss1(objData, true,
        new CryptoPP::Base64Encoder(
           new CryptoPP::StringSink(objDataBase64), noNewLineInBase64Output));
      throw cta::exception::Exception(std::string("In ObjectOps<") + typeid(PayloadType).name() +
              ">::getHeaderFromObjectData(): could not parse header: " + m_header.InitializationErrorString() +
              " size=" + std::to_string(objData.size()) + " data(b64)=\"" +
              objDataBase64 + "\"");
    }
    if (m_header.type() != payloadTypeId) {
      std::stringstream err;
      err << "In ObjectOps::getHeaderFromObjectStore wrong object type: "
          << "found=" << m_header.type() << " expected=" << payloadTypeId;
      throw ObjectOpsBase::WrongType(err.str());
    }
    m_headerInterpreted = true;
  }

  void getHeaderFromObjectStore () {
    auto objData=m_objectStore.read(getAddressIfSet());
    getHeaderFromObjectData(objData);
  }

public:
  /**
   * Fill up the header and object with its default contents
   */
  virtual void initialize() {
    if (m_headerInterpreted || m_existingObject)
      throw NotNewObject("In ObjectOps::initialize: trying to initialize an exitsting object");
    m_header.set_type(payloadTypeId);
    m_header.set_version(0);
    m_header.set_owner("");
    m_header.set_backupowner("");
    m_headerInterpreted = true;
  }

  virtual void insert() {
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

}} // namespace cta::objectstore
