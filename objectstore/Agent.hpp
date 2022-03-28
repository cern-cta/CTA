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

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include "common/Timer.hpp"
#include <cxxabi.h>
#include <list>

namespace cta { namespace objectstore {

class GenericObject;
class AgentReference;
class GarbageCollector;
class Sorter;

/**
 * Class containing agent information and managing the update of the
 * agent's persitent representation in the object store.
 * This object also manages the object id generator, and keeps track of
 * a ContextHandles
 * In all the agent is the case class for all actions.
 * It handles (in the base class):
 */

class Agent: public ObjectOps<serializers::Agent, serializers::Agent_t> {
  friend class AgentReference;
  friend class GarbageCollector;
  friend class AgentWrapper;
  friend class Sorter;
public:
  CTA_GENERATE_EXCEPTION_CLASS(AgentStillOwnsObjects);
  Agent(GenericObject & go);
  Agent(const std::string & name, Backend & os);

  void initialize();

  void insertAndRegisterSelf(log::LogContext & lc);

  void removeAndUnregisterSelf(log::LogContext & lc);

  bool isEmpty();

  void setBeingGarbageCollected();

  bool isBeingGarbageCollected();

  void setNeedsGarbageCollection();

  bool needsGarbageCollection();

  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;

private:
  void addToOwnership(const std::string& name);

  void removeFromOwnership(std::string name);

public:
  std::list<std::string> getOwnershipList();

  std::set<std::string> getOwnershipSet();

  void resetOwnership(const std::set<std::string>& ownershipSet);

  size_t getOwnershipListSize();

  std::string dump();

  void bumpHeartbeat();

  uint64_t getHeartbeatCount();

  // Timeout in s
  double getTimeout();

  // We set the timeout as an integer number of us.
  void setTimeout_us(uint64_t timeout);
  /**
   * Helper function to transfer ownership of the next valid head object of a
   * container to the agent.
   * The object is returned locked on the lock passed as reference.
   * @param container
   * @param object
   */
  template <class Cont, class Obj>
  void popFromContainer (Cont & container, Obj & object, ScopedExclusiveLock & objLock) {
    // Lock the container for write first.
    ScopedExclusiveLock contLock(container);
    while(true) {
      // Check there is an object to pop.
      // This throws an exception if nothing's available, we just let it through
      std::string nextObjName = container.peek();
      // Check that the object exists, is of the right type (implicit), etc...
      // Set the name of the object
      object.setName(nextObjName);
      // Validate that the object exists.
      // If not, it is a dangling pointer. Pop and continue
      if (!object.exists()) {
        container.pop();
        container.commit();
        continue;
      }
      // Try to lock the object. Exception will be let through
      objLock.lock(object);
      // Fetch the object. Exception will be let through.
      object.fetch();
      // Check that the container owns the object. If not, it is a dangling pointer
      if (container.getNameIfSet() != object.getOwner()) {
        objLock.release();
        container.pop();
        container.commit();
        continue;
      }
      // If we get here, then we can proceed with the ownership transfer
      // First, add a pointer to the object on the agent
      ScopedExclusiveLock agentLock(*this);
      fetch();
      addToOwnership(nextObjName);
      commit();
      agentLock.release();
      // Then make the pointer agent's ownership official
      object.setOwner(getAddressIfSet());
      // The container should be the backup owner, so let's make sure!
      object.setBackupOwner(container.getNameIfSet());
      // Commit the object
      object.commit();
      // And remove the now dangling pointer from the container
      container.pop();
      container.commit();
      return;
    }
  }

private:
  uint64_t m_nextId;
};

}}
