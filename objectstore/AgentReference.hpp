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

#include "objectstore/Backend.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/log/Logger.hpp"
#include "common/log/LogContext.hpp"
#include "AgentReferenceInterface.hpp"
#include <atomic>
#include <string>
#include <future>
#include <list>
#include <set>

namespace cta::objectstore {

class Agent;

/**
 * A class allowing the passing of the address of an Agent object, plus a thread safe
 * object name generator, that will allow unique name generation by several threads.
 * This object should be created once and for all per session (as the corresponding
 * Agent object in the object store).
 * A process
 */
class AgentReference: public AgentReferenceInterface{
public:
  /**
   * Constructor will implicitly generate the address of the Agent object.
   * @param clientType is an indicative string used to generate the agent object's name.
   */
  AgentReference(const std::string &clientType, log::Logger &logger);

  /**
   * Generates a unique address for a newly created child object. This function is thread
   * safe.
   * @param childType the name of the child object type.
   * @return a unique address for the child object, derived from the agent's address.
   */
  std::string nextId(const std::string & childType);

  /**
   * Adds an object address to the referenced agent. The additions and removals
   * are queued in memory so that several threads can share the same access.
   * The execution order is guaranteed.
   * @param objectAddress
   * @param backend reference to the backend to use.
   */
  void addToOwnership(const std::string &objectAddress, objectstore::Backend& backend) override;

  /**
   * Adds a list of object addresses to the referenced agent. The addition is immediate.
   * @param objectAdresses
   * @param backend reference to the backend to use.
   */
  void addBatchToOwnership(const std::list<std::string> &objectAdresses, objectstore::Backend& backend) override;

  /**
   * Removes an object address from the referenced agent. The additions and removals
   * are queued in memory so that several threads can share the same access.
   * The execution order is guaranteed.
   * @param objectAddress
   */
  void removeFromOwnership(const std::string &objectAddress, objectstore::Backend& backend) override;

  /**
   * Removes a list of object addresses to the referenced agent. The removal is immediate.
   * @param objectAdresses
   * @param backend reference to the backend to use.
   */
  void removeBatchFromOwnership(const std::list<std::string> &objectAdresses, objectstore::Backend& backend) override;

  /**
   * Bumps up the heart beat of the agent. This action is queued in memory like the
   * additions and removals from ownership.
   */
  void bumpHeatbeat(objectstore::Backend& backend);

  /**
   * Gets the address of the Agent object generated on construction.
   * @return the agent object address.
   */
  std::string getAgentAddress() override;
private:
  static std::atomic<uint64_t> g_nextAgentId;
  std::atomic<uint64_t> m_nextId;
  std::string m_agentAddress;

  /**
   * An enumeration describing all the queueable operations
   */
  enum class AgentOperation: char {
    Add,
    Remove,
    AddBatch,
    RemoveBatch,
    Heartbeat
  };

  /**
   * A set used to test for ownership modifying actions.
   */
  std::set<AgentOperation> m_ownerShipModifyingOperations = { AgentOperation::Add, AgentOperation::Remove,
    AgentOperation::AddBatch, AgentOperation::RemoveBatch };

  /**
   * An operation with its parameter and promise
   */
  struct Action {
    Action(AgentOperation op, const std::string & objectAddress, const std::list<std::string> & objectAddressSet):
      op(op), objectAddress(objectAddress), objectAddressSet(objectAddressSet) {}
    AgentOperation op;
    const std::string & objectAddress;
    const std::list<std::string> & objectAddressSet;
    std::promise<void> promise;
    /***
     * A mutex ensuring the object will not be released before the promise's result
     * is fully pushed.
     */
    threading::Mutex mutex;
    ~Action() {
      // The setting of promise result will be protected by this mutex, so destruction
      // will only happen after promise setting is complete.
      threading::MutexLocker ml(mutex);
    }
  };

  /**
   * The queue with the lock and flush control
   */
  struct ActionQueue {
    threading::Mutex mutex;
    std::list<std::shared_ptr<Action>> queue;
  };

  /**
   * Helper function applying the action to the already fetched agent.
   * Ownership operations are done on the pre-extracted ownershipSet.
   * @param action
   * @param agent
   */
  void appyAction(Action& action, objectstore::Agent& agent,
    std::set<std::string> & ownershipSet, log::LogContext &lc);

  /**
   * The global function actually doing the job: creates a queue if needed, add
   * the action to it and flushes them based on time and count. Uses an algorithm
   * similar to queueing in ArchiveQueues and RetrieveQeueues.
   * @param action the action
   */
  void queueAndExecuteAction(std::shared_ptr<Action> action, objectstore::Backend& backend);

  threading::Mutex m_currentQueueMutex;
  std::shared_ptr<ActionQueue> m_currentQueue;
  /**
   * This pointer holds a promise that will be picked up by the thread managing
   * the a queue in memory (promise(n)). The same thread will leave a fresh promise
   * (promise(n+1) in this pointer for the next thread to pick up. The thread will
   * then wait for promise(n) to be fullfilled to flush to queue to the object store
   * and will fullfill promise(n+1) after doing so.
   * This will ensure that the queues will be flushed in order, one at a time.
   * One at a time also minimize contention on the object store.
   */
  std::shared_ptr<std::promise<void>> m_nextQueueExecutionPromise;
  /**
   * This future will be immediately extracted from the m_nextQueueExecutionPromise before any other thread touches it.
   */
  std::future<void> m_nextQueueExecutionFuture;

  /**
   * The logger allows printing debug information
   */
  log::Logger &m_logger;
};

} // namespace cta::objectstore
