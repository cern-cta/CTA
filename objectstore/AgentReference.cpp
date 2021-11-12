/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <sys/syscall.h>
#include <unistd.h>

#include <iomanip>
#include <sstream>

#include "Agent.hpp"
#include "AgentReference.hpp"
#include "common/exception/Errnum.hpp"
#include "common/utils/utils.hpp"

namespace cta { namespace objectstore {

std::atomic <uint64_t> AgentReference::g_nextAgentId(0);

AgentReference::AgentReference(const std::string & clientType, log::Logger &logger):
m_logger(logger) {
  m_nextId=0;
  std::stringstream aid;
  // Get time
  time_t now = time(0);
  struct tm localNow;
  localtime_r(&now, &localNow);
  // Get hostname
  char host[200];
  cta::exception::Errnum::throwOnMinusOne(::gethostname(host, sizeof(host)),
    "In AgentId::AgentId:  failed to gethostname");
  // gettid is a safe system call (never fails)
  uint64_t id=g_nextAgentId++;
  aid << clientType << "-" << host << "-" << syscall(SYS_gettid) << "-"
    << 1900 + localNow.tm_year
    << std::setfill('0') << std::setw(2)
    << 1 + localNow.tm_mon
    << std::setw(2) << localNow.tm_mday << "-"
    << std::setw(2) << localNow.tm_hour << ":"
    << std::setw(2) << localNow.tm_min << ":"
    << std::setw(2) << localNow.tm_sec << "-"
    << id;
  m_agentAddress = aid.str();
  // Initialize the serialization token for queued actions (lock will make helgrind
  // happy, but not really needed
  threading::MutexLocker ml(m_currentQueueMutex);
  m_nextQueueExecutionPromise.reset(new std::promise<void>);
  m_nextQueueExecutionFuture = m_nextQueueExecutionPromise->get_future();
  m_nextQueueExecutionPromise->set_value();
}

std::string AgentReference::getAgentAddress() {
  return m_agentAddress;
}

std::string AgentReference::nextId(const std::string& childType) {
  std::stringstream id;
  id << childType << "-" << m_agentAddress << "-" << m_nextId++;
  return id.str();
}

void AgentReference::addToOwnership(const std::string& objectAddress, objectstore::Backend& backend) {
  std::shared_ptr<Action> a (new Action(AgentOperation::Add, objectAddress, std::list<std::string>()));
  queueAndExecuteAction(a, backend);
}

void AgentReference::addBatchToOwnership(const std::list<std::string>& objectAdresses, objectstore::Backend& backend) {
  std::shared_ptr<Action> a (new Action(AgentOperation::AddBatch, "", objectAdresses));
  queueAndExecuteAction(a, backend);
}

void AgentReference::removeFromOwnership(const std::string& objectAddress, objectstore::Backend& backend) {
  std::shared_ptr<Action> a (new Action(AgentOperation::Remove, objectAddress, std::list<std::string>()));
  queueAndExecuteAction(a, backend);
}

void AgentReference::removeBatchFromOwnership(const std::list<std::string>& objectAdresses, objectstore::Backend& backend) {
  std::shared_ptr<Action> a (new Action(AgentOperation::RemoveBatch, "", objectAdresses));
  queueAndExecuteAction(a, backend);
}

void AgentReference::bumpHeatbeat(objectstore::Backend& backend) {
  std::shared_ptr<Action> a (new Action(AgentOperation::Heartbeat, "", std::list<std::string>()));
  queueAndExecuteAction(a, backend);
}


void AgentReference::queueAndExecuteAction(std::shared_ptr<Action> action, objectstore::Backend& backend) {
  // First, we need to determine if a queue exists or not.
  // If so, we just use it, and if not, we create and serve it.
  threading::MutexLocker ulGlobal(m_currentQueueMutex);
  if (m_currentQueue) {
    // There is already a queue
    threading::MutexLocker ulQueue(m_currentQueue->mutex);
    m_currentQueue->queue.push_back(action);
    // Get hold of the future before the promise gets a chance to be accessed
    auto actionFuture=action->promise.get_future();
    // Release the locks and wait for action execution
    ulQueue.unlock();
    ulGlobal.unlock();
    actionFuture.get();
  } else {
    // There is no queue, so we need to create and serve it ourselves.
    // To make sure there is no lifetime issues, we make it a shared_ptr
    std::shared_ptr<ActionQueue> q(new ActionQueue);
    // Lock the queue
    threading::MutexLocker ulq(q->mutex);
    // Get it referenced
    m_currentQueue = q;
    // Get our execution promise and future and leave one behind.
    std::shared_ptr<std::promise<void>> promiseForThisQueue(m_nextQueueExecutionPromise);
    auto futureForThisQueue = std::move(m_nextQueueExecutionFuture);
    // Leave a promise behind for the next queue, and set the future.
    m_nextQueueExecutionPromise.reset(new std::promise<void>);
    m_nextQueueExecutionFuture=m_nextQueueExecutionPromise->get_future();
    // Keep a pointer to it, so we will signal our own completion to our successor queue.
    std::shared_ptr<std::promise<void>> promiseForNextQueue = m_nextQueueExecutionPromise;
    // We can now unlock the queue and the general lock: queuing is open.
    ulq.unlock();
    ulGlobal.unlock();
    // Wait for previous queue to complete so we will not contend with other threads while
    // updating the object store.
    futureForThisQueue.get();
    // Make sure we are not listed anymore as the queue taking jobs.
    // We should still be the listed queue
    ulGlobal.lock();
    if (m_currentQueue != q) {
      throw cta::exception::Exception("In AgentReference::queueAndExecuteAction(): our queue is not the listed one as expected.");
    }
    m_currentQueue.reset();
    ulGlobal.unlock();
    // Make sure no leftover thread is still writing to the queue.
    ulq.lock();
    // Off we go! Add the actions to the queue
    try {
      objectstore::Agent ag(m_agentAddress, backend);
      log::LogContext lc(m_logger);
      log::ScopedParamContainer params(lc);
      params.add("agentObject", m_agentAddress);
      utils::Timer t;
      objectstore::ScopedExclusiveLock agl(ag);
      double agentLockTime = t.secs(utils::Timer::resetCounter);
      ag.fetch();
      if (ag.isBeingGarbageCollected()) {
        log::ScopedParamContainer params(lc);
        params.add("agentObject", ag.getAddressIfSet());
        lc.log(log::CRIT, "In AgentReference::queueAndExecuteAction(): agent object being garbage collected. Exiting (segfault).");
        cta::utils::segfault();
        ::exit(EXIT_FAILURE);
      }
      double agentFetchTime = t.secs(utils::Timer::resetCounter);
      size_t agentOwnershipSizeBefore = ag.getOwnershipListSize();
      size_t operationsCount = q->queue.size() + 1;
      bool ownershipModification = false;
      // First, determine if any action is an ownership modification
      if (m_ownerShipModifyingOperations.count(action->op))
        ownershipModification = true;
      if (!ownershipModification) {
        for (auto &a: q->queue) {
          if (m_ownerShipModifyingOperations.count(a->op)) {
            ownershipModification = true;
            break;
          }
        }
      }
      std::set<std::string> ownershipSet;
      // If necessary, we will dump the ownership list into a set, manipulate it in memory,
      // and then recreate it.
      if (ownershipModification) ownershipSet = ag.getOwnershipSet();
      // First we apply our own modification
      appyAction(*action, ag, ownershipSet, lc);
      // Then those of other threads
      for (auto a: q->queue) {
        threading::MutexLocker ml(a->mutex);
        appyAction(*a, ag, ownershipSet, lc);
      }
      // Record the new ownership if needed.
      if (ownershipModification) ag.resetOwnership(ownershipSet);
      size_t agentOwnershipSizeAfter = ag.getOwnershipListSize();
      double agentUpdateTime = t.secs(utils::Timer::resetCounter);
      // and commit
      ag.commit();
      double agentCommitTime = t.secs(utils::Timer::resetCounter);
      if (ownershipModification && false) { // Log disabled to not log too much.
        log::ScopedParamContainer params(lc);
        params.add("agentOwnershipSizeBefore", agentOwnershipSizeBefore)
              .add("agentOwnershipSizeAfter", agentOwnershipSizeAfter)
              .add("operationsCount", operationsCount)
              .add("agentLockTime", agentLockTime)
              .add("agentFetchTime", agentFetchTime)
              .add("agentUpdateTime", agentUpdateTime)
              .add("agentCommitTime", agentCommitTime);
        lc.log(log::INFO, "In AgentReference::queueAndExecuteAction(): executed a batch of actions.");
      }
      // We avoid global log (with a count) as we would get one for each heartbeat.
    } catch (...) {
      // Something wend wrong: , we release the next batch of changes
      promiseForNextQueue->set_value();
      // We now pass the exception to all threads
      for (auto a: q->queue) {
        threading::MutexLocker ml(a->mutex);
        a->promise.set_exception(std::current_exception());
      }
      // And to our own caller
      throw;
    }
    // Things went well. We pass the token to the next queue
    promiseForNextQueue->set_value();
    // and release the other threads
    for (auto a: q->queue) {
      threading::MutexLocker ml(a->mutex);
      a->promise.set_value();
    }
  }
}

void AgentReference::appyAction(Action& action, objectstore::Agent& agent,
    std::set<std::string> & ownershipSet, log::LogContext &lc) {
  switch (action.op) {
  case AgentOperation::Add:
  {
    ownershipSet.insert(action.objectAddress);
    log::ScopedParamContainer params(lc);
    params.add("ownedObject", action.objectAddress);
    lc.log(log::DEBUG, "In AgentReference::appyAction(): added object to ownership.");
    break;
  }
  case AgentOperation::AddBatch:
  {
    for (const auto & oa: action.objectAddressSet) {
      ownershipSet.insert(oa);
      log::ScopedParamContainer params(lc);
      params.add("ownedObject", oa);
      lc.log(log::DEBUG, "In AgentReference::appyAction(): added object to ownership (by batch).");
    }

    break;
  }
  case AgentOperation::Remove:
  {
    ownershipSet.erase(action.objectAddress);
    log::ScopedParamContainer params(lc);
    params.add("ownedObject", action.objectAddress);
    lc.log(log::DEBUG, "In AgentReference::appyAction(): removed object from ownership.");
    break;
  }
  case AgentOperation::RemoveBatch:
  {
    for (const auto & oa: action.objectAddressSet) {
      ownershipSet.erase(oa);
      log::ScopedParamContainer params(lc);
      params.add("ownedObject", oa);
      lc.log(log::DEBUG, "In AgentReference::appyAction(): removed object from ownership (by batch).");
    }
    break;
  }
  case AgentOperation::Heartbeat:
    agent.bumpHeartbeat();
    break;
  default:
    throw cta::exception::Exception("In AgentReference::appyAction(): unknown operation.");
  }
}


}} // namespace cta::objectstore
