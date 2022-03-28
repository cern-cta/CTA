
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

#include <unistd.h>
#include <syscall.h>

#include <future>

#include "common/log/LogContext.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"
#include "objectstore/ArchiveQueue.hpp"
#include "objectstore/ArchiveRequest.hpp"
#include "objectstore/Helpers.hpp"
#include "objectstore/RetrieveQueue.hpp"
#include "objectstore/RetrieveRequest.hpp"

namespace cta {
// Forward declaration
class OStoreDB;
  namespace ostoredb {
/**
 * A container to which the ownership of the archive queue (and more important,
 * its lock) will be passed. This container will be passed as a shared pointer
 * to the caller of sharedAddToArchiveQueue, so they can delete their copy AFTER
 * updating the ownership of their requests.
 */
template <class Queue, class Request>
class SharedQueueLock {
  template <class, class>
  friend class MemQueue;
public:
  SharedQueueLock(log::LogContext & logContext): m_logContext(logContext) {}
  ~SharedQueueLock();
private:
  std::unique_ptr<objectstore::ScopedExclusiveLock> m_lock;
  std::unique_ptr<Queue> m_queue;
  std::string m_queueIndex;
  std::shared_ptr<std::promise<void>> m_promiseForSuccessor;
  log::LogContext m_logContext;
  utils::Timer m_timer;
};

template <class Queue, class Request>
SharedQueueLock<Queue, Request>::~SharedQueueLock() {
  double waitTime = m_timer.secs(utils::Timer::resetCounter);
  bool skipQueuesTrim=false;
  if (m_lock.get() && m_lock->isLocked()) {
    m_lock->release();
  } else {
    m_logContext.log(log::ERR, "In SharedQueueLock::~SharedQueueLock(): the lock was not present or not locked. Skipping unlock.");
    skipQueuesTrim=true;
  }
  double queueUnlockTime = m_timer.secs(utils::Timer::resetCounter);
  // The next update of the queue can now proceed
  if (m_promiseForSuccessor.get()) {
    m_promiseForSuccessor->set_value();
  } else {
    m_logContext.log(log::ERR, "In SharedQueueLock::~SharedQueueLock(): the promise was not present. Skipping value setting.");
    skipQueuesTrim=true;
  }
  if (skipQueuesTrim) return;
  double successorUnlockTime = m_timer.secs(utils::Timer::resetCounter);
  // We can now cleanup the promise/future couple if they were not picked up to trim the maps.
  // A next thread finding them unlocked or absent will be equivalent.
  threading::MutexLocker globalLock(MemQueue<Queue, Request>::g_mutex);
  // If there is an promise AND it is ours, we remove it.
  try {
    if (MemQueue<Queue, Request>::g_promises.at(m_queueIndex).get() == m_promiseForSuccessor.get()) {
      MemQueue<Queue, Request>::g_futures.erase(m_queueIndex);
      MemQueue<Queue, Request>::g_promises.erase(m_queueIndex);
    }
  } catch (std::out_of_range &) {}
  double inMemoryQueuesCleanupTime = m_timer.secs();
  log::ScopedParamContainer params(m_logContext);
  params.add("objectQueue", m_queue->getAddressIfSet())
        .add("waitTime", waitTime)
        .add("queueUnlockTime", queueUnlockTime)
        .add("successorUnlockTime", successorUnlockTime)
        .add("inMemoryQueuesCleanupTime", inMemoryQueuesCleanupTime);
  m_logContext.log(log::INFO, "In SharedQueueLock::~SharedQueueLock(): unlocked the archive queue pointer.");
}

template <class Request, class Queue>
class MemQueueRequest {
  template <class, class>
  friend class MemQueue;
public:
  MemQueueRequest(typename Request::JobDump & job,
    Request & archiveRequest): m_job(job), m_request(archiveRequest), m_tid(::syscall(SYS_gettid)) {}
  virtual ~MemQueueRequest() {
    threading::MutexLocker ml(m_mutex);
  }
private:
  typename Request::JobDump & m_job;
  Request & m_request;
  std::promise<void> m_promise;
  std::shared_ptr<SharedQueueLock<Queue, Request>> m_returnValue;
  // Mutex protecting users against premature deletion
  threading::Mutex m_mutex;
  // Helper for debugging
  pid_t m_tid;
};

template <class Request, class Queue>
class MemQueue {
  template <class, class>
  friend class SharedQueueLock;
public:
  /**
   * This function adds ArchiveRequeuest to an ArchiveQueue in batch.
   * A static function that will either create the shared queue for a given
   * tape pool if none exist, of add the job to it otherwise. When adding
   * the job, the first calling thread will be woken up if enough jobs have been
   * accumulated.
   * The creation and action are done using the global lock, which should be
   * sufficiently fast as we work in memory.
   * All calls sharing the same batch will either succeed of throw the same
   * exception.
   * The address of the archive queue object will be updated in both parameters
   * (job and archiveRequest).
   *
   * @param job to be added to the ArchiveQueue (contains the tape pool name)
   * @param request the request itself.
   * @param oStoreDB reference to the object store, allowing creation of the queue
   * if needed
   * @param logContext log context to log addition of jobs to the queue.
   */
  static std::shared_ptr<SharedQueueLock<Queue, Request>> sharedAddToQueue(typename Request::JobDump & job,
    const std::string & queueIndex, Request & request, OStoreDB & oStoreDB, log::LogContext & logContext);

private:
  /** Mutex that should be locked before attempting any operation */
  threading::Mutex m_mutex;
  /** Add the object */
  void add(std::shared_ptr<MemQueueRequest<Request, Queue>>& request);
  std::list<std::shared_ptr<MemQueueRequest<Request, Queue>>> m_requests;
  static threading::Mutex g_mutex;
  /**
   * A set of per tape pool queues. Their presence indicates that a queue is currently being built up and new objects to be queued
   * should piggy back on it.
   */
  static std::map<std::string, std::shared_ptr<MemQueue>> g_queues;
  /**
   * A set of per tape pool promises. Their presence indicates that a previous queue is currently pending commit. Threads creating
   * a new queue (when there is no queue, yet a promise is present) will wait on them to prevent contention with previous queue update.
   * When a thread creates a queue (there was none) and finds no promise, it will create is own queue and promise, post the queue,
   * immediately push the content of the queue (which is likely, but not guaranteed to be just its own job), and release the promise
   * when done.
   * At completion, if the initial promise is still present in the map, the creating thread will remove it, as no promise of a
   * fulfilled promise are equivalent.
   */
  static std::map<std::string, std::shared_ptr<std::promise<void>>> g_promises;
  /**
   * A set of futures, extracted from the corresponding g_promises.
   */
  static std::map<std::string, std::future<void>> g_futures;

  /** Helper function for sharedAddToArchiveQueue */
  static std::shared_ptr<SharedQueueLock<Queue, Request>> sharedAddToNewQueue(typename Request::JobDump & job, const std::string & queueIndex,
    Request & request, OStoreDB & oStoreDB, log::LogContext & logContext, threading::MutexLocker &globalLock);

  /** Struct holding the job plus request data */
  struct JobAndRequest {
    typename Request::JobDump & job;
    Request & request;
  };

  /** Helper function handling the difference between archive and retrieve (vid vs tapepool) */
  static void specializedAddJobsToQueueAndCommit(std::list<JobAndRequest> & jobsToAdd, Queue & queue,
    objectstore::AgentReference & agentReference, log::LogContext & logContext);

  /** Helper function updating the cached retrieve queue stats. Noop for archive queues */
  static void specializedUpdateCachedQueueStats(Queue &queue);
};

template <class Request, class Queue>
cta::threading::Mutex MemQueue<Request, Queue>::g_mutex;

template <class Request, class Queue>
std::map<std::string, std::shared_ptr<MemQueue<Request, Queue>>> MemQueue<Request, Queue>::g_queues;

template <class Request, class Queue>
std::map<std::string, std::shared_ptr<std::promise<void>>> MemQueue<Request, Queue>::g_promises;

template <class Request, class Queue>
std::map<std::string, std::future<void>> MemQueue<Request, Queue>::g_futures;

template <class Request, class Queue>
std::shared_ptr<SharedQueueLock<Queue, Request>> MemQueue<Request, Queue>::sharedAddToQueue(typename Request::JobDump& job,
    const std::string & queueIndex, Request& request, OStoreDB & oStoreDB, log::LogContext & logContext) {
  // 1) Take the global lock (implicit in the constructor)
  threading::MutexLocker globalLock(g_mutex);
  std::shared_ptr<MemQueue> q;
  try {
    // 2) Determine if the queue exists already or not
    q = g_queues.at(queueIndex);
  } catch (std::out_of_range &) {
    // The queue is not there. We will just create a new one.
    return sharedAddToNewQueue(job, queueIndex, request, oStoreDB, logContext, globalLock);
  }
  // It does: we just ride the train: queue ourselves
  // Lock the queue.
  threading::MutexLocker ulq(q->m_mutex);
  std::shared_ptr<MemQueueRequest<Request, Queue>> maqr(new MemQueueRequest<Request, Queue>(job, request));
  // Extract the future before the other thread gets a chance to touch the promise.
  auto resultFuture = maqr->m_promise.get_future();
  q->add(maqr);
  // Release the queue, forget the queue, and release the global lock
  ulq.unlock();
  q.reset();
  globalLock.unlock();
  // Wait for our request completion (this could throw, if there was a problem)
  resultFuture.get();
  auto ret=maqr->m_returnValue;
  __attribute__((unused)) auto debugMaqr=maqr.get();
  maqr.reset();
  return ret;
}

template <class Request, class Queue>
std::shared_ptr<SharedQueueLock<Queue, Request>> MemQueue<Request, Queue>::sharedAddToNewQueue(
  typename Request::JobDump& job, const std::string & queueIndex, Request& request,
  OStoreDB& oStoreDB, log::LogContext& logContext, threading::MutexLocker &globalLock) {
  utils::Timer timer;
  // Re-check the queue is not there
  if (g_queues.end() != g_queues.find(queueIndex)) {
    throw cta::exception::Exception("In MemQueue::sharedAddToNewQueue(): the queue is present, while it should not!");
  }
  // Create the queue and reference it.
  auto maq = (g_queues[queueIndex] = std::make_shared<MemQueue>());
  // Get the promise from the previous future (if any). This will create the slot
  // as a side effect, but we will populate it right after.
  std::shared_ptr<std::promise<void>> promiseFromPredecessor = g_promises[queueIndex];
  // If the promise from predecessor was present, also extract the future.
  std::future<void> futureFromPredecessor;
  if (promiseFromPredecessor.get()) {
    try {
      futureFromPredecessor = std::move(g_futures.at(queueIndex));
    } catch (std::out_of_range &) {
      throw cta::exception::Exception("In MemQueue::sharedAddToNewQueue(): the future is not present, while it should!");
    }
  }
  // Create the promise and future for successor.
  std::shared_ptr<std::promise<void>> promiseForSuccessor = (g_promises[queueIndex] = std::make_shared<std::promise<void>>());
  g_futures[queueIndex] = promiseForSuccessor->get_future();
  // Release the global list
  globalLock.unlock();
  // Wait on out future, if necessary
  if (promiseFromPredecessor.get()) {
    futureFromPredecessor.get();
  }
  // We are now clear to update the queue in object store.
  // Re-take the global and make sure the queue is not referenced anymore.
  globalLock.lock();
  // Remove the queue for our tape pool. It should be present.
  try {
    if (g_queues.at(queueIndex).get() != maq.get()) {
      throw cta::exception::Exception("In MemQueue::sharedAddToNewQueue(): the queue is not ours, while it should!");
    }
  } catch (std::out_of_range &) {
    throw cta::exception::Exception("In MemQueue::sharedAddToNewQueue(): the queue is not present, while it should!");
  }
  // Checks are fine, let's just drop the queue from the map
  g_queues.erase(queueIndex);
  // Lock the queue, to make sure the last user is done posting. We could do this after
  // releasing the global lock, but helgrind objects.
  threading::MutexLocker ulq(maq->m_mutex);
  // Our mem queue is now unreachable so we can let the global part go
  globalLock.unlock();
  double waitTime = timer.secs(utils::Timer::resetCounter);
  // We can now proceed with the queuing of the jobs in the object store.
  try {
    std::shared_ptr<SharedQueueLock<Queue, Request>> ret(new SharedQueueLock<Queue, Request>(logContext));
    ret->m_promiseForSuccessor=promiseForSuccessor;
    ret->m_queueIndex=queueIndex;
    ret->m_queue.reset(new Queue(oStoreDB.m_objectStore));
    ret->m_lock.reset(new objectstore::ScopedExclusiveLock);
    auto & queue = *ret->m_queue;
    auto & aql = *ret->m_lock;
    objectstore::Helpers::getLockedAndFetchedJobQueue<Queue>(queue, aql,
            *oStoreDB.m_agentReference, queueIndex, common::dataStructures::JobQueueType::JobsToTransferForUser, logContext);
    double getFetchedQueueTime = timer.secs(utils::Timer::resetCounter);
    auto summaryBefore=queue.getJobsSummary();
    size_t addedJobs=1;
    // Build the list of jobs to add to the queue
    std::list<JobAndRequest> jta;
    // First add the job for this thread
    jta.push_back({job, request});
    // We are done with the queue: release the lock to make helgrind happy.
    ulq.unlock();
    // We do the same for all the queued requests
    for (auto &maqr: maq->m_requests) {
      // Add the job
      jta.push_back({maqr->m_job, maqr->m_request});
      addedJobs++;
    }
    // Actually add the jobs.
    specializedAddJobsToQueueAndCommit(jta, queue, *oStoreDB.m_agentReference, logContext);
    double queueProcessAndCommitTime = timer.secs(utils::Timer::resetCounter);
    // Update the cache stats in memory as we hold the queue.
    specializedUpdateCachedQueueStats(queue);
    double cacheUpdateTime = timer.secs(utils::Timer::resetCounter);
    // Log
    auto summaryAfter=queue.getJobsSummary();
    {
      log::ScopedParamContainer params(logContext);
      if (typeid(Queue) == typeid(objectstore::ArchiveQueue)) {
        params.add("type", "Archive")
              .add("tapePool", queueIndex);
      } else if (typeid(Queue) == typeid(objectstore::RetrieveQueue)) {
        params.add("type", "Retrieve")
              .add("tapeVid", queueIndex);
      }
      params.add("objectQueue", queue.getAddressIfSet())
            .add("jobsBefore", summaryBefore.jobs)
            .add("jobsAfter", summaryAfter.jobs)
            .add("bytesBefore", summaryBefore.bytes)
            .add("bytesAfter", summaryAfter.bytes)
            .add("addedJobs", addedJobs)
            .add("waitTime", waitTime)
            .add("getFetchedQueueTime", getFetchedQueueTime)
            .add("queueProcessAndCommitTime", queueProcessAndCommitTime)
            .add("cacheUpdateTime", cacheUpdateTime)
            .add("totalEnqueueTime", getFetchedQueueTime + queueProcessAndCommitTime
                                    + cacheUpdateTime + timer.secs());
      logContext.log(log::INFO, "In MemQueue::sharedAddToNewQueue(): added batch of jobs to the queue.");
    }
    // We will also count how much time we mutually wait for the other threads.
    ret->m_timer.reset();
    // And finally release all the user threads
    for (auto &maqr: maq->m_requests) {
      {
        threading::MutexLocker (maqr->m_mutex);
        maqr->m_returnValue=ret;
        maqr->m_promise.set_value();
      }
    }
    // Done!
    return ret;
  } catch (...) {
    try {
      std::rethrow_exception(std::current_exception());
    } catch (cta::exception::Exception &ex) {
      log::ScopedParamContainer params(logContext);
      params.add("message", ex.getMessageValue());
      logContext.log(log::ERR, "In MemQueue::sharedAddToNewQueue(): got an exception writing. Will propagate to other threads.");
    } catch (std::exception & ex) {
      log::ScopedParamContainer params(logContext);
      params.add("exceptionWhat", ex.what());
      logContext.log(log::ERR, "In MemQueue::sharedAddToNewQueue(): got a standard exception writing. Will propagate to other threads.");
    } catch (...) {
      logContext.log(log::ERR, "In MemQueue::sharedAddToNewQueue(): got an unknown exception writing. Will propagate to other threads.");
    }
    size_t exceptionsNotPassed = 0;
    // Something went wrong. We should inform the other threads
    for (auto & maqr: maq->m_requests) {
      try {
        threading::MutexLocker (maqr->m_mutex);
        maqr->m_promise.set_exception(std::current_exception());
      } catch (...) {
        exceptionsNotPassed++;
      }
    }
    // And we inform the caller in our thread too
    if (exceptionsNotPassed) {
      try {
        std::rethrow_exception(std::current_exception());
      } catch (std::exception & ex) {
        std::stringstream err;
        err << "In MemQueue::sharedAddToNewQueue(), in main thread, failed to notify "
            << exceptionsNotPassed << " other threads out of  " << maq->m_requests.size()
            << " : " << ex.what();
        log::ScopedParamContainer params(logContext);
        params.add("what", ex.what())
              .add("exceptionsNotPassed", exceptionsNotPassed);
        logContext.log(log::ERR, "In MemQueue::sharedAddToNewQueue(): Failed to propagate exceptions to other threads.");

        throw cta::exception::Exception(err.str());
      }
    } else
      throw;
  }
}

template <class Request, class Queue>
void MemQueue<Request, Queue>::add(std::shared_ptr<MemQueueRequest<Request, Queue>>& request) {
  m_requests.emplace_back(request);
}

typedef MemQueue<objectstore::ArchiveRequest, objectstore::ArchiveQueue> MemArchiveQueue;
typedef MemQueue<objectstore::RetrieveRequest, objectstore::RetrieveQueue> MemRetrieveQueue;

}} // namespace cta::ostoreDBUtils
