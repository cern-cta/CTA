
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
#include "common/helgrind_annotator.hpp"
#include "objectstore/ArchiveRequest.hpp"
#include "objectstore/ArchiveQueue.hpp"
#include "common/log/LogContext.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"
#include <unistd.h>
#include <syscall.h>

#include <future>

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
class SharedQueueLock {
  friend class MemArchiveQueue;
public:
  SharedQueueLock(log::LogContext & logContext): m_logContext(logContext) {}
  ~SharedQueueLock();
private:
  std::unique_ptr<objectstore::ScopedExclusiveLock> m_lock;
  std::unique_ptr<objectstore::ArchiveQueue> m_queue;
  log::LogContext m_logContext;
  utils::Timer m_timer;
};

class MemArchiveQueueRequest {
  friend class MemArchiveQueue;
public:
  MemArchiveQueueRequest(objectstore::ArchiveRequest::JobDump & job,
    objectstore::ArchiveRequest & archiveRequest): m_job(job), m_archiveRequest(archiveRequest), m_tid(::syscall(SYS_gettid)) {}
  virtual ~MemArchiveQueueRequest() {
    threading::MutexLocker ml(m_mutex);
  }
private:
  objectstore::ArchiveRequest::JobDump & m_job;
  objectstore::ArchiveRequest & m_archiveRequest;
  std::promise<void> m_promise;
  std::shared_ptr<SharedQueueLock> m_returnValue;
  // Mutex protecting users against premature deletion
  threading::Mutex m_mutex;
  // Helper for debugging
  pid_t m_tid;
};

class MemArchiveQueue {
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
   * @param archiveRequest the request itself.
   * @param oStoreDB reference to the object store, allowing creation of the queue
   * if needed
   * @param logContext log context to log addition of jobs to the queue.
   */
  static std::shared_ptr<SharedQueueLock> sharedAddToArchiveQueue(objectstore::ArchiveRequest::JobDump & job,
    objectstore::ArchiveRequest & archiveRequest, OStoreDB & oStoreDB, log::LogContext & logContext);
  
private:
  /** Mutex that should be locked before attempting any operation */
  threading::Mutex m_mutex;
  /** Add the object */
  void add(std::shared_ptr<MemArchiveQueueRequest>& request);
  std::list<std::shared_ptr<MemArchiveQueueRequest>> m_requests;
  static threading::Mutex g_mutex;
  /**
   * A set of per tape pool queues. Their presence indicates that a queue is currently being built up and new objects to be queued
   * should piggy back on it.
   */
  static std::map<std::string, std::shared_ptr<MemArchiveQueue>> g_queues;
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
  static std::shared_ptr<SharedQueueLock> sharedAddToArchiveQueueWithNewQueue(objectstore::ArchiveRequest::JobDump & job,
    objectstore::ArchiveRequest & archiveRequest, OStoreDB & oStoreDB, log::LogContext & logContext, threading::MutexLocker &globalLock);
};

}} // namespace cta::ostoreDBUtils