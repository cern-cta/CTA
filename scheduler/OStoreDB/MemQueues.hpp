
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

#include "objectstore/ArchiveRequest.hpp"
#include "objectstore/ArchiveQueue.hpp"

#include <future>

namespace cta { 
// Forward declaration
class OStoreDB;
  namespace ostoredb {

class MemArchiveQueueRequest {
  friend class MemArchiveQueue;
public:
  MemArchiveQueueRequest(objectstore::ArchiveRequest::JobDump & job,
    objectstore::ArchiveRequest & archiveRequest): m_job(job), m_archiveRequest(archiveRequest) {}
private:
  objectstore::ArchiveRequest::JobDump & m_job;
  objectstore::ArchiveRequest & m_archiveRequest;
  std::promise<void> m_promise;
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
   */
  static void sharedAddToArchiveQueue(objectstore::ArchiveRequest::JobDump & job,
    objectstore::ArchiveRequest & archiveRequest, OStoreDB & oStoreDB);
  
private:
  /** Mutex that should be locked before attempting any operation */
  std::mutex m_mutex;
  /** Add the object */
  void add(MemArchiveQueueRequest & request);
  /** Static function implementing the shared addition of archive requests to 
   * the object store queue */
  static const size_t g_maxQueuedElements = 100;
  std::list<MemArchiveQueueRequest *> m_requests;
  std::promise<void> m_promise;
  static std::mutex g_mutex;
  static std::map<std::string, MemArchiveQueue *> g_queues;
};

}} // namespace cta::ostoreDBUtils