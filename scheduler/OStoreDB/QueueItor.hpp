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

#pragma once

#include "common/dataStructures/JobQueueType.hpp"
#include <objectstore/Backend.hpp>
#include <objectstore/ObjectOps.hpp>

namespace cta {

/*!
 * Iterator class for Archive/Retrieve job queues
 *
 * Allows asynchronous access to job queues for streaming responses
 */
template<typename JobQueuesQueue, typename JobQueue>
class QueueItor {
public:
  typedef typename std::list<typename JobQueue::JobDump> jobQueue_t;

  /*!
   * Default constructor
   */
  QueueItor(objectstore::Backend &objectStore, common::dataStructures::JobQueueType queueType, const std::string &queue_id = "");

  /*!
   * No assignment constructor
   */
  QueueItor operator=(QueueItor &rhs) = delete;

  // Default copy constructor is deleted in favour of move constructor

  /*!
   * Move constructor
   *
   * Same as the default move constructor, with one small difference: the moved list iterator will point
   * to the correct queue entry except in one case. If the iterator is set to end() (including the case
   * where the queue is empty), it will be invalidated by the move. In this case we need to set it
   * explicitly.
   */
  QueueItor(QueueItor &&rhs) :
    m_objectStore(std::move(rhs).m_objectStore),
    m_onlyThisQueueId(std::move(rhs).m_onlyThisQueueId),
    m_isEndQueue(std::move(rhs).m_isEndQueue),
    m_jobQueuesQueue(std::move(rhs).m_jobQueuesQueue),
    m_jobQueuesQueueIt(std::move(rhs).m_jobQueuesQueueIt),
    m_jobQueue(std::move(std::move(rhs).m_jobQueue)),
    m_jobCache(std::move(rhs).m_jobCache)
  {
    if(m_jobQueuesQueueIt == rhs.m_jobQueuesQueue.end()) {
      m_jobQueuesQueueIt = m_jobQueuesQueue.end();
    }
  }

  /*!
   * Increment iterator
   *
   * Increments to the end of the current job queue. Incrementing again moves to the next queue,
   * except when we are limited to one tapepool/vid.
   */
  void operator++() {
    m_jobCache.pop_front();

    if(m_jobCache.empty()) {
      updateJobCache();
      if(m_jobCache.empty()) {
        // We have reached the end of the current queue,
        m_isEndQueue = true;
        // advance to next queue which contains jobs
        for(nextJobQueue(); m_jobQueuesQueueIt != m_jobQueuesQueue.end(); nextJobQueue()) {
          getJobQueue();
          if(!m_jobCache.empty()) break;
        }
      }
    }
  }

  /*!
   * Reset end queue flag
   *
   * If we need to detect when we have reached the end of the current queue (for example when calculating
   * summary statistics), call this method at the beginning of each queue to reset the end queue flag.
   */
  void beginq() {
    m_isEndQueue = m_jobCache.empty();
  }

  /*!
   * True if the last call to operator++() took us past the end of a queue (or the queue was empty to begin with)
   */
  bool endq() const {
    return m_isEndQueue;
  }

  /*!
   * True if there are no more queues to process
   */
  bool end() const {
    return m_jobQueuesQueueIt == m_jobQueuesQueue.end();
  }

  /*!
   * Queue ID
   *
   * Returns tapepool for archive queues and vid for retrieve queues.
   */
  const std::string &qid() const;

  /*!
   * Dereference the QueueItor
   */
  const typename JobQueue::job_t &operator*() const {
    return m_jobCache.front();
  }

private:
  /*!
   * Advance to the next job queue
   */
  void nextJobQueue()
  {
    if(m_onlyThisQueueId) {
      // If we are filtering by tapepool or vid, skip the other queues
      m_jobQueuesQueueIt = m_jobQueuesQueue.end();
    } else {
      ++m_jobQueuesQueueIt;
    }
  }

  /*!
   * Get the list of jobs in the job queue
   */
  void getJobQueue()
  {
    try {
      JobQueue osq(m_jobQueuesQueueIt->address, m_objectStore);
      objectstore::ScopedSharedLock ostpl(osq);
      osq.fetch();
      m_jobQueue = osq.dumpJobs();
    } catch(...) {
      // Behaviour is racy: it's possible that the queue can disappear before we read it.
      // In this case, we ignore the error and move on.
      m_jobQueue.resize(0);
      m_isEndQueue = true;
    }

    // Grab the first batch of jobs from the current queue
    updateJobCache();
  }

  /*!
   * Update the cache of queue jobs
   */
  void updateJobCache()
  {
    while(!m_jobQueue.empty() && m_jobCache.empty()) {
      auto chunksize = m_jobQueue.size() < JOB_CACHE_SIZE ? m_jobQueue.size() : JOB_CACHE_SIZE;
      auto jobQueueChunkEnd = m_jobQueue.begin();
      std::advance(jobQueueChunkEnd, chunksize);

      jobQueue_t jobQueueChunk;
      jobQueueChunk.splice(jobQueueChunk.end(), m_jobQueue, m_jobQueue.begin(), jobQueueChunkEnd);
      getQueueJobs(jobQueueChunk);
    }
  }

  /*!
   * Populate the cache with a chunk of queue jobs from the objectstore
   */
  void getQueueJobs(const jobQueue_t &jobQueueChunk);

  //! Maximum number of jobs to asynchronously fetch from the objectstore at once
  const size_t JOB_CACHE_SIZE = 300;

  objectstore::Backend                               &m_objectStore;         //!< Reference to ObjectStore Backend
  bool                                                m_onlyThisQueueId;     //!< true if a queue_id parameter was passed to the constructor
  bool                                                m_isEndQueue;          //!< true if the last increment++ took us past the end of a queue

  typename std::list<JobQueuesQueue>                  m_jobQueuesQueue;      //!< list of Archive or Retrieve Job Queues
  typename std::list<JobQueuesQueue>::const_iterator  m_jobQueuesQueueIt;    //!< iterator across m_jobQueuesQueue
  jobQueue_t                                          m_jobQueue;            //!< list of Archive or Retrieve Jobs
  typename std::list<typename JobQueue::job_t>        m_jobCache;            //!< local cache of queue jobs
};

} // namespace cta
