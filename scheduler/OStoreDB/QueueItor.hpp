/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Iterator class for Archive/Retrieve job queues
 * @copyright      Copyright 2018 CERN
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

#include <iostream>

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
  QueueItor(objectstore::Backend &objectStore, const std::string &queue_id = "");

  /*!
   * No assignment constructor
   */
  QueueItor operator=(QueueItor &rhs) = delete;

  // Default copy constructor is deleted in favour of move constructor

  /*!
   * Move constructor
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
std::cerr << "QueueItor move constructor" << std::endl;
    // The move constructor works for all queue members, including begin(), but not for end() which
    // does not point to an actual object! In the case where the iterator points to end(), including
    // the case of an empty queue, we need to explicitly set it.

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
std::cerr << "QueueItor inc++" << std::endl;
    m_isEndQueue = false;

    m_jobCache.pop_front();

    if(m_jobCache.empty()) {
      updateJobCache();
      if(m_jobCache.empty()) {
        m_isEndQueue = true;
        nextJobQueue();
      }
    }
  }

  /*!
   * True if we are at the end of the last queue
   */
  bool end() const {
std::cerr << "QueueItor end()" << std::endl;
    return m_jobQueuesQueueIt == m_jobQueuesQueue.end();
  }

  /*!
   * True if the last call to operator++() took us past the end of a queue
   */
  bool endq() const {
std::cerr << "QueueItor endq()" << std::endl;
    return m_isEndQueue;
  }

  /*!
   * Queue ID (returns tapepool for archives/vid for retrieves)
   */
  const std::string &qid() const;

  /*!
   * Dereference the QueueItor
   */
  const typename JobQueue::job_t &operator*() const {
std::cerr << "QueueItor operator*" << std::endl;
     return m_jobCache.front();
  }

private:
  /*!
   * Get the list of jobs in the job queue
   */
  void getJobQueue();

  /*!
   * Get a chunk of queue jobs from the objectstore
   */
  void getQueueJobs(const jobQueue_t &jobQueueChunk);

  /*!
   * Advance to the next job queue
   */
  void nextJobQueue()
  {
std::cerr << "QueueItor nextJobQueue()" << std::endl;
    if(m_onlyThisQueueId) {
      // If we are filtering on a specific queue, ignore the other queues
      m_jobQueuesQueueIt = m_jobQueuesQueue.end();
    } else {
      m_jobQueuesQueueIt++;
    }
  }

  /*!
   * Update the cache of queue jobs
   */
  void updateJobCache()
  {
std::cerr << "RetrieveQueue updateJobCache(): jobQueue has " << m_jobQueue.size() << " entries." << std::endl;
    while(!m_jobQueue.empty() && m_jobCache.empty())
    {
      // Get a chunk of retrieve jobs from the current retrieve queue

      auto chunksize = m_jobQueue.size() < JOB_CACHE_SIZE ? m_jobQueue.size() : JOB_CACHE_SIZE;
      auto jobQueueChunkEnd = m_jobQueue.begin();
      std::advance(jobQueueChunkEnd, chunksize);

      jobQueue_t jobQueueChunk;
      jobQueueChunk.splice(jobQueueChunk.end(), m_jobQueue, m_jobQueue.begin(), jobQueueChunkEnd);
std::cerr << "RetrieveQueue updateJobCache(): jobQueueChunk has " << jobQueueChunk.size() << " entries, jobQueue has " << m_jobQueue.size() << " entries." << std::endl;
      getQueueJobs(jobQueueChunk);
    }
std::cerr << "RetrieveQueue updateJobCache(): jobCache updated with " << m_jobCache.size() << " entries." << std::endl;
  }

  //! Maximum number of jobs to asynchronously fetch from the queue at once
  const size_t JOB_CACHE_SIZE = 111;

  objectstore::Backend                               &m_objectStore;         //!< Reference to ObjectStore Backend
  bool                                                m_onlyThisQueueId;     //!< true if a queue_id parameter was passed to the constructor
  bool                                                m_isEndQueue;          //!< true if the last increment++ took us past the end of a queue

  typename std::list<JobQueuesQueue>                  m_jobQueuesQueue;      //!< list of Archive or Retrieve Job Queues
  typename std::list<JobQueuesQueue>::const_iterator  m_jobQueuesQueueIt;    //!< iterator across m_jobQueuesQueue
  jobQueue_t                                          m_jobQueue;            //!< list of Archive or Retrieve Jobs
  typename std::list<typename JobQueue::job_t>        m_jobCache;            //!< local cache of queue jobs
};

} // namespace cta
