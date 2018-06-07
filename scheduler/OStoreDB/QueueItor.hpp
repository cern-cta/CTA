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
  //! Constructor
  QueueItor(objectstore::Backend &objectStore, const std::string &queue_id = "");

  // Default copy constructor is deleted in favour of move constructor

  //! Move constructor
  QueueItor(QueueItor &&rhs) :
    m_objectStore(std::move(rhs).m_objectStore),
    m_onlyThisQueueId(std::move(rhs).m_onlyThisQueueId),
    m_jobQueuesQueue(std::move(rhs).m_jobQueuesQueue),
    m_jobQueuesQueueIt(std::move(rhs).m_jobQueuesQueueIt),
    m_jobQueue(std::move(std::move(rhs).m_jobQueue)),
    m_jobQueueIt(std::move(rhs).m_jobQueueIt)
  {
    // The move constructor works for all queue members, including begin(), but not for end() which
    // does not point to an actual object! In the case where the iterator points to end(), including
    // the case of an empty queue, we need to explicitly set it.

    if(m_jobQueuesQueueIt == rhs.m_jobQueuesQueue.end()) {
      m_jobQueuesQueueIt = m_jobQueuesQueue.end();
    }
    if(m_jobQueueIt == rhs.m_jobQueue.end()) {
      m_jobQueueIt = m_jobQueue.end();
    }
  }

  //! No assignment constructor
  QueueItor operator=(QueueItor &rhs) = delete;

  /*!
   * Increment iterator
   *
   * Increments to the end of the current job queue. Incrementing again moves to the next queue,
   * except when we are limited to one tapepool/vid.
   */
  void operator++() {
    if(m_jobQueueIt != m_jobQueue.end()) {
      ++m_jobQueueIt;
    } else if(!m_onlyThisQueueId) {
      ++m_jobQueuesQueueIt;
      if(m_jobQueuesQueueIt != m_jobQueuesQueue.end()) {
        getQueueJobs();
      }
    }
  }

  /*!
   * Ensures that the QueueItor points to a valid object, returning false if there
   * are no further objects.
   *
   * If we have reached the end of a job queue, attempts to advance to the next
   * queue and rechecks validity.
   *
   * @retval true  if internal iterators point to a valid object
   * @retval false if we have advanced past the last valid job
   */
  bool is_valid() {
    // Case 1: no more queues
    if(m_jobQueuesQueueIt == m_jobQueuesQueue.end()) return false;

    // Case 2: we are in a queue and haven't reached the end of it
    if(m_jobQueueIt != m_jobQueue.end()) return true;

    // Case 3: we have reached the end of the current queue and this is the only queue we care about
    if(m_onlyThisQueueId) return false;

    // Case 4: we have reached the end of the current queue, try to advance to the next queue
    for(++m_jobQueuesQueueIt; m_jobQueuesQueueIt != m_jobQueuesQueue.end(); ++m_jobQueuesQueueIt) {
      getQueueJobs();
      if(m_jobQueueIt != m_jobQueue.end()) break;
    }
    return m_jobQueueIt != m_jobQueue.end();
  }

  /*!
   * True if we are at the end of the current queue
   */
  bool endq() const { return m_jobQueueIt == m_jobQueue.end(); }

  /*!
   * True if we are at the end of the last queue
   */
  bool end() const {
    return m_jobQueuesQueueIt == m_jobQueuesQueue.end() || (m_onlyThisQueueId && endq());
  }

  //! Queue ID (returns tapepool for archives/vid for retrieves)
  const std::string &qid() const;

  //! Get the current job, bool is set to true if the data retrieved is valid
  std::pair<bool,typename JobQueue::job_t> getJob() const;
private:
  //!  Get the list of jobs in the queue
  void getQueueJobs();

  objectstore::Backend                                           &m_objectStore;         // Reference to ObjectStore Backend
  bool                                                            m_onlyThisQueueId;     // true if a queue_id parameter was passed to the constructor
  typename std::list<JobQueuesQueue>                              m_jobQueuesQueue;      // list of Archive or Retrieve Job Queues
  typename std::list<JobQueuesQueue>::const_iterator              m_jobQueuesQueueIt;    // iterator across m_jobQueuesQueue
  typename std::list<typename JobQueue::JobDump>                  m_jobQueue;            // list of Archive or Retrieve Jobs
  typename std::list<typename JobQueue::JobDump>::const_iterator  m_jobQueueIt;          // iterator across m_jobQueue
};

} // namespace cta
