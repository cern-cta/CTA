/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "rdbms/ConnPool.hpp"

#include <memory>
#include <stack>

namespace cta::schedulerdb {

// Forward declarations
class ArchiveRdbJob;
class RetrieveRdbJob;

template<typename T>
class JobPool : public std::enable_shared_from_this<JobPool<T>> {
public:
  // Constructor initializes the pool with a connection pool reference or other parameters
  explicit JobPool(rdbms::ConnPool& connPool, size_t poolSize = 100000);

  // Acquire a job from the pool (or create a new one if the pool is empty)
  std::unique_ptr<T> acquireJob();

  // Return a job to the pool for reuse
  bool releaseJob(T* job);

private:
  std::stack<std::unique_ptr<T>> m_pool;  // Stack to store reusable jobs
  size_t m_poolSize;                      // Initial pool size
  rdbms::ConnPool& m_connPool;
  std::mutex m_poolMutex;  // Mutex to protect access to m_pool
};

// Constructor to initialize the job pool
template<typename T>
JobPool<T>::JobPool(rdbms::ConnPool& connPool, size_t poolSize) : m_poolSize(poolSize),
                                                                  m_connPool(connPool) {
  // Optionally, pre-fill the pool with some job objects
  for (size_t i = 0; i < m_poolSize; ++i) {
    auto job = std::make_unique<T>(m_connPool);
    m_pool.push(std::move(job));
  }
}

// Acquire a job from the pool
template<typename T>
std::unique_ptr<T> JobPool<T>::acquireJob() {
  std::scoped_lock lock(m_poolMutex);
  if (!m_pool.empty()) {
    // Get a job from the pool if available
    auto job = std::move(m_pool.top());
    m_pool.pop();
    job->setPool(this->shared_from_this());
    return job;
  }

  // If the pool is empty, create a new job
  return std::make_unique<T>(m_connPool);
}

// Release a job back into the pool
template<typename T>
bool JobPool<T>::releaseJob(T* job) {
  std::scoped_lock lock(m_poolMutex);
  if (m_pool.size() < m_poolSize) {
    // Reset the job's state as needed before reusing
    job->reset();
    job->setPool(this->shared_from_this());
    m_pool.push(std::unique_ptr<T>(job));
    return true;
  } else {
    return false;
  }
}
}  // namespace cta::schedulerdb
