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

#include <memory>
#include <stack>

namespace cta::schedulerdb {

  // Forward declaration of ArchiveRdbJob
  class ArchiveRdbJob;

  template<typename T>
  class JobPool {
  public:
    // Constructor initializes the pool with a connection pool reference or other parameters
    explicit JobPool(rdbms::ConnPool& connPool, size_t initialPoolSize = 4000);

    // Acquire a job from the pool (or create a new one if the pool is empty)
    std::unique_ptr<T> acquireJob();

    // Return a job to the pool for reuse
    void releaseJob(std::unique_ptr<T> job);

  private:
    std::stack<std::unique_ptr<T>> m_pool;  // Stack to store reusable jobs
    size_t m_poolSize;  // Initial pool size
    rdbms::ConnPool& m_connPool;
  };

  // Constructor to initialize the job pool
  template<typename T>
  JobPool<T>::JobPool(rdbms::ConnPool& connPool, size_t initialPoolSize)
    : m_poolSize(initialPoolSize), m_connPool(connPool) {
    // Optionally, pre-fill the pool with some job objects
    for (size_t i = 0; i < m_poolSize; ++i) {
      m_pool.push(std::make_unique<T>(m_connPool));
    }
  }

// Acquire a job from the pool
  template<typename T>
  std::unique_ptr<T> JobPool<T>::acquireJob() {
    if (!m_pool.empty()) {
      // Get a job from the pool if available
      auto job = std::move(m_pool.top());
      m_pool.pop();
      return job;
    }

    // If the pool is empty, create a new job
    return std::make_unique<T>(m_connPool);
  }

// Release a job back into the pool
  template<typename T>
  void JobPool<T>::releaseJob(std::unique_ptr<T> job) {
    // Reset the job's state as needed before reusing
    job->reset();
    m_pool.push(std::move(job));
  }
}  // namespace cta::schedulerdb
