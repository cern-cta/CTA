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

#include "JobPool.hpp"
#include "ArchiveRdbJob.hpp"

namespace cta::schedulerdb {

// Constructor to initialize the job pool
  template<typename T>
  JobPool<T>::JobPool(size_t initialPoolSize) : m_poolSize(initialPoolSize) {
    // Optionally, pre-fill the pool with some job objects
    for (size_t i = 0; i < m_poolSize; ++i) {
      m_pool.push(std::make_unique<T>());
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
    return std::make_unique<T>();
  }

// Release a job back into the pool
  template<typename T>
  void JobPool<T>::releaseJob(std::unique_ptr<T> job) {
    // Reset the job's state as needed before reusing
    job->reset();
    m_pool.push(std::move(job));
  }

}  // namespace cta::schedulerdb
