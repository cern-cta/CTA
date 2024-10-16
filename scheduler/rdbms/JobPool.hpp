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
    explicit JobPool(size_t initialPoolSize = 4000);

    // Acquire a job from the pool (or create a new one if the pool is empty)
    std::unique_ptr<T> acquireJob();

    // Return a job to the pool for reuse
    void releaseJob(std::unique_ptr<T> job);

  private:
    std::stack<std::unique_ptr<T>> m_pool;  // Stack to store reusable jobs
    size_t m_poolSize;  // Initial pool size
  };

}  // namespace cta::schedulerdb
