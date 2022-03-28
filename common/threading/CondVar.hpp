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

#include <memory>
#include <pthread.h>

namespace cta {
namespace threading {

/**
 * Forward declaration of the class representing a mutex locker.
 */
class MutexLocker;

/**
 * Class representing a POSIX thread conditional variable.
 */
class CondVar {
public:

  /**
   * Constructor.
   */
  CondVar();

  /**
   * Destructor.
   */
  ~CondVar();

  /**
   * Delete the copy constructor.
   */
  CondVar(const CondVar &) = delete;

  /**
   * Delete the move constructor.
   */
  CondVar(const CondVar &&) = delete;

  /**
   * Delete the copy assignment operator.
   */
  CondVar& operator=(const CondVar &) = delete;

  /**
   * Delete the move assignment operator.
   */
  CondVar& operator=(const CondVar &&) = delete;

  /**
   * Waits on the specified MutexLocker and its corresponding Mutex.
   */
  void wait(MutexLocker &);

  /**
   * Unblocks at least one waiting thread.
   */
  void signal();

  /**
   * Unblocks all waiting threads.
   */
  void broadcast();

private:

  /**
   * The underlying POSIX thread condition variable.
   */
  pthread_cond_t m_cond;
}; // class CondVar

} // namespace threading
} // namespace cta
