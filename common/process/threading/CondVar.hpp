/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-FileCopyrightText: 2026 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <condition_variable>

namespace cta::threading {

/**
 * Forward declaration of the class representing a mutex locker.
 */
class MutexLocker;

/**
 * Class representing a condition variable.
 * Now uses std::condition_variable instead of pthread_cond_t.
 */
class CondVar {
public:
  /**
   * Constructor.
   */
  CondVar() = default;

  /**
   * Destructor.
   */
  ~CondVar() = default;

  /**
   * Delete the copy constructor.
   */
  CondVar(const CondVar&) = delete;

  /**
   * Delete the move constructor.
   */
  CondVar(const CondVar&&) = delete;

  /**
   * Delete the copy assignment operator.
   */
  CondVar& operator=(const CondVar&) = delete;

  /**
   * Delete the move assignment operator.
   */
  CondVar& operator=(const CondVar&&) = delete;

  /**
   * Waits on the specified MutexLocker and its corresponding Mutex.
   */
  void wait(MutexLocker&);

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
   * The underlying condition variable.
   */
  std::condition_variable m_cond;
};  // class CondVar

}  // namespace cta::threading
