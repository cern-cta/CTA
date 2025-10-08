/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <memory>
#include <pthread.h>

namespace cta::threading {

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

} // namespace cta::threading
