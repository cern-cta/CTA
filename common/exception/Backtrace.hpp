/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>
#include <pthread.h>

namespace cta::exception {

class Backtrace {
public:
  explicit Backtrace(bool fake = false);

  const std::string& str() const { return m_trace; }

private:
  std::string m_trace;
  /**
   * Singleton lock around the apparently racy backtrace().
   * We write it with no error check as it's used only here.
   * We need a class in order to have a constructor for the global object.
   */
  class mutex {
  public:
    mutex() { pthread_mutex_init(&m_mutex, nullptr); }
    void lock() { pthread_mutex_lock(&m_mutex); }
    void unlock() { pthread_mutex_unlock(&m_mutex); }
  private:
    pthread_mutex_t m_mutex;
  };
  static mutex g_lock;
};

} // namespace cta::exception
