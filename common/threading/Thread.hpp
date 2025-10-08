/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "common/threading/Mutex.hpp"

#include <optional>
#include <pthread.h>
#include <semaphore.h>

namespace cta::threading {

/**
* An exception class thrown by the Thread class.
*/
CTA_GENERATE_EXCEPTION_CLASS(UncaughtExceptionInThread);

/**
* A Thread class, based on the Qt interface. To be used, on should
* inherit from it, and implement the run() method.
* The thread is started with start() and joined with wait().
*/
class Thread {
public:
  Thread(): m_hadException(false), m_what(""), m_started(false) {}
  explicit Thread(std::optional<size_t> stackSize): m_hadException(false), m_what(""), m_started(false), m_stackSize(stackSize) {}

  virtual ~Thread() = default;
  void start();
  void wait();
  void kill();
protected:
  virtual void run () = 0;
private:
  pthread_t m_thread;
  bool m_hadException;
  std::string m_what;
  std::string m_type;
  static void * pthread_runner (void * arg);
  bool m_started;
  std::optional<size_t> m_stackSize;
};

} // namespace cta::threading
