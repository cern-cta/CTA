/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-FileCopyrightText: 2026 DESY
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Thread.hpp"

#include <cxxabi.h>
#include <iostream>
#include <typeinfo>

namespace cta::threading {

/* Implementations of the threading primitives */
//------------------------------------------------------------------------------
// start
//------------------------------------------------------------------------------
void Thread::start() {
  m_thread = std::jthread([this](std::stop_token stop_token) {
    m_stopToken = stop_token;
    try {
      run();
    } catch (std::exception& e) {
      m_hadException = true;
      int status = -1;
      char* demangled = abi::__cxa_demangle(typeid(e).name(), nullptr, nullptr, &status);
      if (!status) {
        m_type += demangled;
      } else {
        m_type = typeid(e).name();
      }
      free(demangled);
      m_what = e.what();
    } catch (...) {
      m_hadException = true;
      m_type = "unknown";
      m_what = "uncaught non-standard exception";
      throw;
    }
  });
}

//------------------------------------------------------------------------------
// wait
//------------------------------------------------------------------------------
void Thread::wait() const {
  if (m_thread.joinable()) {
    m_thread.join();
  }

  if (m_hadException) {
    std::string w = "Uncaught exception of type \"";
    w += m_type;
    w += "\" in Thread.run(): >>>>";
    w += m_what;
    w += "<<<< End of uncaught exception";
    throw UncaughtExceptionInThread(w);
  }
}

//------------------------------------------------------------------------------
// stop_requested
//------------------------------------------------------------------------------
bool Thread::stop_requested() const {
  return m_stopToken.stop_requested();
}

//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void Thread::stop() const {
  if (m_thread.joinable()) {
    m_thread.request_stop();
  }
}

}  // namespace cta::threading
