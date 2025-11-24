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

#include <signal.h>
#include <sys/prctl.h>
#include <chrono>
#include <thread>

#include "SignalReactor.hpp"

#include "common/exception/Errnum.hpp"
#include "common/semconv/Attributes.hpp"

namespace cta {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SignalReactor::SignalReactor(cta::log::LogContext& lc,
                             sigset_t sigset,
                             std::unordered_map<int, std::function<void()>> signalFunctions)
    : m_lc(lc),
      m_sigset(sigset),
      m_signalFunctions(signalFunctions) {}

SignalReactor::~SignalReactor() {
  // Gracefully shutdown the reactor
  stop();
}

//------------------------------------------------------------------------------
// SignalReactor::start
//------------------------------------------------------------------------------
void SignalReactor::start() {
  // Block signals everywhere to ensure we can properly consume them.
  pthread_sigmask(SIG_BLOCK, &m_sigset, NULL);
  m_thread = std::thread(&SignalReactor::run, this);
}

//------------------------------------------------------------------------------
// SignalReactor::stop
//------------------------------------------------------------------------------
void SignalReactor::stop() {
  m_stopRequested = true;
  m_thread.join();
}

//------------------------------------------------------------------------------
// SignalReactor::run
//------------------------------------------------------------------------------
void SignalReactor::run() {
  m_stopRequested = false;
  timespec ts;
  ts.tv_sec = m_waitTimeoutSec;
  ts.tv_nsec = 0;

  try {
    while (!m_stopRequested) {
      siginfo_t si {};
      int signal = sigtimedwait(&m_sigset, &si, &ts);
      // Handle errors
      if (signal == -1) {
        int e = errno;
        // Just a timeout
        if (e == EAGAIN) {
          continue;
        }
        // Something else
        log::ScopedParamContainer params(m_lc);
        params.add("errno", std::to_string(e));
        params.add("errorMessage", ::strerror(e));
        m_lc.log(log::ERR, "In SignalReactor::run(): sigtimedwait failed");
        continue;
      }

      // Check whether we have something to do for this signal
      if (m_signalFunctions.count(signal) == 0) {
        log::ScopedParamContainer params(m_lc);
        params.add("signal", std::to_string(signal));
        m_lc.log(log::INFO, "In SignalReactor::run(): no action for signal");
        continue;
      }
      m_signalFunctions[signal]();
    }
  } catch (std::exception& ex) {
    log::ScopedParamContainer exParams(m_lc);
    exParams.add("exceptionMessage", ex.what());
    m_lc.log(log::ERR, "In SignalReactor::run(): received a std::exception.");
    throw ex;
  } catch (...) {
    m_lc.log(log::ERR, "In SignalReactor::run(): received an unknown exception.");
    throw;
  }
}

}  // namespace cta
