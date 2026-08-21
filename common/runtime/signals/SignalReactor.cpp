/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SignalReactor.hpp"

#include "SignalUtils.hpp"
#include "common/exception/Errnum.hpp"
#include "common/log/LogContext.hpp"
#include "common/semconv/Attributes.hpp"

#include <chrono>
#include <poll.h>
#include <signal.h>
#include <sys/prctl.h>
#include <thread>

namespace cta::runtime {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SignalReactor::SignalReactor(cta::log::Logger& log,
                             const std::unordered_map<int, std::function<void()>>& signalFunctions,
                             uint32_t waitTimeoutMsecs)
    : m_log(log),
      m_signalFunctions(signalFunctions),
      m_waitTimeoutMsecs(waitTimeoutMsecs) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SignalReactor::~SignalReactor() {
  // Gracefully shutdown the reactor
  stop();
}

//------------------------------------------------------------------------------
// SignalReactor::start
//------------------------------------------------------------------------------
void SignalReactor::start() {
  m_log(log::DEBUG, "In SignalReactor::start(): Blocking and registering signals");
  sigemptyset(&m_sigset);
  for (const auto& [signal, func] : m_signalFunctions) {
    sigaddset(&m_sigset, signal);
  }
  cta::exception::Errnum::throwOnNonZero(::pthread_sigmask(SIG_BLOCK, &m_sigset, &m_previousSigset),
                                         "In SignalReactor::start(): pthread_sigmask() failed");
  m_startThread = ::pthread_self();
  m_maskNeedsRestore = true;
  try {
    m_thread =
      std::jthread([this](std::stop_token st) { run(st, m_signalFunctions, m_sigset, m_log, m_waitTimeoutMsecs); });
  } catch (...) {
    ::pthread_sigmask(SIG_SETMASK, &m_previousSigset, nullptr);
    m_maskNeedsRestore = false;
    throw;
  }
}

//------------------------------------------------------------------------------
// SignalReactor::stop
//------------------------------------------------------------------------------
void SignalReactor::stop() noexcept {
  m_log(log::DEBUG, "In SignalReactor::stop(): stopping SignalReactor");
  m_thread.request_stop();
  if (m_thread.joinable()) {
    try {
      m_thread.join();
    } catch (std::system_error& e) {
      m_log(log::ERR,
            "In SignalReactor::stop(): failed to join thread",
            {
              {semconv::log::exceptionMessage, e.what()}
      });
    }
  }
  if (m_maskNeedsRestore) {
    if (!::pthread_equal(m_startThread, ::pthread_self())) {
      m_log(
        log::ERR,
        "In SignalReactor::stop(): cannot restore the signal mask from a thread other than the one that called start()",
        {});
      return;
    }
    const int rc = ::pthread_sigmask(SIG_SETMASK, &m_previousSigset, nullptr);
    if (rc != 0) {
      m_log(log::ERR,
            "In SignalReactor::stop(): failed to restore the signal mask",
            {
              {"errno",                    std::to_string(rc)},
              {semconv::log::errorMessage, ::strerror(rc)    }
      });
      return;
    }
    m_maskNeedsRestore = false;
  }
}

//------------------------------------------------------------------------------
// SignalReactor::run
//------------------------------------------------------------------------------
void SignalReactor::run(std::stop_token st,
                        const std::unordered_map<int, std::function<void()>>& signalFunctions,
                        const sigset_t& sigset,
                        cta::log::Logger& log,
                        const uint32_t waitTimeoutMsecs) {
  cta::log::LogContext lc(log);
  lc.log(log::INFO, "In SignalReactor::run(): Starting SignalReactor");
  timespec ts;
  ts.tv_sec = waitTimeoutMsecs / 1000;
  ts.tv_nsec = (waitTimeoutMsecs % 1000) * 1e6;

  try {
    while (!st.stop_requested()) {
      siginfo_t si {};
      int signal = sigtimedwait(&sigset, &si, &ts);
      // Handle errors
      if (signal == -1) {
        int e = errno;
        // Just a timeout
        if (e == EAGAIN || e == EINTR) {
          continue;
        }
        // Something else
        log::ScopedParamContainer params(lc);
        params.add("errno", std::to_string(e));
        params.add(semconv::log::errorMessage, ::strerror(e));
        lc.log(log::WARNING, "In SignalReactor::run(): sigtimedwait failed");
        continue;
      }
      lc.log(log::INFO, "In SignalReactor::run(): received " + utils::signalToString(signal));
      // Check whether we have something to do for this signal
      if (!signalFunctions.contains(signal)) {
        log::ScopedParamContainer params(lc);
        params.add("signal", utils::signalToString(signal));
        lc.log(log::INFO, "In SignalReactor::run(): no action for signal");
        continue;
      }

      try {
        signalFunctions.at(signal)();
      } catch (const std::exception& ex) {
        log::ScopedParamContainer exParams(lc);
        exParams.add("signal", utils::signalToString(signal));
        exParams.add(semconv::log::exceptionMessage, ex.what());
        lc.log(log::ERR, "In SignalReactor::run(): signal callback threw an exception");
      } catch (...) {
        log::ScopedParamContainer exParams(lc);
        exParams.add("signal", utils::signalToString(signal));
        lc.log(log::ERR, "In SignalReactor::run(): signal callback threw an unknown exception");
      }
    }
  } catch (std::exception& ex) {
    log::ScopedParamContainer exParams(lc);
    exParams.add(semconv::log::exceptionMessage, ex.what());
    lc.log(log::ERR, "In SignalReactor::run(): received a std::exception.");
  } catch (...) {
    lc.log(log::ERR, "In SignalReactor::run(): received an unknown exception.");
  }
  lc.log(log::INFO, "In SignalReactor::run(): SignalReactor stopped listening");
}

}  // namespace cta::runtime
