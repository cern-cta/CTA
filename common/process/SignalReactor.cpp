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
SignalReactor::SignalReactor(cta::log::LogContext& lc, uint32_t sleepInterval)
    : m_lc(lc),
      m_sleepInterval(sleepInterval) {}

//------------------------------------------------------------------------------
// SignalReactor::registerSignalFunction
//------------------------------------------------------------------------------
void SignalReactor::registerSignalFunction(uint32_t signal, std::function<void()> func) {
  if (m_signalFunctions.count(signal) > 0) {
    m_lc.log(log::ERR, "Function is already registered for signal " + std::to_string(signal));
    return;
  }
  m_signalFunctions[signal] = func;
}

//------------------------------------------------------------------------------
// SignalReactor::stop
//------------------------------------------------------------------------------
void SignalReactor::stop() {
  m_stopRequested = true;
}

//------------------------------------------------------------------------------
// SignalReactor::run
//------------------------------------------------------------------------------
void SignalReactor::run() {
  m_stopRequested = false;
  try {
    while (!m_stopRequested) {
      std::set<uint32_t> sigSet = m_signalReader->processAndGetSignals(m_lc);
      for (uint32_t signal : sigSet) {
        if (m_signalFunctions.count(signal) == 0) {
          m_lc.log(log::INFO, "In SignalReactor::run(): nothing to do for signal");
          continue;
        }
        m_signalFunctions[signal]();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(std::chrono::milliseconds(m_sleepInterval)));
    };
  } catch (exception::Exception& ex) {
    log::ScopedParamContainer exParams(m_lc);
    exParams.add("exceptionMessage", ex.getMessageValue());
    m_lc.log(log::ERR, "In SignalReactor::run(): received an exception. Backtrace follows.");
    m_lc.logBacktrace(log::INFO, ex.backtrace());
    throw ex;
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
