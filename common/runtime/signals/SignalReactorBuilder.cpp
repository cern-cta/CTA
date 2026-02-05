/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SignalReactorBuilder.hpp"

#include "SignalUtils.hpp"
#include "common/exception/Errnum.hpp"
#include "common/semconv/Attributes.hpp"

#include <signal.h>

namespace cta::runtime {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SignalReactorBuilder::SignalReactorBuilder() {
  sigemptyset(&m_sigset);
}

//------------------------------------------------------------------------------
// SignalReactorBuilder::addSignalFunction
//------------------------------------------------------------------------------
SignalReactorBuilder&
SignalReactorBuilder::addSignalFunction(int signal, const std::function<void()>& func, bool overwrite) {
  if (m_signalFunctions.contains(signal) && !overwrite) {
    throw exception::Exception("Function already registered for " + utils::signalToString(signal)
                               + ", while overwrite was disabled");
    return *this;
  }
  sigaddset(&m_sigset, signal);
  m_signalFunctions[signal] = func;
  return *this;
}

//------------------------------------------------------------------------------
// SignalReactorBuilder::withTimeoutMsecs
//------------------------------------------------------------------------------
SignalReactorBuilder& SignalReactorBuilder::withTimeoutMsecs(uint32_t msecs) {
  m_waitTimeoutMsecs = msecs;
  return *this;
}

//------------------------------------------------------------------------------
// SignalReactorBuilder::build
//------------------------------------------------------------------------------
SignalReactor SignalReactorBuilder::build(cta::log::Logger& log) {
  return SignalReactor(log, m_sigset, std::move(m_signalFunctions), m_waitTimeoutMsecs);
}

}  // namespace cta::runtime
