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
SignalReactorBuilder::SignalReactorBuilder(cta::log::LogContext& lc) : m_lc(lc) {
  sigemptyset(&m_sigset);
}

//------------------------------------------------------------------------------
// SignalReactorBuilder::addSignalFunction
//------------------------------------------------------------------------------
SignalReactorBuilder& SignalReactorBuilder::addSignalFunction(int signal, const std::function<void()>& func) {
  if (m_signalFunctions.contains(signal)) {
    m_lc.log(log::WARNING,
             "In SignalReactorBuilder::addSignalFunction(): Function already registered for signal "
               + std::to_string(signal));
    return *this;
  }
  m_lc.log(log::INFO,
           "In SignalReactorBuilder::addSignalFunction(): Adding function for signal " + utils::signalToString(signal));
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
SignalReactor SignalReactorBuilder::build() {
  return SignalReactor(m_lc, m_sigset, std::move(m_signalFunctions), m_waitTimeoutMsecs);
}

}  // namespace cta::runtime
