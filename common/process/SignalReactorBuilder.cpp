/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
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

#include "SignalReactorBuilder.hpp"
#include <signal.h>

#include "common/exception/Errnum.hpp"
#include "common/semconv/Attributes.hpp"

namespace cta {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
SignalReactorBuilder::SignalReactorBuilder(cta::log::LogContext& lc) : m_lc(lc) {}

//------------------------------------------------------------------------------
// SignalReactorBuilder::addSignalFunction
//------------------------------------------------------------------------------
SignalReactorBuilder& SignalReactorBuilder::addSignalFunction(int signal, const std::function<void()>& func) {
  if (!m_signalFunctions.contains(signal)) {
    m_lc.log(log::WARNING,
             "In SignalReactorBuilder::addSignalFunction(): Function already registered for signal " +
               std::to_string(signal));
    return *this;
  }
  sigaddset(&m_sigset, signal);
  m_signalFunctions[signal] = func;
  return *this;
}

//------------------------------------------------------------------------------
// SignalReactorBuilder::build
//------------------------------------------------------------------------------
SignalReactor SignalReactorBuilder::build() {
  return SignalReactor(m_lc, m_sigset, std::move(m_signalFunctions));
}

}  // namespace cta
