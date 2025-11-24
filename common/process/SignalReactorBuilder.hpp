/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#pragma once

#include "SignalReactor.hpp"
#include "common/log/LogContext.hpp"

#include <unordered_map>
#include <functional>

namespace cta {

/**
 * This builder allows for the construction of an immutable SignalReactor object
 */
class SignalReactorBuilder {
public:
  SignalReactorBuilder(cta::log::LogContext& lc);

  SignalReactorBuilder& addSignalFunction(int signal, std::function<void()> func);

  SignalReactor build() const;

private:
  cta::log::LogContext& m_lc;
  std::unordered_map<int, std::function<void()>> m_signalFunctions;
  sigset_t m_sigset;
};

}  // namespace cta
