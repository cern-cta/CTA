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

#include "SignalReader.hpp"
#include "common/log/LogContext.hpp"
#include <unordered_map>
#include <functional>

namespace cta {

class SignalReactor {
public:
  SignalReactor(cta::log::LogContext& lc, uint32_t sleepInterval = 1000);

  ~SignalReactor() = default;

  void registerSignalFunction(uint32_t signal, std::function<void()> func);

  /**
   * Periodically checks for signals and executes the functions registered with said signal (if any)
   */
  void run();

  void stop();

private:
  cta::log::LogContext& m_lc;
  uint32_t m_sleepInterval;

  std::unordered_map<uint32_t, std::function<void()>> m_signalFunctions;
  std::unique_ptr<cta::SignalReader> m_signalReader = std::make_unique<cta::SignalReader>();
  std::atomic<bool> m_stopRequested;
};

}  // namespace cta
