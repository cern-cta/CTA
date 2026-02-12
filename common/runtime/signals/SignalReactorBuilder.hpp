/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "SignalReactor.hpp"
#include "common/log/LogContext.hpp"

#include <functional>
#include <unordered_map>

namespace cta::runtime {

/**
 * This builder allows for the construction of an immutable SignalReactor object
 */
class SignalReactorBuilder final {
public:
  explicit SignalReactorBuilder();

  SignalReactorBuilder& addSignalFunction(int signal, const std::function<void()>& func, bool overwrite = false);
  SignalReactorBuilder& withTimeoutMsecs(uint32_t msecs);

  SignalReactor build(cta::log::Logger& log);

private:
  std::unordered_map<int, std::function<void()>> m_signalFunctions;
  sigset_t m_sigset;
  uint32_t m_waitTimeoutMsecs = 1000;
};

}  // namespace cta::runtime
